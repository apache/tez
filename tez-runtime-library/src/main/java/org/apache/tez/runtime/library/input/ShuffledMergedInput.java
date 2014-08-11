/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.ValuesIterator;
import org.apache.tez.runtime.library.common.shuffle.impl.Shuffle;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

import com.google.common.base.Preconditions;


/**
 * <code>ShuffleMergedInput</code> in a {@link AbstractLogicalInput} which shuffles
 * intermediate sorted data, merges them and provides key/<values> to the
 * consumer.
 *
 * The Copy and Merge will be triggered by the initialization - which is handled
 * by the Tez framework. Input is not consumable until the Copy and Merge are
 * complete. Methods are provided to check for this, as well as to wait for
 * completion. Attempting to get a reader on a non-complete input will block.
 *
 */
public class ShuffledMergedInput extends AbstractLogicalInput {

  static final Log LOG = LogFactory.getLog(ShuffledMergedInput.class);

  protected TezRawKeyValueIterator rawIter = null;
  protected Configuration conf;
  protected Shuffle shuffle;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private long firstEventReceivedTime = -1;
  @SuppressWarnings("rawtypes")
  protected ValuesIterator vIter;

  private TezCounter inputKeyCounter;
  private TezCounter inputValueCounter;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public ShuffledMergedInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public synchronized List<Event> initialize() throws IOException {
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());

    if (this.getNumPhysicalInputs() == 0) {
      getContext().requestInitialMemory(0l, null);
      isStarted.set(true);
      getContext().inputIsReady();
      LOG.info("input fetch not required since there are 0 physical inputs for input vertex: "
          + getContext().getSourceVertexName());
      return Collections.emptyList();
    }

    long initialMemoryRequest = Shuffle.getInitialMemoryRequirement(conf,
        getContext().getTotalMemoryAvailableToTask());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext().requestInitialMemory(initialMemoryRequest, memoryUpdateCallbackHandler);

    this.inputKeyCounter = getContext().getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
    this.inputValueCounter = getContext().getCounters().findCounter(
        TaskCounter.REDUCE_INPUT_RECORDS);
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws IOException {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      // Start the shuffle - copy and merge
      shuffle = new Shuffle(getContext(), conf, getNumPhysicalInputs(), memoryUpdateCallbackHandler.getMemoryAssigned());
      shuffle.run();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initialized the handlers in shuffle..Safe to start processing..");
      }
      List<Event> pending = new LinkedList<Event>();
      pendingEvents.drainTo(pending);
      if (pending.size() > 0) {
        LOG.info("NoAutoStart delay in processing first event: "
            + (System.currentTimeMillis() - firstEventReceivedTime));
        shuffle.handleEvents(pending);
      }
      isStarted.set(true);
    }
  }

  /**
   * Check if the input is ready for consumption
   *
   * @return true if the input is ready for consumption, or if an error occurred
   *         processing fetching the input. false if the shuffle and merge are
   *         still in progress
   * @throws InterruptedException
   * @throws IOException
   */
  public synchronized boolean isInputReady() throws IOException, InterruptedException {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (getNumPhysicalInputs() == 0) {
      return true;
    }
    return shuffle.isInputReady();
  }

  /**
   * Waits for the input to become ready for consumption
   * @throws IOException
   * @throws InterruptedException
   */
  public void waitForInputReady() throws IOException, InterruptedException {
    // Cannot synchronize entire method since this is called form user code and can block.
    Shuffle localShuffleCopy = null;
    synchronized (this) {
      Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
      if (getNumPhysicalInputs() == 0) {
        return;
      }
      localShuffleCopy = shuffle;
    }

    TezRawKeyValueIterator localRawIter = localShuffleCopy.waitForInput();
    synchronized(this) {
      rawIter = localRawIter;
      createValuesIterator();
    }
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    if (this.getNumPhysicalInputs() != 0 && rawIter != null) {
      rawIter.close();
    }
    if (shuffle != null) {
      shuffle.shutdown();
    }
    return Collections.emptyList();
  }

  /**
   * Get a KVReader for the Input.</p> This method will block until the input is
   * ready - i.e. the copy and merge stages are complete. Users can use the
   * isInputReady method to check if the input is ready, which gives an
   * indication of whether this method will block or not.
   *
   * NOTE: All values for the current K-V pair must be read prior to invoking
   * moveToNext. Once moveToNext() is called, the valueIterator from the
   * previous K-V pair will throw an Exception
   *
   * @return a KVReader over the sorted input.
   */
  @Override
  public KeyValuesReader getReader() throws IOException {
    // Cannot synchronize entire method since this is called form user code and can block.
    TezRawKeyValueIterator rawIterLocal;
    synchronized (this) {
      rawIterLocal = rawIter;
      if (getNumPhysicalInputs() == 0) {
        return new KeyValuesReader() {
          @Override
          public boolean next() throws IOException {
            return false;
          }

          @Override
          public Object getCurrentKey() throws IOException {
            throw new RuntimeException("No data available in Input");
          }

          @Override
          public Iterable<Object> getCurrentValues() throws IOException {
            throw new RuntimeException("No data available in Input");
          }
        };
      }
    }
    if (rawIterLocal == null) {
      try {
        waitForInputReady();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for input ready", e);
      }
    }
    @SuppressWarnings("rawtypes")
    ValuesIterator valuesIter = null;
    synchronized(this) {
      valuesIter = vIter;
    }
    return new ShuffledMergedKeyValuesReader(valuesIter);
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws IOException {
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        throw new RuntimeException("No input events expected as numInputs is 0");
      }
      if (!isStarted.get()) {
        if (firstEventReceivedTime == -1) {
          firstEventReceivedTime = System.currentTimeMillis();
        }
        pendingEvents.addAll(inputEvents);
        return;
      }
    }
    shuffle.handleEvents(inputEvents);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected synchronized void createValuesIterator()
      throws IOException {
    // Not used by ReduceProcessor
    vIter = new ValuesIterator(rawIter,
        (RawComparator) ConfigUtils.getIntermediateInputKeyComparator(conf),
        ConfigUtils.getIntermediateInputKeyClass(conf),
        ConfigUtils.getIntermediateInputValueClass(conf), conf, inputKeyCounter, inputValueCounter);

  }

  @SuppressWarnings("rawtypes")
  public RawComparator getInputKeyComparator() {
    return (RawComparator) ConfigUtils.getIntermediateInputKeyComparator(conf);
  }

  @SuppressWarnings("rawtypes")
  private static class ShuffledMergedKeyValuesReader extends KeyValuesReader {

    private final ValuesIterator valuesIter;

    ShuffledMergedKeyValuesReader(ValuesIterator valuesIter) {
      this.valuesIter = valuesIter;
    }

    @Override
    public boolean next() throws IOException {
      return valuesIter.moveToNext();
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return valuesIter.getKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Object> getCurrentValues() throws IOException {
      return valuesIter.getValues();
    }
  };


  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_BUFFER_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_GROUP_NAME_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_NAME_MAX_KEYS);
    confKeys.add(TezConfiguration.TEZ_AM_COUNTERS_GROUPS_MAX_KEYS);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }

}

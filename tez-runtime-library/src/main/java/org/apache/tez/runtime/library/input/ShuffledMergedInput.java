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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.ValuesIterator;
import org.apache.tez.runtime.library.common.shuffle.impl.Shuffle;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

import com.google.common.base.Preconditions;


/**
 * <code>ShuffleMergedInput</code> in a {@link LogicalInput} which shuffles
 * intermediate sorted data, merges them and provides key/<values> to the
 * consumer.
 *
 * The Copy and Merge will be triggered by the initialization - which is handled
 * by the Tez framework. Input is not consumable until the Copy and Merge are
 * complete. Methods are provided to check for this, as well as to wait for
 * completion. Attempting to get a reader on a non-complete input will block.
 *
 */
public class ShuffledMergedInput implements LogicalInput {

  static final Log LOG = LogFactory.getLog(ShuffledMergedInput.class);

  protected TezInputContext inputContext;
  protected TezRawKeyValueIterator rawIter = null;
  protected Configuration conf;
  protected int numInputs = 0;
  protected Shuffle shuffle;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private long firstEventReceivedTime = -1;
  @SuppressWarnings("rawtypes")
  protected ValuesIterator vIter;

  private TezCounter inputKeyCounter;
  private TezCounter inputValueCounter;
  
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  @Override
  public synchronized List<Event> initialize(TezInputContext inputContext) throws IOException {
    this.inputContext = inputContext;
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());

    if (this.numInputs == 0) {
      inputContext.requestInitialMemory(0l, null);
      isStarted.set(true);
      inputContext.inputIsReady();
      LOG.info("input fetch not required since there are 0 physical inputs for input vertex: "
          + inputContext.getSourceVertexName());
      return Collections.emptyList();
    }
    
    long initialMemoryRequest = Shuffle.getInitialMemoryRequirement(conf,
        inputContext.getTotalMemoryAvailableToTask());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    inputContext.requestInitialMemory(initialMemoryRequest, memoryUpdateCallbackHandler);

    this.inputKeyCounter = inputContext.getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
    this.inputValueCounter = inputContext.getCounters().findCounter(
        TaskCounter.REDUCE_INPUT_RECORDS);
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, inputContext.getWorkDirs());

    return Collections.emptyList();
  }
  
  @Override
  public synchronized void start() throws IOException {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      // Start the shuffle - copy and merge
      shuffle = new Shuffle(inputContext, conf, numInputs, memoryUpdateCallbackHandler.getMemoryAssigned());
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
   */
  public synchronized boolean isInputReady() {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (this.numInputs == 0) {
      return true;
    }
    return shuffle.isInputReady();
  }

  /**
   * Waits for the input to become ready for consumption
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void waitForInputReady() throws IOException, InterruptedException {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (this.numInputs == 0) {
      return;
    }
    rawIter = shuffle.waitForInput();
    createValuesIterator();
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    if (this.numInputs != 0 && rawIter != null) {
      rawIter.close();
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
  public synchronized KeyValuesReader getReader() throws IOException {
    if (this.numInputs == 0) {
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
    if (rawIter == null) {
      try {
        waitForInputReady();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for input ready", e);
      }
    }
    return new KeyValuesReader() {

      @Override
      public boolean next() throws IOException {
        return vIter.moveToNext();
      }

      public Object getCurrentKey() throws IOException {
        return vIter.getKey();
      }
      
      @SuppressWarnings("unchecked")
      public Iterable<Object> getCurrentValues() throws IOException {
        return vIter.getValues();
      }
    };
  }

  @Override
  public synchronized void handleEvents(List<Event> inputEvents) {
    if (numInputs == 0) {
      throw new RuntimeException("No input events expected as numInputs is 0");
    }
    if (!isStarted.get()) {
      if (firstEventReceivedTime == -1) {
        firstEventReceivedTime = System.currentTimeMillis();
      }
      pendingEvents.addAll(inputEvents);
      return;
    }
    shuffle.handleEvents(inputEvents);
  }

  @Override
  public synchronized void setNumPhysicalInputs(int numInputs) {
    this.numInputs = numInputs;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void createValuesIterator()
      throws IOException {
    // Not used by ReduceProcessor
    vIter = new ValuesIterator(rawIter,
        (RawComparator) ConfigUtils.getIntermediateInputKeyComparator(conf),
        ConfigUtils.getIntermediateInputKeyClass(conf),
        ConfigUtils.getIntermediateInputValueClass(conf), conf, inputKeyCounter, inputValueCounter);

  }
}

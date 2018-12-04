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
package org.apache.tez.runtime.library.output;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import com.google.common.base.Preconditions;

/**
 * {@link OrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which sorts
 * key/value pairs written to it. It also partitions the output based on a
 * {@link Partitioner}
 */
@Public
public class OrderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG = LoggerFactory.getLogger(OrderedPartitionedKVOutput.class);

  protected ExternalSorter sorter;
  protected Configuration conf;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private long startTime;
  private long endTime;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final Deflater deflater;

  @VisibleForTesting
  boolean pipelinedShuffle;
  private boolean sendEmptyPartitionDetails;
  @VisibleForTesting
  boolean finalMergeEnabled;

  public OrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
    deflater = TezCommonUtils.newBestCompressionDeflater();
  }

  @Override
  public synchronized List<Event> initialize() throws IOException {
    this.startTime = System.nanoTime();
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    // Initializing this parametr in this conf since it is used in multiple
    // places (wherever LocalDirAllocator is used) - TezTaskOutputFiles,
    // TezMerger, etc.
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext().requestInitialMemory(
        ExternalSorter.getInitialMemoryRequirement(conf,
            getContext().getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);

    sendEmptyPartitionDetails = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      String sorterClass = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS,
          TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS_DEFAULT).toUpperCase(Locale.ENGLISH);
      SorterImpl sorterImpl = null;
      try {
        sorterImpl = SorterImpl.valueOf(sorterClass);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid sorter class specified in config"
            + ", propertyName=" + TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS
            + ", value=" + sorterClass
            + ", validValues=" + Arrays.asList(SorterImpl.values()));
      }

      finalMergeEnabled = conf.getBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
          TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);

      pipelinedShuffle = this.conf.getBoolean(TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, TezRuntimeConfiguration
          .TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

      if (pipelinedShuffle) {
        if (finalMergeEnabled) {
          LOG.info(getContext().getDestinationVertexName() + " disabling final merge as "
              + TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED + " is enabled.");
          finalMergeEnabled = false;
          conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
        }

        Preconditions.checkArgument(sorterImpl.equals(SorterImpl.PIPELINED),
            TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED
              + "only works with PipelinedSorter.");
      }

      if (sorterImpl.equals(SorterImpl.PIPELINED)) {
        sorter = new PipelinedSorter(getContext(), conf, getNumPhysicalOutputs(),
            memoryUpdateCallbackHandler.getMemoryAssigned());
      } else if (sorterImpl.equals(SorterImpl.LEGACY)) {
        sorter = new DefaultSorter(getContext(), conf, getNumPhysicalOutputs(),
            memoryUpdateCallbackHandler.getMemoryAssigned());
      } else {
        throw new UnsupportedOperationException("Unsupported sorter class specified in config"
            + ", propertyName=" + TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS
            + ", value=" + sorterClass
            + ", validValues=" + Arrays.asList(SorterImpl.values()));
      }

      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws IOException {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return new KeyValuesWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }

      @Override
      public void write(Object key, Iterable<Object> values) throws IOException {
        sorter.write(key, values);
      }
    };
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    // Not expecting any events.
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    List<Event> returnEvents = Lists.newLinkedList();
    if (sorter != null) {
      sorter.flush();
      returnEvents.addAll(sorter.close());
      this.endTime = System.nanoTime();
      returnEvents.addAll(generateEvents());
      sorter = null;
    } else {
      LOG.warn(getContext().getDestinationVertexName() +
          ": Attempting to close output {} of type {} before it was started. Generating empty events",
          getContext().getDestinationVertexName(), this.getClass().getSimpleName());
      returnEvents = generateEmptyEvents();
    }

    return returnEvents;
  }

  private List<Event> generateEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    if (finalMergeEnabled && !pipelinedShuffle) {
      boolean isLastEvent = true;
      String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      ShuffleUtils.generateEventOnSpill(eventList, finalMergeEnabled, isLastEvent,
          getContext(), 0, new TezSpillRecord(sorter.getFinalIndexFile(), conf),
          getNumPhysicalOutputs(), sendEmptyPartitionDetails, getContext().getUniqueIdentifier(),
          sorter.getPartitionStats(), sorter.reportDetailedPartitionStats(), auxiliaryService, deflater);
    }
    return eventList;
  }

  private List<Event> generateEmptyEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    ShuffleUtils.generateEventsForNonStartedOutput(eventList, getNumPhysicalOutputs(), getContext(), true, true, deflater);
    return eventList;
  }


  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_SORT_THREADS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_MIN_BLOCK_SIZE_IN_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SORTER_LAZY_ALLOCATE_MEMORY);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INTERNAL_SORTER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}

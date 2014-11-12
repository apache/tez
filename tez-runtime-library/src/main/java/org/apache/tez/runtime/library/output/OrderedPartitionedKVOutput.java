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
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * {@link OrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which sorts
 * key/value pairs written to it. It also partitions the output based on a
 * {@link Partitioner}
 */
@Public
public class OrderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Log LOG = LogFactory.getLog(OrderedPartitionedKVOutput.class);

  protected ExternalSorter sorter;
  protected Configuration conf;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private long startTime;
  private long endTime;
  private boolean sendEmptyPartitionDetails;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public OrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
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

    sendEmptyPartitionDetails = this.conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      if (this.conf.getInt(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS,
          TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS_DEFAULT) > 1) {
        sorter = new PipelinedSorter(getContext(), conf, getNumPhysicalOutputs(),
            memoryUpdateCallbackHandler.getMemoryAssigned());
      } else {
        sorter = new DefaultSorter(getContext(), conf, getNumPhysicalOutputs(),
            memoryUpdateCallbackHandler.getMemoryAssigned());
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
    if (sorter != null) {
      sorter.flush();
      sorter.close();
      this.endTime = System.nanoTime();
      return generateEventsOnClose();
    } else {
      LOG.warn("Attempting to close output " + getContext().getDestinationVertexName()
          + " before it was started");
      return Collections.emptyList();
    }
  }
  
  protected List<Event> generateEventsOnClose() throws IOException {
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    boolean outputGenerated = true;
    if (sendEmptyPartitionDetails) {
      Path indexFile = sorter.getMapOutput().getOutputIndexFile();
      TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
      BitSet emptyPartitionDetails = new BitSet();
      int emptyPartitions = 0;
      for(int i=0;i<spillRecord.size();i++) {
        TezIndexRecord indexRecord = spillRecord.getIndex(i);
        if (!indexRecord.hasData()) {
          emptyPartitionDetails.set(i);
          emptyPartitions++;
        }
      }
      outputGenerated = (spillRecord.size() != emptyPartitions);
      if (emptyPartitions > 0) {
        ByteString emptyPartitionsBytesString =
            TezCommonUtils.compressByteArrayToByteString(
                TezUtilsInternal.toByteArray(emptyPartitionDetails));
        payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
        LOG.info("EmptyPartition bitsetSize=" + emptyPartitionDetails.cardinality() + ", numOutputs="
                + getNumPhysicalOutputs() + ", emptyPartitions=" + emptyPartitions
              + ", compressedSize=" + emptyPartitionsBytesString.size());
      }
    }
    if (!sendEmptyPartitionDetails || outputGenerated) {
      String host = System.getenv(ApplicationConstants.Environment.NM_HOST
          .toString());
      ByteBuffer shuffleMetadata = getContext()
          .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
      int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(shufflePort);
      payloadBuilder.setPathComponent(getContext().getUniqueIdentifier());
    }

    payloadBuilder.setRunDuration((int) ((endTime - startTime) / 1000));
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    ByteBuffer payload = payloadProto.toByteString().asReadOnlyByteBuffer();

    long outputSize = getContext().getCounters()
        .findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    VertexManagerEventPayloadProto.Builder vmBuilder = VertexManagerEventPayloadProto
        .newBuilder();
    vmBuilder.setOutputSize(outputSize);
    VertexManagerEvent vmEvent = VertexManagerEvent.create(
        getContext().getDestinationVertexName(), vmBuilder.build().toByteString().asReadOnlyByteBuffer());

    List<Event> events = Lists.newArrayListWithCapacity(getNumPhysicalOutputs() + 1);
    events.add(vmEvent);

    CompositeDataMovementEvent csdme =
        CompositeDataMovementEvent.create(0, getNumPhysicalOutputs(), payload);
    events.add(csdme);

    return events;
  }


  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_SORT_THREADS);
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
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}
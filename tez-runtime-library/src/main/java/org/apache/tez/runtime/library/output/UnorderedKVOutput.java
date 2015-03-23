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

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.broadcast.output.FileBasedKVWriter;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * {@link UnorderedKVOutput} is a {@link LogicalOutput} that writes key
 * value data without applying any ordering or grouping constraints. This can be
 * used to write raw key value data as is.
 */
@Public
public class UnorderedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedKVOutput.class);

  private FileBasedKVWriter kvWriter;
  
  private Configuration conf;
  
  private boolean dataViaEventsEnabled;
  private int dataViaEventsMaxSize;

  public UnorderedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }


  @Override
  public synchronized List<Event> initialize()
      throws Exception {
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS,
        getContext().getWorkDirs());

    getContext().requestInitialMemory(0l, null); // mandatory call

    this.dataViaEventsEnabled = conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED,
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED_DEFAULT);
    this.dataViaEventsMaxSize = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE,
        TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);
    
    LOG.info(this.getClass().getSimpleName() + " running with params -> "
        + "dataViaEventsEnabled: " + dataViaEventsEnabled
        + ", dataViaEventsMaxSize: " + dataViaEventsMaxSize);
    
    this.kvWriter = new FileBasedKVWriter(getContext(), conf);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() {
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws Exception {
    // Eventually, disallow multiple invocations.
    return kvWriter;
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    throw new TezUncheckedException("Not expecting any events");
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    boolean outputGenerated = this.kvWriter.close();
    
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();
    
    LOG.info("Closing KVOutput: RawLength: " + this.kvWriter.getRawLength()
        + ", CompressedLength: " + this.kvWriter.getCompressedLength());

    if (dataViaEventsEnabled && outputGenerated && this.kvWriter.getCompressedLength() <= dataViaEventsMaxSize) {
      LOG.info("Serialzing actual data into DataMovementEvent, dataSize: " + this.kvWriter.getCompressedLength());
      byte[] data = this.kvWriter.getData();
      DataProto.Builder dataProtoBuilder = DataProto.newBuilder();
      dataProtoBuilder.setData(ByteString.copyFrom(data));
      dataProtoBuilder.setRawLength((int)this.kvWriter.getRawLength());
      dataProtoBuilder.setCompressedLength((int)this.kvWriter.getCompressedLength());
      payloadBuilder.setData(dataProtoBuilder.build());
    }

    // Set the list of empty partitions - single partition on this case.
    if (!outputGenerated) {
      LOG.info("No output was generated");
      BitSet emptyPartitions = new BitSet();
      emptyPartitions.set(0);
      ByteString emptyPartitionsBytesString =
          TezCommonUtils.compressByteArrayToByteString(TezUtilsInternal.toByteArray(emptyPartitions));
      payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
    }
    if (outputGenerated) {
      String host = getHost();
      ByteBuffer shuffleMetadata = getContext()
          .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
      int shufflePort = ShuffleUtils
          .deserializeShuffleProviderMetaData(shuffleMetadata);
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(shufflePort);
      payloadBuilder.setPathComponent(getContext().getUniqueIdentifier());
    }
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();

    DataMovementEvent dmEvent = DataMovementEvent.create(0, payloadProto.toByteString().asReadOnlyByteBuffer());
    List<Event> events = Lists.newArrayListWithCapacity(1);
    events.add(dmEvent);
    return events;
  }

  @VisibleForTesting
  @Private
  String getHost() {
    return getContext().getExecutionContext().getHostName();
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
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

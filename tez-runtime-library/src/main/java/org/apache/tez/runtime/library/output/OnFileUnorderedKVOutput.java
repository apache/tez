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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.broadcast.output.FileBasedKVWriter;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class OnFileUnorderedKVOutput implements LogicalOutput {

  private static final Log LOG = LogFactory.getLog(OnFileUnorderedKVOutput.class);

  private TezOutputContext outputContext;
  private FileBasedKVWriter kvWriter;
  
  private Configuration conf;
  
  private boolean dataViaEventsEnabled;
  private int dataViaEventsMaxSize;

  public OnFileUnorderedKVOutput() {
  }

  @Override
  public synchronized List<Event> initialize(TezOutputContext outputContext)
      throws Exception {
    this.outputContext = outputContext;
    this.conf = TezUtils.createConfFromUserPayload(outputContext
        .getUserPayload());
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS,
        outputContext.getWorkDirs());

    this.outputContext.requestInitialMemory(0l, null); // mandatory call

    this.dataViaEventsEnabled = conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED,
        TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_ENABLED_DEFAULT);
    this.dataViaEventsMaxSize = conf.getInt(
        TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE,
        TezJobConfig.TEZ_RUNTIME_BROADCAST_DATA_VIA_EVENTS_MAX_SIZE_DEFAULT);
    
    LOG.info(this.getClass().getSimpleName() + " running with params -> "
        + "dataViaEventsEnabled: " + dataViaEventsEnabled
        + ", dataViaEventsMaxSize: " + dataViaEventsMaxSize);
    
    this.kvWriter = new FileBasedKVWriter(outputContext, conf);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() {
  }

  @Override
  public synchronized KeyValueWriter getWriter() throws Exception {
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

    String host = getHost();
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils
        .deserializeShuffleProviderMetaData(shuffleMetadata);
    // Set the list of empty partitions - single partition on this case.
    if (!outputGenerated) {
      LOG.info("No output was generated");
      BitSet emptyPartitions = new BitSet();
      emptyPartitions.set(0);
      ByteString emptyPartitionsBytesString =
          TezUtils.compressByteArrayToByteString(TezUtils.toByteArray(emptyPartitions));
      payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
    }
    if (outputGenerated) {
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(shufflePort);
      payloadBuilder.setPathComponent(outputContext.getUniqueIdentifier());
    }
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();

    DataMovementEvent dmEvent = new DataMovementEvent(0,
        payloadProto.toByteArray());
    List<Event> events = Lists.newArrayListWithCapacity(1);
    events.add(dmEvent);
    return events;
  }

  @Override
  public synchronized void setNumPhysicalOutputs(int numOutputs) {
    Preconditions.checkArgument(numOutputs == 1,
        "Number of outputs can only be 1 for " + this.getClass().getName());
  }
  
  @VisibleForTesting
  @Private
  String getHost() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
  }

}

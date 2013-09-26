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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.library.api.KVWriter;
import org.apache.tez.runtime.library.broadcast.output.FileBasedKVWriter;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class OnFileUnorderedKVOutput implements LogicalOutput {

  private TezOutputContext outputContext;
  private FileBasedKVWriter kvWriter;

  public OnFileUnorderedKVOutput() {
  }

  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws Exception {
    this.outputContext = outputContext;
    this.kvWriter = new FileBasedKVWriter(outputContext);
    return Collections.emptyList();
  }

  @Override
  public KVWriter getWriter() throws Exception {
    return kvWriter;
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    throw new TezUncheckedException("Not expecting any events");
  }

  @Override
  public List<Event> close() throws Exception {
    boolean outputGenerated = this.kvWriter.close();
    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    String host = getHost();
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils
        .deserializeShuffleProviderMetaData(shuffleMetadata);
    payloadBuilder.setOutputGenerated(outputGenerated);
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
  public void setNumPhysicalOutputs(int numOutputs) {
    Preconditions.checkArgument(numOutputs == 1,
        "Number of outputs can only be 1 for " + this.getClass().getName());
  }
  
  @VisibleForTesting
  @Private
  String getHost() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
  }

}

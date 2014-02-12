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

package org.apache.tez.runtime.api.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

public class TezOutputContextImpl extends TezTaskContextImpl
    implements TezOutputContext {

  private final byte[] userPayload;
  private final String destinationVertexName;
  private final EventMetaData sourceInfo;
  private final int outputIndex;

  @Private
  public TezOutputContextImpl(Configuration conf, int appAttemptNumber,
      TezUmbilical tezUmbilical, String taskVertexName,
      String destinationVertexName,
      TezTaskAttemptID taskAttemptID, TezCounters counters, int outputIndex,
      byte[] userPayload, RuntimeTask runtimeTask,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      OutputDescriptor outputDescriptor) {
    super(conf, appAttemptNumber, taskVertexName, taskAttemptID,
        counters, runtimeTask, tezUmbilical, serviceConsumerMetadata,
        auxServiceEnv, memDist, outputDescriptor);
    this.userPayload = userPayload;
    this.outputIndex = outputIndex;
    this.destinationVertexName = destinationVertexName;
    this.sourceInfo = new EventMetaData(EventProducerConsumerType.OUTPUT,
        taskVertexName, destinationVertexName, taskAttemptID);
  }

  @Override
  public void sendEvents(List<Event> events) {
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent tEvt = new TezEvent(e, sourceInfo);
      tezEvents.add(tEvt);
    }
    tezUmbilical.addEvents(tezEvents);
  }

  @Override
  public byte[] getUserPayload() {
    return userPayload;
  }

  @Override
  public String getDestinationVertexName() {
    return destinationVertexName;
  }

  @Override
  public void fatalError(Throwable exception, String message) {
    super.signalFatalError(exception, message, sourceInfo);
  }

  @Override
  public int getOutputIndex() {
    return outputIndex;
  }
}

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

import com.google.common.base.Preconditions;
import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUserPayload;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.RuntimeTask;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

public class TezInputContextImpl extends TezTaskContextImpl
    implements TezInputContext {

  private final TezUserPayload userPayload;
  private final String sourceVertexName;
  private final EventMetaData sourceInfo;
  private final int inputIndex;
  private final Input input;
  private final InputReadyTracker inputReadyTracker;

  @Private
  public TezInputContextImpl(Configuration conf, String[] workDirs, int appAttemptNumber,
      TezUmbilical tezUmbilical, String dagName, String taskVertexName,
      String sourceVertexName, TezTaskAttemptID taskAttemptID,
      TezCounters counters, int inputIndex, @Nullable byte[] userPayload,
      RuntimeTask runtimeTask, Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> auxServiceEnv, MemoryDistributor memDist,
      InputDescriptor inputDescriptor,  Input input, InputReadyTracker inputReadyTracker) {
    super(conf, workDirs, appAttemptNumber, dagName, taskVertexName, taskAttemptID,
        wrapCounters(counters, taskVertexName, sourceVertexName, conf),
        runtimeTask, tezUmbilical, serviceConsumerMetadata,
        auxServiceEnv, memDist, inputDescriptor);
    checkNotNull(inputIndex, "inputIndex is null");
    checkNotNull(sourceVertexName, "sourceVertexName is null");
    checkNotNull(input, "input is null");
    checkNotNull(inputReadyTracker, "inputReadyTracker is null");
    this.userPayload = DagTypeConverters.convertToTezUserPayload(userPayload);
    this.inputIndex = inputIndex;
    this.sourceVertexName = sourceVertexName;
    this.sourceInfo = new EventMetaData(
        EventProducerConsumerType.INPUT, taskVertexName, sourceVertexName,
        taskAttemptID);
    this.input = input;
    this.inputReadyTracker = inputReadyTracker;
  }

  private static TezCounters wrapCounters(TezCounters tezCounters, String taskVertexName,
      String edgeVertexName, Configuration conf) {
    if (conf.getBoolean(TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO,
        TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT)) {
      return new TezCountersDelegate(tezCounters, taskVertexName, edgeVertexName, "INPUT");
    } else {
      return tezCounters;
    }
  }

  @Override
  public void sendEvents(List<Event> events) {
    Preconditions.checkNotNull(events, "events are null");
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent tEvt = new TezEvent(e, sourceInfo);
      tezEvents.add(tEvt);
    }
    tezUmbilical.addEvents(tezEvents);
  }

  @Nullable
  @Override
  public byte[] getUserPayload() {
    return userPayload.getPayload();
  }
  
  @Override
  public int getInputIndex() {
    return inputIndex;
  }

  @Override
  public String getSourceVertexName() {
    return sourceVertexName;
  }

  @Override
  public void fatalError(Throwable exception, String message) {
    super.signalFatalError(exception, message, sourceInfo);
  }

  @Override
  public void inputIsReady() {
    inputReadyTracker.setInputIsReady(input);
  }
}

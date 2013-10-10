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

package org.apache.tez.dag.app.dag.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexScheduler;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.TaskEventAddTezEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RootInputVertexManager implements VertexScheduler {

  private final Vertex managedVertex;
  private final EventMetaData sourceInfo;
  private final Map<String, EventMetaData> destInfoMap;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  
  @SuppressWarnings("rawtypes")
  public RootInputVertexManager(Vertex vertex, EventHandler eventHandler) {
    this.managedVertex = vertex;
    this.eventHandler = eventHandler;
    this.sourceInfo = new EventMetaData(EventProducerConsumerType.INPUT,
        vertex.getName(), "NULL", null);
    Map<String, RootInputLeafOutputDescriptor<InputDescriptor>> inputs = this.managedVertex
        .getAdditionalInputs();
    this.destInfoMap = Maps.newHashMapWithExpectedSize(inputs.size());
    for (RootInputLeafOutputDescriptor input : inputs.values()) {
      EventMetaData destInfo = new EventMetaData(
          EventProducerConsumerType.INPUT, vertex.getName(),
          input.getEntityName(), null);
      destInfoMap.put(input.getEntityName(), destInfo);
    }
  }

  @Override
  public void initialize(Configuration conf) {
  }

  @Override
  public void onVertexStarted() {
    managedVertex.scheduleTasks(managedVertex.getTasks().keySet());
  }

  @Override
  public void onSourceTaskCompleted(TezTaskAttemptID attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    boolean dataInformationEventSeen = false;
    Preconditions.checkState(EnumSet.of(VertexState.INITIALIZING,
        VertexState.NEW).contains(managedVertex.getState()));
    for (Event event : events) {
      if (event instanceof RootInputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions
            .checkState(
                managedVertex.getTotalTasks() == -1,
                "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism");
        RootInputConfigureVertexTasksEvent cEvent = (RootInputConfigureVertexTasksEvent) event;
        managedVertex.setParallelism(cEvent.getNumTasks(), null);
        managedVertex.setVertexLocationHint(new VertexLocationHint(cEvent
            .getNumTasks(), cEvent.getTaskLocationHints()));
      }
      if (event instanceof RootInputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(((RootInputUpdatePayloadEvent) event)
            .getUserPayload());
      } else if (event instanceof RootInputDataInformationEvent) {
        dataInformationEventSeen = true;
        // # Tasks should have been set by this point.
        Preconditions.checkState(managedVertex.getTasks().size() != 0);
        TezEvent tezEvent = new TezEvent(event, sourceInfo);
        tezEvent.setDestinationInfo(destInfoMap.get(inputName));
        sendEventToTask(new TezTaskID(managedVertex.getVertexId(),
            ((RootInputDataInformationEvent) event).getIndex()), tezEvent);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEventToTask(TezTaskID taskId, TezEvent tezEvent) {
    eventHandler.handle(new TaskEventAddTezEvent(taskId, tezEvent));
  }

}

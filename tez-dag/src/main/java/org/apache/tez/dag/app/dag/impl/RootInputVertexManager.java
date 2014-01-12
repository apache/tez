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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskEventAddTezEvent;
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

@SuppressWarnings("rawtypes")
public class RootInputVertexManager implements VertexManagerPlugin {

  VertexManagerPluginContext context;
  private EventMetaData sourceInfo;
  private Map<String, EventMetaData> destInfoMap;
  
  Vertex deleteVertex;
  EventHandler deleteHandler;
  
  public RootInputVertexManager(Vertex v, EventHandler h) {
    deleteVertex = v;
    deleteHandler = h;
  }

  @Override
  public void initialize(byte[] payload, VertexManagerPluginContext context) {
    this.context = context;
    Set<String> inputs = this.context.getVertexInputNames();
    this.destInfoMap = Maps.newHashMapWithExpectedSize(inputs.size());
    for (String inputName : inputs) {
      EventMetaData destInfo = new EventMetaData(
          EventProducerConsumerType.INPUT, context.getVertexName(),
          inputName, null);
      destInfoMap.put(inputName, destInfo);
    }
    this.sourceInfo = new EventMetaData(EventProducerConsumerType.INPUT,
        context.getVertexName(), "NULL", null);
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = context.getVertexNumTasks(context.getVertexName());
    List<Integer> scheduledTasks = new ArrayList<Integer>(numTasks);
    for (int i=0; i<numTasks; ++i) {
      scheduledTasks.add(new Integer(i));
    }
    context.scheduleVertexTasks(scheduledTasks);
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof RootInputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions
            .checkState(
                context.getVertexNumTasks(context.getVertexName()) == -1,
                "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism");
        RootInputConfigureVertexTasksEvent cEvent = (RootInputConfigureVertexTasksEvent) event;
        context.setVertexLocationHint(new VertexLocationHint(cEvent
            .getNumTasks(), cEvent.getTaskLocationHints()));
        context.setVertexParallelism(cEvent.getNumTasks(), null);
      }
      if (event instanceof RootInputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(((RootInputUpdatePayloadEvent) event)
            .getUserPayload());
      } else if (event instanceof RootInputDataInformationEvent) {
        dataInformationEventSeen = true;
        // # Tasks should have been set by this point.
        Preconditions.checkState(context.getVertexNumTasks(context.getVertexName()) != 0);
        TezEvent tezEvent = new TezEvent(event, sourceInfo);
        tezEvent.setDestinationInfo(destInfoMap.get(inputName));
        sendEventToTask(TezTaskID.getInstance(deleteVertex.getVertexId(),
            ((RootInputDataInformationEvent) event).getIndex()), tezEvent);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEventToTask(TezTaskID taskId, TezEvent tezEvent) {
    deleteHandler.handle(new TaskEventAddTezEvent(taskId, tezEvent));
  }

}

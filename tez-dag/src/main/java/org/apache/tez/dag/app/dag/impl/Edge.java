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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskEventAddTezEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.engine.newapi.events.DataMovementEvent;
import org.apache.tez.engine.newapi.events.InputFailedEvent;
import org.apache.tez.engine.newapi.events.InputReadErrorEvent;
import org.apache.tez.engine.newapi.impl.EventMetaData;
import org.apache.tez.engine.newapi.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.engine.newapi.impl.InputSpec;
import org.apache.tez.engine.newapi.impl.OutputSpec;
import org.apache.tez.engine.newapi.impl.TezEvent;

public class Edge {

  private EdgeProperty edgeProperty;
  private EdgeManager edgeManager;
  @SuppressWarnings("rawtypes")
  private EventHandler eventHandler;
  private AtomicBoolean bufferEvents = new AtomicBoolean(false);
  private List<TezEvent> destinationEventBuffer = new ArrayList<TezEvent>();
  private List<TezEvent> sourceEventBuffer = new ArrayList<TezEvent>();
  private Vertex sourceVertex;
  private Vertex destinationVertex; // this may end up being a list for shared edge
  
  @SuppressWarnings("rawtypes")
  public Edge(EdgeProperty edgeProperty, EventHandler eventHandler) {
    this.edgeProperty = edgeProperty;
    this.eventHandler = eventHandler;
    switch (edgeProperty.getDataMovementType()) {
    case ONE_TO_ONE:
      edgeManager = new OneToOneEdgeManager();
      break;
    case BROADCAST:
      edgeManager = new BroadcastEdgeManager();
      break;
    case SCATTER_GATHER:
      edgeManager = new ScatterGatherEdgeManager();
      break;
    default:
      String message = "Unknown edge data movement type: "
          + edgeProperty.getDataMovementType();
      throw new TezUncheckedException(message);
    }
  }
  
  public EdgeProperty getEdgeProperty() {
    return this.edgeProperty;
  }
  
  public EdgeManager getEdgeManager() {
    return this.edgeManager;
  }
  
  public void setEdgeManager(EdgeManager edgeManager) {
    if(edgeManager == null) {
      throw new TezUncheckedException("Edge manager cannot be null");
    }
    this.edgeManager = edgeManager;
  }
  
  public void setSourceVertex(Vertex sourceVertex) {
    if (this.sourceVertex != null && this.sourceVertex != sourceVertex) {
      throw new TezUncheckedException("Source vertex exists: "
          + sourceVertex.getName());
    }
    this.sourceVertex = sourceVertex;
  }

  public void setDestinationVertex(Vertex destinationVertex) {
    if (this.destinationVertex != null && this.destinationVertex != destinationVertex) {
      throw new TezUncheckedException("Destination vertex exists: "
          + destinationVertex.getName());
    }
    this.destinationVertex = destinationVertex;
  }
  
  public InputSpec getDestinationSpec(int destinationTaskIndex) {
    return new InputSpec(sourceVertex.getName(),
        edgeProperty.getEdgeDestination(),
        edgeManager.getNumDestinationTaskInputs(sourceVertex, destinationTaskIndex));
 }
  
  public OutputSpec getSourceSpec(int sourceTaskIndex) {
    return new OutputSpec(destinationVertex.getName(),
        edgeProperty.getEdgeSource(), 
        edgeManager.getNumSourceTaskOutputs(destinationVertex, sourceTaskIndex));
  }
  
  public void startEventBuffering() {
    bufferEvents.set(true);
  }
  
  public void stopEventBuffering() {
    // assume only 1 entity will start and stop event buffering
    bufferEvents.set(false);
    for(TezEvent event : destinationEventBuffer) {
      sendTezEventToDestinationTasks(event);
    }
    destinationEventBuffer.clear();
    for(TezEvent event : sourceEventBuffer) {
      sendTezEventToSourceTasks(event);
    }
    sourceEventBuffer.clear();
  }
  
  public void sendTezEventToSourceTasks(TezEvent tezEvent) {
    if (bufferEvents.get()) {
      switch (tezEvent.getEventType()) {
      case INPUT_READ_ERROR_EVENT:
        InputReadErrorEvent event = (InputReadErrorEvent) tezEvent.getEvent();
        TezTaskAttemptID destAttemptId = tezEvent.getSourceInfo().getTaskAttemptID();
        int destTaskIndex = destAttemptId.getTaskID().getId();
        int srcTaskIndex = edgeManager.routeEventToSourceTasks(destTaskIndex, event);
        // TODO this is BROKEN. TEZ-431
//        TezTaskID srcTaskId = sourceVertex.getTask(srcTaskIndex).getTaskId();
//        sendEventToTask(srcTaskId, tezEvent);
        break;
      default:
        throw new TezUncheckedException("Unhandled tez event type: "
            + tezEvent.getEventType());
      }
    } else {
      sourceEventBuffer.add(tezEvent);
    }
  }
  
  public void sendTezEventToDestinationTasks(TezEvent tezEvent) {
    if (bufferEvents.get()) {
      List<Integer> destTaskIndices = new ArrayList<Integer>();
      switch (tezEvent.getEventType()) {
      case DATA_MOVEMENT_EVENT:
        DataMovementEvent dmEvent = (DataMovementEvent) tezEvent.getEvent();
        TezTaskAttemptID dmSourceAttemptId = tezEvent.getSourceInfo().getTaskAttemptID();
        int dmSourceTaskIndex = dmSourceAttemptId.getTaskID().getId();
        edgeManager.routeEventToDestinationTasks(dmEvent, dmSourceTaskIndex,
            destinationVertex.getTotalTasks(), destTaskIndices);
        for(Integer destTaskIndex : destTaskIndices) {
          EventMetaData destMeta = new EventMetaData(EventProducerConsumerType.INPUT, 
              destinationVertex.getName(), 
              sourceVertex.getName(), 
              null); // will be filled by Task when sending the event. Is it needed?
          destMeta.setIndex(dmEvent.getTargetIndex());
          tezEvent.setDestinationInfo(destMeta);
          TezTaskID destTaskId = destinationVertex.getTask(destTaskIndex).getTaskId();
          sendEventToTask(destTaskId, tezEvent);
        }        
        break;
      case INPUT_FAILED_EVENT:
        InputFailedEvent ifEvent = (InputFailedEvent) tezEvent.getEvent();
        TezTaskAttemptID ifSourceAttemptId = tezEvent.getSourceInfo().getTaskAttemptID();
        int ifSourceTaskIndex = ifSourceAttemptId.getTaskID().getId();
        edgeManager.routeEventToDestinationTasks(ifEvent, ifSourceTaskIndex,
            destinationVertex.getTotalTasks(), destTaskIndices);
        for(Integer destTaskIndex : destTaskIndices) {
          EventMetaData destMeta = new EventMetaData(EventProducerConsumerType.INPUT, 
              destinationVertex.getName(), 
              sourceVertex.getName(), 
              null); // will be filled by Task when sending the event. Is it needed?
          destMeta.setIndex(ifEvent.getTargetIndex());
          tezEvent.setDestinationInfo(destMeta);
          TezTaskID destTaskId = destinationVertex.getTask(destTaskIndex).getTaskId();
          sendEventToTask(destTaskId, tezEvent);
        }        
      default:
        throw new TezUncheckedException("Unhandled tez event type: "
            + tezEvent.getEventType());
      }
    } else {
      destinationEventBuffer.add(tezEvent);
    }
  }
  
  private void sendEventToDestination(List<Integer> destTaskIndeces, TezEvent tezEvent) {
    for(Integer destTaskIndex : destTaskIndeces) {
      TezTaskID destTaskId = destinationVertex.getTask(destTaskIndex).getTaskId();
      sendEventToTask(destTaskId, tezEvent);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sendEventToTask(TezTaskID taskId, TezEvent tezEvent) {
    eventHandler.handle(new TaskEventAddTezEvent(taskId, tezEvent));
  }
  
}

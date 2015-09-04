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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventNullEdgeInitialized;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class Edge {

  private static final Log LOG = LogFactory.getLog(Edge.class);

  class EdgeManagerPluginContextImpl implements EdgeManagerPluginContext {

    private final UserPayload userPayload;

    EdgeManagerPluginContextImpl(UserPayload userPayload) {
      this.userPayload = userPayload;
    }

    @Override
    public UserPayload getUserPayload() {
      return userPayload;
    }

    @Override
    public String getSourceVertexName() {
      return sourceVertex.getName();
    }

    @Override
    public String getDestinationVertexName() {
      return destinationVertex.getName();
    }
    
    @Override
    public int getSourceVertexNumTasks() {
      return sourceVertex.getTotalTasks();
    }

    @Override
    public int getDestinationVertexNumTasks() {
      return destinationVertex.getTotalTasks();
    }

  }

  private EdgeProperty edgeProperty;
  private EdgeManagerPluginContext edgeManagerContext;
  private EdgeManagerPlugin edgeManager;
  @SuppressWarnings("rawtypes")
  private EventHandler eventHandler;
  private AtomicBoolean bufferEvents = new AtomicBoolean(false);
  private List<TezEvent> destinationEventBuffer = new ArrayList<TezEvent>();
  private List<TezEvent> sourceEventBuffer = new ArrayList<TezEvent>();
  private Vertex sourceVertex;
  private Vertex destinationVertex; // this may end up being a list for shared edge
  private EventMetaData destinationMetaInfo;

  @SuppressWarnings("rawtypes")
  public Edge(EdgeProperty edgeProperty, EventHandler eventHandler) throws TezException {
    this.edgeProperty = edgeProperty;
    this.eventHandler = eventHandler;
    createEdgeManager();
  }

  private void createEdgeManager() throws TezException {
    switch (edgeProperty.getDataMovementType()) {
      case ONE_TO_ONE:
        edgeManagerContext = new EdgeManagerPluginContextImpl(null);
        edgeManager = new OneToOneEdgeManager(edgeManagerContext);
        break;
      case BROADCAST:
        edgeManagerContext = new EdgeManagerPluginContextImpl(null);
        edgeManager = new BroadcastEdgeManager(edgeManagerContext);
        break;
      case SCATTER_GATHER:
        edgeManagerContext = new EdgeManagerPluginContextImpl(null);
        edgeManager = new ScatterGatherEdgeManager(edgeManagerContext);
        break;
      case CUSTOM:
        if (edgeProperty.getEdgeManagerDescriptor() != null) {
          UserPayload payload = null;
          if (edgeProperty.getEdgeManagerDescriptor().getUserPayload() != null) {
            payload = edgeProperty.getEdgeManagerDescriptor().getUserPayload();
          }
          edgeManagerContext = new EdgeManagerPluginContextImpl(payload);
          String edgeManagerClassName = edgeProperty.getEdgeManagerDescriptor().getClassName();
          edgeManager = ReflectionUtils
              .createClazzInstance(edgeManagerClassName, new Class[]{EdgeManagerPluginContext.class},
                  new Object[]{edgeManagerContext});
        }
        break;
      default:
        String message = "Unknown edge data movement type: "
            + edgeProperty.getDataMovementType();
        throw new TezException(message);
    }
  }

  public void initialize() throws AMUserCodeException {
    if (edgeManager != null) {
      try {
        edgeManager.initialize();
      } catch (Exception e) {
        throw new AMUserCodeException(Source.EdgeManager, "Fail to initialize Edge,"
            + getEdgeInfo(), e);
      }
    }
    destinationMetaInfo = new EventMetaData(EventProducerConsumerType.INPUT, 
        destinationVertex.getName(), 
        sourceVertex.getName(), 
        null);
  }

  public synchronized void setCustomEdgeManager(EdgeManagerPluginDescriptor descriptor)
      throws AMUserCodeException {
    EdgeProperty modifiedEdgeProperty =
        EdgeProperty.create(descriptor,
            edgeProperty.getDataSourceType(),
            edgeProperty.getSchedulingType(),
            edgeProperty.getEdgeSource(),
            edgeProperty.getEdgeDestination());
    this.edgeProperty = modifiedEdgeProperty;
    boolean wasUnInitialized = (edgeManager == null);
    try {
      createEdgeManager();
    } catch (TezException e) {
      throw new AMUserCodeException(Source.EdgeManager, e);
    }
    initialize();
    if (wasUnInitialized) {
      sendEvent(new VertexEventNullEdgeInitialized(sourceVertex.getVertexId(), this, destinationVertex));
      sendEvent(new VertexEventNullEdgeInitialized(destinationVertex.getVertexId(), this, sourceVertex));
    }
  }

  public EdgeProperty getEdgeProperty() {
    return this.edgeProperty;
  }
  
  public EdgeManagerPlugin getEdgeManager() {
    return this.edgeManager;
  }

  public void setSourceVertex(Vertex sourceVertex) {
    if (this.sourceVertex != null && this.sourceVertex != sourceVertex) {
      throw new TezUncheckedException("Source vertex exists: "
          + sourceVertex.getLogIdentifier());
    }
    this.sourceVertex = sourceVertex;
  }

  public void setDestinationVertex(Vertex destinationVertex) {
    if (this.destinationVertex != null
        && this.destinationVertex != destinationVertex) {
      throw new TezUncheckedException("Destination vertex exists: "
          + destinationVertex.getLogIdentifier());
    }
    this.destinationVertex = destinationVertex;
  }

  public InputSpec getDestinationSpec(int destinationTaskIndex) throws AMUserCodeException {
    Preconditions.checkState(edgeManager != null, 
        "Edge Manager must be initialized by this time");
    try {
      int physicalInputCount = edgeManager.getNumDestinationTaskPhysicalInputs(destinationTaskIndex);
      Preconditions.checkArgument(physicalInputCount >= 0,
          "PhysicalInputCount should not be negative, "
          + "physicalInputCount=" + physicalInputCount);
      return new InputSpec(sourceVertex.getName(),
          edgeProperty.getEdgeDestination(),
          physicalInputCount);
    } catch (Exception e) {
      throw new AMUserCodeException(Source.EdgeManager,
          "Fail to getDestinationSpec, destinationTaskIndex="
          + destinationTaskIndex +", " + getEdgeInfo(), e);
    }
  }

  public OutputSpec getSourceSpec(int sourceTaskIndex) throws AMUserCodeException {
    Preconditions.checkState(edgeManager != null, 
        "Edge Manager must be initialized by this time");
    try {
      int physicalOutputCount = edgeManager.getNumSourceTaskPhysicalOutputs(
          sourceTaskIndex);
      Preconditions.checkArgument(physicalOutputCount >= 0,
          "PhysicalOutputCount should not be negative,"
          + "physicalOutputCount=" + physicalOutputCount);
      return new OutputSpec(destinationVertex.getName(),
          edgeProperty.getEdgeSource(), physicalOutputCount);
    } catch (Exception e) {
      throw new AMUserCodeException(Source.EdgeManager,
          "Fail to getSourceSpec, sourceTaskIndex="
          + sourceTaskIndex + ", " + getEdgeInfo(), e);
    }
  }
  
  public void startEventBuffering() {
    bufferEvents.set(true);
  }
  
  public void stopEventBuffering() throws AMUserCodeException {
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
  
  public void sendTezEventToSourceTasks(TezEvent tezEvent) throws AMUserCodeException {
    Preconditions.checkState(edgeManager != null, 
        "Edge Manager must be initialized by this time");
    if (!bufferEvents.get()) {
      switch (tezEvent.getEventType()) {
      case INPUT_READ_ERROR_EVENT:
        InputReadErrorEvent event = (InputReadErrorEvent) tezEvent.getEvent();
        TezTaskAttemptID destAttemptId = tezEvent.getSourceInfo()
            .getTaskAttemptID();
        int destTaskIndex = destAttemptId.getTaskID().getId();
        int srcTaskIndex;
        int numConsumers;
        try {
          srcTaskIndex = edgeManager.routeInputErrorEventToSource(event,
              destTaskIndex, event.getIndex());
          Preconditions.checkArgument(srcTaskIndex >= 0,
              "SourceTaskIndex should not be negative,"
              + "srcTaskIndex=" + srcTaskIndex);
          numConsumers = edgeManager.getNumDestinationConsumerTasks(
              srcTaskIndex);
          Preconditions.checkArgument(numConsumers > 0,
              "ConsumerTaskNum must be positive,"
              + "numConsumers=" + numConsumers);
        } catch (Exception e) {
          throw new AMUserCodeException(Source.EdgeManager,
              "Fail to sendTezEventToSourceTasks, "
              + "TezEvent:" + tezEvent.getEvent()
              + "sourceInfo:" + tezEvent.getSourceInfo()
              + "destinationInfo:" + tezEvent.getDestinationInfo()
              + ", " + getEdgeInfo(), e);
        }
        Task srcTask = sourceVertex.getTask(srcTaskIndex);
        if (srcTask == null) {
          throw new TezUncheckedException("Unexpected null task." +
              " sourceVertex=" + sourceVertex.getLogIdentifier() +
              " srcIndex = " + srcTaskIndex +
              " destAttemptId=" + destAttemptId +
              " destIndex=" + destTaskIndex + 
              " edgeManager=" + edgeManager.getClass().getName());
        }
        TezTaskID srcTaskId = srcTask.getTaskId();
        int taskAttemptIndex = event.getVersion();
        TezTaskAttemptID srcTaskAttemptId = TezTaskAttemptID.getInstance(srcTaskId,
            taskAttemptIndex);
        sendEvent(new TaskAttemptEventOutputFailed(srcTaskAttemptId,
            tezEvent, numConsumers));
        break;
      default:
        throw new TezUncheckedException("Unhandled tez event type: "
            + tezEvent.getEventType());
      }
    } else {
      sourceEventBuffer.add(tezEvent);
    }
  }
  

  private void handleCompositeDataMovementEvent(TezEvent tezEvent) throws AMUserCodeException {
    CompositeDataMovementEvent compEvent = (CompositeDataMovementEvent) tezEvent.getEvent();
    EventMetaData srcInfo = tezEvent.getSourceInfo();
    
    for (DataMovementEvent dmEvent : compEvent.getEvents()) {
      TezEvent newEvent = new TezEvent(dmEvent, srcInfo);
      sendTezEventToDestinationTasks(newEvent);
    }
  }
  
  void sendDmEventOrIfEventToTasks(TezEvent tezEvent, int srcTaskIndex,
      boolean isDataMovementEvent,
      Map<Integer, List<Integer>> taskAndInputIndices) {    
    Preconditions.checkState(edgeManager != null, 
        "Edge Manager must be initialized by this time");
    Event event = tezEvent.getEvent();
    boolean isFirstEvent = true;
    // cache of event object per input index
    Map<Integer, TezEvent> inputIndicesWithEvents = Maps.newHashMap(); 
    for (Map.Entry<Integer, List<Integer>> entry : taskAndInputIndices.entrySet()) {
      int destTaskIndex = entry.getKey();
      List<Integer> inputIndices = entry.getValue();
      for(int i=0; i<inputIndices.size(); ++i) {
        Integer inputIndex = inputIndices.get(i);
        TezEvent tezEventToSend = inputIndicesWithEvents.get(inputIndex);
        if (tezEventToSend == null) {
          if (isFirstEvent) {
            isFirstEvent = false;
            // this is the first item - reuse the event object
            if (isDataMovementEvent) {
              ((DataMovementEvent) event).setTargetIndex(inputIndex);
            } else {
              ((InputFailedEvent) event).setTargetIndex(inputIndex);
            }
            tezEventToSend = tezEvent;
          } else {
            // create new event object for this input index
            Event e;
            if (isDataMovementEvent) {
              DataMovementEvent dmEvent = (DataMovementEvent) event;
              e = DataMovementEvent.create(dmEvent.getSourceIndex(),
                  inputIndex, dmEvent.getVersion(), dmEvent.getUserPayload());
            } else {
              InputFailedEvent ifEvent = ((InputFailedEvent) event);
              e = InputFailedEvent.create(inputIndex, ifEvent.getVersion());
            }
            tezEventToSend = new TezEvent(e, tezEvent.getSourceInfo());
          }
          tezEventToSend.setDestinationInfo(destinationMetaInfo);
          // cache the event object per input because are unique per input index
          inputIndicesWithEvents.put(inputIndex, tezEventToSend);
        }
        Task destTask = destinationVertex.getTask(destTaskIndex);
        if (destTask == null) {
          throw new TezUncheckedException("Unexpected null task." +
              " sourceVertex=" + sourceVertex.getLogIdentifier() +
              " srcTaskIndex = " + srcTaskIndex +
              " destVertex=" + destinationVertex.getLogIdentifier() +
              " destTaskIndex=" + destTaskIndex + 
              " destNumTasks=" + destinationVertex.getTotalTasks() + 
              " edgeManager=" + edgeManager.getClass().getName());
        }
        sendEventToTask(destTask, tezEventToSend);
      }
    }
  }
  
  public void sendTezEventToDestinationTasks(TezEvent tezEvent) throws AMUserCodeException {
    Preconditions.checkState(edgeManager != null, 
        "Edge Manager must be initialized by this time");
    if (!bufferEvents.get()) {
      boolean isDataMovementEvent = true;
      switch (tezEvent.getEventType()) {
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        handleCompositeDataMovementEvent(tezEvent);
        break;
      case INPUT_FAILED_EVENT:
        isDataMovementEvent = false;
        // fall through
      case DATA_MOVEMENT_EVENT:
        Map<Integer, List<Integer>> destTaskAndInputIndices = Maps
        .newHashMap();
        TezTaskAttemptID srcAttemptId = tezEvent.getSourceInfo()
            .getTaskAttemptID();
        int srcTaskIndex = srcAttemptId.getTaskID().getId();

        boolean routingRequired = true;
        if (edgeManagerContext.getDestinationVertexNumTasks() == 0) {
          routingRequired = false;
          LOG.info("Not routing events since destination vertex has 0 tasks" +
              generateCommonDebugString(srcTaskIndex, tezEvent));
        } else if (edgeManagerContext.getDestinationVertexNumTasks() < 0) {
          throw new TezUncheckedException(
              "Internal error. Not expected to route events to a destination until parallelism is determined" +
                  generateCommonDebugString(srcTaskIndex, tezEvent));
        }

        if (routingRequired) {
          try {
            if (isDataMovementEvent) {
              DataMovementEvent dmEvent = (DataMovementEvent) tezEvent.getEvent();
              edgeManager.routeDataMovementEventToDestination(dmEvent,
                  srcTaskIndex, dmEvent.getSourceIndex(),
                  destTaskAndInputIndices);
            } else {
              edgeManager.routeInputSourceTaskFailedEventToDestination(srcTaskIndex,
                  destTaskAndInputIndices);
            }
          } catch (Exception e){
            throw new AMUserCodeException(Source.EdgeManager,
                "Fail to sendTezEventToDestinationTasks, event:" + tezEvent.getEvent()
                + ", sourceInfo:" + tezEvent.getSourceInfo() + ", destinationInfo:"
                + tezEvent.getDestinationInfo() + ", " + getEdgeInfo(), e);
          }
        }

        if (!destTaskAndInputIndices.isEmpty()) {
          sendDmEventOrIfEventToTasks(tezEvent, srcTaskIndex, isDataMovementEvent,
              destTaskAndInputIndices);
        } else if (routingRequired) {
          throw new TezUncheckedException("Event must be routed." +
              generateCommonDebugString(srcTaskIndex, tezEvent));
        }

        break;
      default:
        throw new TezUncheckedException("Unhandled tez event type: "
            + tezEvent.getEventType());
      }
    } else {
      destinationEventBuffer.add(tezEvent);
    }
  }
  
  private void sendEventToTask(Task task, TezEvent tezEvent) {
    task.registerTezEvent(tezEvent);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void sendEvent(org.apache.hadoop.yarn.event.Event event) {
    eventHandler.handle(event);
  }

  public String getSourceVertexName() {
    return this.sourceVertex.getName();
  }

  public String getDestinationVertexName() {
    return this.destinationVertex.getName();
  }

  private String generateCommonDebugString(int srcTaskIndex, TezEvent tezEvent) {
    return new StringBuilder()
        .append(" sourceVertex=").append(sourceVertex.getLogIdentifier())
        .append(" srcIndex = ").append(srcTaskIndex)
        .append(" destAttemptId=").append(destinationVertex.getLogIdentifier())
        .append(" edgeManager=").append(edgeManager.getClass().getName())
        .append(" Event type=").append(tezEvent.getEventType()).toString();
  }

  private String getEdgeInfo() {
    return "EdgeInfo: sourceVertexName=" + getSourceVertexName() + ", destinationVertexName="
        + getDestinationVertexName();
  }
}

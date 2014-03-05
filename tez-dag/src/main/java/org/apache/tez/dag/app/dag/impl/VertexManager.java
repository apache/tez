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

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.RuntimeUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class VertexManager {
  VertexManagerPluginDescriptor pluginDesc;
  VertexManagerPlugin plugin;
  Vertex managedVertex;
  VertexManagerPluginContextImpl pluginContext;
  byte[] payload = null;
  AppContext appContext;
  
  private static final Log LOG = LogFactory.getLog(VertexManager.class);
  
  class VertexManagerPluginContextImpl implements VertexManagerPluginContext {
    // TODO Add functionality to allow VertexManagers to send VertexManagerEvents
    
    private EventMetaData rootEventSourceMetadata = new EventMetaData(EventProducerConsumerType.INPUT,
        managedVertex.getName(), "NULL_VERTEX", null);
    private Map<String, EventMetaData> destinationEventMetadataMap = Maps.newHashMap();
    
    @Override
    public Map<String, EdgeProperty> getInputVertexEdgeProperties() {
      // TODO Something similar for Initial Inputs - payload etc visible
      Map<Vertex, Edge> inputs = managedVertex.getInputVertices();
      Map<String, EdgeProperty> vertexEdgeMap = 
                          Maps.newHashMapWithExpectedSize(inputs.size());
      for (Map.Entry<Vertex, Edge> entry : inputs.entrySet()) {
        vertexEdgeMap.put(entry.getKey().getName(), entry.getValue().getEdgeProperty());
      }
      return vertexEdgeMap;
    }

    @Override
    public String getVertexName() {
      return managedVertex.getName();
    }

    @Override
    public int getVertexNumTasks(String vertexName) {
      return appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
    }

    @Override
    public boolean setVertexParallelism(int parallelism, VertexLocationHint vertexLocationHint,
        Map<String, EdgeManagerDescriptor> sourceEdgeManagers) {
      return managedVertex.setParallelism(parallelism, vertexLocationHint, sourceEdgeManagers);
    }

    @Override
    public void scheduleVertexTasks(List<Integer> taskIDs) {
      managedVertex.scheduleTasks(taskIDs);
    }

    @Override
    public Set<String> getVertexInputNames() {
      Set<String> inputNames = null;
      Map<String, RootInputLeafOutputDescriptor<InputDescriptor>> inputs = 
          managedVertex.getAdditionalInputs();
      if (inputs != null) {
        inputNames = inputs.keySet();
      }
      return inputNames;
    }

    @Override
    public byte[] getUserPayload() {
      return payload;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addRootInputEvents(final String inputName,
        Collection<RootInputDataInformationEvent> events) {
      verifyIsRootInput(inputName);
      Iterable<TezEvent> tezEvents = Iterables.transform(events,
          new Function<RootInputDataInformationEvent, TezEvent>() {
            @Override
            public TezEvent apply(RootInputDataInformationEvent riEvent) {
              TezEvent tezEvent = new TezEvent(riEvent, rootEventSourceMetadata);
              tezEvent.setDestinationInfo(getDestinationMetaData(inputName));
              return tezEvent;
            }
          });
      appContext.getEventHandler().handle(
          new VertexEventRouteEvent(managedVertex.getVertexId(), Lists.newArrayList(tezEvents)));
      // Recovery handling is taken care of by the Vertex.
    }


    @Override
    public void setVertexLocationHint(VertexLocationHint locationHint) {
      managedVertex.setVertexLocationHint(locationHint);
    }

    @Override
    public int getDAGAttemptNumber() {
      return appContext.getApplicationAttemptId().getAttemptId();
    }

    private void verifyIsRootInput(String inputName) {
      Preconditions.checkState(managedVertex.getAdditionalInputs().get(inputName) != null,
          "Cannot add events for non-root inputs");
    }

    private EventMetaData getDestinationMetaData(String inputName) {
      EventMetaData destMeta = destinationEventMetadataMap.get(inputName);
      if (destMeta == null) {
        destMeta = new EventMetaData(EventProducerConsumerType.INPUT, managedVertex.getName(),
            inputName, null);
        destinationEventMetadataMap.put(inputName, destMeta);
      }
      return destMeta;
    }

    @Override
    public Resource getVertexTaskResource() {
      return managedVertex.getTaskResource();
    }

    @Override
    public Resource getTotalAVailableResource() {
      return appContext.getTaskScheduler().getTotalResources();
    }

    @Override
    public int getNumClusterNodes() {
      return appContext.getTaskScheduler().getNumClusterNodes();
    }
  }
  
  public VertexManager(VertexManagerPlugin plugin, 
      Vertex managedVertex, AppContext appContext) {
    checkNotNull(plugin, "plugin is null");
    checkNotNull(managedVertex, "managedVertex is null");
    checkNotNull(appContext, "appContext is null");
    this.plugin = plugin;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
  }
  
  public VertexManager(VertexManagerPluginDescriptor pluginDesc, 
      Vertex managedVertex, AppContext appContext) {
    checkNotNull(pluginDesc, "pluginDesc is null");
    checkNotNull(managedVertex, "managedVertex is null");
    checkNotNull(appContext, "appContext is null");
    this.pluginDesc = pluginDesc;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
  }
  
  public VertexManagerPlugin getPlugin() {
    return plugin;
  }
  
  public void initialize() {
    pluginContext = new VertexManagerPluginContextImpl();
    if (pluginDesc != null) {
      plugin = RuntimeUtils.createClazzInstance(pluginDesc.getClassName());
      payload = pluginDesc.getUserPayload();
    }
    if (payload == null) {
      // Ease of use. If no payload present then give the common configuration
      // TODO TEZ-744 Don't do this - AMConf should not be used to configure vertexManagers.
      try {
        payload = TezUtils.createUserPayloadFromConf(appContext.getAMConf());
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
    plugin.initialize(pluginContext);
  }

  public void onVertexStarted(List<TezTaskAttemptID> completions) {
    Map<String, List<Integer>> pluginCompletionsMap = Maps.newHashMap();
    if (completions != null && !completions.isEmpty()) {
      for (TezTaskAttemptID tezTaskAttemptID : completions) {
        Integer taskId = new Integer(tezTaskAttemptID.getTaskID().getId());
        String vertexName =
            appContext.getCurrentDAG().getVertex(
                tezTaskAttemptID.getTaskID().getVertexID()).getName();
        List<Integer> taskIdList = pluginCompletionsMap.get(vertexName);
        if (taskIdList == null) {
          taskIdList = Lists.newArrayList();
          pluginCompletionsMap.put(vertexName, taskIdList);
        }
        taskIdList.add(taskId);
      }
    }
    plugin.onVertexStarted(pluginCompletionsMap);
  }

  public void onSourceTaskCompleted(TezTaskID tezTaskId) {
    Integer taskId = new Integer(tezTaskId.getId());
    String vertexName = 
        appContext.getCurrentDAG().getVertex(tezTaskId.getVertexID()).getName();
    plugin.onSourceTaskCompleted(vertexName, taskId);
  }

  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    plugin.onVertexManagerEventReceived(vmEvent);
  }

  public void onRootVertexInitialized(String inputName, 
      InputDescriptor inputDescriptor, List<Event> events) {
    plugin.onRootVertexInitialized(inputName, inputDescriptor, events);
  }
}

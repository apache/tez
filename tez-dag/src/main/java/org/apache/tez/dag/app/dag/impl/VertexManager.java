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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.RuntimeUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class VertexManager {
  VertexManagerPluginDescriptor pluginDesc;
  VertexManagerPlugin plugin;
  Vertex managedVertex;
  VertexManagerPluginContextImpl pluginContext;
  AppContext appContext;
  
  class VertexManagerPluginContextImpl implements VertexManagerPluginContext {
    
    @Override
    public Map<String, EdgeProperty> getInputVertexEdgeProperties() {
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
      return managedVertex.getTotalTasks();
    }

    @Override
    public boolean setVertexParallelism(int parallelism,
        Map<String, EdgeManager> sourceEdgeManagers) {
      return managedVertex.setParallelism(parallelism, sourceEdgeManagers);
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
    public void setVertexLocationHint(VertexLocationHint locationHint) {
      managedVertex.setVertexLocationHint(locationHint);
    }

  }
  
  public VertexManager(VertexManagerPlugin plugin, 
      Vertex managedVertex, AppContext appContext) {
    this.plugin = plugin;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
  }
  
  public VertexManager(VertexManagerPluginDescriptor pluginDesc, 
      Vertex managedVertex, AppContext appContext) {
    this.pluginDesc = pluginDesc;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
  }
  
  public VertexManagerPlugin getPlugin() {
    return plugin;
  }
  
  public void initialize() {
    pluginContext = new VertexManagerPluginContextImpl();
    byte[] payload = null;
    if (pluginDesc != null) {
      plugin = RuntimeUtils.createClazzInstance(pluginDesc.getClassName());
      payload = pluginDesc.getUserPayload();
    }
    if (payload == null) {
      // Ease of use. If no payload present then give the common configuration
      try {
        payload = TezUtils.createUserPayloadFromConf(appContext.getAMConf());
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
    plugin.initialize(payload, pluginContext);
  }

  public void onVertexStarted(List<TezTaskAttemptID> completions) {
    Map<String, List<Integer>> pluginCompletionsMap = Maps.newHashMap();
    for (TezTaskAttemptID attemptId : completions) {
      TezTaskID tezTaskId = attemptId.getTaskID();
      Integer taskId = new Integer(tezTaskId.getId());
      String vertexName = 
          appContext.getCurrentDAG().getVertex(tezTaskId.getVertexID()).getName();
      List<Integer> taskIdList = pluginCompletionsMap.get(vertexName);
      if (taskIdList == null) {
        taskIdList = Lists.newArrayList();
        pluginCompletionsMap.put(vertexName, taskIdList);
      }
      taskIdList.add(taskId);
    }
    plugin.onVertexStarted(pluginCompletionsMap);
  }

  public void onSourceTaskCompleted(TezTaskAttemptID attemptId) {
    TezTaskID tezTaskId = attemptId.getTaskID();
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

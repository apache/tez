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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
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
  UserPayload payload = null;
  AppContext appContext;
  ConcurrentHashMap<String, List<TezEvent>> cachedRootInputEventMap;

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
        Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
        Map<String, InputSpecUpdate> rootInputSpecUpdate) {
      return managedVertex.setParallelism(parallelism, vertexLocationHint, sourceEdgeManagers,
          rootInputSpecUpdate);
    }

    @Override
    public void scheduleVertexTasks(List<TaskWithLocationHint> tasks) {
      managedVertex.scheduleTasks(tasks);
    }

    @Nullable
    @Override
    public Set<String> getVertexInputNames() {
      Set<String> inputNames = null;
      Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>
          inputs = managedVertex.getAdditionalInputs();
      if (inputs != null) {
        inputNames = inputs.keySet();
      }
      return inputNames;
    }

    @Override
    public UserPayload getUserPayload() {
      return payload;
    }

    @Override
    public void addRootInputEvents(final String inputName,
        Collection<InputDataInformationEvent> events) {
      verifyIsRootInput(inputName);
      Iterable<TezEvent> tezEvents = Iterables.transform(events,
          new Function<InputDataInformationEvent, TezEvent>() {
            @Override
            public TezEvent apply(InputDataInformationEvent riEvent) {
              TezEvent tezEvent = new TezEvent(riEvent, rootEventSourceMetadata);
              tezEvent.setDestinationInfo(getDestinationMetaData(inputName));
              return tezEvent;
            }
          });

      cachedRootInputEventMap.put(inputName,Lists.newArrayList(tezEvents));
      // Recovery handling is taken care of by the Vertex.
    }


    @Override
    public void setVertexLocationHint(VertexLocationHint locationHint) {
      Preconditions.checkNotNull(locationHint, "locationHint is null");
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
    public Resource getTotalAvailableResource() {
      return appContext.getTaskScheduler().getTotalResources();
    }

    @Override
    public int getNumClusterNodes() {
      return appContext.getTaskScheduler().getNumClusterNodes();
    }

    @Override
    public Container getTaskContainer(String vertexName, Integer taskIndex) {
      Vertex vertex = appContext.getCurrentDAG().getVertex(vertexName);
      Task task = vertex.getTask(taskIndex.intValue());
      TaskAttempt attempt = task.getSuccessfulAttempt();
      if (attempt != null) {
        return attempt.getAssignedContainer();
      }
      return null;
    }
  }

  public VertexManager(VertexManagerPluginDescriptor pluginDesc,
      Vertex managedVertex, AppContext appContext) {
    checkNotNull(pluginDesc, "pluginDesc is null");
    checkNotNull(managedVertex, "managedVertex is null");
    checkNotNull(appContext, "appContext is null");
    this.pluginDesc = pluginDesc;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
    this.cachedRootInputEventMap = new ConcurrentHashMap<String, List<TezEvent>>();
  }

  public VertexManagerPlugin getPlugin() {
    return plugin;
  }

  public void initialize() {
    pluginContext = new VertexManagerPluginContextImpl();
    if (pluginDesc != null) {
      plugin = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
          new Class[]{VertexManagerPluginContext.class}, new Object[]{pluginContext});
      payload = pluginDesc.getUserPayload();
    }
    plugin.initialize();
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

  public List<TezEvent> onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    plugin.onRootVertexInitialized(inputName, inputDescriptor, events);
    return cachedRootInputEventMap.get(inputName);
  }
}

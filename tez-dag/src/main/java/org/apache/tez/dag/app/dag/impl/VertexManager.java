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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.VertexEventManagerUserCodeError;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.app.dag.VertexStateUpdateListener;
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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class VertexManager {
  VertexManagerPluginDescriptor pluginDesc;
  VertexManagerPlugin plugin;
  Vertex managedVertex;
  VertexManagerPluginContextImpl pluginContext;
  UserPayload payload = null;
  AppContext appContext;
  BlockingQueue<TezEvent> rootInputInitEventQueue;
  StateChangeNotifier stateChangeNotifier;

  private static final Log LOG = LogFactory.getLog(VertexManager.class);

  class VertexManagerPluginContextImpl implements VertexManagerPluginContext, VertexStateUpdateListener {

    private EventMetaData rootEventSourceMetadata = new EventMetaData(EventProducerConsumerType.INPUT,
        managedVertex.getName(), "NULL_VERTEX", null);
    private Map<String, EventMetaData> destinationEventMetadataMap = Maps.newHashMap();
    private final List<String> notificationRegisteredVertices = Lists.newArrayList();
    AtomicBoolean isComplete = new AtomicBoolean(false);

    private void checkAndThrowIfDone() {
      if (isComplete()) {
        throw new TezUncheckedException("Cannot invoke context methods after reporting done");
      }
    }
    
    @Override
    public synchronized Map<String, EdgeProperty> getInputVertexEdgeProperties() {
      checkAndThrowIfDone();
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
    public synchronized String getVertexName() {
      checkAndThrowIfDone();
      return managedVertex.getName();
    }

    @Override
    public synchronized int getVertexNumTasks(String vertexName) {
      checkAndThrowIfDone();
      return appContext.getCurrentDAG().getVertex(vertexName).getTotalTasks();
    }

    @Override
    public synchronized void setVertexParallelism(int parallelism, VertexLocationHint vertexLocationHint,
        Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
        Map<String, InputSpecUpdate> rootInputSpecUpdate) {
      checkAndThrowIfDone();
      try {
        managedVertex.setParallelism(parallelism, vertexLocationHint, sourceEdgeManagers,
            rootInputSpecUpdate, true);
      } catch (AMUserCodeException e) {
        // workaround: convert it to TezUncheckedException which would be caught in VM
        throw new TezUncheckedException(e);
      }
    }

    @Override
    public synchronized void scheduleVertexTasks(List<TaskWithLocationHint> tasks) {
      checkAndThrowIfDone();
      managedVertex.scheduleTasks(tasks);
    }

    @Nullable
    @Override
    public synchronized Set<String> getVertexInputNames() {
      checkAndThrowIfDone();
      Set<String> inputNames = null;
      Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>
          inputs = managedVertex.getAdditionalInputs();
      if (inputs != null) {
        inputNames = inputs.keySet();
      }
      return inputNames;
    }

    @Override
    public synchronized UserPayload getUserPayload() {
      checkAndThrowIfDone();
      return payload;
    }

    @Override
    public synchronized void addRootInputEvents(final String inputName,
        Collection<InputDataInformationEvent> events) {
      checkAndThrowIfDone();
      verifyIsRootInput(inputName);
      Collection<TezEvent> tezEvents = Collections2.transform(events,
          new Function<InputDataInformationEvent, TezEvent>() {
            @Override
            public TezEvent apply(InputDataInformationEvent riEvent) {
              TezEvent tezEvent = new TezEvent(riEvent, rootEventSourceMetadata);
              tezEvent.setDestinationInfo(getDestinationMetaData(inputName));
              return tezEvent;
            }
          });

      if (LOG.isDebugEnabled()) {
        LOG.debug("vertex:" + managedVertex.getName() + "; Added " + events.size() + " for input " +
                "name " + inputName);
      }
      rootInputInitEventQueue.addAll(tezEvents);
      // Recovery handling is taken care of by the Vertex.
    }


    @Override
    public synchronized void setVertexLocationHint(VertexLocationHint locationHint) {
      checkAndThrowIfDone();
      Preconditions.checkNotNull(locationHint, "locationHint is null");
      managedVertex.setVertexLocationHint(locationHint);
    }

    @Override
    public synchronized int getDAGAttemptNumber() {
      checkAndThrowIfDone();
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
    public synchronized Resource getVertexTaskResource() {
      checkAndThrowIfDone();
      return managedVertex.getTaskResource();
    }

    @Override
    public synchronized Resource getTotalAvailableResource() {
      checkAndThrowIfDone();
      return appContext.getTaskScheduler().getTotalResources();
    }

    @Override
    public synchronized int getNumClusterNodes() {
      checkAndThrowIfDone();
      return appContext.getTaskScheduler().getNumClusterNodes();
    }

    @Override
    public synchronized Container getTaskContainer(String vertexName, Integer taskIndex) {
      checkAndThrowIfDone();
      Vertex vertex = appContext.getCurrentDAG().getVertex(vertexName);
      Task task = vertex.getTask(taskIndex.intValue());
      TaskAttempt attempt = task.getSuccessfulAttempt();
      if (attempt != null) {
        return attempt.getAssignedContainer();
      }
      return null;
    }

    @Override
    public synchronized void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      checkAndThrowIfDone();
      synchronized(notificationRegisteredVertices) {
        notificationRegisteredVertices.add(vertexName);
      }
      stateChangeNotifier.registerForVertexUpdates(vertexName, stateSet, this);
    }

    private void unregisterForVertexStateUpdates() {
      synchronized (notificationRegisteredVertices) {
        for (String vertexName : notificationRegisteredVertices) {
          stateChangeNotifier.unregisterForVertexUpdates(vertexName, this);
        }

      }
    }
    
    boolean isComplete() {
      return (isComplete.get() == true);
    }

    // TODO add later after TEZ-1714 @Override
    public synchronized void vertexManagerDone() {
      checkAndThrowIfDone();
      LOG.info("Vertex Manager reported done for : " + managedVertex.getLogIdentifier());
      this.isComplete.set(true);
      unregisterForVertexStateUpdates();
    }

    @Override
    public synchronized void vertexReconfigurationPlanned() {
      checkAndThrowIfDone();
      managedVertex.vertexReconfigurationPlanned();
    }

    @Override
    public synchronized void doneReconfiguringVertex() {
      checkAndThrowIfDone();
      managedVertex.doneReconfiguringVertex();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void onStateUpdated(VertexStateUpdate event) {
      if (isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Dropping state update for vertex=" + event.getVertexName() + ", state=" +
              event.getVertexState() +
              " since vertexmanager for " + managedVertex.getLogIdentifier() + " is complete.");
        }
      } else {
        try {
          plugin.onVertexStateUpdated(event);
        } catch (Exception e) {
          // state change must be triggered via an event transition
          appContext.getEventHandler().handle(
              new VertexEventManagerUserCodeError(managedVertex.getVertexId(),
                  new AMUserCodeException(Source.VertexManager, e)));
        }
      }
    }

  }

  public VertexManager(VertexManagerPluginDescriptor pluginDesc,
      Vertex managedVertex, AppContext appContext, StateChangeNotifier stateChangeNotifier) throws TezException {
    checkNotNull(pluginDesc, "pluginDesc is null");
    checkNotNull(managedVertex, "managedVertex is null");
    checkNotNull(appContext, "appContext is null");
    checkNotNull(stateChangeNotifier, "notifier is null");
    this.pluginDesc = pluginDesc;
    this.managedVertex = managedVertex;
    this.appContext = appContext;
    this.stateChangeNotifier = stateChangeNotifier;
    // don't specify the size of rootInputInitEventQueue, otherwise it will fail when addAll
    this.rootInputInitEventQueue = new LinkedBlockingQueue<TezEvent>();
  }

  public VertexManagerPlugin getPlugin() {
    return plugin;
  }

  public void initialize() throws AMUserCodeException {
    pluginContext = new VertexManagerPluginContextImpl();
    if (pluginDesc != null) {
      try {
        plugin = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
            new Class[]{VertexManagerPluginContext.class}, new Object[]{pluginContext});
      } catch (TezReflectionException e) {
        throw new AMUserCodeException(Source.VertexManager, e);
      }
      payload = pluginDesc.getUserPayload();
    }
    try {
      if (!pluginContext.isComplete()) {
        plugin.initialize();
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
  }

  public void onVertexStarted(List<TezTaskAttemptID> completions) throws AMUserCodeException {
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
    try {
      if (!pluginContext.isComplete()) {
        plugin.onVertexStarted(pluginCompletionsMap);
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
  }

  public void onSourceTaskCompleted(TezTaskID tezTaskId) throws AMUserCodeException {
    Integer taskId = new Integer(tezTaskId.getId());
    String vertexName =
        appContext.getCurrentDAG().getVertex(tezTaskId.getVertexID()).getName();
    try {
      if (!pluginContext.isComplete()) {
        plugin.onSourceTaskCompleted(vertexName, taskId);
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
  }

  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws AMUserCodeException {
    try {
      if (!pluginContext.isComplete()) {
        plugin.onVertexManagerEventReceived(vmEvent);
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
  }

  public List<TezEvent> onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) throws AMUserCodeException {
    try {
      if (!pluginContext.isComplete()) {
        plugin.onRootVertexInitialized(inputName, inputDescriptor, events);
      }
    } catch (Exception e) {
      throw new AMUserCodeException(Source.VertexManager, e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("vertex:" + managedVertex.getName() + "; after call of VertexManagerPlugin.onRootVertexInitialized"
          + " on input:" + inputName + ", current task events size is " + rootInputInitEventQueue.size());
    }
    List<TezEvent> resultEvents = new ArrayList<TezEvent>();
    rootInputInitEventQueue.drainTo(resultEvents);
    return resultEvents;
  }
}

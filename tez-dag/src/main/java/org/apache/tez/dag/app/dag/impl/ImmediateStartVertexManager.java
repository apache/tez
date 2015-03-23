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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts all tasks immediately on vertex start
 */
public class ImmediateStartVertexManager extends VertexManagerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(ImmediateStartVertexManager.class);

  private final Map<String, Boolean> srcVertexConfigured = Maps.newConcurrentMap();
  private int managedTasks;
  private boolean tasksScheduled = false;
  private AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);

  public ImmediateStartVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    managedTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      String srcVertex = entry.getKey();
      //track vertices with task count > 0
      if (getContext().getVertexNumTasks(srcVertex) > 0) {
        LOG.info("Task count in " + srcVertex + ": " + getContext().getVertexNumTasks(srcVertex));
        srcVertexConfigured.put(srcVertex, false);
        getContext().registerForVertexStateUpdates(srcVertex, EnumSet.of(VertexState.CONFIGURED));
      } else {
        LOG.info("Vertex: " + getContext().getVertexName() + "; Ignoring " + srcVertex
            + " as it has got 0 tasks");
      }
    }
    onVertexStartedDone.set(true);
    scheduleTasks();
  }

  private void scheduleTasks() {
    if (!onVertexStartedDone.get()) {
      // vertex not started yet
      return;
    }
    if (tasksScheduled) {
      // already scheduled
      return;
    }

    if (!canScheduleTasks()) {
      return;
    }
    
    tasksScheduled = true;
    List<TaskWithLocationHint> tasksToStart = Lists.newArrayListWithCapacity(managedTasks);
    for (int i = 0; i < managedTasks; ++i) {
      tasksToStart.add(new TaskWithLocationHint(i, null));
    }

    if (!tasksToStart.isEmpty()) {
      LOG.info("Starting " + tasksToStart.size() + " in " + getContext().getVertexName());
      getContext().scheduleVertexTasks(tasksToStart);
    }
    // all tasks scheduled. Can call vertexManagerDone().
    // TODO TEZ-1714 for locking issues getContext().vertexManagerDone();
  }

  private boolean canScheduleTasks() {
    // check for source vertices completely configured
    for (Map.Entry<String, Boolean> entry : srcVertexConfigured.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        // vertex not configured
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for vertex: " + entry.getKey() + " in vertex: " + getContext().getVertexName());
        }
        return false;
      }
    }

    return true;
  }
  
  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    Preconditions.checkArgument(stateUpdate.getVertexState() == VertexState.CONFIGURED,
        "Received incorrect state notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    Preconditions.checkArgument(srcVertexConfigured.containsKey(stateUpdate.getVertexName()),
        "Received incorrect vertex notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    Preconditions.checkState(srcVertexConfigured.put(stateUpdate.getVertexName(), true)
        .booleanValue() == false);
    LOG.info("Received configured notification: " + stateUpdate.getVertexState() + " for vertex: "
        + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    scheduleTasks();
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void initialize() {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
  }

}

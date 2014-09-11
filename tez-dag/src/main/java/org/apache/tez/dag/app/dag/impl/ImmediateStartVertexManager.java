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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import java.util.List;
import java.util.Map;

/**
 * Starts all tasks immediately on vertex start
 */
public class ImmediateStartVertexManager extends VertexManagerPlugin {

  private static final Log LOG = LogFactory.getLog(ImmediateStartVertexManager.class);

  private final Map<String, SourceVertexInfo> srcVertexInfo = Maps.newHashMap();
  private int managedTasks;
  private boolean tasksScheduled = false;

  class SourceVertexInfo {
    EdgeProperty edgeProperty;
    int numFinishedTasks;

    SourceVertexInfo(EdgeProperty edgeProperty) {
      this.edgeProperty = edgeProperty;
    }
  }

  public ImmediateStartVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    managedTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      String srcVertex = entry.getKey();
      EdgeProperty edgeProp = entry.getValue();
      srcVertexInfo.put(srcVertex, new SourceVertexInfo(edgeProp));
    }

    //handle completions
    for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
      for (Integer task : entry.getValue()) {
        handleSourceTaskFinished(entry.getKey(), task);
      }
    }
    scheduleTasks();
  }

  private void handleSourceTaskFinished(String vertex, Integer taskId) {
    SourceVertexInfo srcInfo = srcVertexInfo.get(vertex);
    //Not mandatory to check for duplicate completions here
    srcInfo.numFinishedTasks++;
  }

  private void scheduleTasks() {
    if (!canScheduleTasks()) {
      return;
    }

    List<TaskWithLocationHint> tasksToStart = Lists.newArrayListWithCapacity(managedTasks);
    for (int i = 0; i < managedTasks; ++i) {
      tasksToStart.add(new TaskWithLocationHint(new Integer(i), null));
    }

    if (!tasksToStart.isEmpty()) {
      LOG.info("Starting " + tasksToStart.size() + " in " + getContext().getVertexName());
      getContext().scheduleVertexTasks(tasksToStart);
    }
    tasksScheduled = true;
  }

  private boolean canScheduleTasks() {
    //Check if at least 1 task is finished from each source vertex (in case of broadcast &
    // one-to-one or custom)
    for (Map.Entry<String, SourceVertexInfo> entry : srcVertexInfo.entrySet()) {
      SourceVertexInfo srcVertexInfo = entry.getValue();
      switch(srcVertexInfo.edgeProperty.getDataMovementType()) {
      case ONE_TO_ONE:
      case BROADCAST:
      case CUSTOM:
        if (srcVertexInfo.numFinishedTasks == 0) {
          //do not schedule tasks until a task from source task is complete
          return false;
        }
      default:
        break;
      }
    }
    return true;
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
    handleSourceTaskFinished(srcVertexName, attemptId);
    if (!tasksScheduled) {
      scheduleTasks();
    }
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

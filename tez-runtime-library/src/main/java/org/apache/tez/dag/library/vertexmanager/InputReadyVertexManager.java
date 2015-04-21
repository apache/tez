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

package org.apache.tez.dag.library.vertexmanager;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

@Private
public class InputReadyVertexManager extends VertexManagerPlugin {
  private static final Logger LOG = 
      LoggerFactory.getLogger(InputReadyVertexManager.class);

  Map<String, SourceVertexInfo> srcVertexInfo = Maps.newHashMap();
  boolean taskIsStarted[];
  int oneToOneSrcTasksDoneCount[];
  TaskLocationHint oneToOneLocationHints[];
  int numOneToOneEdges;
  int numConfiguredSources;
  Multimap<String, Integer> pendingCompletions = LinkedListMultimap.create();
  AtomicBoolean configured;
  AtomicBoolean started;

  public InputReadyVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  static class SourceVertexInfo {
    EdgeProperty edgeProperty;
    int numTasks;
    int numFinishedTasks;
    Boolean taskIsFinished[];
    
    SourceVertexInfo(int numTasks, EdgeProperty edgeProperty) {
      this.numTasks = numTasks;
      this.numFinishedTasks = 0;
      this.edgeProperty = edgeProperty;
      this.taskIsFinished = new Boolean[numTasks];
    }
  }
  
  private void configure() {
    Preconditions.checkState(!configured.get(), "Vertex: " + getContext().getVertexName());
    int numManagedTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    LOG.info("Managing " + numManagedTasks + " tasks for vertex: " + getContext().getVertexName());

    // find out about all input edge types. If there is a custom edge then 
    // TODO Until TEZ-1013 we cannot handle custom input formats
    Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
    int oneToOneSrcTaskCount = 0;
    numOneToOneEdges = 0;
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      EdgeProperty edgeProp = entry.getValue();
      String srcVertex = entry.getKey();
      int numSrcTasks = getContext().getVertexNumTasks(srcVertex);
      switch (edgeProp.getDataMovementType()) {
      case CUSTOM:
        throw new TezUncheckedException("Cannot handle custom edge");
      case ONE_TO_ONE:
        numOneToOneEdges++;
        if (oneToOneSrcTaskCount == 0) {
          oneToOneSrcTaskCount = numSrcTasks;
        } else if (oneToOneSrcTaskCount != numSrcTasks) {
          throw new TezUncheckedException(
              "All 1-1 source vertices must have identical concurrency");
        }
        break;
      case SCATTER_GATHER:
      case BROADCAST:
        break;
      default:
        throw new TezUncheckedException(
            "Unknown edge type: " + edgeProp.getDataMovementType());
      }
      srcVertexInfo.put(srcVertex, new SourceVertexInfo(numSrcTasks, edgeProp));
    }
    
    if (numOneToOneEdges > 0) {
      Preconditions
          .checkState(oneToOneSrcTaskCount >= 0, "Vertex: " + getContext().getVertexName());
      if (oneToOneSrcTaskCount != numManagedTasks) {
        numManagedTasks = oneToOneSrcTaskCount;
        // must change parallelism to make them the same
        LOG.info("Update parallelism of vertex: " + getContext().getVertexName() + 
            " to " + oneToOneSrcTaskCount + " to match source 1-1 vertices.");
        getContext().reconfigureVertex(oneToOneSrcTaskCount, null, null);
      }
      oneToOneSrcTasksDoneCount = new int[oneToOneSrcTaskCount];
      oneToOneLocationHints = new TaskLocationHint[oneToOneSrcTaskCount];
    }
    
    Preconditions.checkState(numManagedTasks >=0, "Vertex: " + getContext().getVertexName());
    taskIsStarted = new boolean[numManagedTasks];

    // allow scheduling
    configured.set(true);
    getContext().doneReconfiguringVertex();
    trySchedulingPendingCompletions();
  }
  
  private boolean readyToSchedule() {
    return (configured.get() && started.get());
  }
  
  private void trySchedulingPendingCompletions() {
    if (readyToSchedule() && !pendingCompletions.isEmpty()) {
      for (Map.Entry<String, Collection<Integer>> entry : pendingCompletions.asMap().entrySet()) {
        for (Integer i : entry.getValue()) {
          onSourceTaskCompleted(entry.getKey(), i);
        }
      }
    }
  }
  
  @Override
  public void initialize() {
    // this will prevent vertex from starting until we notify we are done
    getContext().vertexReconfigurationPlanned();
    Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
    // wait for sources and self to start
    numConfiguredSources = 0;
    configured = new AtomicBoolean(false);
    started = new AtomicBoolean(false);
    for (String entry : edges.keySet()) {
      getContext().registerForVertexStateUpdates(entry, EnumSet.of(VertexState.CONFIGURED));
    }
  }
  
  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception {
    numConfiguredSources++;
    int target = getContext().getInputVertexEdgeProperties().size();
    LOG.info("For vertex: " + getContext().getVertexName() + " Received configured signal from: "
        + stateUpdate.getVertexName() + " numConfiguredSources: " + numConfiguredSources
        + " needed: " + target);
    Preconditions.checkState(numConfiguredSources <= target, "Vertex: " + getContext().getVertexName());
    if (numConfiguredSources == target) {
      configure();
    }
  }

  @Override
  public synchronized void onVertexStarted(Map<String, List<Integer>> completions) {
    for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
      pendingCompletions.putAll(entry.getKey(), entry.getValue());
    }    

    // allow scheduling
    started.set(true);
    
    trySchedulingPendingCompletions();
  }

  @Override
  public synchronized void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
    if (readyToSchedule()) {
      // configured and started. try to schedule
      handleSourceTaskFinished(srcVertexName, taskId);
    } else {
      pendingCompletions.put(srcVertexName, taskId);
    }
  }

  @Override
  public synchronized void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public synchronized void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
  }
  
  void handleSourceTaskFinished(String vertex, Integer taskId) {
    SourceVertexInfo srcInfo = srcVertexInfo.get(vertex);
    if (srcInfo.taskIsFinished[taskId.intValue()] == null) {
      // not a duplicate completion
      srcInfo.taskIsFinished[taskId.intValue()] = Boolean.valueOf(true);
      srcInfo.numFinishedTasks++;
      if (srcInfo.edgeProperty.getDataMovementType() == DataMovementType.ONE_TO_ONE) {
        oneToOneSrcTasksDoneCount[taskId.intValue()]++;
        // keep the latest container that completed as the location hint
        // After there is standard data size info available then use it
        oneToOneLocationHints[taskId.intValue()] = TaskLocationHint.createTaskLocationHint(vertex, taskId);
      }
    }
    
    // custom edge needs to tell us which of our tasks its connected to
    // for now only-built in edges supported
    // Check if current source task's vertex is completed.
    if (srcInfo.edgeProperty.getDataMovementType() != DataMovementType.ONE_TO_ONE
        && srcInfo.numTasks != srcInfo.numFinishedTasks) {
      // we depend on all tasks to finish. So nothing to do now.
      return;
    }
    
    // currently finished vertex task may trigger us to schedule
    for (SourceVertexInfo vInfo : srcVertexInfo.values()) {
      if (vInfo.edgeProperty.getDataMovementType() != DataMovementType.ONE_TO_ONE) {
        // we depend on all tasks to finish.
        if (vInfo.numTasks != vInfo.numFinishedTasks) {
          // we depend on all tasks to finish. So nothing to do now.
          return;
        }
      }
    }
    
    // all source vertices will full dependencies are done
    List<TaskWithLocationHint> tasksToStart = null;
    if (numOneToOneEdges == 0) {
      // no 1-1 dependency. Start all tasks
      int numTasks = taskIsStarted.length;
      LOG.info("Starting all " + numTasks + "tasks for vertex: " + getContext().getVertexName());
      tasksToStart = Lists.newArrayListWithCapacity(numTasks);
      for (int i=0; i<numTasks; ++i) {
        taskIsStarted[i] = true;
        tasksToStart.add(new TaskWithLocationHint(Integer.valueOf(i), null));
      }
    } else {
      // start only the ready 1-1 tasks
      tasksToStart = Lists.newLinkedList();
      for (int i=0; i<taskIsStarted.length; ++i) {
        if (!taskIsStarted[i] && oneToOneSrcTasksDoneCount[i] == numOneToOneEdges) {
          taskIsStarted[i] = true;
          TaskLocationHint locationHint = null;
          if (oneToOneLocationHints[i] != null) {
            locationHint = oneToOneLocationHints[i];
          }
          LOG.info("Starting task " + i + " for vertex: "
              + getContext().getVertexName() + " with location: "
              + ((locationHint != null) ? locationHint.getAffinitizedTask() : "null"));
          tasksToStart.add(new TaskWithLocationHint(Integer.valueOf(i), locationHint));
        }
      }
    }
    
    if (tasksToStart != null && !tasksToStart.isEmpty()) {
      getContext().scheduleVertexTasks(tasksToStart);
    }
    
  }

}

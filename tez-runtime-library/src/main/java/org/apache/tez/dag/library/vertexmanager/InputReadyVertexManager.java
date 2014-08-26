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

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Private
public class InputReadyVertexManager extends VertexManagerPlugin {
  private static final Log LOG = 
      LogFactory.getLog(InputReadyVertexManager.class);

  Map<String, SourceVertexInfo> srcVertexInfo = Maps.newHashMap();
  boolean taskIsStarted[];
  int oneToOneSrcTasksDoneCount[];
  Container oneToOneLocationHints[];
  int numOneToOneEdges;

  public InputReadyVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  class SourceVertexInfo {
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
  
  @Override
  public void initialize() {
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numManagedTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    LOG.info("Managing " + numManagedTasks + " tasks for vertex: " + getContext().getVertexName());
    taskIsStarted = new boolean[numManagedTasks];

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
      if (oneToOneSrcTaskCount != numManagedTasks) {
        throw new TezUncheckedException(
            "Managed task number must equal 1-1 source task number");
      }
      oneToOneSrcTasksDoneCount = new int[oneToOneSrcTaskCount];
      oneToOneLocationHints = new Container[oneToOneSrcTaskCount];
    }

    for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
      for (Integer task : entry.getValue()) {
        handleSourceTaskFinished(entry.getKey(), task);
      }
    }
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
    handleSourceTaskFinished(srcVertexName, taskId);
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
  }
  
  void handleSourceTaskFinished(String vertex, Integer taskId) {
    SourceVertexInfo srcInfo = srcVertexInfo.get(vertex);
    if (srcInfo.taskIsFinished[taskId.intValue()] == null) {
      // not a duplicate completion
      srcInfo.taskIsFinished[taskId.intValue()] = new Boolean(true);
      srcInfo.numFinishedTasks++;
      if (srcInfo.edgeProperty.getDataMovementType() == DataMovementType.ONE_TO_ONE) {
        oneToOneSrcTasksDoneCount[taskId.intValue()]++;
        // keep the latest container that completed as the location hint
        // After there is standard data size info available then use it
        oneToOneLocationHints[taskId.intValue()] = getContext().getTaskContainer(vertex, taskId);
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
        tasksToStart.add(new TaskWithLocationHint(new Integer(i), null));
      }
    } else {
      // start only the ready 1-1 tasks
      tasksToStart = Lists.newLinkedList();
      for (int i=0; i<taskIsStarted.length; ++i) {
        if (!taskIsStarted[i] && oneToOneSrcTasksDoneCount[i] == numOneToOneEdges) {
          taskIsStarted[i] = true;
          TaskLocationHint locationHint = null;
          if (oneToOneLocationHints[i] != null) {
            locationHint = TaskLocationHint.createTaskLocationHint(oneToOneLocationHints[i].getId());
          }
          LOG.info("Starting task " + i + " for vertex: "
              + getContext().getVertexName() + " with location: "
              + ((locationHint != null) ? locationHint.getAffinitizedContainer() : "null"));
          tasksToStart.add(new TaskWithLocationHint(new Integer(i), locationHint));
        }
      }
    }
    
    if (tasksToStart != null && !tasksToStart.isEmpty()) {
      getContext().scheduleVertexTasks(tasksToStart);
    }
    
  }

}

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
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class InputReadyVertexManager implements VertexManagerPlugin {
  private static final Log LOG = 
      LogFactory.getLog(InputReadyVertexManager.class);

  VertexManagerPluginContext context;
  Map<String, SourceVertexInfo> srcVertexInfo = Maps.newHashMap();
  boolean taskIsStarted[];
  int templateOneToOne[];
  int numOneToOneEdges;
  
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
  public void initialize(VertexManagerPluginContext context) {
    this.context = context;
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numManagedTasks = context.getVertexNumTasks(context.getVertexName());
    LOG.info("Managing " + numManagedTasks + " for vertex: " + context.getVertexName());
    taskIsStarted = new boolean[numManagedTasks];

    // find out about all input edge types. If there is a custom edge then 
    // TODO Until TEZ-1013 we cannot handle custom input formats
    Map<String, EdgeProperty> edges = context.getInputVertexEdgeProperties();
    int oneToOneSrcTaskCount = 0;
    numOneToOneEdges = 0;
    for (Map.Entry<String, EdgeProperty> entry : edges.entrySet()) {
      EdgeProperty edgeProp = entry.getValue();
      String srcVertex = entry.getKey();
      int numSrcTasks = context.getVertexNumTasks(srcVertex);
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
      templateOneToOne = new int[oneToOneSrcTaskCount];
    }

    for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
      for (Integer task : entry.getValue()) {
        handleSouceTaskFinished(entry.getKey(), task);
      }
    }
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
    handleSouceTaskFinished(srcVertexName, taskId);
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
  }
  
  void handleSouceTaskFinished(String vertex, Integer taskId) {
    SourceVertexInfo srcInfo = srcVertexInfo.get(vertex);
    if (srcInfo.taskIsFinished[taskId.intValue()] == null) {
      // not a duplicate completion
      srcInfo.taskIsFinished[taskId.intValue()] = new Boolean(true);
      srcInfo.numFinishedTasks++;
      if (srcInfo.edgeProperty.getDataMovementType() == DataMovementType.ONE_TO_ONE) {
        templateOneToOne[taskId.intValue()]++;
      }
    }
    
    // custom edge needs to tell us which of our tasks its connected to
    // for now only-built in edges supported
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
    List<Integer> tasksToStart = null;
    if (numOneToOneEdges == 0) {
      // no 1-1 dependency. Start all tasks
      tasksToStart = Lists.newArrayListWithCapacity(taskIsStarted.length);
      for (int i=0; i<taskIsStarted.length; ++i) {
        taskIsStarted[i] = true;
        tasksToStart.add(new Integer(i));
      }
    } else {
      // start only the ready 1-1 tasks
      tasksToStart = Lists.newLinkedList();
      for (int i=0; i<taskIsStarted.length; ++i) {
        if (!taskIsStarted[i] && templateOneToOne[i] == numOneToOneEdges) {
          taskIsStarted[i] = true;
          tasksToStart.add(new Integer(i));
        }
      }
    }
    
    if (tasksToStart != null && !tasksToStart.isEmpty()) {
      // TODO determine placement after TEZ-1018
      context.scheduleVertexTasks(tasksToStart);
    }
    
  }

}

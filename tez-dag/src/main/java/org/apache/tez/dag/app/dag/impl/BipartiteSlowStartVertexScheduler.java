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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexScheduler;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

/**
 * Starts scheduling tasks when number of completed source tasks crosses 
 * <code>slowStartMinSrcCompletionFraction</code> and schedules all tasks 
 *  when <code>slowStartMaxSrcCompletionFraction</code> is reached
 */
public class BipartiteSlowStartVertexScheduler implements VertexScheduler {
  
  private static final Log LOG = 
                   LogFactory.getLog(BipartiteSlowStartVertexScheduler.class);

  final Vertex managedVertex;
  final float slowStartMinSrcCompletionFraction;
  final float slowStartMaxSrcCompletionFraction;
  
  int numSourceTasks = 0;
  int numSourceTasksCompleted = 0;
  boolean slowStartThresholdReached = false;
  ArrayList<TezTaskID> pendingTasks;
  int totalTasksToSchedule = 0;
  HashMap<TezVertexID, Vertex> bipartiteSources = 
                                            new HashMap<TezVertexID, Vertex>();
  
  public BipartiteSlowStartVertexScheduler(Vertex managedVertex,
                                            float slowStartMinSrcCompletionFraction,
                                            float slowStartMaxSrcCompletionFraction) {
    this.managedVertex = managedVertex;
    this.slowStartMinSrcCompletionFraction = slowStartMinSrcCompletionFraction;
    this.slowStartMaxSrcCompletionFraction = slowStartMaxSrcCompletionFraction;
    
    if(slowStartMinSrcCompletionFraction < 0 || 
       slowStartMaxSrcCompletionFraction < slowStartMinSrcCompletionFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinSrcCompletionFraction" + 
          "/slowStartMaxSrcCompletionFraction. Min cannot be < 0 and " + 
          "max cannot be < min.");
    }
    
    Map<Vertex, EdgeProperty> inputs = managedVertex.getInputVertices();
    for(Map.Entry<Vertex, EdgeProperty> entry : inputs.entrySet()) {
      if(entry.getValue().getConnectionPattern() == ConnectionPattern.BIPARTITE) {
        Vertex vertex = entry.getKey();
        bipartiteSources.put(vertex.getVertexId(), vertex);
      }
    }
    if(bipartiteSources.isEmpty()) {
      throw new TezException("Atleast 1 bipartite source should exist");
    }
  }
  
  @Override
  public void onVertexStarted() {
    //targetVertex.scheduleTasks(targetVertex.getTasks().keySet()); 
    pendingTasks = new ArrayList<TezTaskID>(managedVertex.getTotalTasks());
    // track the tasks in this vertex
    pendingTasks.addAll(managedVertex.getTasks().keySet());
    totalTasksToSchedule = pendingTasks.size();
    
    // track source vertices
    for(Vertex vertex : bipartiteSources.values()) {
      numSourceTasks += vertex.getTotalTasks();
    }
    
    LOG.info("OnVertexStarted vertex: " + managedVertex.getVertexId() + 
             " with " + numSourceTasks + " source tasks and " + 
             totalTasksToSchedule + " pending tasks");
    
    schedulePendingTasks();
  }

  @Override
  public void onVertexCompleted() {
  }

  @Override
  public void onSourceTaskCompleted(TezTaskAttemptID attemptId) {
    TezVertexID vertexId = attemptId.getTaskID().getVertexID();
    if(bipartiteSources.containsKey(vertexId)) {
      ++numSourceTasksCompleted;
      schedulePendingTasks();
    }
  }
  
  void schedulePendingTasks(int numTasksToSchedule) {
    ArrayList<TezTaskID> scheduledTasks = new ArrayList<TezTaskID>(numTasksToSchedule);
    while(!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
      numTasksToSchedule--;
      scheduledTasks.add(pendingTasks.get(0));
      pendingTasks.remove(0);
    }
    managedVertex.scheduleTasks(scheduledTasks);
  }
  
  void schedulePendingTasks() {    
    int numPendingTasks = pendingTasks.size();
    if (numPendingTasks == 0) {
      return;
    }
    
    if (numSourceTasksCompleted == numSourceTasks && numPendingTasks > 0) {
      LOG.info("All source tasks assigned. " +
          "Ramping up " + numPendingTasks + 
          " remaining tasks for vertex: " + managedVertex.getName());
      schedulePendingTasks(numPendingTasks);
      return;
    }

    float completedSourceTaskFraction = 0f;
    if (numSourceTasks != 0) {//support for 0 source tasks
      completedSourceTaskFraction = (float)numSourceTasksCompleted/numSourceTasks;
    } else {
      completedSourceTaskFraction = 1;
    }
    
    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are 
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1; 
    float percentRange = slowStartMaxSrcCompletionFraction - 
                          slowStartMinSrcCompletionFraction;
    if (percentRange > 0) {
      tasksFractionToSchedule = 
            (completedSourceTaskFraction - slowStartMinSrcCompletionFraction)/
            percentRange;
    } else {
      // min and max are equal. schedule 100% on reaching min
      if(completedSourceTaskFraction < slowStartMinSrcCompletionFraction) {
        tasksFractionToSchedule = 0;
      }
    }
    
    if (tasksFractionToSchedule > 1) {
      tasksFractionToSchedule = 1;
    } else if (tasksFractionToSchedule < 0) {
      tasksFractionToSchedule = 0;
    }
    
    int numTasksToSchedule = 
        ((int)(tasksFractionToSchedule * totalTasksToSchedule) - 
         (totalTasksToSchedule - numPendingTasks));
    
    if (numTasksToSchedule > 0) {
      // numTasksToSchedule can be -ve if numSourceTasksCompleted does not 
      // does not increase monotonically
      LOG.info("Scheduling " + numTasksToSchedule + " tasks for vertex: " + 
               managedVertex.getVertexId() + " with totalTasks: " + 
               totalTasksToSchedule + ". " + numSourceTasksCompleted + 
               " source tasks completed out of " + numSourceTasks + 
               ". SourceTaskCompletedFraction: " + completedSourceTaskFraction);
      schedulePendingTasks(numTasksToSchedule);
    }
  }

}

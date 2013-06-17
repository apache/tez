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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;

@SuppressWarnings("rawtypes")
public class DAGSchedulerMRR implements DAGScheduler {
  
  private static final Log LOG = LogFactory.getLog(DAGSchedulerMRR.class);
  
  private final DAG dag;
  private final TaskSchedulerEventHandler taskScheduler;
  private final EventHandler handler;
  private Vertex currentPartitioner = null;
  private Vertex currentShuffler = null;
  private int currentShufflerDepth = 0;
  
  int numShuffleTasksScheduled = 0;
  List<TaskAttempt> pendingShuffleTasks = new LinkedList<TaskAttempt>();
  
  public DAGSchedulerMRR(DAG dag, EventHandler dispatcher,
      TaskSchedulerEventHandler taskScheduler) {
    this.dag = dag;
    this.handler = dispatcher;
    this.taskScheduler = taskScheduler;
  }
  
  @Override
  public void vertexCompleted(Vertex vertex) {
    if(currentPartitioner != null) {
      if(vertex != currentPartitioner) {
        String message = vertex.getVertexId() + " finished. Expecting"
            + " current partitioner " + currentPartitioner.getVertexId()
            + " to finish.";
        LOG.fatal(message);
        throw new TezUncheckedException(message);
      }
      LOG.info("Current partitioner " + currentPartitioner.getVertexId()
          + " is completed. " 
          + (currentShuffler!=null ? 
             currentShuffler.getVertexId() + " is new partitioner":
             "No current shuffler to replace the partitioner"));
      currentPartitioner = currentShuffler;
      currentShuffler = null;
      // schedule all pending shuffle tasks
      schedulePendingShuffles(pendingShuffleTasks.size());
      assert pendingShuffleTasks.isEmpty();
      numShuffleTasksScheduled = 0;
    }
    
  }
  
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();
    
    if(currentPartitioner == null) {
      // no partitioner. so set it.
      currentPartitioner = vertex;
      currentShufflerDepth = vertexDistanceFromRoot;
      LOG.info(vertex.getVertexId() + " is new partitioner at depth "
          + vertexDistanceFromRoot);
    } else if (currentShuffler == null && 
        vertexDistanceFromRoot > currentShufflerDepth) {
      // vertex not a partitioner. no shuffler set. has more depth than current
      // shuffler. this must be the new shuffler.
      currentShuffler = vertex;
      currentShufflerDepth = vertexDistanceFromRoot;
      LOG.info(vertex.getVertexId() + " is new shuffler at depth "
          + currentShufflerDepth);
    }
    
    if(currentShuffler == vertex) {
      pendingShuffleTasks.add(attempt);
      schedulePendingShuffles(getNumShufflesToSchedule());
      return;
    }
    
    // sanity check
    // task should be a partitioner, a shuffler or a retry of an ancestor
    if(currentPartitioner != vertex && currentShuffler != vertex && 
       vertexDistanceFromRoot >= currentPartitioner.getDistanceFromRoot()) {
      String message = vertex.getVertexId() + " is neither the "
          + " current partitioner: " + currentPartitioner.getVertexId()
          + " nor the current shuffler: " + currentShuffler.getVertexId();
      LOG.fatal(message);
      throw new TezUncheckedException(message);      
    }
    
    scheduleTaskAttempt(attempt);
  }
  
  @Override
  public void taskSucceeded(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    if (currentPartitioner == vertex) {
      // resources now available. try to schedule pending shuffles
      schedulePendingShuffles(getNumShufflesToSchedule());
    }
  }
  
  int getNumShufflesToSchedule() {
    assert currentPartitioner != null;
    
    if(pendingShuffleTasks.isEmpty()) {
      return 0;
    }
    
    assert currentShuffler != null;
    
    // get total resource limit
    Resource totalResources = taskScheduler.getTotalResources();
    int totalMem = totalResources.getMemory();
    
    // get resources needed by partitioner
    Resource partitionerResource = currentPartitioner.getTaskResource();
    int partitionerTaskMem = partitionerResource.getMemory();
    int numPartionersLeft = currentPartitioner.getTotalTasks()
        - currentPartitioner.getSucceededTasks();
    int partitionerMemNeeded = numPartionersLeft * partitionerTaskMem;
    
    // find leftover resources for shuffler
    int shufflerMemLeft = totalMem - partitionerMemNeeded;
    
    Resource shufflerResource = currentShuffler.getTaskResource();
    int shufflerTaskMem = shufflerResource.getMemory();
    int shufflerMemAssigned = shufflerTaskMem * numShuffleTasksScheduled;
    shufflerMemLeft -= shufflerMemAssigned;

    LOG.info("TotalMem: " + totalMem + 
             " Headroom: " + taskScheduler.getAvailableResources().getMemory() +
             " PartitionerTaskMem: " + partitionerTaskMem +
             " ShufflerTaskMem: " + shufflerTaskMem + 
             " PartitionerMemNeeded:" + partitionerMemNeeded +
             " ShufflerMemAssigned: " + shufflerMemAssigned + 
             " ShufflerMemLeft: " + shufflerMemLeft +
             " Pending shufflers: " + pendingShuffleTasks.size());

    if(shufflerMemLeft < 0) {
      // not enough resource to schedule a shuffler
      return 0;
    }

    if(shufflerTaskMem == 0) {
      return pendingShuffleTasks.size();
    }
    
    return shufflerMemLeft / shufflerTaskMem;
  }
  
  void schedulePendingShuffles(int scheduleCount) {
    while(!pendingShuffleTasks.isEmpty() && scheduleCount>0) {
      --scheduleCount;
      TaskAttempt shuffleAttempt = pendingShuffleTasks.remove(0);
      scheduleTaskAttempt(shuffleAttempt);
      if(!shuffleAttempt.getIsRescheduled()) {
        // dont double count same shuffle task
        numShuffleTasksScheduled++;
      }
    }
  }
  
  void scheduleTaskAttempt(TaskAttempt attempt) {
    boolean reOrderPriority = false;
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();
    
    // natural priority. Handles failures and retries.
    int priority = (vertexDistanceFromRoot + 1) * 3;
    
    if(currentShuffler == vertex) {
      // current shuffler vertex. assign special priority
      reOrderPriority = true;
    }
    
    if(reOrderPriority) {
      // special priority for current reducers while current partitioners are 
      // still running. Schedule at priority one higher than natural priority 
      // of previous vertex.
      priority -= 4;  // this == (partitionerDepth+1)*3 - 1     
    } else {
      if(attempt.getIsRescheduled()) {
        // higher priority for retries of failed attempts. Only makes sense in
        // case the task is faulty and we want to retry before other tasks in 
        // the same vertex to fail fast. But looks like this may happen also for
        // other cases like retry because outputs were unavailable.
        priority -= 2;
      }
    }
    
    LOG.info("Scheduling " + attempt.getID() + 
             " with depth " + vertexDistanceFromRoot + 
             " at priority " + priority);

    TaskAttemptEventSchedule attemptEvent = new TaskAttemptEventSchedule(
        attempt.getID(), Priority.newInstance(priority)); 
                                      
    sendEvent(attemptEvent);
  }
  
  @SuppressWarnings("unchecked")
  void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }
}

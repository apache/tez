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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.records.TezTaskID;

@SuppressWarnings("rawtypes")
public class DAGSchedulerMRR implements DAGScheduler {
  
  private static final Log LOG = LogFactory.getLog(DAGSchedulerMRR.class);
  
  private final DAG dag;
  private final TaskSchedulerEventHandler taskScheduler;
  private final EventHandler handler;
  
  private final float minReservedShuffleResource;
  
  private Vertex currentPartitioner = null;
  private Vertex currentShuffler = null;
  private int currentShufflerDepth = 0;
  
  int numShuffleTasksScheduled = 0;
  List<TaskAttempt> pendingShuffleTasks = new LinkedList<TaskAttempt>();
  Set<TezTaskID> unassignedShuffleTasks = new HashSet<TezTaskID>();
  Resource realShufflerResource = null;

  Set<TezTaskID> unassignedPartitionTasks = new HashSet<TezTaskID>();
  Resource realPartitionerResource = null;

  public DAGSchedulerMRR(DAG dag, EventHandler dispatcher,
      TaskSchedulerEventHandler taskScheduler, float minReservedShuffleResource) {
    this.dag = dag;
    this.handler = dispatcher;
    this.taskScheduler = taskScheduler;
    this.minReservedShuffleResource = minReservedShuffleResource;
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
      assert unassignedPartitionTasks.isEmpty();
      unassignedPartitionTasks.addAll(unassignedShuffleTasks);
      unassignedShuffleTasks.clear();
      realPartitionerResource = realShufflerResource;
      realShufflerResource = null;
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
    
    LOG.info("Schedule task: " + attempt.getID());
    
    if(currentPartitioner == null) {
      // no partitioner. so set it.
      currentPartitioner = vertex;
      currentShufflerDepth = vertexDistanceFromRoot;
      assert realPartitionerResource == null;
      Resource partitionerResource = currentPartitioner.getTaskResource();
      realPartitionerResource = Resource.newInstance(
          partitionerResource.getMemory(),
          partitionerResource.getVirtualCores());
      LOG.info(vertex.getVertexId() + " is new partitioner at depth "
          + vertexDistanceFromRoot);
    } else if (currentShuffler == null && 
        vertexDistanceFromRoot > currentShufflerDepth) {
      // vertex not a partitioner. no shuffler set. has more depth than current
      // shuffler. this must be the new shuffler.
      currentShuffler = vertex;
      currentShufflerDepth = vertexDistanceFromRoot;
      assert realShufflerResource == null;
      Resource shufflerResource = currentShuffler.getTaskResource();
      realShufflerResource = Resource.newInstance(
          shufflerResource.getMemory(),
          shufflerResource.getVirtualCores());
      LOG.info(vertex.getVertexId() + " is new shuffler at depth "
          + currentShufflerDepth);
    }
    
    if(currentShuffler == vertex) {
      pendingShuffleTasks.add(attempt);
      unassignedShuffleTasks.add(attempt.getTaskID());
      schedulePendingShuffles(getNumShufflesToSchedule());
      return;
    }
    
    if(currentPartitioner == vertex) {
      unassignedPartitionTasks.add(attempt.getTaskID());
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
  public void taskScheduled(DAGEventSchedulerUpdateTAAssigned event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    LOG.info("Task assigned: " + attempt.getID() + " Vertex: Total:"
        + vertex.getTotalTasks() + " succeeded: " + vertex.getSucceededTasks()
        + " Resource: " + event.getContainer().getResource().getMemory());

    if (currentPartitioner == vertex) {
      unassignedPartitionTasks.remove(attempt.getTaskID());
      Resource resource = event.getContainer().getResource();
      if(resource.getMemory() > realPartitionerResource.getMemory()) {
        realPartitionerResource.setMemory(resource.getMemory());
      }
    } else if (currentShuffler == vertex) {
      unassignedShuffleTasks.remove(attempt.getTaskID());
      Resource resource = event.getContainer().getResource();
      if(resource.getMemory() > realShufflerResource.getMemory()) {
        realShufflerResource.setMemory(resource.getMemory());
      }
    }
    schedulePendingShuffles(getNumShufflesToSchedule());
  }
  
  @Override
  public void taskSucceeded(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    LOG.info("Task succeeded: " + attempt.getID() + " Vertex: Total:" + vertex.getTotalTasks() + 
        " succeeded: " + vertex.getSucceededTasks());

    // resources now available. try to schedule pending shuffles
    schedulePendingShuffles(getNumShufflesToSchedule());
  }
  
  int getNumShufflesToSchedule() {
    assert currentPartitioner != null;
    
    if(pendingShuffleTasks.isEmpty()) {
      return 0;
    }
    
    if(unassignedPartitionTasks.isEmpty()) {
      LOG.info("All partitioners assigned. Scheduling all shufflers.");
      return pendingShuffleTasks.size();
    }
    
    assert currentShuffler != null;
    
    // get total resource limit
    Resource totalResources = taskScheduler.getTotalResources();
    Resource freeResources = taskScheduler.getAvailableResources();
    int totalMem = totalResources.getMemory();
    int freeMem = freeResources.getMemory();
    int partitionerTaskMem = realPartitionerResource.getMemory();
    int shufflerTaskMem = realShufflerResource.getMemory();
    int shufflerMemAssigned = shufflerTaskMem * numShuffleTasksScheduled;
    
    // get resources needed by partitioner
    int numPartitioners = currentPartitioner.getTotalTasks();
    int numPartionersSucceeded = currentPartitioner.getSucceededTasks();
    int numPartionersLeft = numPartitioners - numPartionersSucceeded;
    int partitionerMemNeeded = numPartionersLeft * partitionerTaskMem;
    
    // find leftover resources for shuffler
    int shufflerMemLeft = totalMem - partitionerMemNeeded;

    int maxShufflerMem = (int) (totalMem *
        (Math.min(minReservedShuffleResource, 
                  numPartionersSucceeded/(float)numPartitioners)));
    
    if(shufflerMemLeft < maxShufflerMem) {
      shufflerMemLeft = maxShufflerMem;
    }
    
    shufflerMemLeft -= shufflerMemAssigned;

    LOG.info("TotalMem: " + totalMem + 
             " Headroom: " + freeMem +
             " PartitionerTaskMem: " + partitionerTaskMem +
             " ShufflerTaskMem: " + shufflerTaskMem + 
             " MaxShuffleMem: " + maxShufflerMem +
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
    
    int shufflersToSchedule = shufflerMemLeft / shufflerTaskMem;
    shufflerMemAssigned += shufflerTaskMem * shufflersToSchedule;
    
    if(totalMem - shufflerMemAssigned < partitionerTaskMem) {
      // safety check when reduce ramp up limit is aggressively high
      LOG.info("Not scheduling more shufflers as it starves partitioners");
      return 0;
    }
    
    return shufflersToSchedule;
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
      assert currentPartitioner != null;
      // assign higher priority only if its needed. If all partitioners are done
      // then no need to do so.
      // TODO fix with assigned instead of succeeded
      if (!unassignedPartitionTasks.isEmpty()) {
        // current shuffler vertex to be scheduled while current partitioner is
        // still running. This needs to be higher priority or else it wont get 
        // allocated. This higher priority will be lower than the priority of a 
        // partitioner task that is a retry. so retries are safe.
        // assign special priority
        reOrderPriority = true;
      }
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

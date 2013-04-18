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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;

@SuppressWarnings("rawtypes")
public class DAGSchedulerMRR implements DAGScheduler {
  
  private static final Log LOG = LogFactory.getLog(DAGSchedulerMRR.class);
  
  private final DAG dag;
  private final EventHandler handler;
  private Vertex currentPartitioner = null;
  private Vertex currentShuffler = null;
  private int currentShufflerDepth = 0;
  
  public DAGSchedulerMRR(DAG dag, EventHandler dispatcher) {
    this.dag = dag;
    this.handler = dispatcher;
  }
  
  @Override
  public void vertexCompleted(Vertex vertex) {
    if(currentPartitioner!= null) {
      if(vertex != currentPartitioner) {
        String message = vertex.getVertexId() + " finished. Expecting "
            + currentPartitioner + " to finish.";
        LOG.fatal(message);
        throw new TezException(message);
      }
      LOG.info("Current partitioner " + currentPartitioner.getVertexId()
          + " is completed. " 
          + (currentShuffler!=null?currentShuffler.getVertexId():"null")
          + " is new partitioner");
      currentPartitioner = currentShuffler;
      currentShuffler = null;
    } else {
      if(vertex != currentShuffler) {
        String message = vertex.getVertexId() + " finished. Expecting "
            + currentShuffler.getVertexId() + " to finish";
        LOG.fatal(message);
        throw new TezException(message);
      }      
    }
  }
  
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getID().getTaskID().getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();
    if(vertexDistanceFromRoot == 0) {
      currentPartitioner = vertex;
      LOG.info(vertex.getVertexId() + " is first partitioner");
    }
    if(vertexDistanceFromRoot > currentShufflerDepth) {
      if(currentShuffler == null) {
        currentShuffler = vertex;
        currentShufflerDepth = vertexDistanceFromRoot;
        LOG.info(currentShuffler.getVertexId() + " is new shuffler at depth " + 
                 currentShufflerDepth);
      } else {
        if(currentShufflerDepth+1 == vertexDistanceFromRoot && 
           currentPartitioner == null
           ) {
          currentPartitioner = currentShuffler;
          currentShuffler = vertex;
          currentShufflerDepth = vertexDistanceFromRoot;
          LOG.info("Shuffler " + currentPartitioner.getVertexId() + 
                   " becomes partitioner as new shuffler " + 
                   currentShuffler.getVertexId() + " has started at depth " + 
                   currentShufflerDepth);          
        } else {
          String message = vertex.getVertexId()
              + " has scheduled tasks at depth " + vertexDistanceFromRoot
              + " greater than depth " + currentShufflerDepth
              + " of current shuffler " + currentShuffler.getVertexId()
              + ". Unexpected.";
          LOG.fatal(message);
          throw new TezException(message);
        }
      }
    }

    // natural priority. Handles failures and retries.
    int priority = (vertexDistanceFromRoot + 1) * 3;
    
    if(currentShuffler == vertex) {
      if(currentPartitioner != null) {
        // special priority for current reducers while current partitioners are 
        // still running. Schedule at priority one higher than natural priority 
        // of previous vertex.
        priority -= 4;  // this == (partitionerDepth+1)*3 - 1     
      }
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

    TaskAttemptEventSchedule attemptEvent = 
        new TaskAttemptEventSchedule(attempt.getID(), 
                                      BuilderUtils.newPriority(priority));
    sendEvent(attemptEvent);
  }
  
  @SuppressWarnings("unchecked")
  void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }
}

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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezUncheckedException;
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
    }
  }
  
  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();
    boolean reOrderPriority = false;
    
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
      // current shuffler vertex. assign special priority
      reOrderPriority = true;
    }
    
    // sanity check
    if(currentPartitioner != vertex && currentShuffler != vertex) {
      String message = vertex.getVertexId() + " is neither the "
          + " current partitioner: " + currentPartitioner.getVertexId()
          + " nor the current shuffler: " + currentShuffler.getVertexId();
      LOG.fatal(message);
      throw new TezUncheckedException(message);      
    }    

    // natural priority. Handles failures and retries.
    int priority = (vertexDistanceFromRoot + 1) * 3;
    
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

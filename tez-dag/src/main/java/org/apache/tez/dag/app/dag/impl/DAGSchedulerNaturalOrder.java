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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;

@SuppressWarnings("rawtypes")
public class DAGSchedulerNaturalOrder implements DAGScheduler {
  
  private static final Logger LOG = 
                            LoggerFactory.getLogger(DAGSchedulerNaturalOrder.class);

  private final DAG dag;
  private final EventHandler handler;
  
  public DAGSchedulerNaturalOrder(DAG dag, EventHandler dispatcher) {
    this.dag = dag;
    this.handler = dispatcher;
  }
  
  @Override
  public void vertexCompleted(Vertex vertex) {
  }

  @Override
  public void scheduleTask(DAGEventSchedulerUpdate event) {
    TaskAttempt attempt = event.getAttempt();
    Vertex vertex = dag.getVertex(attempt.getVertexID());
    int vertexDistanceFromRoot = vertex.getDistanceFromRoot();

    // natural priority. Handles failures and retries.
    int priorityLowLimit = (vertexDistanceFromRoot + 1) * 3;
    int priorityHighLimit = priorityLowLimit - 2;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scheduling " + attempt.getID() + " between priorityLow: " + priorityLowLimit
          + " and priorityHigh: " + priorityHighLimit);
    }
    
    TaskAttemptEventSchedule attemptEvent = new TaskAttemptEventSchedule(
        attempt.getID(), priorityLowLimit, priorityHighLimit);
                                      
    sendEvent(attemptEvent);
  }
  
  @Override
  public void taskScheduled(DAGEventSchedulerUpdateTAAssigned event) {
  }

  @Override
  public void taskSucceeded(DAGEventSchedulerUpdate event) {
  }
  
  @SuppressWarnings("unchecked")
  void sendEvent(TaskAttemptEventSchedule event) {
    handler.handle(event);
  }

}

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

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestDAGScheduler {

  class MockEventHandler implements EventHandler<TaskAttemptEventSchedule> {
    TaskAttemptEventSchedule event;
    @Override
    public void handle(TaskAttemptEventSchedule event) {
      this.event = event;
    }
    
  }
  
  MockEventHandler mockEventHandler = new MockEventHandler();
  
  @Test(timeout=5000)
  public void testDAGSchedulerNaturalOrder() {
    DAG mockDag = mock(DAG.class);
    Vertex mockVertex = mock(Vertex.class);
    TaskAttempt mockAttempt = mock(TaskAttempt.class);
    when(mockDag.getVertex((TezVertexID) any())).thenReturn(mockVertex);
    when(mockVertex.getDistanceFromRoot()).thenReturn(0).thenReturn(1)
        .thenReturn(2);
    
    DAGEventSchedulerUpdate event = new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt);    
    
    DAGScheduler scheduler = new DAGSchedulerNaturalOrder(mockDag,
        mockEventHandler);
    scheduler.scheduleTask(event);
    Assert.assertEquals(1, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(3, mockEventHandler.event.getPriorityLowLimit());
    scheduler.scheduleTask(event);
    Assert.assertEquals(4, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(6, mockEventHandler.event.getPriorityLowLimit());
    scheduler.scheduleTask(event);
    Assert.assertEquals(7, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(9, mockEventHandler.event.getPriorityLowLimit());
  }
  
}

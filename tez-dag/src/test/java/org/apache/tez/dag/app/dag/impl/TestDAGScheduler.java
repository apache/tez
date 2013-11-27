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
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Ignore;
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
  
  @Test(timeout=10000)
  public void testDAGSchedulerNaturalOrder() {
    DAG mockDag = mock(DAG.class);
    Vertex mockVertex = mock(Vertex.class);
    TaskAttempt mockAttempt = mock(TaskAttempt.class);
    when(mockDag.getVertex((TezVertexID) any())).thenReturn(mockVertex);
    when(mockVertex.getDistanceFromRoot()).thenReturn(0).thenReturn(1)
        .thenReturn(2);
    when(mockAttempt.getIsRescheduled()).thenReturn(false);
    
    DAGEventSchedulerUpdate event = new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt);    
    
    DAGScheduler scheduler = new DAGSchedulerNaturalOrder(mockDag,
        mockEventHandler);
    scheduler.scheduleTask(event);
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 2);
    scheduler.scheduleTask(event);
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 4);
    scheduler.scheduleTask(event);
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    
    when(mockAttempt.getIsRescheduled()).thenReturn(true);
    scheduler.scheduleTask(event);
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 5);
  }
  
  @Ignore
  @Test(timeout=10000)
  public void testDAGSchedulerMRR() {
    DAG mockDag = mock(DAG.class);
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    
    TaskSchedulerEventHandler mockTaskScheduler = 
        mock(TaskSchedulerEventHandler.class);

    Vertex mockVertex1 = mock(Vertex.class);
    TezVertexID mockVertexId1 = TezVertexID.getInstance(dagId, 1);
    when(mockVertex1.getVertexId()).thenReturn(mockVertexId1);
    when(mockVertex1.getDistanceFromRoot()).thenReturn(0);
    TaskAttempt mockAttempt1 = mock(TaskAttempt.class);
    when(mockAttempt1.getVertexID()).thenReturn(mockVertexId1);
    when(mockAttempt1.getIsRescheduled()).thenReturn(false);
    when(mockDag.getVertex(mockVertexId1)).thenReturn(mockVertex1);
    
    Vertex mockVertex2 = mock(Vertex.class);
    TezVertexID mockVertexId2 = TezVertexID.getInstance(dagId, 2);
    when(mockVertex2.getVertexId()).thenReturn(mockVertexId2);
    when(mockVertex2.getDistanceFromRoot()).thenReturn(1);
    TaskAttempt mockAttempt2 = mock(TaskAttempt.class);
    when(mockAttempt2.getVertexID()).thenReturn(mockVertexId2);
    when(mockAttempt2.getIsRescheduled()).thenReturn(false);
    when(mockDag.getVertex(mockVertexId2)).thenReturn(mockVertex2);
    TaskAttempt mockAttempt2f = mock(TaskAttempt.class);
    when(mockAttempt2f.getVertexID()).thenReturn(mockVertexId2);
    when(mockAttempt2f.getIsRescheduled()).thenReturn(true);
    
    Vertex mockVertex3 = mock(Vertex.class);
    TezVertexID mockVertexId3 = TezVertexID.getInstance(dagId, 3);
    when(mockVertex3.getVertexId()).thenReturn(mockVertexId3);
    when(mockVertex3.getDistanceFromRoot()).thenReturn(2);
    TaskAttempt mockAttempt3 = mock(TaskAttempt.class);
    when(mockAttempt3.getVertexID()).thenReturn(mockVertexId3);
    when(mockAttempt3.getIsRescheduled()).thenReturn(false);
    when(mockDag.getVertex(mockVertexId3)).thenReturn(mockVertex3);

    DAGEventSchedulerUpdate mockEvent1 = mock(DAGEventSchedulerUpdate.class);
    when(mockEvent1.getAttempt()).thenReturn(mockAttempt1);
    DAGEventSchedulerUpdate mockEvent2 = mock(DAGEventSchedulerUpdate.class);
    when(mockEvent2.getAttempt()).thenReturn(mockAttempt2);
    DAGEventSchedulerUpdate mockEvent2f = mock(DAGEventSchedulerUpdate.class);
    when(mockEvent2f.getAttempt()).thenReturn(mockAttempt2f);
    DAGEventSchedulerUpdate mockEvent3 = mock(DAGEventSchedulerUpdate.class);
    when(mockEvent3.getAttempt()).thenReturn(mockAttempt3);
    DAGScheduler scheduler = new DAGSchedulerMRR(mockDag, mockEventHandler,
        mockTaskScheduler, 0.5f);
    
    // M starts. M completes. R1 starts. R1 completes. R2 starts. R2 completes
    scheduler.scheduleTask(mockEvent1); // M starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.scheduleTask(mockEvent1); // M runs another
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.vertexCompleted(mockVertex1); // M completes
    scheduler.scheduleTask(mockEvent2); // R1 starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.scheduleTask(mockEvent2); // R1 runs another
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.scheduleTask(mockEvent2f); // R1 runs retry. Retry priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 4);
    scheduler.vertexCompleted(mockVertex2); // R1 completes
    scheduler.scheduleTask(mockEvent3); // R2 starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 9);
    scheduler.scheduleTask(mockEvent3); // R2 runs another
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 9);
    scheduler.vertexCompleted(mockVertex3); // R2 completes  
    
    // M starts. R1 starts. M completes. R2 starts. R1 completes. R2 completes
    scheduler.scheduleTask(mockEvent1); // M starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.scheduleTask(mockEvent2); // R1 starts. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 2);
    scheduler.scheduleTask(mockEvent1); // M runs another
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.scheduleTask(mockEvent2); // R1 runs another. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 2);
    scheduler.scheduleTask(mockEvent2f); // R1 runs retry. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 2);
    scheduler.vertexCompleted(mockVertex1); // M completes
    scheduler.scheduleTask(mockEvent3); // R2 starts. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 5);
    scheduler.scheduleTask(mockEvent2); // R1 runs another. Normal priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.scheduleTask(mockEvent2f); // R1 runs retry. Retry priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 4);
    scheduler.scheduleTask(mockEvent3); // R2 runs another. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 5);
    scheduler.vertexCompleted(mockVertex2); // R1 completes
    scheduler.vertexCompleted(mockVertex3); // R2 completes      
    
    // M starts. M completes. R1 starts. R2 starts. R1 completes. R2 completes
    scheduler.scheduleTask(mockEvent1); // M starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.vertexCompleted(mockVertex1); // M completes
    scheduler.scheduleTask(mockEvent2); // R1 starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.scheduleTask(mockEvent3); // R2 starts. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 5);
    scheduler.scheduleTask(mockEvent2); // R1 runs another
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.vertexCompleted(mockVertex2); // R1 completes
    scheduler.vertexCompleted(mockVertex3); // R2 completes
    
    // M starts. R1 starts. M completes. R1 completes. R2 starts. R2 completes
    scheduler.scheduleTask(mockEvent1); // M starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 3);
    scheduler.scheduleTask(mockEvent2); // R1 starts. Reordered priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 2);
    scheduler.vertexCompleted(mockVertex1); // M completes
    scheduler.scheduleTask(mockEvent2); // R1 starts. Normal priority
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 6);
    scheduler.vertexCompleted(mockVertex2); // R1 completes
    scheduler.scheduleTask(mockEvent3); // R2 starts
    Assert.assertTrue(mockEventHandler.event.getPriority().getPriority() == 9);
    scheduler.vertexCompleted(mockVertex3); // R2 completes  
  }
}

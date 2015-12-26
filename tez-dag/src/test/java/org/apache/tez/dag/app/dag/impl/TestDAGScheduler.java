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
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.*;

import java.util.List;

public class TestDAGScheduler {

  class MockEventHandler implements EventHandler<TaskAttemptEventSchedule> {
    TaskAttemptEventSchedule event;
    List<TaskAttemptEventSchedule> events = Lists.newLinkedList();
    @Override
    public void handle(TaskAttemptEventSchedule event) {
      this.event = event;
      this.events.add(event);
    }
  }
  
  
  @Test(timeout=5000)
  public void testDAGSchedulerNaturalOrder() {
    MockEventHandler mockEventHandler = new MockEventHandler();
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
    scheduler.scheduleTaskEx(event);
    Assert.assertEquals(1, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(3, mockEventHandler.event.getPriorityLowLimit());
    scheduler.scheduleTaskEx(event);
    Assert.assertEquals(4, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(6, mockEventHandler.event.getPriorityLowLimit());
    scheduler.scheduleTaskEx(event);
    Assert.assertEquals(7, mockEventHandler.event.getPriorityHighLimit());
    Assert.assertEquals(9, mockEventHandler.event.getPriorityLowLimit());
  }
  
  @Test(timeout=5000)
  public void testConcurrencyLimit() {
    MockEventHandler mockEventHandler = new MockEventHandler();
    DAG mockDag = mock(DAG.class);
    TezVertexID vId0 = TezVertexID.fromString("vertex_1436907267600_195589_1_00");
    TezVertexID vId1 = TezVertexID.fromString("vertex_1436907267600_195589_1_01");
    TezTaskID tId0 = TezTaskID.getInstance(vId0, 0);
    TezTaskID tId1 = TezTaskID.getInstance(vId1, 0);
    
    TaskAttempt mockAttempt;

    Vertex mockVertex = mock(Vertex.class);
    when(mockDag.getVertex((TezVertexID) any())).thenReturn(mockVertex);
    when(mockVertex.getDistanceFromRoot()).thenReturn(0);
    
    DAGScheduler scheduler = new DAGSchedulerNaturalOrder(mockDag,
        mockEventHandler);
    scheduler.addVertexConcurrencyLimit(vId0, 0); // not effective
    
    // schedule beyond limit and it gets scheduled
    mockAttempt = mock(TaskAttempt.class);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId0, 0));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(1, mockEventHandler.events.size());
    mockAttempt = mock(TaskAttempt.class);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId0, 1));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(2, mockEventHandler.events.size());
    mockAttempt = mock(TaskAttempt.class);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId0, 2));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(3, mockEventHandler.events.size());
    
    mockEventHandler.events.clear();
    List<TaskAttempt> mockAttempts = Lists.newArrayList();
    int completed = 0;
    int requested = 0;
    int scheduled = 0;
    scheduler.addVertexConcurrencyLimit(vId1, 2); // effective    
    
    // schedule beyond limit and it gets buffered
    mockAttempt = mock(TaskAttempt.class);
    mockAttempts.add(mockAttempt);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId1, requested++));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(scheduled+1, mockEventHandler.events.size()); // scheduled
    Assert.assertEquals(mockAttempts.get(scheduled).getID(),
        mockEventHandler.events.get(scheduled).getTaskAttemptID()); // matches order
    scheduled++;
    
    mockAttempt = mock(TaskAttempt.class);
    mockAttempts.add(mockAttempt);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId1, requested++));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(scheduled+1, mockEventHandler.events.size()); // scheduled
    Assert.assertEquals(mockAttempts.get(scheduled).getID(),
        mockEventHandler.events.get(scheduled).getTaskAttemptID()); // matches order
    scheduled++;
    
    mockAttempt = mock(TaskAttempt.class);
    mockAttempts.add(mockAttempt);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId1, requested++));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(scheduled, mockEventHandler.events.size()); // buffered

    mockAttempt = mock(TaskAttempt.class);
    mockAttempts.add(mockAttempt);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId1, requested++));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(scheduled, mockEventHandler.events.size()); // buffered

    scheduler.taskCompleted(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_COMPLETED, mockAttempts.get(completed++)));
    Assert.assertEquals(scheduled+1, mockEventHandler.events.size()); // scheduled
    Assert.assertEquals(mockAttempts.get(scheduled).getID(),
        mockEventHandler.events.get(scheduled).getTaskAttemptID()); // matches order
    scheduled++;

    scheduler.taskCompleted(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_COMPLETED, mockAttempts.get(completed++)));
    Assert.assertEquals(scheduled+1, mockEventHandler.events.size()); // scheduled
    Assert.assertEquals(mockAttempts.get(scheduled).getID(),
        mockEventHandler.events.get(scheduled).getTaskAttemptID()); // matches order
    scheduled++;

    scheduler.taskCompleted(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_COMPLETED, mockAttempts.get(completed++)));
    Assert.assertEquals(scheduled, mockEventHandler.events.size()); // no extra scheduling

    mockAttempt = mock(TaskAttempt.class);
    mockAttempts.add(mockAttempt);
    when(mockAttempt.getID()).thenReturn(TezTaskAttemptID.getInstance(tId1, requested++));
    scheduler.scheduleTask(new DAGEventSchedulerUpdate(
        DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt));
    Assert.assertEquals(scheduled+1, mockEventHandler.events.size()); // scheduled
    Assert.assertEquals(mockAttempts.get(scheduled).getID(),
        mockEventHandler.events.get(scheduled).getTaskAttemptID()); // matches order
    scheduled++;

  }
  
  
  
}

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl.DataEventDependencyInfo;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Lists;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTaskAttemptRecovery {

  private TaskAttemptImpl ta;
  private EventHandler mockEventHandler;
  private long creationTime = System.currentTimeMillis();
  private long allocationTime = creationTime + 5000;
  private long startTime = allocationTime + 5000;
  private long finishTime = startTime + 5000;

  private TezTaskAttemptID taId;
  private String vertexName = "v1";

  private AppContext mockAppContext;
  private MockHistoryEventHandler mockHistoryEventHandler;
  private Task mockTask;
  private Vertex mockVertex;

  public static class MockHistoryEventHandler extends HistoryEventHandler {

    private List<DAGHistoryEvent> events;

    public MockHistoryEventHandler(AppContext context) {
      super(context);
      events = new ArrayList<DAGHistoryEvent>();
    }

    @Override
    public void handle(DAGHistoryEvent event) {
      events.add(event);
    }

    @Override
    public void handleCriticalEvent(DAGHistoryEvent event) throws IOException {
      events.add(event);
    }

    void verfiyTaskAttemptFinishedEvent(TezTaskAttemptID taId, TaskAttemptState finalState, int expectedTimes) {
      int actualTimes = 0;
      for (DAGHistoryEvent event : events) {
        if (event.getHistoryEvent().getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
          TaskAttemptFinishedEvent tfEvent = (TaskAttemptFinishedEvent)event.getHistoryEvent();
          if (tfEvent.getTaskAttemptID().equals(taId) &&
              tfEvent.getState().equals(finalState)) {
            actualTimes ++;
          }
        }
      }
      assertEquals(expectedTimes, actualTimes);
    }

    void verifyTaskFinishedEvent(TezTaskID taskId, TaskState finalState, int expectedTimes) {
      int actualTimes = 0;
      for (DAGHistoryEvent event : events) {
        if (event.getHistoryEvent().getEventType() == HistoryEventType.TASK_FINISHED) {
          TaskFinishedEvent tfEvent = (TaskFinishedEvent)event.getHistoryEvent();
          if (tfEvent.getTaskID().equals(taskId) && tfEvent.getState().equals(finalState)) {
            actualTimes ++;
          }
        }
      }
      assertEquals(expectedTimes, actualTimes);
    }
  }

  @Before
  public void setUp() {
    mockTask = mock(Task.class);
    mockVertex = mock(Vertex.class);
    when(mockTask.getVertex()).thenReturn(mockVertex);
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    when(mockAppContext.getCurrentDAG().getVertex(any(TezVertexID.class))
      .getTask(any(TezTaskID.class)))
      .thenReturn(mockTask);
    mockHistoryEventHandler = new MockHistoryEventHandler(mockAppContext);
    when(mockAppContext.getHistoryHandler()).thenReturn(mockHistoryEventHandler);
    mockEventHandler = mock(EventHandler.class);
    TezTaskID taskId =
        TezTaskID.fromString("task_1407371892933_0001_1_00_000000");
    ta =
        new TaskAttemptImpl(taskId, 0, mockEventHandler,
            mock(TaskAttemptListener.class), new Configuration(),
            new SystemClock(), mock(TaskHeartbeatHandler.class),
            mockAppContext, false, Resource.newInstance(1, 1),
            mock(ContainerContext.class), false, mockTask);
    taId = ta.getID();
  }

  private void restoreFromTAStartEvent() {
    TaskAttemptState recoveredState =
        ta.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName,
            startTime, mock(ContainerId.class), mock(NodeId.class), "", "", ""));
    assertEquals(startTime, ta.getLaunchTime());
    assertEquals(TaskAttemptState.RUNNING, recoveredState);
  }

  private void restoreFromTAFinishedEvent(TaskAttemptState state) {
    String diag = "test_diag";
    TezCounters counters = mock(TezCounters.class);
    TezTaskAttemptID causalId = TezTaskAttemptID.getInstance(taId.getTaskID(), taId.getId()+1);
    
    TaskAttemptTerminationCause errorEnum = null;
    if (state != TaskAttemptState.SUCCEEDED) {
      errorEnum = TaskAttemptTerminationCause.APPLICATION_ERROR;
    }

    long lastDataEventTime = 1024;
    TezTaskAttemptID lastDataEventTA = mock(TezTaskAttemptID.class);
    List<DataEventDependencyInfo> events = Lists.newLinkedList();
    events.add(new DataEventDependencyInfo(lastDataEventTime, lastDataEventTA));
    events.add(new DataEventDependencyInfo(lastDataEventTime, lastDataEventTA));
    TaskAttemptState recoveredState =
        ta.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            startTime, finishTime, state, errorEnum, diag, counters, events, creationTime,
            causalId, allocationTime));
    assertEquals(causalId, ta.getCreationCausalAttempt());
    assertEquals(creationTime, ta.getCreationTime());
    assertEquals(allocationTime, ta.getAllocationTime());
    assertEquals(startTime, ta.getLaunchTime());
    assertEquals(finishTime, ta.getFinishTime());
    assertEquals(counters, ta.reportedStatus.counters);
    assertEquals(1.0f, ta.reportedStatus.progress, 1e-6);
    assertEquals(state, ta.reportedStatus.state);
    assertEquals(1, ta.getDiagnostics().size());
    assertEquals(diag, ta.getDiagnostics().get(0));
    assertEquals(state, recoveredState);
    assertEquals(events.size(), ta.lastDataEvents.size());
    assertEquals(lastDataEventTime, ta.lastDataEvents.get(0).getTimestamp());
    assertEquals(lastDataEventTA, ta.lastDataEvents.get(0).getTaskAttemptId());
    if (state != TaskAttemptState.SUCCEEDED) {
      assertEquals(errorEnum, ta.getTerminationCause());
    } else {
      assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, ta.getTerminationCause());
    }
  }

  private void verifyEvents(List<Event> events, Class<? extends Event> eventClass,
      int expectedTimes) {
    int actualTimes = 0;
    for (Event event : events) {
      if (eventClass.isInstance(event)) {
        actualTimes ++;
      }
    }
    assertEquals(expectedTimes, actualTimes);
  }

  /**
   * No any event to restore -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testTARecovery_NEW() {
    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());

    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertEquals(2, events.size());
    verifyEvents(events, TaskEventTAUpdate.class, 1);
    // one for task killed
    verifyEvents(events, DAGEventCounterUpdate.class, 1);
  }

  /**
   * restoreFromTAStartEvent -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testTARecovery_START() {
    restoreFromTAStartEvent();

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());

    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(3)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertEquals(3, events.size());
    verifyEvents(events, TaskEventTAUpdate.class, 1);
    // one for task launch, one for task killed
    verifyEvents(events, DAGEventCounterUpdate.class, 2);

    mockHistoryEventHandler.verfiyTaskAttemptFinishedEvent(taId, TaskAttemptState.KILLED, 1);
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (SUCCEED)
   * -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testTARecovery_SUCCEED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.SUCCEEDED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.SUCCEEDED, ta.getInternalState());

    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertEquals(2, events.size());
    // one for task launch, one for task succeeded
    verifyEvents(events, DAGEventCounterUpdate.class, 2);
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (KILLED)
   * -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testTARecovery_KIILED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.KILLED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());

    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertEquals(2, events.size());
    // one for task launch, one for task killed
    verifyEvents(events, DAGEventCounterUpdate.class, 2);
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (FAILED)
   * -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testTARecovery_FAILED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.FAILED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.FAILED, ta.getInternalState());

    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertEquals(2, events.size());
    // one for task launch, one for task killed
    verifyEvents(events, DAGEventCounterUpdate.class, 2);
  }

  /**
   * restoreFromTAFinishedEvent ( killed before started)
   */
  @Test(timeout = 5000)
  public void testRecover_FINISH_BUT_NO_START() {
    TaskAttemptState recoveredState =
        ta.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            startTime, finishTime, TaskAttemptState.KILLED,
            TaskAttemptTerminationCause.APPLICATION_ERROR, "", new TezCounters(), null, 0, null, 0));
    assertEquals(TaskAttemptState.KILLED, recoveredState);
  }
}

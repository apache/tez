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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTaskAttemptRecovery {

  private TaskAttemptImpl ta;
  private EventHandler mockEventHandler;
  private long startTime = System.currentTimeMillis();
  private long finishTime = startTime + 5000;

  private TezTaskAttemptID taId = mock(TezTaskAttemptID.class);
  private String vertexName = "v1";

  @Before
  public void setUp() {
    mockEventHandler = mock(EventHandler.class);
    TezTaskID taskId =
        TezTaskID.fromString("task_1407371892933_0001_1_00_000000");
    ta =
        new TaskAttemptImpl(taskId, 0, mockEventHandler,
            mock(TaskAttemptListener.class), new Configuration(),
            new SystemClock(), mock(TaskHeartbeatHandler.class),
            mock(AppContext.class), false, Resource.newInstance(1, 1),
            mock(ContainerContext.class), false);
  }

  private void restoreFromTAStartEvent() {
    TaskAttemptState recoveredState =
        ta.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName,
            startTime, mock(ContainerId.class), mock(NodeId.class), "", ""));
    assertEquals(startTime, ta.getLaunchTime());
    assertEquals(TaskAttemptState.RUNNING, recoveredState);
  }

  private void restoreFromTAFinishedEvent(TaskAttemptState state) {
    String diag = "test_diag";
    TezCounters counters = mock(TezCounters.class);

    TaskAttemptState recoveredState =
        ta.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            startTime, finishTime, state, diag, counters));
    assertEquals(startTime, ta.getLaunchTime());
    assertEquals(finishTime, ta.getFinishTime());
    assertEquals(counters, ta.reportedStatus.counters);
    assertEquals(1.0f, ta.reportedStatus.progress, 1e-6);
    assertEquals(state, ta.reportedStatus.state);
    assertEquals(1, ta.getDiagnostics().size());
    assertEquals(diag, ta.getDiagnostics().get(0));
    assertEquals(state, recoveredState);
  }

  /**
   * No any event to restore -> RecoverTransition
   */
  @Test
  public void testTARecovery_NEW() {
    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());
    verify(mockEventHandler, times(1)).handle(any(TaskEventTAUpdate.class));
  }

  /**
   * restoreFromTAStartEvent -> RecoverTransition
   */
  @Test
  public void testTARecovery_START() {
    restoreFromTAStartEvent();

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());
    verify(mockEventHandler, times(1)).handle(any(TaskEventTAUpdate.class));
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (SUCCEED)
   * -> RecoverTransition
   */
  @Test
  public void testTARecovery_SUCCEED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.SUCCEEDED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.SUCCEEDED, ta.getInternalState());
    verify(mockEventHandler, never()).handle(any(TaskEventTAUpdate.class));
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (KILLED)
   * -> RecoverTransition
   */
  @Test
  public void testTARecovery_KIILED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.KILLED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.KILLED, ta.getInternalState());
    verify(mockEventHandler, never()).handle(any(TaskEventTAUpdate.class));
  }

  /**
   * restoreFromTAStartEvent -> restoreFromTAFinished (FAILED)
   * -> RecoverTransition
   */
  @Test
  public void testTARecovery_FAILED() {
    restoreFromTAStartEvent();
    restoreFromTAFinishedEvent(TaskAttemptState.FAILED);

    ta.handle(new TaskAttemptEvent(taId, TaskAttemptEventType.TA_RECOVER));
    assertEquals(TaskAttemptStateInternal.FAILED, ta.getInternalState());
    verify(mockEventHandler, never()).handle(any(TaskEventTAUpdate.class));
  }

  /**
   * restoreFromTAFinishedEvent ( no TAStartEvent before TAFinishedEvent )
   */
  @Test
  public void testRecover_FINISH_BUT_NO_START() {
    try {
      restoreFromTAFinishedEvent(TaskAttemptState.SUCCEEDED);
      fail("Should fail due to no TaskAttemptStartEvent before TaskAttemptFinishedEvent");
    } catch (Throwable e) {
      assertEquals("Finished Event seen but"
          + " no Started Event was encountered earlier", e.getMessage());
    }
  }
}

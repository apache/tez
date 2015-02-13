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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.VertexStatus.State;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventRecoverTask;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.TestTaskAttemptRecovery.MockHistoryEventHandler;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTaskRecovery {

  private TaskImpl task;
  private DrainDispatcher dispatcher;

  private int taskAttemptCounter = 0;

  private Configuration conf = new Configuration();
  private AppContext mockAppContext;
  private MockHistoryEventHandler  mockHistoryEventHandler;
  private ApplicationId appId = ApplicationId.newInstance(
      System.currentTimeMillis(), 1);
  private TezDAGID dagId = TezDAGID.getInstance(appId, 1);
  private TezVertexID vertexId = TezVertexID.getInstance(dagId, 1);
  private Vertex vertex;
  private String vertexName = "v1";
  private long taskScheduledTime = 100L;
  private long taskStartTime = taskScheduledTime + 100L;
  private long taskFinishTime = taskStartTime + 100L;
  private TaskAttemptEventHandler taEventHandler =
      new TaskAttemptEventHandler();

  private class TaskEventHandler implements EventHandler<TaskEvent> {
    @Override
    public void handle(TaskEvent event) {
      task.handle(event);
    }
  }

  private class TaskAttemptEventHandler implements
      EventHandler<TaskAttemptEvent> {

    private List<TaskAttemptEvent> events = Lists.newArrayList();

    @Override
    public void handle(TaskAttemptEvent event) {
      events.add(event);
      ((TaskAttemptImpl) task.getAttempt(event.getTaskAttemptID()))
          .handle(event);
    }

    public List<TaskAttemptEvent> getEvents() {
      return events;
    }
  }

  private class TestOutputCommitter extends OutputCommitter {

    boolean recoverySupported = false;
    boolean throwExceptionWhenRecovery = false;

    public TestOutputCommitter(OutputCommitterContext committerContext,
        boolean recoverySupported, boolean throwExceptionWhenRecovery) {
      super(committerContext);
      this.recoverySupported = recoverySupported;
      this.throwExceptionWhenRecovery = throwExceptionWhenRecovery;
    }

    @Override
    public void recoverTask(int taskIndex, int previousDAGAttempt)
        throws Exception {
      if (throwExceptionWhenRecovery) {
        throw new Exception("fail recovery Task");
      }
    }

    @Override
    public boolean isTaskRecoverySupported() {
      return recoverySupported;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void setupOutput() throws Exception {

    }

    @Override
    public void commitOutput() throws Exception {

    }

    @Override
    public void abortOutput(State finalState) throws Exception {

    }

  }

  @Before
  public void setUp() {
    dispatcher = new DrainDispatcher();
    dispatcher.register(DAGEventType.class, mock(EventHandler.class));
    dispatcher.register(VertexEventType.class, mock(EventHandler.class));
    dispatcher.register(TaskEventType.class, new TaskEventHandler());
    dispatcher.register(TaskAttemptEventType.class, taEventHandler);
    dispatcher.init(new Configuration());
    dispatcher.start();

    vertex = mock(Vertex.class, RETURNS_DEEP_STUBS);
    when(vertex.getProcessorDescriptor().getClassName()).thenReturn("");

    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    when(mockAppContext.getCurrentDAG().getVertex(any(TezVertexID.class)))
        .thenReturn(vertex);
    mockHistoryEventHandler = new MockHistoryEventHandler(mockAppContext);
    when(mockAppContext.getHistoryHandler()).thenReturn(mockHistoryEventHandler);
    task =
        new TaskImpl(vertexId, 0, dispatcher.getEventHandler(),
            new Configuration(), mock(TaskAttemptListener.class),
            new SystemClock(), mock(TaskHeartbeatHandler.class),
            mockAppContext, false, Resource.newInstance(1, 1),
            mock(ContainerContext.class), mock(StateChangeNotifier.class));

    Map<String, OutputCommitter> committers =
        new HashMap<String, OutputCommitter>();
    committers.put("out1", new TestOutputCommitter(
        mock(OutputCommitterContext.class), true, false));
    when(task.getVertex().getOutputCommitters()).thenReturn(committers);
  }

  private void restoreFromTaskStartEvent() {
    TaskState recoveredState =
        task.restoreFromEvent(new TaskStartedEvent(task.getTaskId(),
            vertexName, taskScheduledTime, taskStartTime));
    assertEquals(TaskState.SCHEDULED, recoveredState);
    assertEquals(0, task.getFinishedAttemptsCount());
    assertEquals(taskScheduledTime, task.scheduledTime);
    assertEquals(0, task.getAttempts().size());
  }

  private void restoreFromFirstTaskAttemptStartEvent(TezTaskAttemptID taId) {
    long taStartTime = taskStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName,
            taStartTime, mock(ContainerId.class), mock(NodeId.class), "", "", ""));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(0, task.getFinishedAttemptsCount());
    assertEquals(taskScheduledTime, task.scheduledTime);
    assertEquals(1, task.getAttempts().size());
    assertEquals(TaskAttemptStateInternal.NEW,
        ((TaskAttemptImpl) task.getAttempt(taId)).getInternalState());
    assertEquals(1, task.getUncompletedAttemptsCount());
  }

  /**
   * New -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_New() {
    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.NEW, task.getInternalState());
  }

  /**
   * -> restoreFromTaskFinishEvent ( no TaskStartEvent )
   */
  @Test(timeout = 5000)
  public void testRecovery_NoStartEvent() {
    try {
      task.restoreFromEvent(new TaskFinishedEvent(task.getTaskId(), vertexName,
          taskStartTime, taskFinishTime, null, TaskState.SUCCEEDED, "",
          new TezCounters()));
      fail("Should fail due to no TaskStartEvent before TaskFinishEvent");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains(
          "Finished Event seen but"
              + " no Started Event was encountered earlier"));
    }
  }

  /**
   * restoreFromTaskStartedEvent -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Started() {
    restoreFromTaskStartEvent();

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(1, task.getAttempts().size());
    assertEquals(0, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * RecoverTranstion
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    // wait for the second task attempt is scheduled
    dispatcher.await();
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // taskAttempt_1 is recovered to KILLED, and new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_SUCCEEDED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.SUCCEEDED, task.getInternalState());
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);
    mockHistoryEventHandler.verifyTaskFinishedEvent(task.getTaskId(), TaskState.SUCCEEDED, 1);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (FAILED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_FAILED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.FAILED, null,
            "", new TezCounters()));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(1, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(1, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (KILLED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_KILLED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.KILLED, null,
            "", new TezCounters()));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) ->
   * restoreFromTaskFinishedEvent -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_SUCCEEDED_Finished() {

    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    recoveredState =
        task.restoreFromEvent(new TaskFinishedEvent(task.getTaskId(),
            vertexName, taskStartTime, taskFinishTime, taId,
            TaskState.SUCCEEDED, "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(taId, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.SUCCEEDED, task.getInternalState());
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);
    mockHistoryEventHandler.verifyTaskFinishedEvent(task.getTaskId(), TaskState.SUCCEEDED, 1);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) ->
   * restoreFromTaskAttemptFinishedEvent (Failed due to output_failure)
   * restoreFromTaskFinishedEvent -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_SUCCEEDED_FAILED() {

    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    // it is possible for TaskAttempt transit from SUCCEEDED to FAILURE due to output failure.
    recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.FAILED, null,
            "", new TezCounters()));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(1, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(1, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) ->
   * restoreFromTaskAttemptFinishedEvent (KILLED due to node failed )
   * restoreFromTaskFinishedEvent -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_SUCCEEDED_KILLED() {

    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    // it is possible for TaskAttempt transit from SUCCEEDED to KILLED due to node failure.
    recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.KILLED, null,
            "", new TezCounters()));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Commit_Failed_Recovery_Not_Supported() {
    Map<String, OutputCommitter> committers =
        new HashMap<String, OutputCommitter>();
    committers.put("out1", new TestOutputCommitter(
        mock(OutputCommitterContext.class), false, false));
    when(task.getVertex().getOutputCommitters()).thenReturn(committers);

    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    // restoreFromTaskAttemptFinishedEvent (SUCCEEDED)
    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (SUCCEEDED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Commit_Failed_recover_fail() {
    Map<String, OutputCommitter> committers =
        new HashMap<String, OutputCommitter>();
    committers.put("out1", new TestOutputCommitter(
        mock(OutputCommitterContext.class), true, true));
    when(task.getVertex().getOutputCommitters()).thenReturn(committers);

    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);

    // restoreFromTaskAttemptFinishedEvent (SUCCEEDED)
    long taStartTime = taskStartTime + 100L;
    long taFinishTime = taStartTime + 100L;
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.SUCCEEDED, null,
            "", new TezCounters()));
    assertEquals(TaskState.SUCCEEDED, recoveredState);
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(taId, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  @Test(timeout = 5000)
  public void testRecovery_WithDesired_SUCCEEDED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);
    task.handle(new TaskEventRecoverTask(task.getTaskId(), TaskState.SUCCEEDED,
        false));
    assertEquals(TaskStateInternal.SUCCEEDED, task.getInternalState());
    // no TA_Recovery event sent
    assertEquals(0, taEventHandler.getEvents().size());
  }

  @Test(timeout = 5000)
  public void testRecovery_WithDesired_FAILED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);
    task.handle(new TaskEventRecoverTask(task.getTaskId(), TaskState.FAILED,
        false));
    assertEquals(TaskStateInternal.FAILED, task.getInternalState());
    // no TA_Recovery event sent
    assertEquals(0, taEventHandler.getEvents().size());
  }

  @Test(timeout = 5000)
  public void testRecovery_WithDesired_KILLED() {
    restoreFromTaskStartEvent();
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    restoreFromFirstTaskAttemptStartEvent(taId);
    task.handle(new TaskEventRecoverTask(task.getTaskId(), TaskState.KILLED,
        false));
    assertEquals(TaskStateInternal.KILLED, task.getInternalState());
    // no TA_Recovery event sent
    assertEquals(0, taEventHandler.getEvents().size());

  }

  /**
   * restoreFromTaskStartedEvent -> restoreFromTaskAttemptStartedEvent ->
   * restoreFromTaskAttemptFinishedEvent (KILLED) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_OneTAStarted_Killed() {
    restoreFromTaskStartEvent();

    long taStartTime = taskStartTime + 100L;
    TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName,
            taStartTime, mock(ContainerId.class), mock(NodeId.class), "", "", ""));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(TaskAttemptStateInternal.NEW,
        ((TaskAttemptImpl) task.getAttempt(taId)).getInternalState());
    assertEquals(1, task.getAttempts().size());
    assertEquals(0, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    long taFinishTime = taStartTime + 100L;
    recoveredState =
        task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName,
            taStartTime, taFinishTime, TaskAttemptState.KILLED, null,
            "", new TezCounters()));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(TaskAttemptStateInternal.NEW,
        ((TaskAttemptImpl) task.getAttempt(taId)).getInternalState());
    assertEquals(1, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(0, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    // wait for Task send TA_RECOVER to TA and TA complete the RecoverTransition
    dispatcher.await();
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    assertEquals(TaskAttemptStateInternal.KILLED,
        ((TaskAttemptImpl) task.getAttempt(taId)).getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(1, task.getFinishedAttemptsCount());
    assertEquals(0, task.failedAttempts);
    assertEquals(1, task.getUncompletedAttemptsCount());
    assertEquals(null, task.successfulAttempt);
  }

  /**
   * n = maxFailedAttempts, in the previous AM attempt, n task attempts are
   * killed. When recovering, it should continue to be in running state and
   * schedule a new task attempt.
   */
  @Test(timeout = 5000)
  public void testTaskRecovery_MultipleAttempts1() {
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    restoreFromTaskStartEvent();

    for (int i = 0; i < maxFailedAttempts; ++i) {
      TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
      task.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName, 0L,
          mock(ContainerId.class), mock(NodeId.class), "", "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName, 0,
          0, TaskAttemptState.KILLED, null, "", null));
    }
    assertEquals(maxFailedAttempts, task.getAttempts().size());
    assertEquals(0, task.failedAttempts);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    // if the previous task attempt is killed, it should not been take into
    // account when checking whether exceed the max attempts
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // schedule a new task attempt
    assertEquals(maxFailedAttempts + 1, task.getAttempts().size());
  }

  /**
   * n = maxFailedAttempts, in the previous AM attempt, n task attempts are
   * failed. When recovering, it should transit to failed because # of
   * failed_attempt is exceeded.
   */
  @Test(timeout = 5000)
  public void testTaskRecovery_MultipleAttempts2() {
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    restoreFromTaskStartEvent();

    for (int i = 0; i < maxFailedAttempts; ++i) {
      TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
      task.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName, 0L,
          mock(ContainerId.class), mock(NodeId.class), "", "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName, 0,
          0, TaskAttemptState.FAILED, null, "", null));
    }
    assertEquals(maxFailedAttempts, task.getAttempts().size());
    assertEquals(maxFailedAttempts, task.failedAttempts);

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    // it should transit to failed because of the failed task attempt in the
    // last application attempt.
    assertEquals(TaskStateInternal.FAILED, task.getInternalState());
    assertEquals(maxFailedAttempts, task.getAttempts().size());
  }

  /**
   * n = maxFailedAttempts, in the previous AM attempt, n-1 task attempts are
   * killed. And last task attempt is still in running state. When recovering,
   * the last attempt should transit to killed and task is still in running
   * state and new task attempt is scheduled.
   */
  @Test(timeout = 5000)
  public void testTaskRecovery_MultipleAttempts3() throws InterruptedException {
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    restoreFromTaskStartEvent();

    for (int i = 0; i < maxFailedAttempts - 1; ++i) {
      TezTaskAttemptID taId = getNewTaskAttemptID(task.getTaskId());
      task.restoreFromEvent(new TaskAttemptStartedEvent(taId, vertexName, 0L,
          mock(ContainerId.class), mock(NodeId.class), "", "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(taId, vertexName, 0,
          0, TaskAttemptState.FAILED, null, "", null));
    }
    assertEquals(maxFailedAttempts - 1, task.getAttempts().size());
    assertEquals(maxFailedAttempts - 1, task.failedAttempts);

    TezTaskAttemptID newTaskAttemptId = getNewTaskAttemptID(task.getTaskId());
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptStartedEvent(newTaskAttemptId,
            vertexName, 0, mock(ContainerId.class), mock(NodeId.class), "", "", ""));

    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(TaskAttemptStateInternal.NEW,
        ((TaskAttemptImpl) task.getAttempt(newTaskAttemptId))
            .getInternalState());
    assertEquals(maxFailedAttempts, task.getAttempts().size());

    task.handle(new TaskEventRecoverTask(task.getTaskId()));
    // wait until task attempt receive the Recover event from task
    dispatcher.await();

    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    assertEquals(TaskAttemptStateInternal.KILLED,
        ((TaskAttemptImpl) (task.getAttempt(newTaskAttemptId)))
            .getInternalState());
    assertEquals(maxFailedAttempts - 1, task.failedAttempts);

    // new task attempt is added
    assertEquals(maxFailedAttempts + 1, task.getAttempts().size());
  }

  private TezTaskAttemptID getNewTaskAttemptID(TezTaskID taskId) {
    return TezTaskAttemptID.getInstance(taskId, taskAttemptCounter++);
  }

}

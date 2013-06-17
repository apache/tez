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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.junit.Before;
import org.junit.Test;

public class TestTaskImpl {

  private static final Log LOG = LogFactory.getLog(TestTaskImpl.class);

  private int taskCounter = 0;
  private final int partition = 1;

  private InlineDispatcher dispatcher;

  private TezConfiguration conf;
  private TaskAttemptListener taskAttemptListener;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private Token<JobTokenIdentifier> jobToken;
  private Credentials credentials;
  private Clock clock;
  private TaskLocationHint locationHint;

  private ApplicationId appId;
  private TezDAGID dagId;
  private TezVertexID vertexId;
  private AppContext appContext;
  private Resource taskResource;
  private Map<String, LocalResource> localResources;
  private Map<String, String> environment;
  private String javaOpts;
  private boolean leafVertex;

  private MockTaskImpl mockTask;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    dispatcher = new InlineDispatcher();
    conf = new TezConfiguration();
    taskAttemptListener = mock(TaskAttemptListener.class);
    taskHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    jobToken = (Token<JobTokenIdentifier>) mock(Token.class);
    credentials = null;
    clock = new SystemClock();
    locationHint = new TaskLocationHint(new String[1], new String[1]);

    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    dagId = new TezDAGID(appId, 1);
    vertexId = new TezVertexID(dagId, 1);
    appContext = mock(AppContext.class);
    taskResource = Resource.newInstance(1024, 1);
    localResources = new HashMap<String, LocalResource>();
    environment = new HashMap<String, String>();
    javaOpts = "";
    leafVertex = false;

    mockTask = new MockTaskImpl(vertexId, partition,
        dispatcher.getEventHandler(), conf, taskAttemptListener, jobToken,
        credentials, clock, taskHeartbeatHandler, appContext,
        "org.apache.tez.mapreduce.processor.map.MapProcessor", leafVertex,
        locationHint, taskResource, localResources, environment, javaOpts);
  }

  private TezTaskID getNewTaskID() {
    TezTaskID taskID = new TezTaskID(vertexId, ++taskCounter);
    return taskID;
  }

  private void scheduleTaskAttempt(TezTaskID taskId) {
    mockTask.handle(new TaskEvent(taskId, TaskEventType.T_SCHEDULE));
    assertTaskScheduledState();
  }

  private void killTask(TezTaskID taskId) {
    mockTask.handle(new TaskEvent(taskId, TaskEventType.T_KILL));
    assertTaskKillWaitState();
  }

  private void killScheduledTaskAttempt(TezTaskAttemptID attemptId) {
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskScheduledState();
  }

  private void launchTaskAttempt(TezTaskAttemptID attemptId) {
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_LAUNCHED));
    assertTaskRunningState();
  }

  private void commitTaskAttempt(TezTaskAttemptID attemptId) {
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_COMMIT_PENDING));
    assertTaskRunningState();
  }

  private void updateAttemptProgress(MockTaskAttemptImpl attempt, float p) {
    attempt.setProgress(p);
  }

  private void updateAttemptState(MockTaskAttemptImpl attempt,
      TaskAttemptState s) {
    attempt.setState(s);
  }

  private void killRunningTaskAttempt(TezTaskAttemptID attemptId) {
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskRunningState();
  }

  private void failRunningTaskAttempt(TezTaskAttemptID attemptId) {
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_FAILED));
    assertTaskRunningState();
  }

  /**
   * {@link TaskState#NEW}
   */
  private void assertTaskNewState() {
    assertEquals(TaskState.NEW, mockTask.getState());
  }

  /**
   * {@link TaskState#SCHEDULED}
   */
  private void assertTaskScheduledState() {
    assertEquals(TaskState.SCHEDULED, mockTask.getState());
  }

  /**
   * {@link TaskState#RUNNING}
   */
  private void assertTaskRunningState() {
    assertEquals(TaskState.RUNNING, mockTask.getState());
  }

  /**
   * {@link TaskState#KILL_WAIT}
   */
  private void assertTaskKillWaitState() {
    assertEquals(TaskStateInternal.KILL_WAIT, mockTask.getInternalState());
  }

  /**
   * {@link TaskState#SUCCEEDED}
   */
  private void assertTaskSucceededState() {
    assertEquals(TaskState.SUCCEEDED, mockTask.getState());
  }

  @Test
  public void testInit() {
    LOG.info("--- START: testInit ---");
    assertTaskNewState();
    assert (mockTask.getAttemptList().size() == 0);
  }

  @Test
  /**
   * {@link TaskState#NEW}->{@link TaskState#SCHEDULED}
   */
  public void testScheduleTask() {
    LOG.info("--- START: testScheduleTask ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
  }

  @Test
  /**
   * {@link TaskState#SCHEDULED}->{@link TaskState#KILL_WAIT}
   */
  public void testKillScheduledTask() {
    LOG.info("--- START: testKillScheduledTask ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killTask(taskId);
  }

  @Test
  /**
   * Kill attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#SCHEDULED}
   */
  public void testKillScheduledTaskAttempt() {
    LOG.info("--- START: testKillScheduledTaskAttempt ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killScheduledTaskAttempt(mockTask.getLastAttempt().getID());
  }

  @Test
  /**
   * Launch attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#RUNNING}
   */
  public void testLaunchTaskAttempt() {
    LOG.info("--- START: testLaunchTaskAttempt ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
  }

  @Test
  /**
   * Kill running attempt
   * {@link TaskState#RUNNING}->{@link TaskState#RUNNING} 
   */
  public void testKillRunningTaskAttempt() {
    LOG.info("--- START: testKillRunningTaskAttempt ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killRunningTaskAttempt(mockTask.getLastAttempt().getID());
  }

  @Test
  public void testTaskProgress() {
    LOG.info("--- START: testTaskProgress ---");

    // launch task
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    float progress = 0f;
    assert (mockTask.getProgress() == progress);
    launchTaskAttempt(mockTask.getLastAttempt().getID());

    // update attempt1
    progress = 50f;
    updateAttemptProgress(mockTask.getLastAttempt(), progress);
    assert (mockTask.getProgress() == progress);
    progress = 100f;
    updateAttemptProgress(mockTask.getLastAttempt(), progress);
    assert (mockTask.getProgress() == progress);

    progress = 0f;
    // mark first attempt as killed
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.KILLED);
    assert (mockTask.getProgress() == progress);

    // kill first attempt
    // should trigger a new attempt
    // as no successful attempts
    killRunningTaskAttempt(mockTask.getLastAttempt().getID());
    assert (mockTask.getAttemptList().size() == 2);

    assert (mockTask.getProgress() == 0f);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    progress = 50f;
    updateAttemptProgress(mockTask.getLastAttempt(), progress);
    assert (mockTask.getProgress() == progress);
  }

  @Test
  public void testFailureDuringTaskAttemptCommit() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(),
        TaskAttemptState.COMMIT_PENDING);
    commitTaskAttempt(mockTask.getLastAttempt().getID());

    // During the task attempt commit there is an exception which causes
    // the attempt to fail
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.FAILED);
    failRunningTaskAttempt(mockTask.getLastAttempt().getID());

    assertEquals(2, mockTask.getAttemptList().size());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.SUCCEEDED);
    commitTaskAttempt(mockTask.getLastAttempt().getID());
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    assertFalse("First attempt should not commit",
        mockTask.canCommit(mockTask.getAttemptList().get(0).getID()));
    assertTrue("Second attempt should commit",
        mockTask.canCommit(mockTask.getLastAttempt().getID()));

    assertTaskSucceededState();
  }

  @Test
  public void testSpeculativeTaskAttemptSucceedsEvenIfFirstFails() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);

    // Add a speculative task attempt that succeeds
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    commitTaskAttempt(mockTask.getLastAttempt().getID());
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    // The task should now have succeeded
    assertTaskSucceededState();

    // Now fail the first task attempt, after the second has succeeded
    mockTask.handle(new TaskEventTAUpdate(mockTask.getAttemptList().get(0)
        .getID(), TaskEventType.T_ATTEMPT_FAILED));

    // The task should still be in the succeeded state
    assertTaskSucceededState();

  }
  
  // TODO Add test to validate the correct commit attempt.

  @SuppressWarnings("rawtypes")
  private class MockTaskImpl extends TaskImpl {

    private List<MockTaskAttemptImpl> taskAttempts = new LinkedList<MockTaskAttemptImpl>();

    public MockTaskImpl(TezVertexID vertexId, int partition,
        EventHandler eventHandler, TezConfiguration conf,
        TaskAttemptListener taskAttemptListener,
        Token<JobTokenIdentifier> jobToken, Credentials credentials,
        Clock clock, TaskHeartbeatHandler thh, AppContext appContext,
        String processorName, boolean leafVertex,
        TaskLocationHint locationHint, Resource resource,
        Map<String, LocalResource> localResources,
        Map<String, String> environment, String javaOpts) {
      super(vertexId, partition, eventHandler, conf, taskAttemptListener,
          jobToken, credentials, clock, thh, appContext, processorName,
          leafVertex, locationHint, resource, localResources, environment,
          javaOpts);
    }

    @Override
    protected TaskAttemptImpl createAttempt(int attemptNumber) {
      MockTaskAttemptImpl attempt = new MockTaskAttemptImpl(getTaskId(),
          attemptNumber, eventHandler, taskAttemptListener, attemptNumber,
          conf, jobToken, credentials, clock, taskHeartbeatHandler, appContext,
          processorName, locationHint, taskResource, localResources,
          environment, javaOpts, true);
      taskAttempts.add(attempt);
      return attempt;
    }

    @Override
    protected void internalError(TaskEventType type) {
      super.internalError(type);
      fail("Internal error: " + type);
    }

    MockTaskAttemptImpl getLastAttempt() {
      return taskAttempts.get(taskAttempts.size() - 1);
    }

    List<MockTaskAttemptImpl> getAttemptList() {
      return taskAttempts;
    }

    protected void logJobHistoryTaskStartedEvent() {
    }

    protected void logJobHistoryTaskFinishedEvent() {
    }

    protected void logJobHistoryTaskFailedEvent(TaskState finalState) {
    }
  }

  @SuppressWarnings("rawtypes")
  public class MockTaskAttemptImpl extends TaskAttemptImpl {

    private float progress = 0;
    private TaskAttemptState state = TaskAttemptState.NEW;

    public MockTaskAttemptImpl(TezTaskID taskId, int attemptNumber,
        EventHandler eventHandler, TaskAttemptListener tal, int partition,
        TezConfiguration conf, Token<JobTokenIdentifier> jobToken,
        Credentials credentials, Clock clock, TaskHeartbeatHandler thh,
        AppContext appContext, String processorName,
        TaskLocationHint locationHing, Resource resource,
        Map<String, LocalResource> localResources,
        Map<String, String> environment, String javaOpts, boolean isRescheduled) {
      super(taskId, attemptNumber, eventHandler, tal, partition, conf,
          jobToken, credentials, clock, thh, appContext, processorName,
          locationHing, resource, localResources, environment, javaOpts,
          isRescheduled);
    }

    @Override
    public float getProgress() {
      return progress;
    }

    public void setProgress(float progress) {
      this.progress = progress;
    }

    public void setState(TaskAttemptState state) {
      this.state = state;
    }

    @Override
    public TaskAttemptState getState() {
      return state;
    }
  }

}

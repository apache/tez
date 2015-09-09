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
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskEventScheduleTask;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventTermination;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTaskImpl {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskImpl.class);

  private int taskCounter = 0;
  private final int partition = 1;

  private Configuration conf;
  private TaskAttemptListener taskAttemptListener;
  private TaskHeartbeatHandler taskHeartbeatHandler;
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
  private ContainerContext containerContext;
  private ContainerId mockContainerId;
  private Container mockContainer;
  private AMContainer mockAMContainer;
  private NodeId mockNodeId;

  private MockTaskImpl mockTask;
  private TaskSpec mockTaskSpec;
  
  @SuppressWarnings("rawtypes")
  class TestEventHandler implements EventHandler<Event> {
    List<Event> events = new ArrayList<Event>();
    @Override
    public void handle(Event event) {
      events.add(event);
    }
  }
  private TestEventHandler eventHandler;

  @Before
  public void setup() {
    conf = new Configuration();
    taskAttemptListener = mock(TaskAttemptListener.class);
    taskHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    credentials = new Credentials();
    clock = new SystemClock();
    locationHint = TaskLocationHint.createTaskLocationHint(null, null);

    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    dagId = TezDAGID.getInstance(appId, 1);
    vertexId = TezVertexID.getInstance(dagId, 1);
    appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    mockContainerId = mock(ContainerId.class);
    mockContainer = mock(Container.class);
    mockAMContainer = mock(AMContainer.class);
    mockNodeId = mock(NodeId.class);
    when(mockContainer.getId()).thenReturn(mockContainerId);
    when(mockContainer.getNodeId()).thenReturn(mockNodeId);
    when(mockAMContainer.getContainer()).thenReturn(mockContainer);
    when(appContext.getAllContainers().get(mockContainerId)).thenReturn(mockAMContainer);
    taskResource = Resource.newInstance(1024, 1);
    localResources = new HashMap<String, LocalResource>();
    environment = new HashMap<String, String>();
    javaOpts = "";
    leafVertex = false;
    containerContext = new ContainerContext(localResources, credentials,
        environment, javaOpts);
    Vertex vertex = mock(Vertex.class);
    eventHandler = new TestEventHandler();
    
    mockTask = new MockTaskImpl(vertexId, partition,
        eventHandler, conf, taskAttemptListener, clock,
        taskHeartbeatHandler, appContext, leafVertex,
        taskResource, containerContext, vertex);
    mockTaskSpec = mock(TaskSpec.class);
  }

  private TezTaskID getNewTaskID() {
    TezTaskID taskID = TezTaskID.getInstance(vertexId, ++taskCounter);
    return taskID;
  }

  private void scheduleTaskAttempt(TezTaskID taskId) {
    mockTask.handle(new TaskEventScheduleTask(taskId, mockTaskSpec, locationHint));
    assertTaskScheduledState();
    assertEquals(mockTaskSpec, mockTask.getBaseTaskSpec());
    assertEquals(locationHint, mockTask.getTaskLocationHint());
  }

  private void sendTezEventsToTask(TezTaskID taskId, int numTezEvents) {
    EventMetaData eventMetaData = new EventMetaData();
    DataMovementEvent dmEvent = DataMovementEvent.create(null);
    TezEvent tezEvent = new TezEvent(dmEvent, eventMetaData);
    for (int i = 0; i < numTezEvents; i++) {
      mockTask.registerTezEvent(tezEvent);
    }
  }

  private void killTask(TezTaskID taskId) {
    mockTask.handle(new TaskEventTermination(taskId, TaskAttemptTerminationCause.TERMINATED_AT_SHUTDOWN, null));
    assertTaskKillWaitState();
  }

  private void failTask(TezTaskID taskId) {
    mockTask.handle(new TaskEventTermination(taskId, TaskAttemptTerminationCause.TERMINATED_AT_SHUTDOWN, null));
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
    verify(mockTask.getVertex(), times(1)).incrementKilledTaskAttemptCount();
  }

  private void failRunningTaskAttempt(TezTaskAttemptID attemptId) {
    int failedAttempts = mockTask.failedAttempts;
    mockTask.handle(new TaskEventTAUpdate(attemptId,
        TaskEventType.T_ATTEMPT_FAILED));
    assertTaskRunningState();
    Assert.assertEquals(failedAttempts + 1, mockTask.failedAttempts);
    verify(mockTask.getVertex(), times(failedAttempts + 1)).incrementFailedTaskAttemptCount();
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
   * {@link org.apache.tez.dag.app.dag.TaskStateInternal#KILL_WAIT}
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

  @Test(timeout = 5000)
  public void testInit() {
    LOG.info("--- START: testInit ---");
    assertTaskNewState();
    assert (mockTask.getAttemptList().size() == 0);
  }

  @Test(timeout = 5000)
  /**
   * {@link TaskState#NEW}->{@link TaskState#SCHEDULED}
   */
  public void testScheduleTask() {
    LOG.info("--- START: testScheduleTask ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
  }

  @Test(timeout = 5000)
  /**
   * {@link TaskState#SCHEDULED}->{@link TaskState#KILL_WAIT}
   */
  public void testKillScheduledTask() {
    LOG.info("--- START: testKillScheduledTask ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killTask(taskId);
  }
  
  /**
   * {@link TaskState#RUNNING}->{@link TaskState#KILLED}
   */
  @Test(timeout = 5000)
  public void testKillRunningTask() {
    LOG.info("--- START: testKillRunningTask ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killTask(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());
    verifyOutgoingEvents(eventHandler.events, VertexEventType.V_TASK_COMPLETED);
  }

  @Test(timeout = 5000)
  public void testKillRunningTaskPreviousKilledAttempts() {
    LOG.info("--- START: testKillRunningTaskPreviousKilledAttempts ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killRunningTaskAttempt(mockTask.getLastAttempt().getID());
    assertEquals(TaskStateInternal.RUNNING, mockTask.getInternalState());
    killTask(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_KILLED));

    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());
    verifyOutgoingEvents(eventHandler.events, VertexEventType.V_TASK_COMPLETED);
  }

  /**
   * {@link TaskState#RUNNING}->{@link TaskState#KILLED}
   */
  @Test(timeout = 5000)
  public void testKillRunningTaskButAttemptSucceeds() {
    LOG.info("--- START: testKillRunningTaskButAttemptSucceeds ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killTask(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));
    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());
  }
  
  /**
   * {@link TaskState#RUNNING}->{@link TaskState#KILLED}
   */
  @Test(timeout = 5000)
  public void testKillRunningTaskButAttemptFails() {
    LOG.info("--- START: testKillRunningTaskButAttemptFails ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killTask(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_FAILED));
    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());
  }

  @Test(timeout = 5000)
  /**
   * Kill attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#SCHEDULED}
   */
  public void testKillScheduledTaskAttempt() {
    LOG.info("--- START: testKillScheduledTaskAttempt ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    TezTaskAttemptID lastTAId = mockTask.getLastAttempt().getID();
    killScheduledTaskAttempt(mockTask.getLastAttempt().getID());
    // last killed attempt should be causal TA of next attempt
    Assert.assertEquals(lastTAId, mockTask.getLastAttempt().getSchedulingCausalTA());
  }

  @Test(timeout = 5000)
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

  @Test(timeout = 5000)
  /**
   * Kill running attempt
   * {@link TaskState#RUNNING}->{@link TaskState#RUNNING}
   */
  public void testKillRunningTaskAttempt() {
    LOG.info("--- START: testKillRunningTaskAttempt ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    TezTaskAttemptID lastTAId = mockTask.getLastAttempt().getID();
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killRunningTaskAttempt(mockTask.getLastAttempt().getID());
    // last killed attempt should be causal TA of next attempt
    Assert.assertEquals(lastTAId, mockTask.getLastAttempt().getSchedulingCausalTA());
  }

  /**
   * {@link TaskState#KILLED}->{@link TaskState#KILLED}
   */
  @Test(timeout = 5000)
  public void testKilledAttemptAtTaskKilled() {
    LOG.info("--- START: testKilledAttemptAtTaskKilled ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    killTask(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());

    // Send duplicate kill for same attempt
    // This will not happen in practice but this is to simulate handling
    // of killed attempts in killed state.
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(TaskStateInternal.KILLED, mockTask.getInternalState());

  }

  /**
   * {@link TaskState#FAILED}->{@link TaskState#FAILED}
   */
  @Test(timeout = 5000)
  public void testKilledAttemptAtTaskFailed() {
    LOG.info("--- START: testKilledAttemptAtTaskFailed ---");
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    for (int i = 0; i < mockTask.maxFailedAttempts; ++i) {
      mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
          TaskEventType.T_ATTEMPT_FAILED));
    }
    assertEquals(TaskStateInternal.FAILED, mockTask.getInternalState());

    // Send kill for an attempt
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(TaskStateInternal.FAILED, mockTask.getInternalState());

  }



  @Test(timeout = 5000)
  public void testFetchedEventsModifyUnderlyingList() {
    // Tests to ensure that adding an event to a task, does not affect the
    // result of past getTaskAttemptTezEvents calls.
    List<TezEvent> fetchedList;
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    sendTezEventsToTask(taskId, 2);
    TezTaskAttemptID attemptID = mockTask.getAttemptList().iterator().next()
        .getID();
    fetchedList = mockTask.getTaskAttemptTezEvents(attemptID, 0, 100);
    assertEquals(2, fetchedList.size());

    // Add events, make sure underlying list is the same, and no exceptions are
    // thrown while accessing the previous list
    sendTezEventsToTask(taskId, 4);
    assertEquals(2, fetchedList.size());

    fetchedList = mockTask.getTaskAttemptTezEvents(attemptID, 0, 100);
    assertEquals(6, fetchedList.size());
  }

  @Test(timeout = 5000)
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
    failRunningTaskAttempt(mockTask.getLastAttempt().getID());
    assert (mockTask.getAttemptList().size() == 2);
    assertEquals(1, mockTask.failedAttempts);
    verify(mockTask.getVertex(), times(1)).incrementFailedTaskAttemptCount();

    assert (mockTask.getProgress() == 0f);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    progress = 50f;
    updateAttemptProgress(mockTask.getLastAttempt(), progress);
    assert (mockTask.getProgress() == progress);
  }

  @Test(timeout = 5000)
  public void testFailureDuringTaskAttemptCommit() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);
    assertTrue("First attempt should commit",
        mockTask.canCommit(mockTask.getLastAttempt().getID()));

    // During the task attempt commit there is an exception which causes
    // the attempt to fail
    TezTaskAttemptID lastTAId = mockTask.getLastAttempt().getID();
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.FAILED);
    assertEquals(1, mockTask.getAttemptList().size());
    failRunningTaskAttempt(mockTask.getLastAttempt().getID());

    assertEquals(2, mockTask.getAttemptList().size());
    assertEquals(1, mockTask.failedAttempts);
    // last failed attempt should be the causal TA
    Assert.assertEquals(lastTAId, mockTask.getLastAttempt().getSchedulingCausalTA());

    assertFalse("First attempt should not commit",
        mockTask.canCommit(mockTask.getAttemptList().get(0).getID()));
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);
    assertTrue("Second attempt should commit",
        mockTask.canCommit(mockTask.getLastAttempt().getID()));

    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.SUCCEEDED);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    assertTaskSucceededState();
  }
  

  @Test(timeout = 5000)
  public void testEventBacklogDuringTaskAttemptCommit() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    assertEquals(TaskState.SCHEDULED, mockTask.getState());
    // simulate
    // task in scheduled state due to event backlog - real task done and calling canCommit
    assertFalse("Commit should return false to make running task wait",
        mockTask.canCommit(mockTask.getLastAttempt().getID()));
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);
    assertTrue("Task state in AM is running now. Can commit.",
        mockTask.canCommit(mockTask.getLastAttempt().getID()));

    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.SUCCEEDED);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    assertTaskSucceededState();
  }


  @Test(timeout = 5000)
  public void testChangeCommitTaskAttempt() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);
    TezTaskAttemptID lastTAId = mockTask.getLastAttempt().getID();
    
    // Add a speculative task attempt that succeeds
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ADD_SPEC_ATTEMPT));
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);
    
    assertEquals(2, mockTask.getAttemptList().size());
    
    // previous running attempt should be the casual TA of this speculative attempt
    Assert.assertEquals(lastTAId, mockTask.getLastAttempt().getSchedulingCausalTA());
    
    assertTrue("Second attempt should commit",
        mockTask.canCommit(mockTask.getAttemptList().get(1).getID()));
    assertFalse("First attempt should not commit",
        mockTask.canCommit(mockTask.getAttemptList().get(0).getID()));

    // During the task attempt commit there is an exception which causes
    // the second attempt to fail
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.FAILED);
    failRunningTaskAttempt(mockTask.getLastAttempt().getID());

    assertEquals(2, mockTask.getAttemptList().size());
    
    assertFalse("Second attempt should not commit",
        mockTask.canCommit(mockTask.getAttemptList().get(1).getID()));
    assertTrue("First attempt should commit",
        mockTask.canCommit(mockTask.getAttemptList().get(0).getID()));

    updateAttemptState(mockTask.getAttemptList().get(0), TaskAttemptState.SUCCEEDED);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getAttemptList().get(0).getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    assertTaskSucceededState();
  }
  
  @SuppressWarnings("rawtypes")
  @Test(timeout = 5000)
  public void testTaskSucceedAndRetroActiveFailure() {
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    updateAttemptState(mockTask.getLastAttempt(), TaskAttemptState.RUNNING);

    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(),
        TaskEventType.T_ATTEMPT_SUCCEEDED));

    // The task should now have succeeded
    assertTaskSucceededState();
    verify(mockTask.stateChangeNotifier).taskSucceeded(any(String.class), eq(taskId),
        eq(mockTask.getLastAttempt().getID().getId()));

    eventHandler.events.clear();
    // Now fail the attempt after it has succeeded
    TezTaskAttemptID mockDestId = mock(TezTaskAttemptID.class);
    TezEvent mockTezEvent = mock(TezEvent.class);
    EventMetaData meta = new EventMetaData(EventProducerConsumerType.INPUT, "Vertex", "Edge", mockDestId);
    when(mockTezEvent.getSourceInfo()).thenReturn(meta);
    TaskAttemptEventOutputFailed outputFailedEvent = 
        new TaskAttemptEventOutputFailed(mockDestId, mockTezEvent, 1);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt()
        .getID(), TaskEventType.T_ATTEMPT_FAILED, outputFailedEvent));

    // The task should still be in the scheduled state
    assertTaskScheduledState();
    Event event = eventHandler.events.get(0);
    Assert.assertEquals(AMNodeEventType.N_TA_ENDED, event.getType());
    event = eventHandler.events.get(eventHandler.events.size()-1);
    Assert.assertEquals(VertexEventType.V_TASK_RESCHEDULED, event.getType());
    
    // report of output read error should be the causal TA
    List<MockTaskAttemptImpl> attempts = mockTask.getAttemptList();
    Assert.assertEquals(2, attempts.size());
    MockTaskAttemptImpl newAttempt = attempts.get(1);
    Assert.assertEquals(mockDestId, newAttempt.getSchedulingCausalTA());
  }

  @Test(timeout = 5000)
  public void testDiagnostics_TAUpdate(){
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(), TaskEventType.T_ATTEMPT_KILLED));
    assertEquals(1, mockTask.getDiagnostics().size());
    assertEquals("TaskAttempt 0 killed", mockTask.getDiagnostics().get(0));
    
    launchTaskAttempt(mockTask.getLastAttempt().getID());
    mockTask.getLastAttempt().handle(new TaskAttemptEventDiagnosticsUpdate(mockTask.getLastAttempt().getID(), "diagnostics of test"));
    mockTask.handle(new TaskEventTAUpdate(mockTask.getLastAttempt().getID(), TaskEventType.T_ATTEMPT_FAILED));
    assertEquals(2, mockTask.getDiagnostics().size());
    assertEquals("TaskAttempt 1 failed, info=[diagnostics of test]", mockTask.getDiagnostics().get(1));
  }
  
  @Test(timeout = 5000)
  public void testDiagnostics_KillNew(){
    TezTaskID taskId = getNewTaskID();
    mockTask.handle(new TaskEventTermination(taskId, TaskAttemptTerminationCause.TERMINATED_BY_CLIENT, null));
    assertEquals(1, mockTask.getDiagnostics().size());
    assertTrue(mockTask.getDiagnostics().get(0).contains(TaskAttemptTerminationCause.TERMINATED_BY_CLIENT.name()));
    assertEquals(0, mockTask.taskStartedEventLogged);
    assertEquals(1, mockTask.taskFinishedEventLogged);
  }
  
  @Test(timeout = 5000)
  public void testDiagnostics_Kill(){
    TezTaskID taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    mockTask.handle(new TaskEventTermination(taskId, TaskAttemptTerminationCause.TERMINATED_AT_SHUTDOWN, null));
    assertEquals(1, mockTask.getDiagnostics().size());
    assertTrue(mockTask.getDiagnostics().get(0).contains(TaskAttemptTerminationCause.TERMINATED_AT_SHUTDOWN.name()));
  }

  // TODO Add test to validate the correct commit attempt.


  /* Verifies that the specified event types, exist. Does not ensure they are the only ones, however */
  private void verifyOutgoingEvents(List<Event> events,
                                    Enum<?>... expectedTypes) {

    List<Enum<?>> expectedTypeList = new LinkedList<Enum<?>>();
    for (Enum<?> expectedType : expectedTypes) {
      expectedTypeList.add(expectedType);
    }
    for (Event event : events) {
      Iterator<Enum<?>> typeIter = expectedTypeList.iterator();
      while (typeIter.hasNext()) {
        Enum<?> expectedType = typeIter.next();
        if (event.getType() == expectedType) {
          typeIter.remove();
          // Move to the next event.
          break;
        }
      }
    }
    assertTrue("Did not find types : " + expectedTypeList
        + " in outgoing event list", expectedTypeList.isEmpty());
  }

  @SuppressWarnings("rawtypes")
  private class MockTaskImpl extends TaskImpl {

    public int taskStartedEventLogged = 0;
    public int taskFinishedEventLogged = 0;

    private List<MockTaskAttemptImpl> taskAttempts = new LinkedList<MockTaskAttemptImpl>();
    private Vertex vertex;

    public MockTaskImpl(TezVertexID vertexId, int partition,
        EventHandler eventHandler, Configuration conf,
        TaskAttemptListener taskAttemptListener, Clock clock,
        TaskHeartbeatHandler thh, AppContext appContext, boolean leafVertex,
        Resource resource,
        ContainerContext containerContext, Vertex vertex) {
      super(vertexId, partition, eventHandler, conf, taskAttemptListener,
          clock, thh, appContext, leafVertex, resource,
          containerContext, mock(StateChangeNotifier.class), vertex);
      this.vertex = vertex;
    }

    @Override
    protected TaskAttemptImpl createAttempt(int attemptNumber, TezTaskAttemptID schedCausalTA) {
      MockTaskAttemptImpl attempt = new MockTaskAttemptImpl(getTaskId(),
          attemptNumber, eventHandler, taskAttemptListener,
          conf, clock, taskHeartbeatHandler, appContext,
          true, taskResource, containerContext, schedCausalTA);
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
    
    @Override
    public Vertex getVertex() {
      return vertex;
    }

    protected void logJobHistoryTaskStartedEvent() {
      taskStartedEventLogged++;
    }

    protected void logJobHistoryTaskFinishedEvent() {
      taskFinishedEventLogged++;
    }

    protected void logJobHistoryTaskFailedEvent(TaskState finalState) {
      taskFinishedEventLogged++;
    }
  }

  @SuppressWarnings("rawtypes")
  public class MockTaskAttemptImpl extends TaskAttemptImpl {

    private float progress = 0;
    private TaskAttemptState state = TaskAttemptState.NEW;

    public MockTaskAttemptImpl(TezTaskID taskId, int attemptNumber,
        EventHandler eventHandler, TaskAttemptListener tal, Configuration conf,
        Clock clock, TaskHeartbeatHandler thh, AppContext appContext,
        boolean isRescheduled,
        Resource resource, ContainerContext containerContext, TezTaskAttemptID schedCausalTA) {
      super(taskId, attemptNumber, eventHandler, tal, conf, clock, thh,
          appContext, isRescheduled, resource, containerContext, false, mock(TaskImpl.class),
          schedCausalTA);
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
    
    @Override
    public TaskAttemptState getStateNoLock() {
      return state;
    }
    
    @Override
    public ContainerId getAssignedContainerID() {
      return mockContainerId;
    }
  }

}

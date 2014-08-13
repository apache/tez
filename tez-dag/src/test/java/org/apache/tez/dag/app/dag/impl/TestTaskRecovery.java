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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
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
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Before;
import org.junit.Test;

public class TestTaskRecovery {

  private static final Log LOG = LogFactory.getLog(TestTaskImpl.class);

  private int taskCounter = 0;
  private int taskAttemptCounter = 0;

  private Configuration conf;
  private TaskAttemptListener taskAttemptListener;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private Credentials credentials;
  private Clock clock;
  private ApplicationId appId;
  private TezDAGID dagId;
  private TezVertexID vertexId;
  private Vertex vertex;
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

  private TaskImpl task;
  private DrainDispatcher dispatcher;

  class TaskEventHandler implements EventHandler<TaskEvent> {
    @Override
    public void handle(TaskEvent event) {
      task.handle(event);
    }
  }

  class TaskAttemptEventHandler implements EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      ((TaskAttemptImpl) task.getAttempt(event.getTaskAttemptID()))
          .handle(event);
    }
  }

  @Before
  public void setUp() {
    conf = new Configuration();
    taskAttemptListener = mock(TaskAttemptListener.class);
    taskHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    credentials = new Credentials();
    clock = new SystemClock();
    appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    dagId = TezDAGID.getInstance(appId, 1);
    vertexId = TezVertexID.getInstance(dagId, 1);
    vertex = mock(Vertex.class, RETURNS_DEEP_STUBS);
    when(vertex.getProcessorDescriptor().getClassName()).thenReturn("");
    appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    mockContainerId = mock(ContainerId.class);
    mockContainer = mock(Container.class);
    mockAMContainer = mock(AMContainer.class);
    mockNodeId = mock(NodeId.class);
    when(mockContainer.getId()).thenReturn(mockContainerId);
    when(mockContainer.getNodeId()).thenReturn(mockNodeId);
    when(mockAMContainer.getContainer()).thenReturn(mockContainer);
    when(appContext.getAllContainers().get(mockContainerId)).thenReturn(
        mockAMContainer);
    when(appContext.getCurrentDAG().getVertex(any(TezVertexID.class)))
        .thenReturn(vertex);
    when(vertex.getProcessorDescriptor().getClassName()).thenReturn("");

    taskResource = Resource.newInstance(1024, 1);
    localResources = new HashMap<String, LocalResource>();
    environment = new HashMap<String, String>();
    javaOpts = "";
    leafVertex = false;
    containerContext =
        new ContainerContext(localResources, credentials, environment, javaOpts);

    dispatcher = new DrainDispatcher();
    dispatcher.register(DAGEventType.class, mock(EventHandler.class));
    dispatcher.register(VertexEventType.class, mock(EventHandler.class));
    dispatcher.register(TaskEventType.class, new TaskEventHandler());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventHandler());
    dispatcher.init(new Configuration());
    dispatcher.start();

    task =
        new TaskImpl(vertexId, 1, dispatcher.getEventHandler(), conf,
            taskAttemptListener, clock, taskHeartbeatHandler, appContext,
            leafVertex, taskResource, containerContext);
  }

  /**
   * n = maxFailedAttempts, in the previous AM attempt, n task attempts are
   * killed. When recovering, it should continue to be in running state and
   * schedule a new task attempt.
   */
  @Test
  public void testTaskRecovery1() {
    TezTaskID lastTaskId = getNewTaskID();
    TezTaskID taskId = getNewTaskID();
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    task.restoreFromEvent(new TaskStartedEvent(taskId, "v1", 0, 0));
    for (int i = 0; i < maxFailedAttempts; ++i) {
      TezTaskAttemptID attemptId = getNewTaskAttemptID(lastTaskId);
      task.restoreFromEvent(new TaskAttemptStartedEvent(attemptId, "v1", 0,
          mockContainerId, mockNodeId, "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(attemptId, "v1", 0, 0,
          TaskAttemptState.KILLED, "", null));
    }
    assertEquals(maxFailedAttempts, task.getAttempts().size());
    assertEquals(0, task.failedAttempts);

    task.handle(new TaskEventRecoverTask(lastTaskId));
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
  @Test
  public void testTaskRecovery2() {
    TezTaskID lastTaskId = getNewTaskID();
    TezTaskID taskId = getNewTaskID();
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    task.restoreFromEvent(new TaskStartedEvent(taskId, "v1", 0, 0));
    for (int i = 0; i < maxFailedAttempts; ++i) {
      TezTaskAttemptID attemptId = getNewTaskAttemptID(lastTaskId);
      task.restoreFromEvent(new TaskAttemptStartedEvent(attemptId, "v1", 0,
          mockContainerId, mockNodeId, "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(attemptId, "v1", 0, 0,
          TaskAttemptState.FAILED, "", null));
    }
    assertEquals(maxFailedAttempts, task.getAttempts().size());
    assertEquals(maxFailedAttempts, task.failedAttempts);

    task.handle(new TaskEventRecoverTask(lastTaskId));
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
  @Test
  public void testTaskRecovery3() throws InterruptedException {
    TezTaskID lastTaskId = getNewTaskID();
    TezTaskID taskId = getNewTaskID();
    int maxFailedAttempts =
        conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
            TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
    task.restoreFromEvent(new TaskStartedEvent(taskId, "v1", 0, 0));
    for (int i = 0; i < maxFailedAttempts - 1; ++i) {
      TezTaskAttemptID attemptId = getNewTaskAttemptID(lastTaskId);
      task.restoreFromEvent(new TaskAttemptStartedEvent(attemptId, "v1", 0,
          mockContainerId, mockNodeId, "", ""));
      task.restoreFromEvent(new TaskAttemptFinishedEvent(attemptId, "v1", 0, 0,
          TaskAttemptState.FAILED, "", null));
    }
    assertEquals(maxFailedAttempts - 1, task.getAttempts().size());
    assertEquals(maxFailedAttempts - 1, task.failedAttempts);

    TezTaskAttemptID newTaskAttemptId = getNewTaskAttemptID(lastTaskId);
    TaskState recoveredState =
        task.restoreFromEvent(new TaskAttemptStartedEvent(newTaskAttemptId,
            "v1", 0, mockContainerId, mockNodeId, "", ""));
    assertEquals(TaskState.RUNNING, recoveredState);
    assertEquals(TaskAttemptStateInternal.NEW,
        ((TaskAttemptImpl) task.getAttempt(newTaskAttemptId))
            .getInternalState());
    assertEquals(maxFailedAttempts, task.getAttempts().size());

    task.handle(new TaskEventRecoverTask(lastTaskId));
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

  private TezTaskID getNewTaskID() {
    TezTaskID taskID = TezTaskID.getInstance(vertexId, ++taskCounter);
    return taskID;
  }

  private TezTaskAttemptID getNewTaskAttemptID(TezTaskID taskId) {
    return TezTaskAttemptID.getInstance(taskId, taskAttemptCounter++);
  }
}

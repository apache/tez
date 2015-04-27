/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class TestTaskAttemptListenerImplTezDag {
  private ApplicationId appId;
  private AppContext appContext;
  AMContainerMap amContainerMap;
  EventHandler eventHandler;
  DAG dag;
  TaskAttemptListenerImpTezDag taskAttemptListener;
  ContainerTask containerTask;
  AMContainerTask amContainerTask;
  TaskSpec taskSpec;

  TezVertexID vertexID;
  TezTaskID taskID;
  TezTaskAttemptID taskAttemptID;

  @Before
  public void setUp() {
    appId = ApplicationId.newInstance(1000, 1);
    dag = mock(DAG.class);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    vertexID = TezVertexID.getInstance(dagID, 1);
    taskID = TezTaskID.getInstance(vertexID, 1);
    taskAttemptID = TezTaskAttemptID.getInstance(taskID, 1);

    amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();

    eventHandler = mock(EventHandler.class);

    appContext = mock(AppContext.class);
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(appAcls).when(appContext).getApplicationACLs();
    doReturn(amContainerMap).when(appContext).getAllContainers();

    taskAttemptListener = new TaskAttemptListenerImplForTest(appContext,
        mock(TaskHeartbeatHandler.class), mock(ContainerHeartbeatHandler.class), null);

    taskSpec = mock(TaskSpec.class);
    doReturn(taskAttemptID).when(taskSpec).getTaskAttemptID();
    amContainerTask = new AMContainerTask(taskSpec, null, null, false, 0);
    containerTask = null;
  }

  @Test(timeout = 5000)
  public void testGetTask() throws IOException {

    ContainerId containerId1 = createContainerId(appId, 1);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId1);
    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertTrue(containerTask.shouldDie());

    ContainerId containerId2 = createContainerId(appId, 2);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId2);
    ContainerContext containerContext2 = new ContainerContext(containerId2.toString());
    taskAttemptListener.registerRunningContainer(containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertNull(containerTask);

    // Valid task registered
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Task unregistered. Should respond to heartbeats
    taskAttemptListener.unregisterTaskAttempt(taskAttemptID);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertNull(containerTask);

    // Container unregistered. Should send a shouldDie = true
    taskAttemptListener.unregisterRunningContainer(containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertTrue(containerTask.shouldDie());

    ContainerId containerId3 = createContainerId(appId, 3);
    ContainerContext containerContext3 = new ContainerContext(containerId3.toString());
    taskAttemptListener.registerRunningContainer(containerId3);

    // Register task to container3, followed by unregistering container 3 all together
    TaskSpec taskSpec2 = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId2 = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId2).when(taskSpec2).getTaskAttemptID();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec, null, null, false, 0);
    taskAttemptListener.registerTaskAttempt(amContainerTask2, containerId3);
    taskAttemptListener.unregisterRunningContainer(containerId3);
    containerTask = taskAttemptListener.getTask(containerContext3);
    assertTrue(containerTask.shouldDie());
  }

  @Test(timeout = 5000)
  public void testGetTaskMultiplePulls() throws IOException {
    ContainerId containerId1 = createContainerId(appId, 1);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId1);
    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    taskAttemptListener.registerRunningContainer(containerId1);
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertNull(containerTask);

    // Register task
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId1);
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Try pulling again - simulates re-use pull
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertNull(containerTask);
  }

  @Test (timeout = 5000)
  public void testTaskEventRouting() throws Exception {
    List<TezEvent> events =  Arrays.asList(
      new TezEvent(InputInitializerEvent.create("test_vertex", "test_input", null), null),
      new TezEvent(new TaskStatusUpdateEvent(null, 0.0f, null), null)
    );

    EventHandler eventHandler = generateHeartbeat(events);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(2)).handle(arg.capture());
    final List<Event> argAllValues = arg.getAllValues();

    final Event statusUpdateEvent = argAllValues.get(0);
    assertEquals("First event should be status update", TaskAttemptEventType.TA_STATUS_UPDATE,
        statusUpdateEvent.getType());


    final Event vertexEvent = argAllValues.get(1);
    final VertexEventRouteEvent vertexRouteEvent = (VertexEventRouteEvent)vertexEvent;
    assertEquals("Other events should be routed to vertex", VertexEventType.V_ROUTE_EVENT,
        vertexEvent.getType());
    assertEquals(EventType.ROOT_INPUT_INITIALIZER_EVENT,
        vertexRouteEvent.getEvents().get(0).getEventType());
  }

  @Test (timeout = 5000)
  public void testTaskEventRoutingTaskAttemptOnly() throws Exception {
    List<TezEvent> events = Arrays.asList(
      new TezEvent(new TaskAttemptCompletedEvent(), null)
    );
    final EventHandler eventHandler = generateHeartbeat(events);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(1)).handle(arg.capture());
    final List<Event> argAllValues = arg.getAllValues();

    final Event statusUpdateEvent = argAllValues.get(0);
    assertEquals("only event should be task done", TaskAttemptEventType.TA_DONE,
        statusUpdateEvent.getType());
  }

  private EventHandler generateHeartbeat(List<TezEvent> events) throws IOException, TezException {
    ContainerId containerId = createContainerId(appId, 1);
    long requestId = 0;
    Vertex vertex = mock(Vertex.class);
    Task task = mock(Task.class);

    doReturn(vertex).when(dag).getVertex(vertexID);
    doReturn("test_vertex").when(vertex).getName();
    doReturn(task).when(vertex).getTask(taskID);

    doReturn(new ArrayList<TezEvent>()).when(task).getTaskAttemptTezEvents(taskAttemptID, 0, 1);

    taskAttemptListener.registerRunningContainer(containerId);
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId);

    TezHeartbeatRequest request = mock(TezHeartbeatRequest.class);
    doReturn(containerId.toString()).when(request).getContainerIdentifier();
    doReturn(taskAttemptID).when(request).getCurrentTaskAttemptID();
    doReturn(++requestId).when(request).getRequestId();
    doReturn(events).when(request).getEvents();

    taskAttemptListener.heartbeat(request);

    return eventHandler;
  }


  private ContainerId createContainerId(ApplicationId applicationId, long containerIdx) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    return ContainerId.newContainerId(appAttemptId, containerIdx);
  }

  private static class TaskAttemptListenerImplForTest extends TaskAttemptListenerImpTezDag {

    public TaskAttemptListenerImplForTest(AppContext context,
                                          TaskHeartbeatHandler thh,
                                          ContainerHeartbeatHandler chh,
                                          JobTokenSecretManager jobTokenSecretManager) {
      super(context, thh, chh, jobTokenSecretManager);
    }

    @Override
    protected void startRpcServer() {
    }

    @Override
    protected void stopRpcServer() {
    }
  }
}

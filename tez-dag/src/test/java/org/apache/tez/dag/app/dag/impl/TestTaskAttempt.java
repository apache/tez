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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
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
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminating;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventNodeFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.SpeculatorEventTaskAttemptStatusUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAEnded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTALaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTaskAttempt {

  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }

  @BeforeClass
  public static void setup() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @Test(timeout = 5000)
  public void testLocalityRequest() {
    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    Set<String> hosts = new TreeSet<String>();
    hosts.add("host1");
    hosts.add("host2");
    hosts.add("host3");
    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(hosts, null);

    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskAttemptListener.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), mock(AppContext.class),
        locationHint, false, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    TaskAttemptEventSchedule sEvent = mock(TaskAttemptEventSchedule.class);

    sta.transition(taImpl, sEvent);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(1)).handle(arg.capture());
    if (!(arg.getAllValues().get(0) instanceof AMSchedulerEventTALaunchRequest)) {
      fail("Second event not of type "
          + AMSchedulerEventTALaunchRequest.class.getName());
    }
    // TODO Move the Rack request check to the client after TEZ-125 is fixed.
    Set<String> requestedRacks = taImpl.taskRacks;
    assertEquals(1, requestedRacks.size());
    assertEquals(3, taImpl.taskHosts.size());
    for (int i = 0; i < 3; i++) {
      String host = ("host" + (i + 1));
      assertEquals(host, true, taImpl.taskHosts.contains(host));
    }
  }
  
  @Test(timeout = 5000)
  public void testPriority() {
    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskAttemptListener.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), mock(AppContext.class),
        null, false, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    TaskAttemptImpl taImplReScheduled = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskAttemptListener.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), mock(AppContext.class),
        null, true, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    TaskAttemptEventSchedule sEvent = mock(TaskAttemptEventSchedule.class);
    when(sEvent.getPriorityLowLimit()).thenReturn(3);
    when(sEvent.getPriorityHighLimit()).thenReturn(1);
    sta.transition(taImpl, sEvent);
    verify(eventHandler, times(1)).handle(arg.capture());
    AMSchedulerEventTALaunchRequest launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(2, launchEvent.getPriority());
    sta.transition(taImplReScheduled, sEvent);
    verify(eventHandler, times(2)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(1, launchEvent.getPriority());

    when(sEvent.getPriorityLowLimit()).thenReturn(6);
    when(sEvent.getPriorityHighLimit()).thenReturn(4);
    sta.transition(taImpl, sEvent);
    verify(eventHandler, times(3)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(5, launchEvent.getPriority());
    sta.transition(taImplReScheduled, sEvent);
    verify(eventHandler, times(4)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(4, launchEvent.getPriority());

    when(sEvent.getPriorityLowLimit()).thenReturn(5);
    when(sEvent.getPriorityHighLimit()).thenReturn(5);
    sta.transition(taImpl, sEvent);
    verify(eventHandler, times(5)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(5, launchEvent.getPriority());
    sta.transition(taImplReScheduled, sEvent);
    verify(eventHandler, times(6)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest)arg.getValue();
    Assert.assertEquals(5, launchEvent.getPriority());
  }

  @Test(timeout = 5000)
  // Tests that an attempt is made to resolve the localized hosts to racks.
  // TODO Move to the client post TEZ-125.
  public void testHostResolveAttempt() throws Exception {
    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    String hosts[] = new String[] { "127.0.0.1", "host2", "host3" };
    Set<String> resolved = new TreeSet<String>(
        Arrays.asList(new String[]{ "host1", "host2", "host3" }));
    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new TreeSet<String>(Arrays.asList(hosts)), null);

    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskAttemptListener.class), new Configuration(),
        new SystemClock(), mock(TaskHeartbeatHandler.class),
        mock(AppContext.class), locationHint, false, Resource.newInstance(1024,
            1), createFakeContainerContext(), false);

    TaskAttemptImpl spyTa = spy(taImpl);
    when(spyTa.resolveHosts(hosts)).thenReturn(
        resolved.toArray(new String[3]));

    TaskAttemptEventSchedule mockTAEvent = mock(TaskAttemptEventSchedule.class);

    sta.transition(spyTa, mockTAEvent);
    verify(spyTa).resolveHosts(hosts);
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(1)).handle(arg.capture());
    if (!(arg.getAllValues().get(0) instanceof AMSchedulerEventTALaunchRequest)) {
      fail("Second Event not of type ContainerRequestEvent");
    }
    Map<String, Boolean> expected = new HashMap<String, Boolean>();
    expected.put("host1", true);
    expected.put("host2", true);
    expected.put("host3", true);
    Set<String> requestedHosts = spyTa.taskHosts;
    for (String h : requestedHosts) {
      expected.remove(h);
    }
    assertEquals(0, expected.size());
  }

  @Test(timeout = 5000)
  // Ensure the dag does not go into an error state if a attempt kill is
  // received while STARTING
  public void testLaunchFailedWhileKilling() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    AppContext mockAppContext = mock(AppContext.class);
    doReturn(new ClusterInfo()).when(mockAppContext).getClusterInfo();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), mockAppContext, locationHint, false,
        resource, createFakeContainerContext(), false);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null,
        TaskAttemptTerminationCause.TERMINATED_BY_CLIENT));
    // At some KILLING state.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null,
        TaskAttemptTerminationCause.TERMINATED_BY_CLIENT));
    // taImpl.handle(new TaskAttemptEventContainerTerminating(taskAttemptID,
    // null));
    assertFalse(eventHandler.internalError);
  }

  @Test(timeout = 5000)
  // Ensure ContainerTerminating and ContainerTerminated is handled correctly by
  // the TaskAttempt
  public void testContainerTerminationWhileRunning() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEventContainerTerminating(taskAttemptID,
        "Terminating", TaskAttemptTerminationCause.APPLICATION_ERROR));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATING",
        eventHandler.internalError);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals("Task attempt is not in the  FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals("Terminating", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID,
        "Terminated", TaskAttemptTerminationCause.CONTAINER_EXITED));
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    int expectedEventAfterTerminated = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    assertEquals(2, taImpl.getDiagnostics().size());
    assertEquals("Terminated", taImpl.getDiagnostics().get(1));
    
    // check that original error cause is retained
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());
  }


  @Test(timeout = 5000)
  // Ensure ContainerTerminated is handled correctly by the TaskAttempt
  public void testContainerTerminatedWhileRunning() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID, "Terminated",
        TaskAttemptTerminationCause.CONTAINER_EXITED));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATED",
        eventHandler.internalError);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals("Terminated", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.CONTAINER_EXITED, taImpl.getTerminationCause());
    // TODO Ensure TA_TERMINATING after this is ingored.
  }

  @Test(timeout = 5000)
  // Ensure ContainerTerminating and ContainerTerminated is handled correctly by
  // the TaskAttempt
  public void testContainerTerminatedAfterSuccess() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID,
        "Terminated", TaskAttemptTerminationCause.CONTAINER_EXITED));
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    int expectedEventAfterTerminated = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    // Verify that the diagnostic message included in the Terminated event is not
    // captured - TA already succeeded. Error cause is the default value.
    assertEquals(0, taImpl.getDiagnostics().size());
    assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, taImpl.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testFailure() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 4;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(0,
            expectedEventsAtRunning), SpeculatorEventTaskAttemptStatusUpdate.class, 1);
    
    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null)));
    
    taImpl.handle(new TaskAttemptEventAttemptFailed(taskAttemptID, TaskAttemptEventType.TA_FAILED, "0",
        TaskAttemptTerminationCause.APPLICATION_ERROR));

    assertEquals("Task attempt is not in the  FAIL_IN_PROGRESS state", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_IN_PROGRESS);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals("0", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID, "1",
        TaskAttemptTerminationCause.CONTAINER_EXITED));
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    assertEquals(2, taImpl.getDiagnostics().size());
    assertEquals("1", taImpl.getDiagnostics().get(1));
    // err cause does not change
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 5;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());


    Event e = verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    assertEquals(TaskEventType.T_ATTEMPT_FAILED, e.getType());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), SpeculatorEventTaskAttemptStatusUpdate.class, 2);
  }
  
  @Test(timeout = 5000)
  public void testEventSerializingHash() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID1 = TezTaskID.getInstance(vertexID, 1);
    TezTaskID taskID2 = TezTaskID.getInstance(vertexID, 2);
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(taskID1, 0);
    TezTaskAttemptID taID12 = TezTaskAttemptID.getInstance(taskID1, 1);
    TezTaskAttemptID taID21 = TezTaskAttemptID.getInstance(taskID2, 1);
    
    TaskAttemptEvent taEventFail11 = new TaskAttemptEvent(taID11, TaskAttemptEventType.TA_FAILED);
    TaskAttemptEvent taEventKill11 = new TaskAttemptEvent(taID11, TaskAttemptEventType.TA_KILL_REQUEST);
    TaskAttemptEvent taEventKill12 = new TaskAttemptEvent(taID12, TaskAttemptEventType.TA_KILL_REQUEST);
    TaskAttemptEvent taEventKill21 = new TaskAttemptEvent(taID21, TaskAttemptEventType.TA_KILL_REQUEST);
    TaskEvent tEventKill1 = new TaskEvent(taskID1, TaskEventType.T_ATTEMPT_KILLED);
    TaskEvent tEventFail1 = new TaskEvent(taskID1, TaskEventType.T_ATTEMPT_FAILED);
    TaskEvent tEventFail2 = new TaskEvent(taskID2, TaskEventType.T_ATTEMPT_FAILED);
    
    // all of them should have the same value
    assertEquals(taEventFail11.getSerializingHash(), taEventKill11.getSerializingHash());
    assertEquals(taEventKill11.getSerializingHash(), taEventKill12.getSerializingHash());
    assertEquals(tEventFail1.getSerializingHash(), tEventKill1.getSerializingHash());
    assertEquals(taEventFail11.getSerializingHash(), tEventKill1.getSerializingHash());
    assertEquals(taEventKill21.getSerializingHash(), tEventFail2.getSerializingHash());
    // events from different tasks may not have the same value
    assertFalse(tEventFail1.getSerializingHash() == tEventFail2.getSerializingHash());
  }
  
  @Test(timeout = 5000)
  public void testSuccess() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 4;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(0,
            expectedEventsAtRunning), SpeculatorEventTaskAttemptStatusUpdate.class, 1);
    
    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null)));
    
    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 5;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());


    Event e = verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    assertEquals(TaskEventType.T_ATTEMPT_SUCCEEDED, e.getType());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), SpeculatorEventTaskAttemptStatusUpdate.class, 2);
  }
  
  @Test(timeout = 5000)
  // Ensure Container Preemption race with task completion is handled correctly by
  // the TaskAttempt
  public void testContainerPreemptedAfterSuccess() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);

    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEventsAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEventsAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEventsAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEventsAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM));
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    int expectedEventAfterTerminated = expectedEventsAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    // Verify that the diagnostic message included in the Terminated event is not
    // captured - TA already succeeded. Error cause should be the default value
    assertEquals(0, taImpl.getDiagnostics().size());
    assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, taImpl.getTerminationCause());
  }

  @Test(timeout = 5000)
  // Ensure node failure on Successful Non-Leaf tasks cause them to be marked as KILLED
  public void testNodeFailedNonLeafVertex() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    // Send out a Node Failure.
    taImpl.handle(new TaskAttemptEventNodeFailed(taskAttemptID, "NodeDecomissioned",
        TaskAttemptTerminationCause.NODE_FAILED));
    // Verify in KILLED state
    assertEquals("Task attempt is not in the  KILLED state", TaskAttemptState.KILLED,
        taImpl.getState());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    assertEquals(true, taImpl.inputFailedReported);
    // Verify one event to the Task informing it about FAILURE. No events to scheduler. Counter event.
    int expectedEventsNodeFailure = expectedEvenstAfterTerminating + 2;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsNodeFailure)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(expectedEvenstAfterTerminating,
            expectedEventsNodeFailure), TaskEventTAUpdate.class, 1);

    // Verify still in KILLED state
    assertEquals("Task attempt is not in the  KILLED state", TaskAttemptState.KILLED,
        taImpl.getState());
    assertEquals(TaskAttemptTerminationCause.NODE_FAILED, taImpl.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  // Ensure node failure on Successful Leaf tasks do not cause them to be marked as KILLED
  public void testNodeFailedLeafVertex() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), true);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    // Send out a Node Failure.
    taImpl.handle(new TaskAttemptEventNodeFailed(taskAttemptID, "NodeDecomissioned", 
        TaskAttemptTerminationCause.NODE_FAILED));

    // Verify no additional events
    int expectedEventsNodeFailure = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsNodeFailure)).handle(arg.capture());

    // Verify still in SUCCEEDED state
    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    // error cause remains as default value
    assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, taImpl.getTerminationCause());
  }

  @Test(timeout = 5000)
  // Verifies that multiple TooManyFetchFailures are handled correctly by the
  // TaskAttempt.
  public void testMultipleOutputFailed() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler mockEh = new MockEventHandler();
    MockEventHandler eventHandler = spy(mockEh);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    verify(mockHeartbeatHandler).register(taskAttemptID);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);

    int expectedEventsTillSucceeded = 6;
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsTillSucceeded)).handle(arg.capture());
    verifyEventType(arg.getAllValues(), TaskEventTAUpdate.class, 2);

    InputReadErrorEvent mockReEvent = InputReadErrorEvent.create("", 0, 1);
    EventMetaData mockMeta = mock(EventMetaData.class);
    TezTaskAttemptID mockDestId1 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId1);
    TezEvent tzEvent = new TezEvent(mockReEvent, mockMeta);
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 4));
    
    // failure threshold not met. state is SUCCEEDED
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

    // sending same error again doesnt change anything
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 4));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    // default value of error cause
    assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, taImpl.getTerminationCause());

    // different destination attempt reports error. now threshold crossed
    TezTaskAttemptID mockDestId2 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId2);    
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 4));
    
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals(TaskAttemptTerminationCause.OUTPUT_LOST, taImpl.getTerminationCause());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);

    assertEquals(true, taImpl.inputFailedReported);
    int expectedEventsAfterFetchFailure = expectedEventsTillSucceeded + 2;
    arg.getAllValues().clear();
    verify(eventHandler, times(expectedEventsAfterFetchFailure)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsTillSucceeded,
            expectedEventsAfterFetchFailure), TaskEventTAUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 1));
    assertEquals("Task attempt is not in FAILED state, still",
        taImpl.getState(), TaskAttemptState.FAILED);
    assertFalse(
        "InternalError occurred trying to handle TA_TOO_MANY_FETCH_FAILURES",
        eventHandler.internalError);
    // No new events.
    verify(eventHandler, times(expectedEventsAfterFetchFailure)).handle(
        arg.capture());
  }

  private Event verifyEventType(List<Event> events,
      Class<? extends Event> eventClass, int expectedOccurences) {
    int count = 0;
    Event ret = null;
    for (Event e : events) {
      if (eventClass.isInstance(e)) {
        count++;
        ret = e;
      }
    }
    assertEquals(
        "Mismatch in num occurences of event: " + eventClass.getCanonicalName(),
        expectedOccurences, count);
    return ret;
  }

  public static class MockEventHandler implements EventHandler {
    public boolean internalError;

    @Override
    public void handle(Event event) {
      if (event instanceof DAGEvent) {
        DAGEvent je = ((DAGEvent) event);
        if (DAGEventType.INTERNAL_ERROR == je.getType()) {
          internalError = true;
        }
      }
    }
  };

  private class MockTaskAttemptImpl extends TaskAttemptImpl {
    TaskLocationHint locationHint;

    public MockTaskAttemptImpl(TezTaskID taskId, int attemptNumber,
        EventHandler eventHandler, TaskAttemptListener tal,
        Configuration conf, Clock clock,
        TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
        TaskLocationHint locationHint,  boolean isRescheduled,
        Resource resource, ContainerContext containerContext, boolean leafVertex) {
      super(taskId, attemptNumber, eventHandler, tal, conf,
          clock, taskHeartbeatHandler, appContext,
          isRescheduled, resource, containerContext, leafVertex, mock(TaskImpl.class));
      this.locationHint = locationHint;
    }
    
    Vertex mockVertex = mock(Vertex.class);
    boolean inputFailedReported = false;
    
    @Override
    public TaskLocationHint getTaskLocationHint() {
      return locationHint;
    }
    
    @Override
    protected Vertex getVertex() {
      return mockVertex;
    }

    @Override
    protected TaskSpec createRemoteTaskSpec() {
      // FIXME
      return null;
    }

    @Override
    protected void logJobHistoryAttemptStarted() {
    }

    @Override
    protected void logJobHistoryAttemptFinishedEvent(
        TaskAttemptStateInternal state) {

    }

    @Override
    protected void logJobHistoryAttemptUnsuccesfulCompletion(
        TaskAttemptState state) {
    }
    
    @Override
    protected void sendInputFailedToConsumers() {
      inputFailedReported = true;
    }
  }

  private static ContainerContext createFakeContainerContext() {
    return new ContainerContext(new HashMap<String, LocalResource>(),
        new Credentials(), new HashMap<String, String>(), "");
  }
}

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

import org.apache.tez.dag.app.MockClock;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAStateUpdated;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSubmitted;
import org.apache.tez.dag.app.dag.event.TaskEventTAFailed;
import org.apache.tez.dag.app.dag.event.TaskEventTAKilled;
import org.apache.tez.dag.app.dag.event.TaskEventTASucceeded;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskCommunicatorWrapper;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Task;
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
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTezEventUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.SpeculatorEventTaskAttemptStatusUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAEnded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTALaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTaskAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskAttempt.class);

  static public class StubbedFS extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(1, false, 1, 1, 1, f);
    }
  }
  
  AppContext appCtx;
  TezConfiguration vertexConf = new TezConfiguration();
  TaskLocationHint locationHint;
  Vertex mockVertex;
  Task mockTask;
  ServicePluginInfo servicePluginInfo = new ServicePluginInfo()
      .setContainerLauncherName(TezConstants.getTezYarnServicePluginName());

  @BeforeClass
  public static void setup() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }
  
  @Before
  public void setupTest() {
    appCtx = mock(AppContext.class);
    when(appCtx.getAMConf()).thenReturn(new Configuration());
    when(appCtx.getContainerLauncherName(anyInt())).thenReturn(
        TezConstants.getTezYarnServicePluginName());

    createMockVertex(vertexConf);
    mockTask = mock(Task.class);
    when(mockTask.getVertex()).thenReturn(mockVertex);

    HistoryEventHandler mockHistHandler = mock(HistoryEventHandler.class);
    doReturn(mockHistHandler).when(appCtx).getHistoryHandler();
    LogManager.getRootLogger().setLevel(Level.DEBUG);
  }

  private void createMockVertex(Configuration conf) {
    mockVertex = mock(Vertex.class);
    when(mockVertex.getServicePluginInfo()).thenReturn(servicePluginInfo);
    when(mockVertex.getVertexConfig()).thenReturn(
        new VertexImpl.VertexConfigImpl(conf));
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
    locationHint = TaskLocationHint.createTaskLocationHint(hosts, null);

    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx,
        false, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

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
  public void testRetriesAtSamePriorityConfig() {

    // Override the test defaults to setup the config change
    TezConfiguration vertexConf = new TezConfiguration();
    vertexConf.setBoolean(TezConfiguration.TEZ_AM_TASK_RESCHEDULE_HIGHER_PRIORITY, false);
    vertexConf.setBoolean(TezConfiguration.TEZ_AM_TASK_RESCHEDULE_RELAXED_LOCALITY, true);
    when(mockVertex.getVertexConfig()).thenReturn(new VertexImpl.VertexConfigImpl(vertexConf));

    // set locality
    Set<String> hosts = new TreeSet<String>();
    hosts.add("host1");
    locationHint = TaskLocationHint.createTaskLocationHint(hosts, null);

    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx,
        false, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    TaskAttemptImpl taImplReScheduled = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx,
        true, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    TaskAttemptEventSchedule sEvent = mock(TaskAttemptEventSchedule.class);
    when(sEvent.getPriorityLowLimit()).thenReturn(3);
    when(sEvent.getPriorityHighLimit()).thenReturn(1);

    // Verify priority for a non-retried attempt
    sta.transition(taImpl, sEvent);
    verify(eventHandler, times(1)).handle(arg.capture());
    AMSchedulerEventTALaunchRequest launchEvent = (AMSchedulerEventTALaunchRequest) arg.getValue();
    Assert.assertEquals(2, launchEvent.getPriority());
    Assert.assertEquals(1, launchEvent.getLocationHint().getHosts().size());
    Assert.assertTrue(launchEvent.getLocationHint().getHosts().contains("host1"));

    // Verify priority for a retried attempt is the same
    sta.transition(taImplReScheduled, sEvent);
    verify(eventHandler, times(2)).handle(arg.capture());
    launchEvent = (AMSchedulerEventTALaunchRequest) arg.getValue();
    Assert.assertEquals(2, launchEvent.getPriority());
    Assert.assertNull(launchEvent.getLocationHint());
  }

  @Test(timeout = 5000)
  public void testPriority() {
    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx,
        false, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

    TaskAttemptImpl taImplReScheduled = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(), new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx,
        true, Resource.newInstance(1024, 1), createFakeContainerContext(), false);

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
    locationHint = TaskLocationHint.createTaskLocationHint(
        new TreeSet<String>(Arrays.asList(hosts)), null);

    TezTaskID taskID = TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance("1", 1, 1), 1), 1);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        mock(TaskCommunicatorManagerInterface.class), new Configuration(),
        new SystemClock(), mock(TaskHeartbeatHandler.class),
        appCtx, false, Resource.newInstance(1024,
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    AppContext mockAppContext = appCtx;
    doReturn(new ClusterInfo()).when(mockAppContext).getClusterInfo();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), mockAppContext, false,
        resource, createFakeContainerContext(), false);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null,
        TaskAttemptTerminationCause.TERMINATED_BY_CLIENT));
    assertEquals(TaskAttemptStateInternal.KILL_IN_PROGRESS, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEventTezEventUpdate(taImpl.getID(), Collections.EMPTY_LIST));
    assertFalse(
        "InternalError occurred trying to handle TA_TEZ_EVENT_UPDATE in KILL_IN_PROGRESS state",
        eventHandler.internalError);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    assertEquals("Task attempt is not in the STARTING state", taImpl.getState(),
        TaskAttemptState.STARTING);
    assertEquals("Task attempt internal state is not at SUBMITTED", taImpl.getInternalState(),
        TaskAttemptStateInternal.SUBMITTED);
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 5;
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

    Event event = verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAFailed.class, 1);
    TaskEventTAFailed failedEvent = (TaskEventTAFailed) event;
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID,
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

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    int expectedEventsAtRunning = 5;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID, "Terminated",
        TaskAttemptTerminationCause.CONTAINER_EXITED));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATED",
        eventHandler.internalError);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals("Terminated", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.CONTAINER_EXITED, taImpl.getTerminationCause());
    // TODO Ensure TA_TERMINATING after this is ingored.

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    Event event = verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAFailed.class, 1);
    TaskEventTAFailed failedEvent = (TaskEventTAFailed) event;
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID,
        "Terminated", TaskAttemptTerminationCause.CONTAINER_EXITED));
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    int expectedEventAfterTerminated = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 5;
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
            expectedEvenstAfterTerminating), TaskEventTASucceeded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID,
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
  public void testLastDataEventRecording() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    
    long ts1 = 1024;
    long ts2 = 2048;
    TezTaskAttemptID mockId1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID mockId2 = mock(TezTaskAttemptID.class);
    TezEvent mockTezEvent1 = mock(TezEvent.class, RETURNS_DEEP_STUBS);
    when(mockTezEvent1.getEventReceivedTime()).thenReturn(ts1);
    when(mockTezEvent1.getSourceInfo().getTaskAttemptID()).thenReturn(mockId1);
    TezEvent mockTezEvent2 = mock(TezEvent.class, RETURNS_DEEP_STUBS);
    when(mockTezEvent2.getEventReceivedTime()).thenReturn(ts2);
    when(mockTezEvent2.getSourceInfo().getTaskAttemptID()).thenReturn(mockId2);
    TaskAttemptEventStatusUpdate statusEvent =
        new TaskAttemptEventStatusUpdate(taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false));

    assertEquals(0, taImpl.lastDataEvents.size());
    taImpl.setLastEventSent(mockTezEvent1);
    assertEquals(1, taImpl.lastDataEvents.size());
    assertEquals(ts1, taImpl.lastDataEvents.get(0).getTimestamp());
    assertEquals(mockId1, taImpl.lastDataEvents.get(0).getTaskAttemptId());
    taImpl.handle(statusEvent);
    taImpl.setLastEventSent(mockTezEvent2);
    assertEquals(1, taImpl.lastDataEvents.size());
    assertEquals(ts2, taImpl.lastDataEvents.get(0).getTimestamp());
    assertEquals(mockId2, taImpl.lastDataEvents.get(0).getTaskAttemptId()); // over-write earlier value
    statusEvent.setReadErrorReported(true);
    taImpl.handle(statusEvent);
    taImpl.setLastEventSent(mockTezEvent1);
    assertEquals(2, taImpl.lastDataEvents.size());
    assertEquals(ts1, taImpl.lastDataEvents.get(1).getTimestamp());
    assertEquals(mockId1, taImpl.lastDataEvents.get(1).getTaskAttemptId()); // add new event
    taImpl.setLastEventSent(mockTezEvent2);
    assertEquals(2, taImpl.lastDataEvents.size());
    assertEquals(ts2, taImpl.lastDataEvents.get(1).getTimestamp());
    assertEquals(mockId2, taImpl.lastDataEvents.get(1).getTaskAttemptId()); // over-write earlier value
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 6;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(0,
            expectedEventsAtRunning), SpeculatorEventTaskAttemptStatusUpdate.class, 1);
    
    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    
    taImpl.handle(new TaskAttemptEventAttemptFailed(taskAttemptID, TaskAttemptEventType.TA_FAILED,
        TaskFailureType.NON_FATAL, "0",
        TaskAttemptTerminationCause.APPLICATION_ERROR));

    assertEquals("Task attempt is not in the  FAIL_IN_PROGRESS state", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_IN_PROGRESS);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals("0", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());

    assertEquals(TaskAttemptStateInternal.FAIL_IN_PROGRESS, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEventTezEventUpdate(taImpl.getID(), Collections.EMPTY_LIST));
    assertFalse(
        "InternalError occurred trying to handle TA_TEZ_EVENT_UPDATE in FAIL_IN_PROGRESS state",
        eventHandler.internalError);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID, "1",
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
            expectedEvenstAfterTerminating), TaskEventTAFailed.class, 1);
    TaskEventTAFailed failedEvent = (TaskEventTAFailed) e;
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());
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
  public void testFailureFatalError() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 6;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(0,
            expectedEventsAtRunning), SpeculatorEventTaskAttemptStatusUpdate.class, 1);

    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));

    taImpl.handle(new TaskAttemptEventAttemptFailed(taskAttemptID, TaskAttemptEventType.TA_FAILED,
        TaskFailureType.FATAL, "0",
        TaskAttemptTerminationCause.APPLICATION_ERROR));

    assertEquals("Task attempt is not in the  FAIL_IN_PROGRESS state", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_IN_PROGRESS);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals("0", taImpl.getDiagnostics().get(0));
    assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, taImpl.getTerminationCause());

    assertEquals(TaskAttemptStateInternal.FAIL_IN_PROGRESS, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEventTezEventUpdate(taImpl.getID(), Collections.EMPTY_LIST));
    assertFalse(
        "InternalError occurred trying to handle TA_TEZ_EVENT_UPDATE in FAIL_IN_PROGRESS state",
        eventHandler.internalError);

    taImpl.handle(new TaskAttemptEventContainerTerminated(contId, taskAttemptID, "1",
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
            expectedEvenstAfterTerminating), TaskEventTAFailed.class, 1);
    TaskEventTAFailed failedEvent = (TaskEventTAFailed) e;
    assertEquals(TaskFailureType.FATAL, failedEvent.getTaskFailureType());
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

  @Test
  public void testProgressTimeStampUpdate() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setLong(TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS, 75);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    Clock mockClock = mock(Clock.class);
    when(mockClock.getTime()).thenReturn(50l);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, mockClock,
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    when(mockClock.getTime()).thenReturn(100l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    verify(eventHandler, atLeast(1)).handle(arg.capture());
    if (arg.getValue() instanceof TaskAttemptEventAttemptFailed) {
      TaskAttemptEventAttemptFailed fEvent = (TaskAttemptEventAttemptFailed) arg.getValue();
      assertEquals(taImpl.getID(), fEvent.getTaskAttemptID());
      assertEquals(TaskAttemptTerminationCause.NO_PROGRESS, fEvent.getTerminationCause());
      taImpl.handle(fEvent);
      fail("Should not fail since the timestamps do not differ by progress interval config");
    } else {
      Assert.assertEquals("Task Attempt's internal state should be RUNNING!",
          taImpl.getInternalState(), TaskAttemptStateInternal.RUNNING);
    }
    when(mockClock.getTime()).thenReturn(200l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    verify(eventHandler, atLeast(1)).handle(arg.capture());
    Assert.assertTrue("This should have been an attempt failed event!", arg.getValue() instanceof TaskAttemptEventAttemptFailed);
  }

  @Test
  public void testStatusUpdateWithNullCounters() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(), TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    TezCounters counters = new TezCounters();
    counters.findCounter("group", "counter").increment(1);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(counters, 0.1f, null, false)));
    assertEquals(1, taImpl.getCounters().findCounter("group", "counter").getValue());
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    assertEquals(1, taImpl.getCounters().findCounter("group", "counter").getValue());
    counters.findCounter("group", "counter").increment(1);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(counters, 0.1f, null, false)));
    assertEquals(2, taImpl.getCounters().findCounter("group", "counter").getValue());
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    assertEquals(2, taImpl.getCounters().findCounter("group", "counter").getValue());
  }

  @Test (timeout = 60000L)
  public void testProgressAfterSubmit() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setLong(TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS, 50);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockClock mockClock = new MockClock();
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, mockClock,
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    mockClock.incrementTime(20L);
    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    mockClock.incrementTime(55L);
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    verify(eventHandler, atLeast(1)).handle(arg.capture());
    if (arg.getValue() instanceof  TaskAttemptEvent) {
      taImpl.handle((TaskAttemptEvent) arg.getValue());
    }
    Assert.assertEquals("Task Attempt's internal state should be SUBMITTED!",
        taImpl.getInternalState(), TaskAttemptStateInternal.SUBMITTED);
  }

  @Test (timeout = 5000)
  public void testNoProgressFail() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setLong(TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS, 75);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    Clock mockClock = mock(Clock.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, mockClock,
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);
    
    when(mockClock.getTime()).thenReturn(100l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, true)));
    // invocations and time updated
    assertEquals(100l, taImpl.lastNotifyProgressTimestamp);
    when(mockClock.getTime()).thenReturn(150l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, true)));
    // invocations and time updated
    assertEquals(150l, taImpl.lastNotifyProgressTimestamp);
    when(mockClock.getTime()).thenReturn(200l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    // invocations and time not updated
    assertEquals(150l, taImpl.lastNotifyProgressTimestamp);
    when(mockClock.getTime()).thenReturn(250l);
    taImpl.handle(new TaskAttemptEventStatusUpdate(
        taskAttemptID, new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    // invocations and time not updated
    assertEquals(150l, taImpl.lastNotifyProgressTimestamp);
    // failed event sent to self
    verify(eventHandler, atLeast(1)).handle(arg.capture());
    TaskAttemptEventAttemptFailed fEvent = (TaskAttemptEventAttemptFailed) arg.getValue();
    assertEquals(taImpl.getID(), fEvent.getTaskAttemptID());
    assertEquals(TaskAttemptTerminationCause.NO_PROGRESS, fEvent.getTerminationCause());
    assertEquals(TaskFailureType.NON_FATAL, fEvent.getTaskFailureType());
    taImpl.handle(fEvent);

    assertEquals("Task attempt is not in the  FAIL_IN_PROGRESS state", taImpl.getInternalState(),
        TaskAttemptStateInternal.FAIL_IN_PROGRESS);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals(TaskAttemptTerminationCause.NO_PROGRESS, taImpl.getTerminationCause());
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
  public void testCompletedAtSubmitted() throws ServicePluginException {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.STARTING);

    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtStarting = 4;
    verify(eventHandler, times(expectedEventsAtStarting)).handle(arg.capture());

    // Ensure status_updates are handled in the submitted state.
    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID,
        new TaskStatusUpdateEvent(null, 0.1f, null, false)));

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);
    assertEquals(0, taImpl.getDiagnostics().size());

    int expectedEvenstAfterTerminating = expectedEventsAtStarting + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());


    Event e = verifyEventType(
        arg.getAllValues().subList(expectedEventsAtStarting,
            expectedEvenstAfterTerminating), TaskEventTASucceeded.class, 1);
    assertEquals(TaskEventType.T_ATTEMPT_SUCCEEDED, e.getType());
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtStarting,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtStarting,
            expectedEvenstAfterTerminating), DAGEventCounterUpdate.class, 1);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);
    taskConf.setBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED, true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 6;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());
    verifyEventType(
        arg.getAllValues().subList(0,
            expectedEventsAtRunning), SpeculatorEventTaskAttemptStatusUpdate.class, 1);
    
    taImpl.handle(new TaskAttemptEventStatusUpdate(taskAttemptID, 
        new TaskStatusUpdateEvent(null, 0.1f, null, false)));
    
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
            expectedEvenstAfterTerminating), TaskEventTASucceeded.class, 1);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 5;
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
            expectedEventsAfterTerminating), TaskEventTASucceeded.class, 1);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 5;
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
            expectedEvenstAfterTerminating), TaskEventTASucceeded.class, 1);

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
            expectedEventsNodeFailure), TaskEventTAKilled.class, 1);

    // Verify still in KILLED state
    assertEquals("Task attempt is not in the  KILLED state", TaskAttemptState.KILLED,
        taImpl.getState());
    assertEquals(TaskAttemptTerminationCause.NODE_FAILED, taImpl.getTerminationCause());

    assertEquals(TaskAttemptStateInternal.KILLED, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEventTezEventUpdate(taImpl.getID(), Collections.EMPTY_LIST));
    assertFalse(
        "InternalError occurred trying to handle TA_TEZ_EVENT_UPDATE in KILLED state",
        eventHandler.internalError);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), true);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());
    verify(mockHeartbeatHandler).register(taskAttemptID);

    int expectedEventsAtRunning = 5;
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
            expectedEvenstAfterTerminating), TaskEventTASucceeded.class, 1);
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
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();
    HistoryEventHandler mockHistHandler = mock(HistoryEventHandler.class);
    doReturn(mockHistHandler).when(appCtx).getHistoryHandler();
    DAGImpl mockDAG = mock(DAGImpl.class);

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    verify(mockHeartbeatHandler).register(taskAttemptID);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);

    int expectedEventsTillSucceeded = 8;
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    ArgumentCaptor<DAGHistoryEvent> histArg = ArgumentCaptor.forClass(DAGHistoryEvent.class);
    verify(eventHandler, times(expectedEventsTillSucceeded)).handle(arg.capture());
    verify(mockHistHandler, times(2)).handle(histArg.capture()); // start and finish
    DAGHistoryEvent histEvent = histArg.getValue();
    TaskAttemptFinishedEvent finishEvent = (TaskAttemptFinishedEvent)histEvent.getHistoryEvent();
    long finishTime = finishEvent.getFinishTime();
    verifyEventType(arg.getAllValues(), TaskEventTAUpdate.class, 2);

    InputReadErrorEvent mockReEvent = InputReadErrorEvent.create("", 0, 1);
    EventMetaData mockMeta = mock(EventMetaData.class);
    TezTaskAttemptID mockDestId1 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId1);
    TezTaskID destTaskID = mock(TezTaskID.class);
    TezVertexID destVertexID = mock(TezVertexID.class);
    when(mockDestId1.getTaskID()).thenReturn(destTaskID);
    when(destTaskID.getVertexID()).thenReturn(destVertexID);
    Vertex destVertex = mock(VertexImpl.class);
    when(destVertex.getRunningTasks()).thenReturn(11);
    when(mockDAG.getVertex(destVertexID)).thenReturn(destVertex);
    when(appCtx.getCurrentDAG()).thenReturn(mockDAG);
    TezEvent tzEvent = new TezEvent(mockReEvent, mockMeta);
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 11));
    
    // failure threshold not met. state is SUCCEEDED
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

    // sending same error again doesnt change anything
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 11));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    // default value of error cause
    assertEquals(TaskAttemptTerminationCause.UNKNOWN_ERROR, taImpl.getTerminationCause());

    // different destination attempt reports error. now threshold crossed
    TezTaskAttemptID mockDestId2 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId2);
    destTaskID = mock(TezTaskID.class);
    destVertexID = mock(TezVertexID.class);
    when(mockDestId2.getTaskID()).thenReturn(destTaskID);
    when(destTaskID.getVertexID()).thenReturn(destVertexID);
    destVertex = mock(VertexImpl.class);
    when(destVertex.getRunningTasks()).thenReturn(11);
    when(mockDAG.getVertex(destVertexID)).thenReturn(destVertex);
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 11));
    
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    assertEquals(TaskAttemptTerminationCause.OUTPUT_LOST, taImpl.getTerminationCause());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID);
    verify(mockHistHandler, times(3)).handle(histArg.capture());
    histEvent = histArg.getValue();
    finishEvent = (TaskAttemptFinishedEvent)histEvent.getHistoryEvent();
    assertEquals(TaskFailureType.NON_FATAL, finishEvent.getTaskFailureType());
    long newFinishTime = finishEvent.getFinishTime();
    Assert.assertEquals(finishTime, newFinishTime);

    assertEquals(true, taImpl.inputFailedReported);
    int expectedEventsAfterFetchFailure = expectedEventsTillSucceeded + 2;
    arg.getAllValues().clear();
    verify(eventHandler, times(expectedEventsAfterFetchFailure)).handle(arg.capture());
    Event e = verifyEventType(
        arg.getAllValues().subList(expectedEventsTillSucceeded,
            expectedEventsAfterFetchFailure), TaskEventTAFailed.class, 1);
    TaskEventTAFailed failedEvent = (TaskEventTAFailed) e;
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());

    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 1));
    assertEquals("Task attempt is not in FAILED state, still",
        taImpl.getState(), TaskAttemptState.FAILED);
    assertFalse(
        "InternalError occurred trying to handle TA_TOO_MANY_FETCH_FAILURES",
        eventHandler.internalError);
    // No new events.
    verify(eventHandler, times(expectedEventsAfterFetchFailure)).handle(
        arg.capture());

    Configuration newVertexConf = new Configuration(vertexConf);
    newVertexConf.setInt(TezConfiguration.TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES,
        1);
    createMockVertex(newVertexConf);

    TezTaskID taskID2 = TezTaskID.getInstance(vertexID, 2);
    MockTaskAttemptImpl taImpl2 = new MockTaskAttemptImpl(taskID2, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID2 = taImpl2.getID();

    taImpl2.handle(new TaskAttemptEventSchedule(taskAttemptID2, 0, 0));
    taImpl2.handle(new TaskAttemptEventSubmitted(taskAttemptID2, contId));
    taImpl2.handle(new TaskAttemptEventStartedRemotely(taskAttemptID2));
    verify(mockHeartbeatHandler).register(taskAttemptID2);
    taImpl2.handle(new TaskAttemptEvent(taskAttemptID2, TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl2.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID2);

    mockReEvent = InputReadErrorEvent.create("", 1, 1);
    mockMeta = mock(EventMetaData.class);
    mockDestId1 = mock(TezTaskAttemptID.class);
    when(mockDestId1.getTaskID()).thenReturn(destTaskID);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId1);
    tzEvent = new TezEvent(mockReEvent, mockMeta);
    //This should fail even when MAX_ALLOWED_OUTPUT_FAILURES_FRACTION is within limits, as
    // MAX_ALLOWED_OUTPUT_FAILURES has crossed the limit.
    taImpl2.handle(new TaskAttemptEventOutputFailed(taskAttemptID2, tzEvent, 8));
    assertEquals("Task attempt is not in failed state", taImpl2.getState(),
        TaskAttemptState.FAILED);
    assertEquals(TaskAttemptTerminationCause.OUTPUT_LOST, taImpl2.getTerminationCause());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID2);

    Clock mockClock = mock(Clock.class); 
    int readErrorTimespanSec = 1;

    newVertexConf = new Configuration(vertexConf);
    newVertexConf.setInt(TezConfiguration.TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES,
        10);
    newVertexConf.setInt(
        TezConfiguration.TEZ_AM_MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC,
        readErrorTimespanSec);
    createMockVertex(newVertexConf);

    TezTaskID taskID3 = TezTaskID.getInstance(vertexID, 3);
    MockTaskAttemptImpl taImpl3 = new MockTaskAttemptImpl(taskID3, 1, eventHandler,
        taListener, taskConf, mockClock,
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID3 = taImpl3.getID();

    taImpl3.handle(new TaskAttemptEventSchedule(taskAttemptID3, 0, 0));
    taImpl3.handle(new TaskAttemptEventSubmitted(taskAttemptID3, contId));
    taImpl3.handle(new TaskAttemptEventStartedRemotely(taskAttemptID3));
    verify(mockHeartbeatHandler).register(taskAttemptID3);
    taImpl3.handle(new TaskAttemptEvent(taskAttemptID3, TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl3.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID3);

    mockReEvent = InputReadErrorEvent.create("", 1, 1);
    mockMeta = mock(EventMetaData.class);
    mockDestId1 = mock(TezTaskAttemptID.class);
    when(mockDestId1.getTaskID()).thenReturn(destTaskID);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId1);
    tzEvent = new TezEvent(mockReEvent, mockMeta);
    when(mockClock.getTime()).thenReturn(1000L);
    when(destVertex.getRunningTasks()).thenReturn(1000);
    // time deadline not exceeded for a couple of read error events
    taImpl3.handle(new TaskAttemptEventOutputFailed(taskAttemptID3, tzEvent, 1000));
    assertEquals("Task attempt is not in succeeded state", taImpl3.getState(),
        TaskAttemptState.SUCCEEDED);
    when(mockClock.getTime()).thenReturn(1500L);
    taImpl3.handle(new TaskAttemptEventOutputFailed(taskAttemptID3, tzEvent, 1000));
    assertEquals("Task attempt is not in succeeded state", taImpl3.getState(),
        TaskAttemptState.SUCCEEDED);
    // exceed the time threshold
    when(mockClock.getTime()).thenReturn(2001L);
    taImpl3.handle(new TaskAttemptEventOutputFailed(taskAttemptID3, tzEvent, 1000));
    assertEquals("Task attempt is not in FAILED state", taImpl3.getState(),
        TaskAttemptState.FAILED);
    assertEquals(TaskAttemptTerminationCause.OUTPUT_LOST, taImpl3.getTerminationCause());
    // verify unregister is not invoked again
    verify(mockHeartbeatHandler, times(1)).unregister(taskAttemptID3);
  }

  @Test(timeout = 60000)
  public void testTAFailureBasedOnRunningTasks() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler mockEh = new MockEventHandler();
    MockEventHandler eventHandler = spy(mockEh);
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    @SuppressWarnings("deprecation")
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();
    HistoryEventHandler mockHistHandler = mock(HistoryEventHandler.class);
    doReturn(mockHistHandler).when(appCtx).getHistoryHandler();
    DAGImpl mockDAG = mock(DAGImpl.class);

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), false);
    TezTaskAttemptID taskAttemptID = taImpl.getID();

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, 0, 0));
    taImpl.handle(new TaskAttemptEventSubmitted(taskAttemptID, contId));
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID));
    verify(mockHeartbeatHandler).register(taskAttemptID);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    verify(mockHeartbeatHandler).unregister(taskAttemptID);

    int expectedEventsTillSucceeded = 8;
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    ArgumentCaptor<DAGHistoryEvent> histArg = ArgumentCaptor.forClass(DAGHistoryEvent.class);
    verify(eventHandler, times(expectedEventsTillSucceeded)).handle(arg.capture());
    verify(mockHistHandler, times(2)).handle(histArg.capture()); // start and finish
    DAGHistoryEvent histEvent = histArg.getValue();
    TaskAttemptFinishedEvent finishEvent = (TaskAttemptFinishedEvent)histEvent.getHistoryEvent();
    long finishTime = finishEvent.getFinishTime();
    verifyEventType(arg.getAllValues(), TaskEventTAUpdate.class, 2);

    InputReadErrorEvent mockReEvent = InputReadErrorEvent.create("", 0, 1);
    EventMetaData mockMeta = mock(EventMetaData.class);
    TezTaskAttemptID mockDestId1 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId1);
    TezTaskID destTaskID = mock(TezTaskID.class);
    TezVertexID destVertexID = mock(TezVertexID.class);
    when(mockDestId1.getTaskID()).thenReturn(destTaskID);
    when(destTaskID.getVertexID()).thenReturn(destVertexID);
    Vertex destVertex = mock(VertexImpl.class);
    when(destVertex.getRunningTasks()).thenReturn(5);
    when(mockDAG.getVertex(destVertexID)).thenReturn(destVertex);
    when(appCtx.getCurrentDAG()).thenReturn(mockDAG);
    TezEvent tzEvent = new TezEvent(mockReEvent, mockMeta);
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 11));

    // failure threshold is met due to running tasks. state is FAILED
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
  }

  @SuppressWarnings("deprecation")
  @Test(timeout = 5000)
  public void testKilledInNew() throws ServicePluginException {
    ApplicationId appId = ApplicationId.newInstance(1, 2);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 0);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskCommunicatorManagerInterface taListener = createMockTaskAttemptListener();

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    locationHint = TaskLocationHint.createTaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[]{"127.0.0.1"})), null);
    Resource resource = Resource.newInstance(1024, 1);

    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appCtx);
    containers.addContainerIfNew(container, 0, 0, 0);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskHeartbeatHandler mockHeartbeatHandler = mock(TaskHeartbeatHandler.class);
    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mockHeartbeatHandler, appCtx, false,
        resource, createFakeContainerContext(), true);
    Assert.assertEquals(TaskAttemptStateInternal.NEW, taImpl.getInternalState());
    taImpl.handle(new TaskAttemptEventKillRequest(taImpl.getID(), "kill it",
        TaskAttemptTerminationCause.TERMINATED_BY_CLIENT));
    Assert.assertEquals(TaskAttemptStateInternal.KILLED, taImpl.getInternalState());

    Assert.assertEquals(0, taImpl.taskAttemptStartedEventLogged);
    Assert.assertEquals(1, taImpl.taskAttemptFinishedEventLogged);
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
  }

  private class MockTaskAttemptImpl extends TaskAttemptImpl {
    
    public int taskAttemptStartedEventLogged = 0;
    public int taskAttemptFinishedEventLogged = 0;
    public MockTaskAttemptImpl(TezTaskID taskId, int attemptNumber,
        EventHandler eventHandler, TaskCommunicatorManagerInterface tal,
        Configuration conf, Clock clock,
        TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
        boolean isRescheduled,
        Resource resource, ContainerContext containerContext, boolean leafVertex) {
      super(TezBuilderUtils.newTaskAttemptId(taskId, attemptNumber),
          eventHandler, tal, conf,
          clock, taskHeartbeatHandler, appContext,
          isRescheduled, resource, containerContext, leafVertex, mockTask,
          locationHint, null, null);
    }
    
    boolean inputFailedReported = false;
    
    @Override
    protected Vertex getVertex() {
      return mockVertex;
    }

    @Override
    protected void logJobHistoryAttemptStarted() {
      taskAttemptStartedEventLogged++;
      super.logJobHistoryAttemptStarted();
    }

    @Override
    protected void logJobHistoryAttemptFinishedEvent(
        TaskAttemptStateInternal state) {
      taskAttemptFinishedEventLogged++;
      super.logJobHistoryAttemptFinishedEvent(state);
    }

    @Override
    protected void logJobHistoryAttemptUnsuccesfulCompletion(
        TaskAttemptState state, TaskFailureType taskFailureType) {
      taskAttemptFinishedEventLogged++;
      super.logJobHistoryAttemptUnsuccesfulCompletion(state, taskFailureType);
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

  private TaskCommunicatorManagerInterface createMockTaskAttemptListener() throws
      ServicePluginException {
    TaskCommunicatorManagerInterface taListener = mock(TaskCommunicatorManagerInterface.class);
    TaskCommunicator taskComm = mock(TaskCommunicator.class);
    doReturn(new InetSocketAddress("localhost", 0)).when(taskComm).getAddress();
    doReturn(new TaskCommunicatorWrapper(taskComm)).when(taListener).getTaskCommunicator(0);
    return taListener;
  }
}

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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
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
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminating;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventNodeFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAEnded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTALaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
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

  // @Test
  // // Verifies # tasks, attempts and diagnostics for a failing job.
  // // TODO Move to TestTask - to verify # retries
  // public void testMRAppHistoryForMap() throws Exception {
  // MRApp app = new FailingAttemptsMRApp(1, 0);
  // testMRAppHistory(app);
  // }
  //
  // @Test
  // // Verifies # tasks, attempts and diagnostics for a failing job.
  // // Move to TestTask - to verify # retries
  // public void testMRAppHistoryForReduce() throws Exception {
  // MRApp app = new FailingAttemptsMRApp(0, 1);
  // testMRAppHistory(app);
  // }

  @Test(timeout = 5000)
  public void testLocalityRequest() {

    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    Set<String> hosts = new TreeSet<String>();
    hosts.add("host1");
    hosts.add("host2");
    hosts.add("host3");
    TaskLocationHint locationHint = new TaskLocationHint(hosts, null);

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
  // Tests that an attempt is made to resolve the localized hosts to racks.
  // TODO Move to the client post TEZ-125.
  public void testHostResolveAttempt() throws Exception {
    TaskAttemptImpl.ScheduleTaskattemptTransition sta =
        new TaskAttemptImpl.ScheduleTaskattemptTransition();

    EventHandler eventHandler = mock(EventHandler.class);
    String hosts[] = new String[] { "127.0.0.1", "host2", "host3" };
    Set<String> resolved = new TreeSet<String>(
        Arrays.asList(new String[]{ "host1", "host2", "host3" }));
    TaskLocationHint locationHint = new TaskLocationHint(
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

  // @Test(timeout = 5000)
  // // Verifies accounting of slot_milli counters. Time spent in running tasks.
  // // TODO Fix this test to work without MRApp.
  // public void testSlotMillisCounterUpdate() throws Exception {
  // verifySlotMillis(2048, 2048, 1024);
  // verifySlotMillis(2048, 1024, 1024);
  // verifySlotMillis(10240, 1024, 2048);
  // }

  // public void verifySlotMillis(int mapMemMb, int reduceMemMb,
  // int minContainerSize) throws Exception {
  // Clock actualClock = new SystemClock();
  // ControlledClock clock = new ControlledClock(actualClock);
  // clock.setTime(10);
  // MRApp app =
  // new MRApp(1, 1, false, "testSlotMillisCounterUpdate", true, clock);
  // Configuration conf = new Configuration();
  // conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemMb);
  // conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, reduceMemMb);
  // app.setClusterInfo(new ClusterInfo(Resource.newInstance(minContainerSize, 1),
  // Resource.newInstance(10240,1)));
  //
  // Job job = app.submit(conf);
  // app.waitForState(job, JobState.RUNNING);
  // Map<TaskId, Task> tasks = job.getTasks();
  // Assert.assertEquals("Num tasks is not correct", 2, tasks.size());
  // Iterator<Task> taskIter = tasks.values().iterator();
  // Task mTask = taskIter.next();
  // app.waitForState(mTask, TaskState.RUNNING);
  // Task rTask = taskIter.next();
  // app.waitForState(rTask, TaskState.RUNNING);
  // Map<TaskAttemptId, TaskAttempt> mAttempts = mTask.getAttempts();
  // Assert.assertEquals("Num attempts is not correct", 1, mAttempts.size());
  // Map<TaskAttemptId, TaskAttempt> rAttempts = rTask.getAttempts();
  // Assert.assertEquals("Num attempts is not correct", 1, rAttempts.size());
  // TaskAttempt mta = mAttempts.values().iterator().next();
  // TaskAttempt rta = rAttempts.values().iterator().next();
  // app.waitForState(mta, TaskAttemptState.RUNNING);
  // app.waitForState(rta, TaskAttemptState.RUNNING);
  //
  // clock.setTime(11);
  // app.getContext()
  // .getEventHandler()
  // .handle(new TaskAttemptEvent(mta.getID(), TaskAttemptEventType.TA_DONE));
  // app.getContext()
  // .getEventHandler()
  // .handle(new TaskAttemptEvent(rta.getID(), TaskAttemptEventType.TA_DONE));
  // app.waitForState(job, JobState.SUCCEEDED);
  // Assert.assertEquals(mta.getFinishTime(), 11);
  // Assert.assertEquals(mta.getLaunchTime(), 10);
  // Assert.assertEquals(rta.getFinishTime(), 11);
  // Assert.assertEquals(rta.getLaunchTime(), 10);
  // Assert.assertEquals((int) Math.ceil((float) mapMemMb / minContainerSize),
  // job.getAllCounters().findCounter(JobCounter.SLOTS_MILLIS_MAPS)
  // .getValue());
  // Assert.assertEquals(
  // (int) Math.ceil((float) reduceMemMb / minContainerSize), job
  // .getAllCounters().findCounter(JobCounter.SLOTS_MILLIS_REDUCES)
  // .getValue());
  // }
  //

  // private void testMRAppHistory(MRApp app) throws Exception {
  // Configuration conf = new Configuration();
  // Job job = app.submit(conf);
  // app.waitForState(job, JobState.FAILED);
  // Map<TaskId, Task> tasks = job.getTasks();
  //
  // Assert.assertEquals("Num tasks is not correct", 1, tasks.size());
  // Task task = tasks.values().iterator().next();
  // Assert.assertEquals("Task state not correct", TaskState.FAILED, task
  // .getReport().getTaskState());
  // Map<TaskAttemptId, TaskAttempt> attempts = tasks.values().iterator().next()
  // .getAttempts();
  // Assert.assertEquals("Num attempts is not correct", 4, attempts.size());
  //
  // Iterator<TaskAttempt> it = attempts.values().iterator();
  // TaskAttemptReport report = it.next().getReport();
  // Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
  // report.getTaskAttemptState());
  // Assert.assertEquals("Diagnostic Information is not Correct",
  // "Test Diagnostic Event", report.getDiagnosticInfo());
  // report = it.next().getReport();
  // Assert.assertEquals("Attempt state not correct", TaskAttemptState.FAILED,
  // report.getTaskAttemptState());
  // }

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

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, Priority
        .newInstance(3)));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null));
    // At some KILLING state.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null));
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEventContainerTerminating(taskAttemptID,
        "Terminating"));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATING",
        eventHandler.internalError);

    assertEquals("Task attempt is not in the  FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);

    assertEquals(1, taImpl.getDiagnostics().size());
    assertEquals("Terminating", taImpl.getDiagnostics().get(0));

    int expectedEvenstAfterTerminating = expectedEventsAtRunning + 3;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEvenstAfterTerminating)).handle(arg.capture());

    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), TaskEventTAUpdate.class, 1);
    verifyEventType(
        arg.getAllValues().subList(expectedEventsAtRunning,
            expectedEvenstAfterTerminating), AMSchedulerEventTAEnded.class, 1);

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID,
        "Terminated"));
    int expectedEventAfterTerminated = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    assertEquals(2, taImpl.getDiagnostics().size());
    assertEquals("Terminated", taImpl.getDiagnostics().get(1));
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID, "Terminated"));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATED",
        eventHandler.internalError);

    assertEquals("Terminated", taImpl.getDiagnostics().get(0));

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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

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

    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID,
        "Terminated"));
    int expectedEventAfterTerminated = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    // Verify that the diagnostic message included in the Terminated event is not
    // captured - TA already succeeded.
    assertEquals(0, taImpl.getDiagnostics().size());
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", taImpl.getState(),
        TaskAttemptState.RUNNING);

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

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

    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM));
    int expectedEventAfterTerminated = expectedEventsAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventAfterTerminated)).handle(arg.capture());

    // Verify that the diagnostic message included in the Terminated event is not
    // captured - TA already succeeded.
    assertEquals(0, taImpl.getDiagnostics().size());
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());

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

    // Send out a Node Failure.
    taImpl.handle(new TaskAttemptEventNodeFailed(taskAttemptID, "NodeDecomissioned"));
    // Verify in KILLED state
    assertEquals("Task attempt is not in the  KILLED state", TaskAttemptState.KILLED,
        taImpl.getState());
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler eventHandler = spy(new MockEventHandler());
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), true);

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    assertEquals("Task attempt is not in the RUNNING state", TaskAttemptState.RUNNING,
        taImpl.getState());

    int expectedEventsAtRunning = 3;
    verify(eventHandler, times(expectedEventsAtRunning)).handle(arg.capture());

    taImpl.handle(new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE));

    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());

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

    // Send out a Node Failure.
    taImpl.handle(new TaskAttemptEventNodeFailed(taskAttemptID, "NodeDecomissioned"));

    // Verify no additional events
    int expectedEventsNodeFailure = expectedEvenstAfterTerminating + 0;
    arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsNodeFailure)).handle(arg.capture());

    // Verify still in SUCCEEDED state
    assertEquals("Task attempt is not in the  SUCCEEDED state", TaskAttemptState.SUCCEEDED,
        taImpl.getState());
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
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 0);

    MockEventHandler mockEh = new MockEventHandler();
    MockEventHandler eventHandler = spy(mockEh);
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    Configuration taskConf = new Configuration();
    taskConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    taskConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new HashSet<String>(Arrays.asList(new String[] {"127.0.0.1"})), null);
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

    MockTaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, taskConf, new SystemClock(),
        mock(TaskHeartbeatHandler.class), appCtx, locationHint, false,
        resource, createFakeContainerContext(), false);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null));
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);

    int expectedEventsTillSucceeded = 6;
    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(expectedEventsTillSucceeded)).handle(arg.capture());
    verifyEventType(arg.getAllValues(), TaskEventTAUpdate.class, 2);

    InputReadErrorEvent mockReEvent = new InputReadErrorEvent("", 0, 1);
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

    // different destination attempt reports error. now threshold crossed
    TezTaskAttemptID mockDestId2 = mock(TezTaskAttemptID.class);
    when(mockMeta.getTaskAttemptID()).thenReturn(mockDestId2);    
    taImpl.handle(new TaskAttemptEventOutputFailed(taskAttemptID, tzEvent, 4));
    
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);

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

  private void verifyEventType(List<Event> events,
      Class<? extends Event> eventClass, int expectedOccurences) {
    int count = 0;
    for (Event e : events) {
      if (eventClass.isInstance(e)) {
        count++;
      }
    }
    assertEquals(
        "Mismatch in num occurences of event: " + eventClass.getCanonicalName(),
        expectedOccurences, count);
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
          isRescheduled, resource, containerContext, leafVertex);
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

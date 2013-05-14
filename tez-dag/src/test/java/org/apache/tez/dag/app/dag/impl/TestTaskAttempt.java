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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tez.common.TezTaskContext;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.records.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventKillRequest;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestTaskAttempt {

  private static final String MAP_PROCESSOR_NAME =
      "org.apache.tez.mapreduce.processor.map.MapProcessor";

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

  // @Test
  // // Verifies that the launch request is based on the hosts.
  // // TODO Move to the client.
  // // TODO Add a test that verifies that the LocationHint is used as it should
  // be.
  // public void testSingleRackRequest() throws Exception {
  // TaskAttemptImpl.ScheduleTaskattemptTransition sta =
  // new TaskAttemptImpl.ScheduleTaskattemptTransition();
  //
  // EventHandler eventHandler = mock(EventHandler.class);
  // String[] hosts = new String[3];
  // hosts[0] = "host1";
  // hosts[1] = "host2";
  // hosts[2] = "host3";
  // TaskSplitMetaInfo splitInfo = new TaskSplitMetaInfo(hosts, 0,
  // 128 * 1024 * 1024l);
  //
  // TaskAttemptImpl mockTaskAttempt = createMapTaskAttemptImpl2ForTest(
  // eventHandler, splitInfo);
  // TaskAttemptEventSchedule mockTAEvent =
  // mock(TaskAttemptEventSchedule.class);
  // doReturn(false).when(mockTAEvent).isRescheduled();
  //
  // sta.transition(mockTaskAttempt, mockTAEvent);
  //
  // ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
  // verify(eventHandler, times(2)).handle(arg.capture());
  // if (!(arg.getAllValues().get(1) instanceof
  // AMSchedulerTALaunchRequestEvent)) {
  // Assert.fail("Second Event not of type ContainerRequestEvent");
  // }
  // AMSchedulerTALaunchRequestEvent tlrE = (AMSchedulerTALaunchRequestEvent)
  // arg
  // .getAllValues().get(1);
  // String[] requestedRacks = tlrE.getRacks();
  // // Only a single occurrence of /DefaultRack
  // assertEquals(1, requestedRacks.length);
  // }

  // @Test
  // // Tests that an attempt is made to resolve the localized hosts to racks.
  // // TODO Move to the client.
  // public void testHostResolveAttempt() throws Exception {
  // TaskAttemptImpl.ScheduleTaskattemptTransition sta =
  // new TaskAttemptImpl.ScheduleTaskattemptTransition();
  //
  // EventHandler eventHandler = mock(EventHandler.class);
  // String hosts[] = new String[] {"192.168.1.1", "host2", "host3"};
  // String resolved[] = new String[] {"host1", "host2", "host3"};
  // TaskSplitMetaInfo splitInfo =
  // new TaskSplitMetaInfo(hosts, 0, 128 * 1024 * 1024l);
  //
  // TaskAttemptImpl mockTaskAttempt =
  // createMapTaskAttemptImpl2ForTest(eventHandler, splitInfo);
  // TaskAttemptImpl spyTa = spy(mockTaskAttempt);
  // when(spyTa.resolveHosts(hosts)).thenReturn(resolved);
  //
  // TaskAttemptEventSchedule mockTAEvent =
  // mock(TaskAttemptEventSchedule.class);
  // doReturn(false).when(mockTAEvent).isRescheduled();
  //
  // sta.transition(spyTa, mockTAEvent);
  // verify(spyTa).resolveHosts(hosts);
  // ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
  // verify(eventHandler, times(2)).handle(arg.capture());
  // if (!(arg.getAllValues().get(1) instanceof
  // AMSchedulerTALaunchRequestEvent)) {
  // Assert.fail("Second Event not of type ContainerRequestEvent");
  // }
  // Map<String, Boolean> expected = new HashMap<String, Boolean>();
  // expected.put("host1", true);
  // expected.put("host2", true);
  // expected.put("host3", true);
  // AMSchedulerTALaunchRequestEvent cre =
  // (AMSchedulerTALaunchRequestEvent) arg.getAllValues().get(1);
  // String[] requestedHosts = cre.getHosts();
  // for (String h : requestedHosts) {
  // expected.remove(h);
  // }
  // assertEquals(0, expected.size());
  // }
  //

  // @Test
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
  // app.setClusterInfo(new ClusterInfo(BuilderUtils
  // .newResource(minContainerSize, 1), BuilderUtils.newResource(10240,1)));
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

  @Test
  // Ensure the dag does not go into an error state if a attempt kill is
  // received while STARTING
  public void testLaunchFailedWhileKilling() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 0);
    TezDAGID dagID = new TezDAGID(appId, 1);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    TezTaskID taskID = new TezTaskID(vertexID, 1);
    TezTaskAttemptID taskAttemptID = new TezTaskAttemptID(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    tezConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new String[] { "127.0.0.1" }, null);
    Resource resource = BuilderUtils.newResource(1024, 1);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Map<String, String> environment = new HashMap<String, String>();
    String javaOpts = "";

    AppContext mockAppContext = mock(AppContext.class);
    doReturn(new ClusterInfo()).when(mockAppContext).getClusterInfo();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, 1, tezConf, mock(Token.class), new Credentials(),
        new SystemClock(), mock(TaskHeartbeatHandler.class), mockAppContext,
        MAP_PROCESSOR_NAME, locationHint, resource, localResources,
        environment, javaOpts, false);

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, BuilderUtils
        .newPriority(3)));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null));
    // At some KILLING state.
    taImpl.handle(new TaskAttemptEventKillRequest(taskAttemptID, null));
    // taImpl.handle(new TaskAttemptEventContainerTerminating(taskAttemptID,
    // null));
    assertFalse(eventHandler.internalError);
  }

  // TODO Add a similar test for TERMINATING.
  // Ensure ContainerTerminated is handled correctly by the TaskAttempt
  @Test
  public void testContainerTerminatedWhileRunning() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 0);
    TezDAGID dagID = new TezDAGID(appId, 1);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    TezTaskID taskID = new TezTaskID(vertexID, 1);
    TezTaskAttemptID taskAttemptID = new TezTaskAttemptID(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    tezConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new String[] { "127.0.0.1" }, null);
    Resource resource = BuilderUtils.newResource(1024, 1);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Map<String, String> environment = new HashMap<String, String>();
    String javaOpts = "";

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, 1, tezConf, mock(Token.class), new Credentials(),
        new SystemClock(), mock(TaskHeartbeatHandler.class), appCtx,
        MAP_PROCESSOR_NAME, locationHint, resource, localResources,
        environment, javaOpts, false);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null, -1));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID, null));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATED",
        eventHandler.internalError);
    // TODO Verify diagnostics
  }

  @Test
  // Ensure ContainerTerminated is handled correctly by the TaskAttempt
  public void testContainerTerminatedWhileCommitting() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 0);
    TezDAGID dagID = new TezDAGID(appId, 1);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    TezTaskID taskID = new TezTaskID(vertexID, 1);
    TezTaskAttemptID taskAttemptID = new TezTaskAttemptID(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    tezConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new String[] { "127.0.0.1" }, null);
    Resource resource = BuilderUtils.newResource(1024, 1);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Map<String, String> environment = new HashMap<String, String>();
    String javaOpts = "";

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, 1, tezConf, mock(Token.class), new Credentials(),
        new SystemClock(), mock(TaskHeartbeatHandler.class), appCtx,
        MAP_PROCESSOR_NAME, locationHint, resource, localResources,
        environment, javaOpts, false);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null, -1));
    assertEquals("Task attempt is not in running state", taImpl.getState(),
        TaskAttemptState.RUNNING);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_COMMIT_PENDING));
    assertEquals("Task attempt is not in commit pending state",
        taImpl.getState(), TaskAttemptState.COMMIT_PENDING);
    taImpl.handle(new TaskAttemptEventContainerTerminated(taskAttemptID, null));
    assertFalse(
        "InternalError occurred trying to handle TA_CONTAINER_TERMINATED",
        eventHandler.internalError);
    // TODO Verify diagnostics
  }

  @Test
  // Verifies that multiple TooManyFetchFailures are handled correctly by the
  // TaskAttempt.
  public void testMultipleTooManyFetchFailures() throws Exception {
    ApplicationId appId = BuilderUtils.newApplicationId(1, 2);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        appId, 0);
    TezDAGID dagID = new TezDAGID(appId, 1);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    TezTaskID taskID = new TezTaskID(vertexID, 1);
    TezTaskAttemptID taskAttemptID = new TezTaskAttemptID(taskID, 0);

    MockEventHandler eventHandler = new MockEventHandler();
    TaskAttemptListener taListener = mock(TaskAttemptListener.class);
    when(taListener.getAddress()).thenReturn(
        new InetSocketAddress("localhost", 0));

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setClass("fs.file.impl", StubbedFS.class, FileSystem.class);
    tezConf.setBoolean("fs.file.impl.disable.cache", true);

    TaskLocationHint locationHint = new TaskLocationHint(
        new String[] { "127.0.0.1" }, null);
    Resource resource = BuilderUtils.newResource(1024, 1);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Map<String, String> environment = new HashMap<String, String>();
    String javaOpts = "";

    NodeId nid = BuilderUtils.newNodeId("127.0.0.1", 0);
    ContainerId contId = BuilderUtils.newContainerId(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");

    AppContext appCtx = mock(AppContext.class);
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        appCtx);
    containers.addContainerIfNew(container);

    doReturn(new ClusterInfo()).when(appCtx).getClusterInfo();
    doReturn(containers).when(appCtx).getAllContainers();

    TaskAttemptImpl taImpl = new MockTaskAttemptImpl(taskID, 1, eventHandler,
        taListener, 1, tezConf, mock(Token.class), new Credentials(),
        new SystemClock(), mock(TaskHeartbeatHandler.class), appCtx,
        MAP_PROCESSOR_NAME, locationHint, resource, localResources,
        environment, javaOpts, false);

    taImpl.handle(new TaskAttemptEventSchedule(taskAttemptID, null));
    // At state STARTING.
    taImpl.handle(new TaskAttemptEventStartedRemotely(taskAttemptID, contId,
        null, -1));
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_DONE));
    assertEquals("Task attempt is not in succeeded state", taImpl.getState(),
        TaskAttemptState.SUCCEEDED);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES));
    assertEquals("Task attempt is not in FAILED state", taImpl.getState(),
        TaskAttemptState.FAILED);
    taImpl.handle(new TaskAttemptEvent(taskAttemptID,
        TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURES));
    assertEquals("Task attempt is not in FAILED state, still",
        taImpl.getState(), TaskAttemptState.FAILED);
    assertFalse(
        "InternalError occurred trying to handle TA_TOO_MANY_FETCH_FAILURES",
        eventHandler.internalError);
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

    public MockTaskAttemptImpl(TezTaskID taskId, int attemptNumber,
        EventHandler eventHandler, TaskAttemptListener tal, int partition,
        TezConfiguration conf, Token<JobTokenIdentifier> jobToken,
        Credentials credentials, Clock clock,
        TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
        String processorName, TaskLocationHint locationHint, Resource resource,
        Map<String, LocalResource> localResources,
        Map<String, String> environment, String javaOpts, boolean isRescheduled) {
      super(taskId, attemptNumber, eventHandler, tal, partition, conf,
          jobToken, credentials, clock, taskHeartbeatHandler, appContext,
          processorName, locationHint, resource, localResources, environment,
          javaOpts, isRescheduled);
    }

    @Override
    protected TezTaskContext createRemoteTask() {
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

  }
}

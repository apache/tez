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

package org.apache.tez.dag.app.rm;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tez.common.TezUtils;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientAsyncForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AlwaysMatchesContainerMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.CapturingEventHandler;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerContextDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerManagerForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableContext;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.app.rm.node.AMNodeTracker;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestContainerReuse {
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerReuse.class);

  @BeforeClass
  public static void setup() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @Test(timeout = 15000l)
  public void testDelayedReuseContainerBecomesAvailable()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testDelayedReuseContainerBecomesAvailable");
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 3000l);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(conf).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskCommunicatorManagerInterface.class),
      new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
        new TaskSchedulerManagerForTest(
          appContext, eventHandler, rmClient,
          new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(conf));
    TaskSchedulerManager taskSchedulerManager =
        spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(conf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler =
      (TaskSchedulerWithDrainableContext)
        ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback =
      taskScheduler.getDrainableAppCallback();

    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(5);
    String [] host1 = {"host1"};
    String [] host2 = {"host2"};

    String [] defaultRack = {"/default-rack"};

    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(
      TezTaskID.getInstance(vertexID, 1), 1);
    TezTaskAttemptID taID21 = TezTaskAttemptID.getInstance(
      TezTaskID.getInstance(vertexID, 2), 1);
    TezTaskAttemptID taID31 = TezTaskAttemptID.getInstance(
      TezTaskID.getInstance(vertexID, 3), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    TaskAttempt ta31 = mock(TaskAttempt.class);

    AMSchedulerEventTALaunchRequest lrTa11 =
      createLaunchRequestEvent(taID11, ta11, resource, host1,
        defaultRack, priority);
    AMSchedulerEventTALaunchRequest lrTa21 =
      createLaunchRequestEvent(taID21, ta21, resource, host2,
        defaultRack, priority);
    AMSchedulerEventTALaunchRequest lrTa31 =
      createLaunchRequestEvent(taID31, ta31, resource, host1,
        defaultRack, priority);

    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrTa11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrTa21);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(
      Lists.newArrayList(containerHost1, containerHost2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(
      eq(0), eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerManager).taskAllocated(
      eq(0), eq(ta21), any(Object.class), eq(containerHost2));

    // Adding the event later so that task1 assigned to containerHost1
    // is deterministic.
    taskSchedulerManager.handleEvent(lrTa31);

    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(
            ta11, containerHost1.getId(), TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta11, true, null, null);
    verify(taskSchedulerManager, times(1)).taskAllocated(
      eq(0), eq(ta31), any(Object.class), eq(containerHost1));
    verify(rmClient, times(0)).releaseAssignedContainer(
      eq(containerHost1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    taskSchedulerManager.handleEvent(
      new AMSchedulerEventTAEnded(ta21, containerHost2.getId(),
        TaskAttemptState.SUCCEEDED, null, null, 0));

    long currentTs = System.currentTimeMillis();
    Throwable exception = null;
    while (System.currentTimeMillis() < currentTs + 5000l) {
      try {
        verify(taskSchedulerManager,
          times(1)).containerBeingReleased(eq(0), eq(containerHost2.getId()));
        exception = null;
        break;
      } catch (Throwable e) {
        exception = e;
      }
    }
    assertTrue("containerHost2 was not released", exception == null);
    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 15000l)
  public void testDelayedReuseContainerNotAvailable()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testDelayedReuseContainerNotAvailable");
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 1000l);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskCommunicatorManagerInterface.class),
      new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
      new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient,
        new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(conf));
    TaskSchedulerManager taskSchedulerManager =
      spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(conf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler =
      (TaskSchedulerWithDrainableContext)
        ((TaskSchedulerManagerForTest) taskSchedulerManager)
          .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback =
      taskScheduler.getDrainableAppCallback();
    
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(5);
    String [] host1 = {"host1"};
    String [] host2 = {"host2"};

    String [] defaultRack = {"/default-rack"};

    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID, 1), 1);
    TezTaskAttemptID taID21 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID, 2), 1);
    TezTaskAttemptID taID31 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID, 3), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    TaskAttempt ta31 = mock(TaskAttempt.class);

    AMSchedulerEventTALaunchRequest lrTa11 = createLaunchRequestEvent(
      taID11, ta11, resource, host1, defaultRack, priority);
    AMSchedulerEventTALaunchRequest lrTa21 = createLaunchRequestEvent(
      taID21, ta21, resource, host2, defaultRack, priority);
    AMSchedulerEventTALaunchRequest lrTa31 = createLaunchRequestEvent(
      taID31, ta31, resource, host1, defaultRack, priority);

    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrTa11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrTa21);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Lists.newArrayList(containerHost1, containerHost2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta21), any(Object.class),
        eq(containerHost2));

    // Adding the event later so that task1 assigned to containerHost1 is deterministic.
    taskSchedulerManager.handleEvent(lrTa31);

    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta21, containerHost2.getId(),
            TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta21, true, null, null);
    verify(taskSchedulerManager, times(0)).taskAllocated(
        eq(0), eq(ta31), any(Object.class), eq(containerHost2));
    verify(rmClient, times(1)).releaseAssignedContainer(
      eq(containerHost2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 10000l)
  public void testSimpleReuse() throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testSimpleReuse");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class), new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager
        taskSchedulerManagerReal = new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient, new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager = spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler = (TaskSchedulerWithDrainableContext) ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};
    String[] host2 = {"host2"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    TezVertexID vertexID1 = TezVertexID.getInstance(dagID, 1);

    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent1 = createLaunchRequestEvent(taID11, ta11, resource1, host1, racks, priority1);

    //Vertex 1, Task 2, Attempt 1, host1
    TezTaskAttemptID taID12 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 2), 1);
    TaskAttempt ta12 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent2 = createLaunchRequestEvent(taID12, ta12, resource1, host1, racks, priority1);

    //Vertex 1, Task 3, Attempt 1, host2
    TezTaskAttemptID taID13 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 3), 1);
    TaskAttempt ta13 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent3 = createLaunchRequestEvent(taID13, ta13, resource1, host2, racks, priority1);

    //Vertex 1, Task 4, Attempt 1, host2
    TezTaskAttemptID taID14 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 4), 1);
    TaskAttempt ta14 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent4 = createLaunchRequestEvent(taID14, ta14, resource1, host2, racks, priority1);

    taskSchedulerManager.handleEvent(lrEvent1);
    taskSchedulerManager.handleEvent(lrEvent2);
    taskSchedulerManager.handleEvent(lrEvent3);
    taskSchedulerManager.handleEvent(lrEvent4);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta11), any(Object.class),
        eq(container1));

    // Task assigned to container completed successfully. Container should be re-used.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta11, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta11, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta12, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta12, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta13), any(Object.class),
        eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Verify no re-use if a previous task fails.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta13, container1.getId(), TaskAttemptState.FAILED, null,
            "TIMEOUT", 0));
    drainableAppCallback.drain();
    verify(taskSchedulerManager, times(0)).taskAllocated(eq(0), eq(ta14), any(Object.class),
        eq(container1));
    verifyDeAllocateTask(taskScheduler, ta13, false, null, "TIMEOUT");
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    Container container2 = createContainer(2, "host2", resource1, priority1);

    // Second container allocated. Should be allocated to the last task.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta14), any(Object.class),
        eq(container2));

    // Task assigned to container completed successfully. No pending requests. Container should be released.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta14, container2.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta14, true, null, null);
    verify(rmClient).releaseAssignedContainer(eq(container2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 10000l)
  public void testReuseWithTaskSpecificLaunchCmdOption() throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testReuseWithTaskSpecificLaunchCmdOption");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);
    //Profile 3 tasks
    tezConf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, "v1[1,3,4]");
    tezConf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, "dir=/tmp/__VERTEX_NAME__/__TASK_INDEX__");
    TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption =  new TaskSpecificLaunchCmdOption(tezConf);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class), new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    //Use ContainerContextMatcher here.  Otherwise it would not match the JVM options
    TaskSchedulerManager taskSchedulerManagerReal =
        new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient, new ContainerContextMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager = spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler =
        (TaskSchedulerWithDrainableContext) ((TaskSchedulerManagerForTest) taskSchedulerManager)
          .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};
    String[] host2 = {"host2"};
    String[] host3 = {"host3"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    TezVertexID vertexID1 = TezVertexID.getInstance(dagID, 1);
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    String tsLaunchCmdOpts = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 1);

    /**
     * Schedule 2 tasks (1 with additional launch-cmd option and another in normal mode).
     * Container should not be reused in this case.
     */
    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent1 =
        createLaunchRequestEvent(taID11, ta11, resource1, host1, racks,
          priority1, localResources, tsLaunchCmdOpts);

    //Vertex 1, Task 2, Attempt 1, host1
    TezTaskAttemptID taID12 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 2), 1);
    TaskAttempt ta12 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent2 =
        createLaunchRequestEvent(taID12, ta12, resource1, host1, racks, priority1);

    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent1);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent2);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta11), any(Object.class),
        eq(container1));

    // First task had profiling on. This container can not be reused further.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta11, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta11, true, null, null);
    verify(taskSchedulerManager, times(0)).taskAllocated(eq(0), eq(ta12), any(Object.class),
        eq(container1));
    verify(rmClient, times(1)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    /**
     * Schedule 2 tasks (both having different task specific JVM option).
     * Container should not be reused.
     */
    //Vertex 1, Task 3, Attempt 1, host2
    tsLaunchCmdOpts = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 3);
    TezTaskAttemptID taID13 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 3), 1);
    TaskAttempt ta13 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent3 =
        createLaunchRequestEvent(taID13, ta13, resource1, host2, racks,
            priority1, localResources, tsLaunchCmdOpts);

    //Vertex 1, Task 4, Attempt 1, host2
    tsLaunchCmdOpts = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 4);
    TezTaskAttemptID taID14 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 4), 1);
    TaskAttempt ta14 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent4 =
        createLaunchRequestEvent(taID14, ta14, resource1, host2, racks,
            priority1, localResources, tsLaunchCmdOpts);

    Container container2 = createContainer(2, "host2", resource1, priority1);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent3);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent4);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    // Container started
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta13), any(Object.class), eq(container2));

    // Verify that the container can not be reused when profiling option is turned on
    // Even for 2 tasks having same profiling option can have container reusability.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta13, container2.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta13, true, null, null);
    verify(taskSchedulerManager, times(0)).taskAllocated(eq(0), eq(ta14), any(Object.class),
        eq(container2));
    verify(rmClient, times(1)).releaseAssignedContainer(eq(container2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    /**
     * Schedule 2 tasks with same jvm profiling option.
     * Container should be reused.
     */
    tezConf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, "v1[1,2,3,5,6]");
    tezConf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, "dummyOpts");
    taskSpecificLaunchCmdOption =  new TaskSpecificLaunchCmdOption(tezConf);

    //Vertex 1, Task 5, Attempt 1, host3
    TezTaskAttemptID taID15 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 3), 1);
    TaskAttempt ta15 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent5 =
        createLaunchRequestEvent(taID15, ta15, resource1, host3, racks,
            priority1, localResources,
            taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 5));

    //Vertex 1, Task 6, Attempt 1, host3
    tsLaunchCmdOpts = taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 4);
    TezTaskAttemptID taID16 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 4), 1);
    TaskAttempt ta16 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent6 =
        createLaunchRequestEvent(taID16, ta16, resource1, host3, racks,
          priority1, localResources, taskSpecificLaunchCmdOption.getTaskSpecificOption("", "v1", 6));

    // Container started
    Container container3 = createContainer(2, "host3", resource1, priority1);
    taskSchedulerManager.handleEvent(lrEvent5);
    taskSchedulerManager.handleEvent(lrEvent6);

    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container3));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta15), any(Object.class),
        eq(container3));

    //Ensure task 6 (of vertex 1) is allocated to same container
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta15, container3.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta15, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta16), any(Object.class), eq(container3));
    eventHandler.reset();

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 30000l)
  public void testReuseNonLocalRequest()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testReuseNonLocalRequest");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100l);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 1000l);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 1000l);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
        spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class),
        new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
        new TaskSchedulerManagerForTest(
          appContext, eventHandler, rmClient,
          new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager =
        spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler =
      (TaskSchedulerWithDrainableContext)
        ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback =
      taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String [] emptyHosts = new String[0];
    String [] racks = { "default-rack" };

    Priority priority = Priority.newInstance(3);

    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);

    //Vertex 1, Task 1, Attempt 1, no locality information.
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    doReturn(vertexID).when(ta11).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(
      taID11, ta11, resource1, emptyHosts, racks, priority);

    //Vertex1, Task2, Attempt 1,  no locality information.
    TezTaskAttemptID taID12 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID, 2), 1);
    TaskAttempt ta12 = mock(TaskAttempt.class);
    doReturn(vertexID).when(ta12).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent12 = createLaunchRequestEvent(
      taID12, ta12, resource1, emptyHosts, racks, priority);

    // Send launch request for task 1 only, deterministic assignment to this task.
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container container1 = createContainer(1, "randomHost", resource1, priority);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(
        eq(0), eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerManager.handleEvent(lrEvent12);

    // Task assigned to container completed successfully.
    // Container should not be immediately assigned to task 2
    // until delay expires.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta11, container1.getId(),
            TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta11, true, null, null);
    verify(taskSchedulerManager, times(0)).taskAllocated(
        eq(0), eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(3000l);
    verify(taskSchedulerManager).taskAllocated(
        eq(0), eq(ta12), any(Object.class), eq(container1));

    // TA12 completed.
    taskSchedulerManager.handleEvent(
      new AMSchedulerEventTAEnded(ta12, container1.getId(),
        TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(3000l);
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 30000l)
  public void testReuseAcrossVertices()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testReuseAcrossVertices");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setLong(
        TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 1l);
    tezConf.setLong(
        TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 2000l);
    tezConf.setInt(
        TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, 1);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskCommunicatorManagerInterface.class),
      new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(true).when(appContext).isSession();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
      new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient,
        new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager =
      spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler =
      (TaskSchedulerWithDrainableContext)
        ((TaskSchedulerManagerForTest) taskSchedulerManager)
          .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(3);
    Priority priority2 = Priority.newInstance(4);

    TezVertexID vertexID1 = TezVertexID.getInstance(dagID, 1);
    TezVertexID vertexID2 = TezVertexID.getInstance(dagID, 2);

    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(
      TezTaskID.getInstance(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    doReturn(vertexID1).when(ta11).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(
        taID11, ta11, resource1, host1, racks, priority1);

    //Vertex2, Task1, Attempt 1, host1
    TezTaskAttemptID taID21 = TezTaskAttemptID.getInstance(
      TezTaskID.getInstance(vertexID2, 1), 1);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    doReturn(vertexID2).when(ta21).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent21 = createLaunchRequestEvent(
      taID21, ta21, resource1, host1, racks, priority2);

    // Send launch request for task 1 onle, deterministic assignment to this task.
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container container1 = createContainer(1, host1[0], resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(
        eq(0), eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerManager.handleEvent(lrEvent21);

    // Task assigned to container completed successfully.
    // Container should  be assigned to task21.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta11, container1.getId(),
            TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta11, true, null, null);
    verify(taskSchedulerManager).taskAllocated(
        eq(0), eq(ta21), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));

    // Task 2 completes.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta21, container1.getId(),
            TaskAttemptState.SUCCEEDED, null, null, 0));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));

    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(3000l);
    // container should not get released due to min held containers
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }
  
  @Test(timeout = 30000l)
  public void testReuseLocalResourcesChanged() throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testReuseLocalResourcesChanged");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, -1);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID1 = TezDAGID.getInstance("0", 1, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    ChangingDAGIDAnswer dagIDAnswer = new ChangingDAGIDAnswer(dagID1);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class), new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(true).when(appContext).isSession();
    doAnswer(dagIDAnswer).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();
    
    TaskSchedulerManager
        taskSchedulerManagerReal = new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient, new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager = spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler = (TaskSchedulerWithDrainableContext) ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    String rsrc1 = "rsrc1";
    String rsrc2 = "rsrc2";
    String rsrc3 = "rsrc3";
    LocalResource lr1 = mock(LocalResource.class);
    LocalResource lr2 = mock(LocalResource.class);
    LocalResource lr3 = mock(LocalResource.class);

    AMContainerEventAssignTA assignEvent = null;
    
    Map<String, LocalResource> dag1LRs = Maps.newHashMap();
    dag1LRs.put(rsrc1, lr1);

    TezVertexID vertexID11 = TezVertexID.getInstance(dagID1, 1);
    
    //Vertex 1, Task 1, Attempt 1, host1, lr1
    TezTaskAttemptID taID111 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID11, 1), 1);
    TaskAttempt ta111 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(taID111, ta111, resource1, host1, racks, priority1, dag1LRs);

    //Vertex 1, Task 2, Attempt 1, host1, lr1
    TezTaskAttemptID taID112 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID11, 2), 1);
    TaskAttempt ta112 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent12 = createLaunchRequestEvent(taID112, ta112, resource1, host1, racks, priority1, dag1LRs);

    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent12);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta111), any(Object.class), eq(container1));
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());
    
    // Task assigned to container completed successfully. Container should be re-used.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta111, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta111, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta112), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta112, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta112, true, null, null);
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Setup DAG2 with additional resources. Make sure the container, even without all resources, is reused.
    TezDAGID dagID2 = TezDAGID.getInstance("0", 2, 0);
    dagIDAnswer.setDAGID(dagID2);
    
    Map<String, LocalResource> dag2LRs = Maps.newHashMap();
    dag2LRs.put(rsrc2, lr2);
    dag2LRs.put(rsrc3, lr3);

    TezVertexID vertexID21 = TezVertexID.getInstance(dagID2, 1);
    
    //Vertex 2, Task 1, Attempt 1, host1, lr2
    TezTaskAttemptID taID211 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID21, 1), 1);
    TaskAttempt ta211 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent21 = createLaunchRequestEvent(taID211, ta211, resource1, host1, racks, priority1, dag2LRs);

    //Vertex 2, Task 2, Attempt 1, host1, lr2
    TezTaskAttemptID taID212 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID21, 2), 1);
    TaskAttempt ta212 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent22 = createLaunchRequestEvent(taID212, ta212, resource1, host1, racks, priority1, dag2LRs);

    taskSchedulerManager.handleEvent(lrEvent21);
    taskSchedulerManager.handleEvent(lrEvent22);
    drainableAppCallback.drain();

    // TODO This is terrible, need a better way to ensure the scheduling loop has run
    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(6000l);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta211), any(Object.class),
        eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(2, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta211, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta211, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta212), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(2, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 30000l)
  public void testReuseConflictLocalResources() throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testReuseLocalResourcesChanged");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, -1);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID1 = TezDAGID.getInstance("0", 1, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    ChangingDAGIDAnswer dagIDAnswer = new ChangingDAGIDAnswer(dagID1);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class), new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(true).when(appContext).isSession();
    doAnswer(dagIDAnswer).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
        new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient,
            new ContainerContextMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager = spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler = (TaskSchedulerWithDrainableContext) ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    String rsrc1 = "rsrc1";
    String rsrc2 = "rsrc2";
    LocalResource lr1 = mock(LocalResource.class);
    LocalResource lr2 = mock(LocalResource.class);
    LocalResource lr3 = mock(LocalResource.class);

    AMContainerEventAssignTA assignEvent = null;

    Map<String, LocalResource> v11LR = Maps.newHashMap();
    v11LR.put(rsrc1, lr1);

    TezVertexID vertexID11 = TezVertexID.getInstance(dagID1, 1);

    //Vertex 1, Task 1, Attempt 1, host1, lr1
    TezTaskAttemptID taID111 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID11, 1), 1);
    TaskAttempt ta111 = mock(TaskAttempt.class);
    doReturn(taID111).when(ta111).getID();
    doReturn("Mock for TA " + taID111.toString()).when(ta111).toString();
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(taID111, ta111, resource1, host1, racks, priority1, v11LR);

    Map<String, LocalResource> v12LR = Maps.newHashMap();
    v12LR.put(rsrc1, lr1);
    v12LR.put(rsrc2, lr2);

    //Vertex 1, Task 2, Attempt 1, host1, lr1
    TezTaskAttemptID taID112 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID11, 2), 1);
    TaskAttempt ta112 = mock(TaskAttempt.class);
    doReturn(taID112).when(ta112).getID();
    doReturn("Mock for TA " + taID112.toString()).when(ta112).toString();
    AMSchedulerEventTALaunchRequest lrEvent12 = createLaunchRequestEvent(taID112, ta112, resource1, host1, racks, priority1, v12LR);

    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent11);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainNotifier.set(false);
    taskSchedulerManager.handleEvent(lrEvent12);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);

    Container container1 = createContainer(1, "host1", resource1, priority1);
    Container container2 = createContainer(2, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta111), any(Object.class), eq(container1));
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());

    // Task assigned to container completed successfully. Container should be re-used.
    taskSchedulerManager.handleEvent(
        new AMSchedulerEventTAEnded(ta111, container1.getId(), TaskAttemptState.SUCCEEDED, null,
            null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta111, true, null, null);
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta112), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerManager.handleEvent(new AMSchedulerEventTAEnded(ta112, container1.getId(), TaskAttemptState.SUCCEEDED, null, null, 0));
    drainableAppCallback.drain();
    verifyDeAllocateTask(taskScheduler, ta112, true, null, null);
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Setup DAG2 with additional resources. Make sure the container, even without all resources, is reused.
    TezDAGID dagID2 = TezDAGID.getInstance("0", 2, 0);
    dagIDAnswer.setDAGID(dagID2);

    Map<String, LocalResource> v21LR = Maps.newHashMap();
    v21LR.put(rsrc1, lr1);
    v21LR.put(rsrc2, lr3);

    TezVertexID vertexID21 = TezVertexID.getInstance(dagID2, 1);

    //Vertex 2, Task 1, Attempt 1, host1, lr2
    TezTaskAttemptID taID211 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID21, 1), 1);
    TaskAttempt ta211 = mock(TaskAttempt.class);
    doReturn(taID211).when(ta211).getID();
    doReturn("Mock for TA " + taID211.toString()).when(ta211).toString();
    AMSchedulerEventTALaunchRequest lrEvent21 = createLaunchRequestEvent(taID211, ta211, resource1,
        host1, racks, priority1, v21LR);

    taskSchedulerManager.handleEvent(lrEvent21);
    drainableAppCallback.drain();

    // TODO This is terrible, need a better way to ensure the scheduling loop has run
    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(6000l);
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));

    Thread.sleep(6000l);
    verify(rmClient, times(1)).releaseAssignedContainer(eq(container1.getId()));
    verify(taskSchedulerManager).taskAllocated(eq(0), eq(ta211), any(Object.class), eq(container2));
    eventHandler.reset();

    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  @Test(timeout = 10000l)
  public void testAssignmentOnShutdown()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Test testAssignmentOnShutdown");
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));

    AppContext appContext = mock(AppContext.class);
    doReturn(new Configuration(false)).when(appContext).getAMConf();
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskCommunicatorManagerInterface.class), new ContainerContextMatcher(), appContext);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    doReturn(DAGAppMasterState.SUCCEEDED).when(appContext).getAMState();
    doReturn(true).when(appContext).isAMInCompletionState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerManager taskSchedulerManagerReal =
        new TaskSchedulerManagerForTest(appContext, eventHandler, rmClient,
            new AlwaysMatchesContainerMatcher(), TezUtils.createUserPayloadFromConf(tezConf));
    TaskSchedulerManager taskSchedulerManager = spy(taskSchedulerManagerReal);
    taskSchedulerManager.init(tezConf);
    taskSchedulerManager.start();

    TaskSchedulerWithDrainableContext taskScheduler = (TaskSchedulerWithDrainableContext)
        ((TaskSchedulerManagerForTest) taskSchedulerManager)
        .getSpyTaskScheduler();
    TaskSchedulerContextDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    taskScheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};
    String[] host2 = {"host2"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    TezVertexID vertexID1 = TezVertexID.getInstance(dagID, 1);

    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent1 = createLaunchRequestEvent(taID11, ta11, resource1,
        host1, racks, priority1);
    taskSchedulerManager.handleEvent(lrEvent1);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(taskSchedulerManager, times(0)).taskAllocated(eq(0), eq(ta11),
        any(Object.class), eq(container1));
    taskScheduler.shutdown();
    taskSchedulerManager.close();
  }

  private Container createContainer(int id, String host, Resource resource, Priority priority) {
    @SuppressWarnings("deprecation")
    ContainerId containerID = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1),
        id);
    NodeId nodeID = NodeId.newInstance(host, 0);
    Container container = Container.newInstance(containerID, nodeID, host + ":0",
        resource, priority, null);
    return container;
  }

  private AMSchedulerEventTALaunchRequest createLaunchRequestEvent(
    TezTaskAttemptID taID, TaskAttempt ta, Resource capability,
    String[] hosts, String[] racks, Priority priority,
    ContainerContext containerContext) {
    TaskLocationHint locationHint = null;
    if (hosts != null || racks != null) {
      Set<String> hostsSet = Sets.newHashSet(hosts);
      Set<String> racksSet = Sets.newHashSet(racks);
      locationHint = TaskLocationHint.createTaskLocationHint(hostsSet, racksSet);
    }
    AMSchedulerEventTALaunchRequest lr = new AMSchedulerEventTALaunchRequest(
      taID, capability, new TaskSpec(taID, "dagName", "vertexName", -1,
        ProcessorDescriptor.create("processorClassName"),
      Collections.singletonList(new InputSpec("vertexName",
          InputDescriptor.create("inputClassName"), 1)),
      Collections.singletonList(new OutputSpec("vertexName",
          OutputDescriptor.create("outputClassName"), 1)), null), ta, locationHint,
      priority.getPriority(), containerContext, 0, 0, 0);
    return lr;
  }

  private AMSchedulerEventTALaunchRequest createLaunchRequestEvent(TezTaskAttemptID taID,
      TaskAttempt ta, Resource capability, String[] hosts, String[] racks, Priority priority) {
    return createLaunchRequestEvent(taID, ta, capability, hosts, racks, priority,
        new HashMap<String, LocalResource>());
  }

  private AMSchedulerEventTALaunchRequest createLaunchRequestEvent(TezTaskAttemptID taID,
      TaskAttempt ta, Resource capability, String[] hosts, String[] racks, Priority priority,
      Map<String, LocalResource> localResources) {
    return createLaunchRequestEvent(taID, ta, capability, hosts, racks, priority,
        localResources, "");
  }

  private AMSchedulerEventTALaunchRequest createLaunchRequestEvent(TezTaskAttemptID taID,
      TaskAttempt ta, Resource capability, String[] hosts, String[] racks, Priority priority,
      Map<String, LocalResource> localResources, String jvmOpts) {
    return createLaunchRequestEvent(taID, ta, capability, hosts, racks, priority,
        new ContainerContext(localResources, new Credentials(), new HashMap<String, String>(), jvmOpts));
  }

  private static class ChangingDAGIDAnswer implements Answer<TezDAGID> {

    private TezDAGID dagID;

    public ChangingDAGIDAnswer(TezDAGID dagID) {
      this.dagID = dagID;
    }

    public void setDAGID(TezDAGID dagID) {
      this.dagID = dagID;
    }

    @Override
    public TezDAGID answer(InvocationOnMock invocation) throws Throwable {
      return this.dagID;
    }
  }

  private void verifyDeAllocateTask(TaskScheduler taskScheduler, Object ta, boolean taskSucceeded,
                                    TaskAttemptEndReason endReason, String diagContains) {
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    try {
      verify(taskScheduler)
          .deallocateTask(eq(ta), eq(taskSucceeded), eq(endReason), argumentCaptor.capture());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertEquals(1, argumentCaptor.getAllValues().size());
    if (diagContains == null) {
      assertNull(argumentCaptor.getValue());
    } else {
      assertTrue(argumentCaptor.getValue().contains(diagContains));
    }
  }
}

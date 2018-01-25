/*
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.MockClock;
import org.apache.tez.dag.app.rm.DagAwareYarnTaskScheduler.AMRMClientAsyncWrapper;
import org.apache.tez.dag.app.rm.DagAwareYarnTaskScheduler.HeldContainer;
import org.apache.tez.dag.app.rm.DagAwareYarnTaskScheduler.TaskRequest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerContextDrainable;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AppFinalStatus;
import org.apache.tez.test.ControlledScheduledExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.createCountingExecutingService;
import static org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.setupMockTaskSchedulerContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDagAwareYarnTaskScheduler {
  private ExecutorService contextCallbackExecutor;

  @BeforeClass
  public static void beforeClass() {

    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @Before
  public void preTest() {
    contextCallbackExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("TaskSchedulerAppCallbackExecutor #%d")
            .setDaemon(true)
            .build());
  }

  @After
  public void postTest() {
    contextCallbackExecutor.shutdownNow();
  }

  private TaskSchedulerContextDrainable createDrainableContext(
      TaskSchedulerContext taskSchedulerContext) {
    TaskSchedulerContextImplWrapper wrapper =
        new TaskSchedulerContextImplWrapper(taskSchedulerContext,
            createCountingExecutingService(contextCallbackExecutor));
    return new TaskSchedulerContextDrainable(wrapper);
  }

  @SuppressWarnings({ "unchecked" })
  @Test(timeout=30000)
  public void testNoReuse() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Object mockTask1 = new MockTask("task1");
    Object mockCookie1 = new Object();
    Resource mockCapability = Resources.createResource(1024, 1);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    Priority mockPriority = Priority.newInstance(1);
    ArgumentCaptor<TaskRequest> requestCaptor =
        ArgumentCaptor.forClass(TaskRequest.class);
    // allocate task
    scheduler.allocateTask(mockTask1, mockCapability, hosts,
        racks, mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
        addContainerRequest(any(TaskRequest.class));

    // returned from task requests before allocation happens
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
        removeContainerRequest(any(TaskRequest.class));
    verify(mockRMClient, times(0)).
        releaseAssignedContainer((ContainerId) any());

    // deallocating unknown task
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
        removeContainerRequest(any(TaskRequest.class));
    verify(mockRMClient, times(0)).
        releaseAssignedContainer((ContainerId) any());

    // allocate tasks
    Object mockTask2 = new MockTask("task2");
    Object mockCookie2 = new Object();
    Object mockTask3 = new MockTask("task3");
    Object mockCookie3 = new Object();
    scheduler.allocateTask(mockTask1, mockCapability, hosts,
        racks, mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).
        addContainerRequest(requestCaptor.capture());
    TaskRequest request1 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask2, mockCapability, hosts,
        racks, mockPriority, null, mockCookie2);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
        addContainerRequest(requestCaptor.capture());
    TaskRequest request2 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask3, mockCapability, hosts,
        racks, mockPriority, null, mockCookie3);
    drainableAppCallback.drain();
    verify(mockRMClient, times(4)).
        addContainerRequest(requestCaptor.capture());
    TaskRequest request3 = requestCaptor.getValue();

    NodeId host1 = NodeId.newInstance("host1", 1);
    NodeId host2 = NodeId.newInstance("host2", 2);
    NodeId host3 = NodeId.newInstance("host3", 3);
    NodeId host4 = NodeId.newInstance("host4", 4);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId1 = ContainerId.newContainerId(attemptId, 1);
    Container mockContainer1 = Container.newInstance(mockCId1, host1, null, mockCapability, mockPriority, null);
    ContainerId mockCId2 = ContainerId.newContainerId(attemptId, 2);
    Container mockContainer2 = Container.newInstance(mockCId2, host2, null, mockCapability, mockPriority, null);
    ContainerId mockCId3 = ContainerId.newContainerId(attemptId, 3);
    Container mockContainer3 = Container.newInstance(mockCId3, host3, null, mockCapability, mockPriority, null);
    ContainerId mockCId4 = ContainerId.newContainerId(attemptId, 4);
    Container mockContainer4 = Container.newInstance(mockCId4, host4, null, mockCapability, mockPriority, null);
    List<Container> containers = new ArrayList<>();
    containers.add(mockContainer1);
    containers.add(mockContainer2);
    containers.add(mockContainer3);
    containers.add(mockContainer4);
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    // first container allocated
    verify(mockApp).taskAllocated(mockTask1, mockCookie1, mockContainer1);
    verify(mockApp).taskAllocated(mockTask2, mockCookie2, mockContainer2);
    verify(mockApp).taskAllocated(mockTask3, mockCookie3, mockContainer3);
    // no other allocations returned
    verify(mockApp, times(3)).taskAllocated(any(), any(), (Container) any());
    verify(mockRMClient).removeContainerRequest(request1);
    verify(mockRMClient).removeContainerRequest(request2);
    verify(mockRMClient).removeContainerRequest(request3);
    // verify unwanted container released
    verify(mockRMClient).releaseAssignedContainer(mockCId4);

    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask1, true, null, null));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(mockCId1);
    verify(mockRMClient).releaseAssignedContainer(mockCId1);
    // deallocate allocated container
    assertEquals(mockTask2, scheduler.deallocateContainer(mockCId2));
    drainableAppCallback.drain();
    verify(mockRMClient).releaseAssignedContainer(mockCId2);
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId) any());

    List<ContainerStatus> statuses = new ArrayList<>();
    ContainerStatus mockStatus1 = mock(ContainerStatus.class);
    when(mockStatus1.getContainerId()).thenReturn(mockCId1);
    statuses.add(mockStatus1);
    ContainerStatus mockStatus2 = mock(ContainerStatus.class);
    when(mockStatus2.getContainerId()).thenReturn(mockCId2);
    statuses.add(mockStatus2);
    ContainerStatus mockStatus3 = mock(ContainerStatus.class);
    when(mockStatus3.getContainerId()).thenReturn(mockCId3);
    statuses.add(mockStatus3);
    ContainerStatus mockStatus4 = mock(ContainerStatus.class);
    when(mockStatus4.getContainerId()).thenReturn(mockCId4);
    statuses.add(mockStatus4);

    scheduler.onContainersCompleted(statuses);
    drainableAppCallback.drain();
    // released container status returned
    verify(mockApp).containerCompleted(mockTask1, mockStatus1);
    verify(mockApp).containerCompleted(mockTask2, mockStatus2);
    // currently allocated container status returned and not released
    verify(mockApp).containerCompleted(mockTask3, mockStatus3);
    // no other statuses returned
    verify(mockApp, times(3)).containerCompleted(any(), (ContainerStatus) any());
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId) any());

    // verify blacklisting
    verify(mockRMClient, times(0)).updateBlacklist(anyListOf(String.class), anyListOf(String.class));
    String badHost = "host6";
    NodeId badNodeId = NodeId.newInstance(badHost, 1);
    scheduler.blacklistNode(badNodeId);
    List<String> badNodeList = Collections.singletonList(badHost);
    verify(mockRMClient, times(1)).updateBlacklist(eq(badNodeList), isNull(List.class));
    Object mockTask4 = new MockTask("task4");
    Object mockCookie4 = new Object();
    scheduler.allocateTask(mockTask4, mockCapability, null,
        null, mockPriority, null, mockCookie4);
    drainableAppCallback.drain();
    verify(mockRMClient, times(5)).addContainerRequest(requestCaptor.capture());
    ContainerId mockCId5 = ContainerId.newContainerId(attemptId, 5);
    Container mockContainer5 = Container.newInstance(mockCId5, badNodeId, null, mockCapability, mockPriority, null);
    containers.clear();
    containers.add(mockContainer5);
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    // no new allocation
    verify(mockApp, times(3)).taskAllocated(any(), any(), (Container) any());
    // verify blacklisted container released
    verify(mockRMClient).releaseAssignedContainer(mockCId5);
    verify(mockRMClient, times(4)).releaseAssignedContainer((ContainerId) any());
    // verify request added back
    verify(mockRMClient, times(6)).addContainerRequest(requestCaptor.capture());
    NodeId host6 = NodeId.newInstance("host6", 6);
    ContainerId mockCId6 = ContainerId.newContainerId(attemptId, 6);
    Container mockContainer6 = Container.newInstance(mockCId6, host6, null, mockCapability, mockPriority, null);
    containers.clear();
    containers.add(mockContainer6);
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    // new allocation
    verify(mockApp, times(4)).taskAllocated(any(), any(), (Container) any());
    verify(mockApp).taskAllocated(mockTask4, mockCookie4, mockContainer6);
    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask4, true, null, null));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(mockCId6);
    verify(mockRMClient).releaseAssignedContainer(mockCId6);
    verify(mockRMClient, times(5)).releaseAssignedContainer((ContainerId) any());
    // test unblacklist
    scheduler.unblacklistNode(badNodeId);
    verify(mockRMClient, times(1)).updateBlacklist(isNull(List.class), eq(badNodeList));
    assertEquals(0, scheduler.getNumBlacklistedNodes());

    float progress = 0.5f;
    when(mockApp.getProgress()).thenReturn(progress);
    assertEquals(progress, scheduler.getProgress(), 0);

    // check duplicate allocation request
    scheduler.allocateTask(mockTask1, mockCapability, hosts, racks,
        mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(7)).addContainerRequest(any(TaskRequest.class));
    verify(mockRMClient, times(6)).
        removeContainerRequest(any(TaskRequest.class));
    scheduler.allocateTask(mockTask1, mockCapability, hosts, racks,
        mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    // old request removed and new one added
    verify(mockRMClient, times(7)).
        removeContainerRequest(any(TaskRequest.class));
    verify(mockRMClient, times(8)).addContainerRequest(any(TaskRequest.class));
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));

    List<NodeReport> mockUpdatedNodes = mock(List.class);
    scheduler.onNodesUpdated(mockUpdatedNodes);
    drainableAppCallback.drain();
    verify(mockApp).nodesUpdated(mockUpdatedNodes);

    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    Exception mockException = new IOException("mockexception");
    scheduler.onError(mockException);
    drainableAppCallback.drain();
    verify(mockApp)
        .reportError(eq(YarnTaskSchedulerServiceError.RESOURCEMANAGER_ERROR), argumentCaptor.capture(),
            any(DagInfo.class));
    assertTrue(argumentCaptor.getValue().contains("mockexception"));

    scheduler.onShutdownRequest();
    drainableAppCallback.drain();
    verify(mockApp).appShutdownRequested();

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testSimpleReuseLocalMatching() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, "host3");
    MockTaskInfo taskv0t2 = new MockTaskInfo("taskv0t2", priorityv0, hostsv0t0);
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, hostsv0t0);
    MockTaskInfo taskv1t1 = new MockTaskInfo("taskv1t1", priorityv1, hostsv0t0);

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    taskRequestCaptor.scheduleTask(taskv0t1);
    TaskRequest reqv0t2 = taskRequestCaptor.scheduleTask(taskv0t2);
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0);
    taskRequestCaptor.scheduleTask(taskv1t1);

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv0t0.capability, priorityv0, null);

    // allocate one container at v0 priority
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // finish v0t0 successfully, verify v0t1 is skipped and v0t2 instead is assigned to the container
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv0t2.task, taskv0t2.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t2);

    // finish v0t2 successfully, verify v1t0 is assigned to the same container
    assertTrue(scheduler.deallocateTask(taskv0t2.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    // fail v1t0 and verify container is released instead of reused for v1t1
    assertTrue(scheduler.deallocateTask(taskv1t0.task, false, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testSimpleReuseRackMatching() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, "host2");
    MockTaskInfo taskv0t2 = new MockTaskInfo("taskv0t2", priorityv0, "host4", "/somerack");
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, "host1");
    MockTaskInfo taskv1t1 = new MockTaskInfo("taskv1t1", priorityv1, "host5");

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    TaskRequest reqv0t1 = taskRequestCaptor.scheduleTask(taskv0t1);
    taskRequestCaptor.scheduleTask(taskv0t2);
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0);
    taskRequestCaptor.scheduleTask(taskv1t1);

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv0t0.capability, priorityv0, null);

    // allocate one container at v0 priority
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // finish v0t0 successfully, verify v0t1 is skipped and v1t0 assigned instead
    // since host locality is preferred to rack locality and lower priority vertex
    // is not blocked by higher priority vertex
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    // finish v1t0 successfully, verify v0t1 is assigned
    assertTrue(scheduler.deallocateTask(taskv1t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv0t1.task, taskv0t1.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t1);

    // fail v0t1 and verify container is released instead of reused for v1t1
    assertTrue(scheduler.deallocateTask(taskv0t1.task, false, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testSimpleReuseAnyMatching() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, "host2");
    MockTaskInfo taskv0t2 = new MockTaskInfo("taskv0t2", priorityv0, "host4", "/rack4");
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, "host1");
    MockTaskInfo taskv1t1 = new MockTaskInfo("taskv1t1", priorityv1, "host6", "/rack6");

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    TaskRequest reqv0t1 = taskRequestCaptor.scheduleTask(taskv0t1);
    TaskRequest reqv0t2 = taskRequestCaptor.scheduleTask(taskv0t2);
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0);
    taskRequestCaptor.scheduleTask(taskv1t1);

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv0t0.capability, priorityv0, null);

    // allocate one container at v0 priority
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // finish v0t0 successfully, verify v0t1 is skipped and v1t0 assigned instead
    // since host locality is preferred to rack locality and lower priority vertex
    // is not blocked by higher priority vertex
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    // finish v1t0 successfully, verify v0t1 is assigned
    assertTrue(scheduler.deallocateTask(taskv1t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv0t1.task, taskv0t1.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t1);

    // finish v0t1 successfully, verify v0t2 is assigned
    assertTrue(scheduler.deallocateTask(taskv0t1.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv0t2.task, taskv0t2.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t2);

    // fail v0t2 and verify container is released instead of reused for v1t1
    assertTrue(scheduler.deallocateTask(taskv0t2.task, false, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testReuseWithAffinity() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, hostsv0t0);

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    taskRequestCaptor.scheduleTask(taskv0t1);

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv0t0.capability, priorityv0, null);

    // allocate one container at v0 priority
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // add a new request for this container
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, "host1");
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0, cid1);

    // finish v0t0 successfully, verify v0t1 is skipped even though it is node-local
    // and v1t0 assigned instead for affinity
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testReuseVertexDescendants() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);

    // vertex 0 and vertex 2 are root vertices and vertex 1 is a child of vertex 0
    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(3);
    when(mockDagInfo.getVertexDescendants(0)).thenReturn(BitSet.valueOf(new long[] { 0x2 }));
    when(mockDagInfo.getVertexDescendants(1)).thenReturn(new BitSet());
    when(mockDagInfo.getVertexDescendants(2)).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    Priority priorityv2 = Priority.newInstance(3);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv0t0.task)).thenReturn(0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, "host3");
    when(mockApp.getVertexIndexForTask(taskv0t1.task)).thenReturn(0);
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv1t0.task)).thenReturn(1);
    MockTaskInfo taskv2t0 = new MockTaskInfo("taskv2t0", priorityv2, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv2t0.task)).thenReturn(2);
    MockTaskInfo taskv2t1 = new MockTaskInfo("taskv2t1", priorityv2, "host3");
    when(mockApp.getVertexIndexForTask(taskv2t1.task)).thenReturn(2);

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    TaskRequest reqv0t1 = taskRequestCaptor.scheduleTask(taskv0t1);
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0);
    TaskRequest reqv2t0 = taskRequestCaptor.scheduleTask(taskv2t0);
    taskRequestCaptor.scheduleTask(taskv2t1);

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv0t0.capability, priorityv0, null);

    // allocate one container at v0 priority
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // finish v0t0 successfully, verify v1t0 is skipped and v2t0 assigned instead
    // since host locality beats rack locality for unblocked vertex v2 and
    // v1 is blocked by pending v0 request
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv2t0.task, taskv2t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv2t0);

    // finish v2t0 successfully, verify v0t1 is assigned since it is higher
    // priority than v2
    assertTrue(scheduler.deallocateTask(taskv2t0.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv0t1.task, taskv0t1.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t1);

    // finish v2t0 successfully, verify v1t0 is assigned since it is now unblocked
    assertTrue(scheduler.deallocateTask(taskv0t1.task, true, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    // fail v1t0 and verify container is released instead of reused for v2t1
    assertTrue(scheduler.deallocateTask(taskv1t0.task, false, null, null));
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=30000)
  public void testSessionContainers() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 4000);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 5000);
    conf.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, 5);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    when(mockApp.isSession()).thenReturn(true);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    final String rack1 = "/r1";
    final String rack2 = "/r2";
    final String rack3 = "/r3";
    final String node1Rack1 = "n1r1";
    final String node2Rack1 = "n2r1";
    final String node1Rack2 = "n1r2";
    final String node2Rack2 = "n2r2";
    final String node1Rack3 = "n1r3";
    MockDNSToSwitchMapping.addRackMapping(node1Rack1, rack1);
    MockDNSToSwitchMapping.addRackMapping(node2Rack1, rack1);
    MockDNSToSwitchMapping.addRackMapping(node1Rack2, rack2);
    MockDNSToSwitchMapping.addRackMapping(node2Rack2, rack2);
    MockDNSToSwitchMapping.addRackMapping(node1Rack3, rack3);

    Priority priorityv0 = Priority.newInstance(1);
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, node1Rack1, rack1);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, node2Rack1, rack1);
    MockTaskInfo taskv0t2 = new MockTaskInfo("taskv0t2", priorityv0, node1Rack1, rack1);
    MockTaskInfo taskv0t3 = new MockTaskInfo("taskv0t3", priorityv0, node2Rack1, rack1);
    MockTaskInfo taskv0t4 = new MockTaskInfo("taskv0t4", priorityv0, node1Rack2, rack2);
    MockTaskInfo taskv0t5 = new MockTaskInfo("taskv0t5", priorityv0, node2Rack2, rack2);
    MockTaskInfo taskv0t6 = new MockTaskInfo("taskv0t6", priorityv0, node1Rack3, rack3);

    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    TaskRequest reqv0t1 = taskRequestCaptor.scheduleTask(taskv0t1);
    TaskRequest reqv0t2 = taskRequestCaptor.scheduleTask(taskv0t2);
    TaskRequest reqv0t3 = taskRequestCaptor.scheduleTask(taskv0t3);
    TaskRequest reqv0t4 = taskRequestCaptor.scheduleTask(taskv0t4);
    TaskRequest reqv0t5 = taskRequestCaptor.scheduleTask(taskv0t5);
    TaskRequest reqv0t6 = taskRequestCaptor.scheduleTask(taskv0t6);

    List<Container> containers = new ArrayList<>();
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    NodeId n1r1 = NodeId.newInstance(node1Rack1, 1);
    Container container1 = Container.newInstance(cid1, n1r1, null, taskv0t0.capability, priorityv0, null);
    containers.add(container1);
    ContainerId cid2 = ContainerId.newContainerId(attemptId, 2);
    NodeId n2r1 = NodeId.newInstance(node2Rack1, 1);
    Container container2 = Container.newInstance(cid2, n2r1, null, taskv0t1.capability, priorityv0, null);
    containers.add(container2);
    ContainerId cid3 = ContainerId.newContainerId(attemptId, 3);
    Container container3 = Container.newInstance(cid3, n1r1, null, taskv0t2.capability, priorityv0, null);
    containers.add(container3);
    ContainerId cid4 = ContainerId.newContainerId(attemptId, 4);
    Container container4 = Container.newInstance(cid4, n2r1, null, taskv0t3.capability, priorityv0, null);
    containers.add(container4);
    ContainerId cid5 = ContainerId.newContainerId(attemptId, 5);
    NodeId n1r2 = NodeId.newInstance(node1Rack2, 1);
    Container container5 = Container.newInstance(cid5, n1r2, null, taskv0t4.capability, priorityv0, null);
    containers.add(container5);
    ContainerId cid6 = ContainerId.newContainerId(attemptId, 6);
    NodeId n2r2 = NodeId.newInstance(node2Rack2, 1);
    Container container6 = Container.newInstance(cid6, n2r2, null, taskv0t5.capability, priorityv0, null);
    containers.add(container6);
    ContainerId cid7 = ContainerId.newContainerId(attemptId, 7);
    NodeId n1r3 = NodeId.newInstance(node1Rack3, 1);
    Container container7 = Container.newInstance(cid7, n1r3, null, taskv0t6.capability, priorityv0, null);
    containers.add(container7);

    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv0t0);
    verify(mockApp).taskAllocated(taskv0t1.task, taskv0t1.cookie, container2);
    verify(mockRMClient).removeContainerRequest(reqv0t1);
    verify(mockApp).taskAllocated(taskv0t2.task, taskv0t2.cookie, container3);
    verify(mockRMClient).removeContainerRequest(reqv0t2);
    verify(mockApp).taskAllocated(taskv0t3.task, taskv0t3.cookie, container4);
    verify(mockRMClient).removeContainerRequest(reqv0t3);
    verify(mockApp).taskAllocated(taskv0t4.task, taskv0t4.cookie, container5);
    verify(mockRMClient).removeContainerRequest(reqv0t4);
    verify(mockApp).taskAllocated(taskv0t5.task, taskv0t5.cookie, container6);
    verify(mockRMClient).removeContainerRequest(reqv0t5);
    verify(mockApp).taskAllocated(taskv0t6.task, taskv0t6.cookie, container7);
    verify(mockRMClient).removeContainerRequest(reqv0t6);

    clock.incrementTime(10000);
    drainableAppCallback.drain();
    assertTrue(scheduler.deallocateTask(taskv0t0.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t1.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t2.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t3.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t4.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t5.task, true, null, null));
    assertTrue(scheduler.deallocateTask(taskv0t6.task, true, null, null));
    verify(mockApp, never()).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, never()).releaseAssignedContainer(any(ContainerId.class));

    // verify only two of the containers were released after idle expiration
    // and the rest were spread across the nodes and racks
    clock.incrementTime(5000);
    drainableAppCallback.drain();
    verify(mockApp, times(2)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(2)).releaseAssignedContainer(any(ContainerId.class));
    Set<String> hosts = new HashSet<>();
    Set<String> racks = new HashSet<>();
    for (HeldContainer hc : scheduler.getSessionContainers()) {
      hosts.add(hc.getHost());
      racks.add(hc.getRack());
    }
    assertEquals(5, hosts.size());
    assertEquals(3, racks.size());
    assertTrue(hosts.contains(node1Rack1));
    assertTrue(hosts.contains(node2Rack1));
    assertTrue(hosts.contains(node1Rack2));
    assertTrue(hosts.contains(node2Rack2));
    assertTrue(hosts.contains(node1Rack3));
    assertTrue(racks.contains(rack1));
    assertTrue(racks.contains(rack2));
    assertTrue(racks.contains(rack3));

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=50000)
  public void testPreemptionNoHeadroom() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_PERCENTAGE, 10);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS, 3);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_MAX_WAIT_TIME_MS, 60 * 1000);

    // vertex 0 and vertex 2 are root vertices and vertex 1 is a child of vertex 0
    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(3);
    when(mockDagInfo.getVertexDescendants(0)).thenReturn(BitSet.valueOf(new long[] { 0x2 }));
    when(mockDagInfo.getVertexDescendants(1)).thenReturn(new BitSet());
    when(mockDagInfo.getVertexDescendants(2)).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Priority priorityv0 = Priority.newInstance(1);
    Priority priorityv1 = Priority.newInstance(2);
    Priority priorityv2 = Priority.newInstance(3);
    String[] hostsv0t0 = { "host1", "host2" };
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv0t0.task)).thenReturn(0);
    MockTaskInfo taskv0t1 = new MockTaskInfo("taskv0t1", priorityv0, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv0t1.task)).thenReturn(0);
    MockTaskInfo taskv1t0 = new MockTaskInfo("taskv1t0", priorityv1, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv1t0.task)).thenReturn(1);
    MockTaskInfo taskv1t1 = new MockTaskInfo("taskv1t1", priorityv1, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv1t1.task)).thenReturn(1);
    MockTaskInfo taskv2t0 = new MockTaskInfo("taskv2t0", priorityv2, hostsv0t0);
    when(mockApp.getVertexIndexForTask(taskv2t0.task)).thenReturn(2);

    // asks for two tasks for vertex 1 and start running one of them
    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv1t0 = taskRequestCaptor.scheduleTask(taskv1t0);
    TaskRequest reqv1t1 = taskRequestCaptor.scheduleTask(taskv1t1);
    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    Container container1 = Container.newInstance(cid1, host1, null, taskv1t0.capability, priorityv1, null);
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv1t0.task, taskv1t0.cookie, container1);
    verify(mockRMClient).removeContainerRequest(reqv1t0);

    // start running the other task for vertex 1 a bit later
    clock.incrementTime(1000);
    ContainerId cid2 = ContainerId.newContainerId(attemptId, 2);
    Container container2 = Container.newInstance(cid2, host1, null, taskv1t0.capability, priorityv1, null);
    scheduler.onContainersAllocated(Collections.singletonList(container2));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv1t1.task, taskv1t1.cookie, container2);
    verify(mockRMClient).removeContainerRequest(reqv1t1);

    // add a request for vertex 0 but there is no headroom
    when(mockRMClient.getAvailableResources()).thenReturn(Resources.none());
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);

    // should preempt after enough heartbeats to get past preemption interval
    // only the youngest container should be preempted to meet the demand
    scheduler.getProgress();
    scheduler.getProgress();
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockApp, times(1)).preemptContainer(any(ContainerId.class));
    verify(mockApp).preemptContainer(cid2);
    assertEquals(taskv1t1.task, scheduler.deallocateContainer(cid2));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid2);
    verify(mockRMClient).releaseAssignedContainer(cid2);
    verify(mockApp, never()).containerBeingReleased(cid1);
    verify(mockRMClient, never()).releaseAssignedContainer(cid1);

    // add a request for vertex 2 and allocate another container
    clock.incrementTime(1000);
    taskRequestCaptor.scheduleTask(taskv2t0);
    ContainerId cid3 = ContainerId.newContainerId(attemptId, 3);
    Container container3 = Container.newInstance(cid3, host1, null, taskv0t0.capability, priorityv0, null);
    scheduler.onContainersAllocated(Collections.singletonList(container3));
    drainableAppCallback.drain();
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container3);
    verify(mockRMClient).removeContainerRequest(reqv0t0);

    // no more preemptions since v1 is not a descendant of v2
    scheduler.getProgress();
    scheduler.getProgress();
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockApp, times(1)).preemptContainer(any(ContainerId.class));

    // adding request for v0 should trigger preemption on next heartbeat
    taskRequestCaptor.scheduleTask(taskv0t1);
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockApp, times(2)).preemptContainer(any(ContainerId.class));
    verify(mockApp).preemptContainer(cid1);
    assertEquals(taskv1t0.task, scheduler.deallocateContainer(cid1));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
        unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
            appMsg, appUrl);
    verify(mockRMClient).stop();
  }

  @Test(timeout=50000)
  public void testIdleContainerAssignment() throws Exception {
    AMRMClientAsyncWrapperForTest mockRMClient = spy(new AMRMClientAsyncWrapperForTest());

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, 100);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 4000);
    conf.setInt(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 5000);
    conf.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, 5);

    DagInfo mockDagInfo = mock(DagInfo.class);
    when(mockDagInfo.getTotalVertices()).thenReturn(10);
    when(mockDagInfo.getVertexDescendants(anyInt())).thenReturn(new BitSet());
    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(appHost, appPort, appUrl, conf);
    when(mockApp.getCurrentDagInfo()).thenReturn(mockDagInfo);
    when(mockApp.isSession()).thenReturn(true);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    MockClock clock = new MockClock(1000);
    NewTaskSchedulerForTest scheduler = new NewTaskSchedulerForTest(drainableAppCallback,
        mockRMClient, clock);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
        regResponse.getApplicationACLs(), regResponse.getClientToAMTokenMasterKey(),
        regResponse.getQueue());

    assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    final String rack1 = "/r1";
    final String rack2 = "/r2";
    final String node1Rack1 = "n1r1";
    final String node2Rack1 = "n2r1";
    final String node1Rack2 = "n1r2";
    MockDNSToSwitchMapping.addRackMapping(node1Rack1, rack1);
    MockDNSToSwitchMapping.addRackMapping(node2Rack1, rack1);
    MockDNSToSwitchMapping.addRackMapping(node1Rack2, rack2);

    Priority priorityv0 = Priority.newInstance(1);
    MockTaskInfo taskv0t0 = new MockTaskInfo("taskv0t0", priorityv0, node1Rack1, rack1);

    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId cid1 = ContainerId.newContainerId(attemptId, 1);
    NodeId n2r1 = NodeId.newInstance(node2Rack1, 1);
    Container container1 = Container.newInstance(cid1, n2r1, null, taskv0t0.capability, priorityv0, null);

    // verify idle container is kept for now
    scheduler.onContainersAllocated(Collections.singletonList(container1));
    clock.incrementTime(2000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(cid1);
    verify(mockRMClient, never()).releaseAssignedContainer(cid1);

    // verify idle container is released without being assigned to a task because rack-local reuse is
    // disabled
    TaskRequestCaptor taskRequestCaptor = new TaskRequestCaptor(mockRMClient,
        scheduler, drainableAppCallback);
    TaskRequest reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0);
    clock.incrementTime(10000);
    drainableAppCallback.drain();
    verify(mockApp, never()).taskAllocated(taskv0t0.task, taskv0t0.cookie, container1);
    verify(mockRMClient, never()).removeContainerRequest(reqv0t0);
    verify(mockApp, never()).containerBeingReleased(cid1);
    verify(mockRMClient).releaseAssignedContainer(cid1);

    // cancel the task request
    assertFalse(scheduler.deallocateTask(taskv0t0.task, false, null, null));

    // allocate another container that's node-local
    ContainerId cid2 = ContainerId.newContainerId(attemptId, 2);
    NodeId n1r1 = NodeId.newInstance(node1Rack1, 1);
    Container container2 = Container.newInstance(cid2, n1r1, null, taskv0t0.capability, priorityv0, null);
    scheduler.onContainersAllocated(Collections.singletonList(container2));
    clock.incrementTime(2000);
    drainableAppCallback.drain();
    verify(mockApp, never()).containerBeingReleased(cid2);
    verify(mockRMClient, never()).releaseAssignedContainer(cid2);

    // reschedule the task, verify it's now scheduled without a container request
    // since node-local idle container is available
    reqv0t0 = taskRequestCaptor.scheduleTask(taskv0t0, false);
    verify(mockApp).taskAllocated(taskv0t0.task, taskv0t0.cookie, container2);
    verify(mockRMClient).removeContainerRequest(reqv0t0);
  }

  static class AMRMClientAsyncWrapperForTest extends AMRMClientAsyncWrapper {
    AMRMClientAsyncWrapperForTest() {
      super(new MockAMRMClient(), 10000, null);
    }

    RegisterApplicationMasterResponse getRegistrationResponse() {
      return ((MockAMRMClient) client).getRegistrationResponse();
    }
  }

  static class MockAMRMClient extends AMRMClientImpl<TaskRequest> {
    private RegisterApplicationMasterResponse mockRegResponse;

    MockAMRMClient() {
      super();
      this.clusterAvailableResources = Resource.newInstance(4000, 4);
      this.clusterNodeCount = 5;
    }

    @Override
    protected void serviceStart() {
    }

    @Override
    protected void serviceStop() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        String appHostName, int appHostPort, String appTrackingUrl) {
      mockRegResponse = mock(RegisterApplicationMasterResponse.class);
      Resource mockMaxResource = Resources.createResource(1024*1024, 1024);
      Map<ApplicationAccessType, String> mockAcls = Collections.emptyMap();
      when(mockRegResponse.getMaximumResourceCapability()).thenReturn(
          mockMaxResource);
      when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
      when(mockRegResponse.getSchedulerResourceTypes()).thenReturn(
          EnumSet.of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU));
      return mockRegResponse;
    }

    @Override
    public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
        String appMessage, String appTrackingUrl) {
    }

    RegisterApplicationMasterResponse getRegistrationResponse() {
      return mockRegResponse;
    }
  }

  static class MockTask {
    final String name;

    MockTask(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static class MockTaskInfo {
    final static Object DEFAULT_SIGNATURE = new Object();

    final MockTask task;
    final Object cookie = new Object();
    final Object signature = DEFAULT_SIGNATURE;
    final String[] hosts;
    final String[] racks;
    final Priority priority;
    final Resource capability;

    MockTaskInfo(String name, Priority priority, String host) {
      this(name, priority, host == null ? null : new String[] { host });
    }

    MockTaskInfo(String name, Priority priority, String[] hosts) {
      this(name, priority, hosts, buildDefaultRacks(hosts));
    }

    MockTaskInfo(String name, Priority priority, String host, String rack) {
      this(name, priority, host == null ? null : new String[] { host },
          rack == null ? null : new String[] { rack });
    }

    MockTaskInfo(String name, Priority priority, String[] hosts, String[] racks) {
      this.task = new MockTask(name);
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
      this.capability = Resource.newInstance(1024, 1);
    }

    static String[] buildDefaultRacks(String[] hosts) {
      if (hosts == null) {
        return null;
      }
      String[] racks = new String[hosts.length];
      Arrays.fill(racks, "/default-rack");
      return racks;
    }
  }

  static class TaskRequestCaptor {
    final AMRMClientAsync<TaskRequest> client;
    final TaskScheduler scheduler;
    final TaskSchedulerContextDrainable drainableAppCallback;
    final ArgumentCaptor<TaskRequest> captor = ArgumentCaptor.forClass(TaskRequest.class);
    int invocationCount = 0;

    TaskRequestCaptor(AMRMClientAsync<TaskRequest> client, TaskScheduler scheduler,
        TaskSchedulerContextDrainable drainableAppCallback) {
      this.client = client;
      this.scheduler = scheduler;
      this.drainableAppCallback = drainableAppCallback;
    }

    TaskRequest scheduleTask(MockTaskInfo taskInfo) throws Exception {
      return scheduleTask(taskInfo, true);
    }

    TaskRequest scheduleTask(MockTaskInfo taskInfo, boolean expectContainerRequest) throws Exception {
      scheduler.allocateTask(taskInfo.task, taskInfo.capability, taskInfo.hosts, taskInfo.racks,
          taskInfo.priority, taskInfo.signature, taskInfo.cookie);
      drainableAppCallback.drain();
      if (expectContainerRequest) {
        ++invocationCount;
      }
      verify(client, times(invocationCount)).addContainerRequest(captor.capture());
      TaskRequest request = captor.getValue();
      assertEquals(request.getTask(), taskInfo.task);
      assertEquals(request.getCookie(), taskInfo.cookie);
      return request;
    }

    TaskRequest scheduleTask(MockTaskInfo taskInfo, ContainerId affinity) throws Exception {
      scheduler.allocateTask(taskInfo.task, taskInfo.capability, affinity, taskInfo.priority,
          taskInfo.signature, taskInfo.cookie);
      drainableAppCallback.drain();
      verify(client, times(++invocationCount)).addContainerRequest(captor.capture());
      TaskRequest request = captor.getValue();
      assertEquals(request.getTask(), taskInfo.task);
      assertEquals(request.getCookie(), taskInfo.cookie);
      return request;
    }
  }

  static class NewTaskSchedulerForTest extends DagAwareYarnTaskScheduler {
    final AMRMClientAsyncWrapper mockClient;
    final MockClock clock;

    NewTaskSchedulerForTest(
        TaskSchedulerContextDrainable appClient,
        AMRMClientAsyncWrapper client, MockClock clock) {
      super(appClient);
      this.mockClient = client;
      this.clock = clock;
      setShouldUnregister();
    }

    @Override
    public void initialize() throws Exception {
      initialize(mockClient);
    }

    @Override
    protected ScheduledExecutorService createExecutor() {
      return new ControlledScheduledExecutorService(clock);
    }

    @Override
    protected long now() {
      return clock.getTime();
    }
  }
}

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

import static org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.createCountingExecutingService;
import static org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.setupMockTaskSchedulerContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
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
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientAsyncForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AlwaysMatchesContainerMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.PreemptionMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerContextDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableContext;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.CookieContainerRequest;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.HeldContainer;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AppFinalStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings("deprecation")
public class TestTaskScheduler {

  static ContainerSignatureMatcher containerSignatureMatcher = new AlwaysMatchesContainerMatcher();
  private ExecutorService contextCallbackExecutor;
  private static final String DEFAULT_APP_HOST = "host";
  private static final String DEFAULT_APP_URL = "url";
  private static final String SUCCEED_APP_MESSAGE = "success";
  private static final int DEFAULT_APP_PORT = 0;

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
  @Test(timeout=10000)
  public void testTaskSchedulerNoReuse() throws Exception {
    AMRMClientAsyncForTest mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    int interval = 100;
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, interval);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf);
    TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    drainableAppCallback.drain();
    // Verifying the validity of the configuration via the interval only instead of making sure
    // it's the same instance.
    verify(mockRMClient).setHeartbeatInterval(interval);

    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL);
    RegisterApplicationMasterResponse regResponse = mockRMClient.getRegistrationResponse();
    verify(mockApp).setApplicationRegistrationData(regResponse.getMaximumResourceCapability(),
                                                   regResponse.getApplicationACLs(),
                                                   regResponse.getClientToAMTokenMasterKey(),
                                                   regResponse.getQueue());

    Assert.assertEquals(scheduler.getClusterNodeCount(), mockRMClient.getClusterNodeCount());

    Object mockTask1 = new MockTask("task1");
    Object mockCookie1 = new Object();
    Resource mockCapability = Resource.newInstance(1024, 1);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    Priority mockPriority = Priority.newInstance(1);
    ArgumentCaptor<CookieContainerRequest> requestCaptor =
                        ArgumentCaptor.forClass(CookieContainerRequest.class);
    // allocate task
    scheduler.allocateTask(mockTask1, mockCapability, hosts,
                           racks, mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
                           addContainerRequest((CookieContainerRequest) any());

    // returned from task requests before allocation happens
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((CookieContainerRequest) any());
    verify(mockRMClient, times(0)).
                                 releaseAssignedContainer((ContainerId) any());

    // deallocating unknown task
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((CookieContainerRequest) any());
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
    CookieContainerRequest request1 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask2, mockCapability, hosts,
        racks, mockPriority, null, mockCookie2);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request2 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask3, mockCapability, hosts,
        racks, mockPriority, null, mockCookie3);
    drainableAppCallback.drain();
    verify(mockRMClient, times(4)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request3 = requestCaptor.getValue();

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
    Assert.assertEquals(mockTask2, scheduler.deallocateContainer(mockCId2));
    drainableAppCallback.drain();
    verify(mockRMClient).releaseAssignedContainer(mockCId2);
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId) any());

    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
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
    verify(mockRMClient, times(0)).addNodeToBlacklist((NodeId)any());
    String badHost = "host6";
    NodeId badNodeId = NodeId.newInstance(badHost, 1);
    scheduler.blacklistNode(badNodeId);
    verify(mockRMClient, times(1)).addNodeToBlacklist(badNodeId);
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
    ContainerId mockCId6 = ContainerId.newContainerId(attemptId, 6);
    NodeId host7 = NodeId.newInstance("host7", 7);
    Container mockContainer6 = Container.newInstance(mockCId6, host7, null, mockCapability, mockPriority, null);
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
    verify(mockRMClient, times(1)).removeNodeFromBlacklist(badNodeId);
    assertEquals(0, scheduler.blacklistedNodes.size());

    float progress = 0.5f;
    when(mockApp.getProgress()).thenReturn(progress);
    Assert.assertEquals(progress, scheduler.getProgress(), 0);
    
    // check duplicate allocation request
    scheduler.allocateTask(mockTask1, mockCapability, hosts, racks,
        mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(7)).addContainerRequest(
        (CookieContainerRequest) any());
    verify(mockRMClient, times(6)).
        removeContainerRequest((CookieContainerRequest) any());
    scheduler.allocateTask(mockTask1, mockCapability, hosts, racks,
        mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    // old request removed and new one added
    verify(mockRMClient, times(7)).
        removeContainerRequest((CookieContainerRequest) any());
    verify(mockRMClient, times(8)).addContainerRequest(
        (CookieContainerRequest) any());
    assertFalse(scheduler.deallocateTask(mockTask1, true, null, null));

    // test speculative node adjustment
    String speculativeNode = "host8";
    NodeId speculativeNodeId = mock(NodeId.class);
    when(speculativeNodeId.getHost()).thenReturn(speculativeNode);
    TaskAttempt mockTask5 = mock(TaskAttempt.class);
    Task task = mock(Task.class);
    when(mockTask5.getTask()).thenReturn(task);
    when(task.getNodesWithRunningAttempts()).thenReturn(Sets.newHashSet(speculativeNodeId));
    Object mockCookie5 = new Object();
    scheduler.allocateTask(mockTask5, mockCapability, hosts, racks,
        mockPriority, null, mockCookie5);
    drainableAppCallback.drain();
    // no new allocation
    verify(mockApp, times(4)).taskAllocated(any(), any(), (Container) any());
    // verify container released
    verify(mockRMClient, times(5)).releaseAssignedContainer((ContainerId) any());
    // verify request added back
    verify(mockRMClient, times(9)).addContainerRequest(requestCaptor.capture());

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

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
                  unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    verify(mockRMClient).stop();
  }

  @Test(timeout=10000)
  public void testTaskSchedulerInitiateStop() throws Exception {

    Configuration conf = new Configuration();
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    // keep containers held for 10 seconds
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 10000);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 10000);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    drainableAppCallback.drain();

    scheduler.start();
    drainableAppCallback.drain();

    Object mockTask1 = new MockTask("task1");
    Object mockCookie1 = new Object();
    Resource mockCapability = Resource.newInstance(1024, 1);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    final Priority mockPriority1 = Priority.newInstance(1);
    final Priority mockPriority2 = Priority.newInstance(2);
    final Priority mockPriority3 = Priority.newInstance(3);
    Priority mockPriority = Priority.newInstance(1);
    Object mockTask2 = new MockTask("task2");
    Object mockCookie2 = new Object();
    Object mockTask3 = new MockTask("task3");
    Object mockCookie3 = new Object();
    ArgumentCaptor<CookieContainerRequest> requestCaptor =
        ArgumentCaptor.forClass(CookieContainerRequest.class);

    scheduler.allocateTask(mockTask1, mockCapability, hosts,
        racks, mockPriority1, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request1 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask2, mockCapability, hosts,
        racks, mockPriority2, null, mockCookie2);
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request2 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask3, mockCapability, hosts,
        racks, mockPriority3, null, mockCookie3);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request3 = requestCaptor.getValue();

    List<Container> containers = new ArrayList<Container>();
    // sending lower priority container first to make sure its not matched
    NodeId host1 = NodeId.newInstance("host1", 1);
    NodeId host2 = NodeId.newInstance("host2", 2);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId1 = ContainerId.newContainerId(attemptId, 1);
    Container mockContainer1 = Container.newInstance(mockCId1, host1, null, mockCapability, mockPriority, null);
    ContainerId mockCId2 = ContainerId.newContainerId(attemptId, 2);
    Container mockContainer2 = Container.newInstance(mockCId2, host2, null, mockCapability, mockPriority, null);
    containers.add(mockContainer1);
    containers.add(mockContainer2);

    ArrayList<CookieContainerRequest> hostContainers =
                             new ArrayList<CookieContainerRequest>();
    hostContainers.add(request1);
    ArrayList<CookieContainerRequest> rackContainers =
                             new ArrayList<CookieContainerRequest>();
    rackContainers.add(request2);
    ArrayList<CookieContainerRequest> anyContainers =
                             new ArrayList<CookieContainerRequest>();
    anyContainers.add(request3);

    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    scheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;

    scheduler.onContainersAllocated(containers);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();

    Assert.assertEquals(2, scheduler.heldContainers.size());
    Assert.assertEquals(1, scheduler.taskRequests.size());
    // 2 containers are allocated and their corresponding taskRequests are removed.
    verify(mockRMClient).removeContainerRequest(request1);
    verify(mockRMClient).removeContainerRequest(request2);

    scheduler.initiateStop();
    // verify all the containers are released
    Assert.assertEquals(0, scheduler.heldContainers.size());
    verify(mockRMClient).releaseAssignedContainer(mockCId1);
    verify(mockRMClient).releaseAssignedContainer(mockCId2);
    // verify taskRequests are removed
    Assert.assertEquals(0, scheduler.taskRequests.size());
    verify(mockRMClient).removeContainerRequest(request3);
  }

  @SuppressWarnings({ "unchecked" })
  @Test(timeout=10000)
  public void testTaskSchedulerWithReuse() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    Configuration conf = new Configuration();
    // to match all in the same pass
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    // to release immediately after deallocate
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);


    scheduler.initialize();
    drainableAppCallback.drain();
    scheduler.start();
    drainableAppCallback.drain();

    Object mockTask1 = new MockTask("task1");
    Object mockCookie1 = new Object();
    Resource mockCapability = Resource.newInstance(1024, 1);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    final Priority mockPriority1 = Priority.newInstance(1);
    final Priority mockPriority2 = Priority.newInstance(2);
    final Priority mockPriority3 = Priority.newInstance(3);
    final Priority mockPriority4 = Priority.newInstance(4);
    final Priority mockPriority5 = Priority.newInstance(5);
    Object mockTask2 = new MockTask("task2");
    Object mockCookie2 = new Object();
    Object mockTask3 = new MockTask("task3");
    Object mockCookie3 = new Object();
    ArgumentCaptor<CookieContainerRequest> requestCaptor =
        ArgumentCaptor.forClass(CookieContainerRequest.class);

    scheduler.allocateTask(mockTask1, mockCapability, hosts,
        racks, mockPriority1, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request1 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask2, mockCapability, hosts,
        racks, mockPriority2, null, mockCookie2);
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request2 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask3, mockCapability, hosts,
        racks, mockPriority3, null, mockCookie3);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request3 = requestCaptor.getValue();

    NodeId host1 = NodeId.newInstance("host1", 1);
    NodeId host2 = NodeId.newInstance("host2", 2);
    NodeId host3 = NodeId.newInstance("host3", 3);
    NodeId host4 = NodeId.newInstance("host4", 4);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId1 = ContainerId.newContainerId(attemptId, 1);
    Container mockContainer1 = Container.newInstance(mockCId1, host1, null, mockCapability, mockPriority1, null);
    ContainerId mockCId2 = ContainerId.newContainerId(attemptId, 2);
    Container mockContainer2 = Container.newInstance(mockCId2, host2, null, mockCapability, mockPriority2, null);
    ContainerId mockCId3 = ContainerId.newContainerId(attemptId, 3);
    Container mockContainer3 = Container.newInstance(mockCId3, host3, null, mockCapability, mockPriority3, null);
    ContainerId mockCId4 = ContainerId.newContainerId(attemptId, 4);
    Container mockContainer4 = Container.newInstance(mockCId4, host4, null, mockCapability, mockPriority4, null);
    // sending lower priority container first to make sure its not matched
    List<Container> containers = new ArrayList<Container>();
    containers.add(mockContainer4);
    containers.add(mockContainer1);
    containers.add(mockContainer2);
    containers.add(mockContainer3);

    AtomicBoolean drainNotifier = new AtomicBoolean(false);
    scheduler.delayedContainerManager.drainedDelayedContainersForTest = drainNotifier;
    
    scheduler.onContainersAllocated(containers);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    // exact number allocations returned
    verify(mockApp, times(3)).taskAllocated(any(), any(), (Container) any());
    // first container allocated
    verify(mockApp).taskAllocated(mockTask1, mockCookie1, mockContainer1);
    verify(mockApp).taskAllocated(mockTask2, mockCookie2, mockContainer2);
    verify(mockApp).taskAllocated(mockTask3, mockCookie3, mockContainer3);
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
    Assert.assertEquals(mockTask2, scheduler.deallocateContainer(mockCId2));
    drainableAppCallback.drain();
    verify(mockRMClient).releaseAssignedContainer(mockCId2);
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId) any());

    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
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
    verify(mockRMClient, times(0)).addNodeToBlacklist((NodeId)any());
    String badHost = "host6";
    NodeId badNodeId = NodeId.newInstance(badHost, 1);
    scheduler.blacklistNode(badNodeId);
    verify(mockRMClient, times(1)).addNodeToBlacklist(badNodeId);
    Object mockTask4 = new MockTask("task4");
    Object mockCookie4 = new Object();
    scheduler.allocateTask(mockTask4, mockCapability, null,
        null, mockPriority4, null, mockCookie4);
    drainableAppCallback.drain();
    verify(mockRMClient, times(4)).addContainerRequest(requestCaptor.capture());
    ContainerId mockCId5 = ContainerId.newContainerId(attemptId, 5);
    Container mockContainer5 = Container.newInstance(mockCId5, badNodeId, null, mockCapability, mockPriority4, null);
    containers.clear();
    containers.add(mockContainer5);
    drainNotifier.set(false);
    scheduler.onContainersAllocated(containers);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    // no new allocation
    verify(mockApp, times(3)).taskAllocated(any(), any(), (Container) any());
    // verify blacklisted container released
    verify(mockRMClient).releaseAssignedContainer(mockCId5);
    verify(mockRMClient, times(4)).releaseAssignedContainer((ContainerId) any());
    // verify request added back
    verify(mockRMClient, times(5)).addContainerRequest(requestCaptor.capture());
    NodeId host7 = NodeId.newInstance("host7", 7);
    ContainerId mockCId6 = ContainerId.newContainerId(attemptId, 6);
    Container mockContainer6 = Container.newInstance(mockCId6, host7, null, mockCapability, mockPriority4, null);
    containers.clear();
    containers.add(mockContainer6);
    drainNotifier.set(false);
    scheduler.onContainersAllocated(containers);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
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
    verify(mockRMClient, times(1)).removeNodeFromBlacklist(badNodeId);
    assertEquals(0, scheduler.blacklistedNodes.size());
    
    // verify container level matching
    // add a dummy task to prevent release of allocated containers
    Object mockTask5 = new MockTask("task5");
    Object mockCookie5 = new Object();
    scheduler.allocateTask(mockTask5, mockCapability, hosts,
        racks, mockPriority5, null, mockCookie5);
    verify(mockRMClient, times(6)).addContainerRequest(requestCaptor.capture());
    drainableAppCallback.drain();
    // add containers so that we can reference one of them for container specific
    // allocation
    containers.clear();
    NodeId host5 = NodeId.newInstance("host5", 5);
    ContainerId mockCId7 = ContainerId.newContainerId(attemptId, 7);
    Container mockContainer7 = Container.newInstance(mockCId7, host5, null, mockCapability, mockPriority5, null);
    containers.add(mockContainer7);
    ContainerId mockCId8 = ContainerId.newContainerId(attemptId, 8);
    Container mockContainer8 = Container.newInstance(mockCId8, host5, null, mockCapability, mockPriority5, null);
    containers.add(mockContainer8);
    drainNotifier.set(false);
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    verify(mockRMClient, times(5)).releaseAssignedContainer((ContainerId) any());    
    Object mockTask6 = new MockTask("task6");
    Object mockCookie6 = new Object();
    // allocate request with container affinity
    scheduler.allocateTask(mockTask6, mockCapability, mockCId7, mockPriority5, null, mockCookie6);
    drainableAppCallback.drain();
    verify(mockRMClient, times(7)).addContainerRequest(requestCaptor.capture());
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(mockApp, times(6)).taskAllocated(any(), any(), (Container) any());
    // container7 allocated to the task with affinity for it
    verify(mockApp).taskAllocated(mockTask6, mockCookie6, mockContainer7);
    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask5, true, null, null));
    assertTrue(scheduler.deallocateTask(mockTask6, true, null, null));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(mockCId7);
    verify(mockApp).containerBeingReleased(mockCId8);
    verify(mockRMClient).releaseAssignedContainer(mockCId7);
    verify(mockRMClient).releaseAssignedContainer(mockCId8);
    verify(mockRMClient, times(7)).releaseAssignedContainer((ContainerId) any());
    

    float progress = 0.5f;
    when(mockApp.getProgress()).thenReturn(progress);
    Assert.assertEquals(progress, scheduler.getProgress(), 0);

    List<NodeReport> mockUpdatedNodes = mock(List.class);
    scheduler.onNodesUpdated(mockUpdatedNodes);
    drainableAppCallback.drain();
    verify(mockApp).nodesUpdated(mockUpdatedNodes);


    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    Exception mockException = new IOException("mockexception");
    scheduler.onError(mockException);
    drainableAppCallback.drain();
    verify(mockApp).reportError(eq(YarnTaskSchedulerServiceError.RESOURCEMANAGER_ERROR), argumentCaptor.capture(),
            any(DagInfo.class));
    assertTrue(argumentCaptor.getValue().contains("mockexception"));

    scheduler.onShutdownRequest();
    drainableAppCallback.drain();
    verify(mockApp).appShutdownRequested();

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
    verify(mockRMClient).
                  unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    verify(mockRMClient).stop();
  }
  
  @Test (timeout=5000)
  public void testTaskSchedulerDetermineMinHeldContainers() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL,
      true, new Configuration());
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    scheduler.start();
    
    String rack1 = "r1";
    String rack2 = "r2";
    String rack3 = "r3";
    String node1Rack1 = "n1r1";
    String node2Rack1 = "n2r1";
    String node1Rack2 = "n1r2";
    String node2Rack2 = "n2r2";
    String node1Rack3 = "n1r3";
    ApplicationAttemptId appId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0);

    NodeId emptyHost = NodeId.newInstance("", 1);
    Resource r = Resource.newInstance(0, 0);
    ContainerId mockCId1 = ContainerId.newInstance(appId, 0);
    Container c1 = Container.newInstance(mockCId1, emptyHost, null, r, null, null);
    HeldContainer hc1 = Mockito.spy(new HeldContainer(c1, 0, 0, null, containerSignatureMatcher));
    when(hc1.getNode()).thenReturn(node1Rack1);
    when(hc1.getRack()).thenReturn(rack1);
    when(hc1.getContainer()).thenReturn(c1);
    ContainerId mockCId2 = ContainerId.newInstance(appId, 1);
    Container c2 = Container.newInstance(mockCId2, emptyHost, null, r, null, null);
    HeldContainer hc2 = Mockito.spy(new HeldContainer(c2, 0, 0, null, containerSignatureMatcher));
    when(hc2.getNode()).thenReturn(node2Rack1);
    when(hc2.getRack()).thenReturn(rack1);
    when(hc2.getContainer()).thenReturn(c2);
    ContainerId mockCId3 = ContainerId.newInstance(appId, 2);
    Container c3 = Container.newInstance(mockCId3, emptyHost, null, r, null, null);
    HeldContainer hc3 = Mockito.spy(new HeldContainer(c3, 0, 0, null, containerSignatureMatcher));
    when(hc3.getNode()).thenReturn(node1Rack1);
    when(hc3.getRack()).thenReturn(rack1);
    when(hc3.getContainer()).thenReturn(c3);
    ContainerId mockCId4 = ContainerId.newInstance(appId, 3);
    Container c4 = Container.newInstance(mockCId4, emptyHost, null, r, null, null);
    HeldContainer hc4 = Mockito.spy(new HeldContainer(c4, 0, 0, null, containerSignatureMatcher));
    when(hc4.getNode()).thenReturn(node2Rack1);
    when(hc4.getRack()).thenReturn(rack1);
    when(hc4.getContainer()).thenReturn(c4);
    ContainerId mockCId5 = ContainerId.newInstance(appId, 4);
    Container c5 = Container.newInstance(mockCId5, emptyHost, null, r, null, null);
    HeldContainer hc5 = Mockito.spy(new HeldContainer(c5, 0, 0, null, containerSignatureMatcher));
    when(hc5.getNode()).thenReturn(node1Rack2);
    when(hc5.getRack()).thenReturn(rack2);
    when(hc5.getContainer()).thenReturn(c5);
    ContainerId mockCId6 = ContainerId.newInstance(appId, 5);
    Container c6 = Container.newInstance(mockCId6, emptyHost, null, r, null, null);
    HeldContainer hc6 = Mockito.spy(new HeldContainer(c6, 0, 0, null, containerSignatureMatcher));
    when(hc6.getNode()).thenReturn(node2Rack2);
    when(hc6.getRack()).thenReturn(rack2);
    when(hc6.getContainer()).thenReturn(c6);
    ContainerId mockCId7 = ContainerId.newInstance(appId, 6);
    Container c7 = Container.newInstance(mockCId7, emptyHost, null, r, null, null);
    HeldContainer hc7 = Mockito.spy(new HeldContainer(c7, 0, 0, null, containerSignatureMatcher));
    when(hc7.getNode()).thenReturn(node1Rack3);
    when(hc7.getRack()).thenReturn(rack3);
    when(hc7.getContainer()).thenReturn(c7);

    scheduler.heldContainers.put(mockCId1, hc1);
    scheduler.heldContainers.put(mockCId2, hc2);
    scheduler.heldContainers.put(mockCId3, hc3);
    scheduler.heldContainers.put(mockCId4, hc4);
    scheduler.heldContainers.put(mockCId5, hc5);
    scheduler.heldContainers.put(mockCId6, hc6);
    scheduler.heldContainers.put(mockCId7, hc7);
    
    // test empty case
    scheduler.sessionNumMinHeldContainers = 0;
    scheduler.determineMinHeldContainers();
    Assert.assertEquals(0, scheduler.sessionMinHeldContainers.size());
    
    // test min >= held
    scheduler.sessionNumMinHeldContainers = 7;
    scheduler.determineMinHeldContainers();
    Assert.assertEquals(7, scheduler.sessionMinHeldContainers.size());
    
    // test min < held
    scheduler.sessionNumMinHeldContainers = 5;
    scheduler.determineMinHeldContainers();
    Assert.assertEquals(5, scheduler.sessionMinHeldContainers.size());
    
    Set<HeldContainer> heldContainers = Sets.newHashSet();
    for (ContainerId cId : scheduler.sessionMinHeldContainers) {
      heldContainers.add(scheduler.heldContainers.get(cId));
    }
    Set<String> racks = Sets.newHashSet();
    Set<String> nodes = Sets.newHashSet();
    for (HeldContainer hc : heldContainers) {
      nodes.add(hc.getNode());
      racks.add(hc.getRack());
    }
    // 1 container from each node in rack1 and rack2. 1 container from rack3.
    // covers not enough containers in rack (rack 3)
    // covers just enough containers in rack (rack 2)
    // covers more than enough containers in rack (rack 1)
    Assert.assertEquals(5, nodes.size());
    Assert.assertTrue(nodes.contains(node1Rack1) && nodes.contains(node2Rack1) &&
        nodes.contains(node1Rack2) && nodes.contains(node2Rack2) &&
        nodes.contains(node1Rack3));
    Assert.assertEquals(3, racks.size());
    Assert.assertTrue(racks.contains(rack1) && racks.contains(rack2) &&
        racks.contains(rack3));
    
    long currTime = System.currentTimeMillis();
    heldContainers.clear();
    heldContainers.addAll(scheduler.heldContainers.values());
    for (HeldContainer hc : heldContainers) {
      when(hc.isNew()).thenReturn(true);
      scheduler.delayedContainerManager.addDelayedContainer(hc.getContainer(), currTime);
    }
    Thread.sleep(1000);
    drainableAppCallback.drain();
    // only the 2 container not in min-held containers are released
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId)any());
    Assert.assertEquals(5, scheduler.heldContainers.size());

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
  }

  @Test (timeout=3000)
  public void testTaskSchedulerHeldContainersReleaseAfterExpired() throws Exception {
    final TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
      new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));
    final TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT,
      DEFAULT_APP_URL, true, new Configuration());
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);
    final TaskSchedulerWithDrainableContext scheduler =
      new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    scheduler.start();

    Resource mockCapability = Resource.newInstance(1024, 1);
    NodeId emptyHost = NodeId.newInstance("", 1);
    ApplicationAttemptId appId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0);
    ContainerId containerId = ContainerId.newInstance(appId, 0);
    Container c1 = Container.newInstance(containerId, emptyHost, null, mockCapability, null, null);

    HeldContainer hc1 = new HeldContainer(c1, -1, -1, null, containerSignatureMatcher);

    // containerExpiryTime = 0
    scheduler.heldContainers.put(containerId, hc1);

    long currTime = System.currentTimeMillis();
    scheduler.delayedContainerManager.addDelayedContainer(hc1.getContainer(), currTime);
    // sleep and wait for mainLoop() check-in to release this expired held container.
    Thread.sleep(1000);

    verify(mockRMClient, times(1)).releaseAssignedContainer((ContainerId)any());
    Assert.assertEquals(0, scheduler.heldContainers.size());

    AppFinalStatus finalStatus =
      new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
  }
  
  @Test(timeout=5000)
  public void testTaskSchedulerRandomReuseExpireTime() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    long minTime = 1000l;
    long maxTime = 100000l;
    Configuration conf1 = new Configuration();
    conf1.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, minTime);
    conf1.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, minTime);

    Configuration conf2 = new Configuration();
    conf2.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, minTime);
    conf2.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, maxTime);

    TaskSchedulerContext mockApp1 = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf1);
    TaskSchedulerContext mockApp2 = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf2);
    final TaskSchedulerContextDrainable drainableAppCallback1 = createDrainableContext(mockApp1);
    final TaskSchedulerContextDrainable drainableAppCallback2 = createDrainableContext(mockApp2);


    TaskSchedulerWithDrainableContext scheduler1 =
        new TaskSchedulerWithDrainableContext(drainableAppCallback1, mockRMClient);
    TaskSchedulerWithDrainableContext scheduler2 =
        new TaskSchedulerWithDrainableContext(drainableAppCallback2, mockRMClient);

    scheduler1.initialize();
    scheduler2.initialize();

    scheduler1.start();
    scheduler2.start();
    
    // when min == max the expire time is always min
    for (int i=0; i<10; ++i) {
      Assert.assertEquals(minTime, scheduler1.getHeldContainerExpireTime(0));
    }
    
    long lastExpireTime = 0;
    // when min < max the expire time is random in between min and max
    for (int i=0; i<10; ++i) {
      long currExpireTime = scheduler2.getHeldContainerExpireTime(0);
      Assert.assertTrue(
          "min: " + minTime + " curr: " + currExpireTime + " max: " + maxTime,
          (minTime <= currExpireTime && currExpireTime <= maxTime));
      Assert.assertNotEquals(lastExpireTime, currExpireTime);
      lastExpireTime = currExpireTime;
    }

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, SUCCEED_APP_MESSAGE, DEFAULT_APP_URL);
    when(mockApp1.getFinalAppStatus()).thenReturn(finalStatus);
    when(mockApp2.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler1.shutdown();
    scheduler2.shutdown();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test (timeout=5000)
  public void testTaskSchedulerPreemption() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS, 3);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL,
      false, null, null, new PreemptionMatcher(), conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    final TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();

    RegisterApplicationMasterResponse mockRegResponse =
                       mock(RegisterApplicationMasterResponse.class);
    when(
        mockRMClient.registerApplicationMaster(anyString(), anyInt(),
            anyString())).thenReturn(mockRegResponse);

    scheduler.start();
    Resource totalResource = Resource.newInstance(4000, 4);
    when(mockRMClient.getAvailableResources()).thenReturn(totalResource);

    // no preemption
    scheduler.getProgress();
    drainableAppCallback.drain();
    Assert.assertEquals(totalResource, scheduler.getTotalResources());
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    // allocate task
    Object mockTask1 = new MockTask("task1");
    Object mockTask2 = new MockTask("task2");
    Object mockTask3 = new MockTask("task3");
    Object mockTask3Wait = new MockTask("task3Wait");
    Object mockTask3Retry = new MockTask("task3Retry");
    Object mockTask3KillA = new MockTask("task3KillA");
    Object mockTask3KillB = new MockTask("task3KillB");
    Object mockTaskPri8 = new MockTask("taskPri8");
    Object obj3 = new Object();
    Priority pri2 = Priority.newInstance(2);
    Priority pri4 = Priority.newInstance(4);
    Priority pri5 = Priority.newInstance(5);
    Priority pri6 = Priority.newInstance(6);
    Priority pri8 = Priority.newInstance(8);

    ArgumentCaptor<CookieContainerRequest> requestCaptor =
        ArgumentCaptor.forClass(CookieContainerRequest.class);
    final ArrayList<CookieContainerRequest> anyContainers =
        new ArrayList<CookieContainerRequest>();

    
    Resource taskAsk = Resource.newInstance(1024, 1);
    scheduler.allocateTask(mockTask1, taskAsk, null,
                           null, pri2, null, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
        addContainerRequest(requestCaptor.capture());
    anyContainers.add(requestCaptor.getValue());
    scheduler.allocateTask(mockTask3, taskAsk, null,
                           null, pri6, obj3, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).
    addContainerRequest(requestCaptor.capture());
    anyContainers.add(requestCaptor.getValue());
    // later one in the allocation gets killed between the two task3's
    scheduler.allocateTask(mockTask3KillA, taskAsk, null,
                           null, pri6, obj3, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
    addContainerRequest(requestCaptor.capture());
    anyContainers.add(requestCaptor.getValue());
    // later one in the allocation gets killed between the two task3's
    scheduler.allocateTask(mockTask3KillB, taskAsk, null,
                           null, pri6, obj3, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(4)).
    addContainerRequest(requestCaptor.capture());
    anyContainers.add(requestCaptor.getValue());

    Resource freeResource = Resource.newInstance(500, 0);
    when(mockRMClient.getAvailableResources()).thenReturn(freeResource);
    scheduler.getProgress();
    drainableAppCallback.drain();
    Assert.assertEquals(totalResource, scheduler.getTotalResources());
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    final List<ArrayList<CookieContainerRequest>> anyList =
        new LinkedList<ArrayList<CookieContainerRequest>>();
    final List<ArrayList<CookieContainerRequest>> emptyList =
        new LinkedList<ArrayList<CookieContainerRequest>>();

    anyList.add(anyContainers);
    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId1 = ContainerId.newContainerId(attemptId, 1);
    Container mockContainer1 = Container.newInstance(mockCId1, host1, null, taskAsk, pri2, null);
    ContainerId mockCId2 = ContainerId.newContainerId(attemptId, 2);
    Container mockContainer2 = Container.newInstance(mockCId2, host1, null, taskAsk, pri6, null);
    ContainerId mockCId3 = ContainerId.newContainerId(attemptId, 3);
    Container mockContainer3 = Container.newInstance(mockCId3, host1, null, taskAsk, pri6, null);
    ContainerId mockCId4 = ContainerId.newContainerId(attemptId, 4);
    Container mockContainer4 = Container.newInstance(mockCId4, host1, null, taskAsk, pri2, null);
    List<Container> containers = new ArrayList<Container>();
    containers.add(mockContainer1);
    containers.add(mockContainer2);
    containers.add(mockContainer3);
    containers.add(mockContainer4);
    when(
        mockRMClient.getMatchingRequests((Priority) any(), eq("host1"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    // RackResolver by default puts hosts in default-rack
    when(
        mockRMClient.getMatchingRequests((Priority) any(), eq("/default-rack"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          int calls = 0;
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            if(calls > 0) {
              anyContainers.remove(0);
            }
            calls++;
            return anyList;
          }

        });
    
    Mockito.doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          ContainerId cId = (ContainerId) args[0];
          scheduler.deallocateContainer(cId);
          return null;
      }})
    .when(mockApp).preemptContainer((ContainerId)any());
    
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    Assert.assertEquals(4, scheduler.taskAllocations.size());
    Assert.assertEquals(4096, scheduler.allocatedResources.getMemory());
    Assert.assertEquals(mockCId1,
        scheduler.taskAllocations.get(mockTask1).getId());
    Assert.assertEquals(mockCId2,
        scheduler.taskAllocations.get(mockTask3).getId());
    Assert.assertEquals(mockCId3,
        scheduler.taskAllocations.get(mockTask3KillA).getId());
    // high priority container assigned to lower pri task. This task should still be preempted 
    // because the task priority is relevant for preemption and not the container priority
    Assert.assertEquals(mockCId4,
        scheduler.taskAllocations.get(mockTask3KillB).getId());

    // no preemption
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    // no need for task preemption until now - so they should match
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);

    // add a pending request that cannot be allocated until resources free up
    Object mockTask3WaitCookie = new Object();
    scheduler.allocateTask(mockTask3Wait, taskAsk, null,
                           null, pri6, obj3, mockTask3WaitCookie);
    // add a pri 8 request for the pri 8 container that will not be matched
    Object mockTaskPri8Cookie = new Object();
    scheduler.allocateTask(mockTaskPri8, taskAsk, null,
                           null, pri8, obj3, mockTaskPri8Cookie);
    // no preemption - same pri
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(6)).addContainerRequest(requestCaptor.capture());
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    ContainerId mockCId5 = ContainerId.newContainerId(attemptId, 5);
    Container mockContainer5 = Container.newInstance(mockCId5, host1, null, taskAsk, pri8, null);
    containers.clear();
    containers.add(mockContainer5);
    
    // new lower pri container added that wont be matched and eventually preempted
    // Fudge new container being present in delayed allocation list due to race
    HeldContainer heldContainer = new HeldContainer(mockContainer5, -1, -1, null,
        containerSignatureMatcher);
    scheduler.delayedContainerManager.delayedContainers.add(heldContainer);
    // no preemption - container assignment attempts < 3
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    // no need for task preemption until now - so they should match
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);

    heldContainer.incrementAssignmentAttempts();
    // no preemption - container assignment attempts < 3
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    heldContainer.incrementAssignmentAttempts();
    heldContainer.incrementAssignmentAttempts();
    // preemption - container released and resource asked again
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId5);
    // internally re-request pri8 task request because we release pri8 new container
    verify(mockRMClient, times(7)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest reAdded = requestCaptor.getValue();
    Assert.assertEquals(pri8, reAdded.getPriority());
    Assert.assertEquals(taskAsk, reAdded.getCapability());
    Assert.assertEquals(mockTaskPri8Cookie, reAdded.getCookie().getAppCookie());
    
    // remove fudging.
    scheduler.delayedContainerManager.delayedContainers.clear();

    // no need for task preemption until now - so they should match
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);

    scheduler.allocateTask(mockTask3Retry, taskAsk, null,
                           null, pri5, obj3, null);
    // no preemption - higher pri. exact match
    scheduler.getProgress();
    // no need for task preemption until now - so they should match
    drainableAppCallback.drain();
    // no need for task preemption until now - so they should match
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);
    verify(mockRMClient, times(1)).releaseAssignedContainer((ContainerId)any());

    for (int i=0; i<11; ++i) {
      scheduler.allocateTask(mockTask2, taskAsk, null,
                             null, pri4, null, null);
    }
    drainableAppCallback.drain();

    // mockTaskPri3KillB gets preempted to clear 10% of outstanding running preemptable tasks
    // this is also a higher priority container than the pending task priority but was running a 
    // lower priority task. Task priority is relevant for preemption and not container priority as
    // containers can run tasks of different priorities
    scheduler.getProgress(); // first heartbeat
    Assert.assertTrue(scheduler.numHeartbeats > scheduler.heartbeatAtLastPreemption);
    drainableAppCallback.drain();
    scheduler.getProgress(); // second heartbeat
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).releaseAssignedContainer((ContainerId)any());
    scheduler.getProgress(); // third heartbeat
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId4);
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);
    // there are pending preemptions.
    scheduler.getProgress(); // first heartbeat
    scheduler.getProgress(); // second heartbeat
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId) any());
    scheduler.getProgress(); // third heartbeat
    drainableAppCallback.drain();
    // Next oldest mockTaskPri3KillA gets preempted to clear 10% of outstanding running preemptable tasks
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId3);

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, "", DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
  }
  
  @Test (timeout=5000)
  public void testTaskSchedulerPreemption2() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    int waitTime = 1000;
    
    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_HEARTBEATS_BETWEEN_PREEMPTIONS, 2);
    conf.setInt(TezConfiguration.TEZ_AM_PREEMPTION_MAX_WAIT_TIME_MS, waitTime);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL,
      false, null, null, new PreemptionMatcher(), conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);

    final TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    scheduler.start();

    // no preemption
    scheduler.getProgress();
    drainableAppCallback.drain();
    Resource totalResource = mockRMClient.getAvailableResources();
    Assert.assertEquals(totalResource, scheduler.getTotalResources());
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    // allocate task
    Object mockTask1 = new MockTask("task1");
    Object mockTask2 = new MockTask("task2");
    Object mockTask3 = new MockTask("task3");
    Object obj3 = new Object();
    Priority pri2 = Priority.newInstance(2);
    Priority pri4 = Priority.newInstance(4);
    Priority pri6 = Priority.newInstance(6);

    ArgumentCaptor<CookieContainerRequest> requestCaptor =
        ArgumentCaptor.forClass(CookieContainerRequest.class);
    final ArrayList<CookieContainerRequest> anyContainers =
        new ArrayList<CookieContainerRequest>();

    
    Resource taskAsk = Resource.newInstance(1024, 1);
    scheduler.allocateTask(mockTask1, taskAsk, null,
                           null, pri4, null, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
        addContainerRequest(requestCaptor.capture());
    anyContainers.add(requestCaptor.getValue());

    scheduler.getProgress();
    drainableAppCallback.drain();
    Assert.assertEquals(totalResource, scheduler.getTotalResources());
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    NodeId host1 = NodeId.newInstance("host1", 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId1 = ContainerId.newContainerId(attemptId, 1);
    Container mockContainer1 = Container.newInstance(mockCId1, host1, null, taskAsk, pri4, null);
    List<Container> containers = new ArrayList<Container>();
    containers.add(mockContainer1);
    
    Mockito.doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          ContainerId cId = (ContainerId) args[0];
          scheduler.deallocateContainer(cId);
          return null;
      }})
    .when(mockApp).preemptContainer((ContainerId)any());
    
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    Assert.assertEquals(1, scheduler.taskAllocations.size());
    Assert.assertEquals(mockCId1,
        scheduler.taskAllocations.get(mockTask1).getId());

    // no preemption
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    // no need for task preemption until now - so they should match
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);

    // add a pending request that cannot be allocated until resources free up
    Object mockTask2Cookie = new Object();
    scheduler.allocateTask(mockTask2, taskAsk, null,
                           null, pri2, obj3, mockTask2Cookie);
    Object mockTask3Cookie = new Object();
    scheduler.allocateTask(mockTask3, taskAsk, null,
                           null, pri6, obj3, mockTask3Cookie);
    // nothing waiting till now
    Assert.assertNull(scheduler.highestWaitingRequestPriority);
    Assert.assertEquals(0, scheduler.highestWaitingRequestWaitStartTime);

    long currTime = System.currentTimeMillis();
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    // enough free resources. preemption not triggered
    Assert.assertEquals(pri2, scheduler.highestWaitingRequestPriority);
    Assert.assertTrue(scheduler.highestWaitingRequestWaitStartTime >= currTime);
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);
    
    Thread.sleep(waitTime + 10);
    long oldStartWaitTime = scheduler.highestWaitingRequestWaitStartTime;
    
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    // enough free resources. deadline crossed. preemption triggered
    Assert.assertEquals(pri2, scheduler.highestWaitingRequestPriority);
    Assert.assertEquals(oldStartWaitTime, scheduler.highestWaitingRequestWaitStartTime);
    Assert.assertTrue(scheduler.numHeartbeats > scheduler.heartbeatAtLastPreemption);

    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId1);
    Assert.assertEquals(scheduler.numHeartbeats, scheduler.heartbeatAtLastPreemption);
    // maintains existing waiting values
    Assert.assertEquals(pri2, scheduler.highestWaitingRequestPriority);
    Assert.assertEquals(oldStartWaitTime, scheduler.highestWaitingRequestWaitStartTime);

    // remove high pri request to test waiting pri change
    scheduler.deallocateTask(mockTask2, false, null, null);
    
    scheduler.getProgress();
    // waiting value changes
    Assert.assertEquals(pri6, scheduler.highestWaitingRequestPriority);
    Assert.assertTrue(oldStartWaitTime < scheduler.highestWaitingRequestWaitStartTime);

    Thread.sleep(waitTime + 10);
    scheduler.getProgress();
    drainableAppCallback.drain();
    // deadlines crossed but nothing lower pri running. so reset
    Assert.assertNull(scheduler.highestWaitingRequestPriority);
    Assert.assertEquals(0, scheduler.highestWaitingRequestWaitStartTime);

    scheduler.getProgress();
    drainableAppCallback.drain();
    // waiting value changes
    Assert.assertEquals(pri6, scheduler.highestWaitingRequestPriority);
    Assert.assertTrue(oldStartWaitTime < scheduler.highestWaitingRequestWaitStartTime);

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, "", DEFAULT_APP_URL);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.shutdown();
    drainableAppCallback.drain();
  }

  @Test(timeout = 5000)
  public void testLocalityMatching() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> amrmClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);

    TaskSchedulerContext appClient = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, "", conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(appClient);

    TaskSchedulerWithDrainableContext taskScheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, amrmClient);

    taskScheduler.initialize();
    taskScheduler.start();
    
    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(1);

    String hostsTask1[] = { "host1" };
    String hostsTask2[] = { "non-allocated-host" };

    String defaultRack[] = { "/default-rack" };
    String otherRack[] = { "/other-rack" };

    Object mockTask1 = new MockTask("task1");
    CookieContainerRequest mockCookie1 = mock(CookieContainerRequest.class,
        RETURNS_DEEP_STUBS);
    when(mockCookie1.getCookie().getTask()).thenReturn(mockTask1);

    Object mockTask2 = new MockTask("task2");
    CookieContainerRequest mockCookie2 = mock(CookieContainerRequest.class,
        RETURNS_DEEP_STUBS);
    when(mockCookie2.getCookie().getTask()).thenReturn(mockTask2);

    Container containerHost1 = createContainer(1, "host1", resource, priority);
    Container containerHost3 = createContainer(2, "host3", resource, priority);
    List<Container> allocatedContainers = new LinkedList<Container>();

    allocatedContainers.add(containerHost3);
    allocatedContainers.add(containerHost1);

    taskScheduler.allocateTask(mockTask1, resource, hostsTask1, defaultRack,
        priority, null, mockCookie1);
    drainableAppCallback.drain();

    List<CookieContainerRequest> host1List = new ArrayList<CookieContainerRequest>();
    host1List.add(mockCookie1);
    List<CookieContainerRequest> defaultRackList = new ArrayList<CookieContainerRequest>();
    defaultRackList.add(mockCookie1);

    List<CookieContainerRequest> nonAllocatedHostList = new ArrayList<YarnTaskSchedulerService.CookieContainerRequest>();
    nonAllocatedHostList.add(mockCookie2);
    List<CookieContainerRequest> otherRackList = new ArrayList<YarnTaskSchedulerService.CookieContainerRequest>();
    otherRackList.add(mockCookie2);
    taskScheduler.allocateTask(mockTask2, resource, hostsTask2, otherRack,
        priority, null, mockCookie2);
    drainableAppCallback.drain();

    List<CookieContainerRequest> anyList = new LinkedList<YarnTaskSchedulerService.CookieContainerRequest>();
    anyList.add(mockCookie1);
    anyList.add(mockCookie2);

    taskScheduler.onContainersAllocated(allocatedContainers);
    drainableAppCallback.drain();

    ArgumentCaptor<Object> taskCaptor = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Container> containerCaptor = ArgumentCaptor
        .forClass(Container.class);
    verify(appClient, times(2)).taskAllocated(taskCaptor.capture(), any(),
        containerCaptor.capture());

    // Expected containerHost1 allocated to task1 due to locality,
    // containerHost3 allocated to task2.

    List<Container> assignedContainers = containerCaptor.getAllValues();
    int container1Pos = assignedContainers.indexOf(containerHost1);
    assertTrue("Container: " + containerHost1 + " was not assigned",
        container1Pos != -1);
    assertEquals("Task 1 was not allocated to containerHost1", mockTask1,
        taskCaptor.getAllValues().get(container1Pos));

    int container2Pos = assignedContainers.indexOf(containerHost3);
    assertTrue("Container: " + containerHost3 + " was not assigned",
        container2Pos != -1);
    assertEquals("Task 2 was not allocated to containerHost3", mockTask2,
        taskCaptor.getAllValues().get(container2Pos));

    AppFinalStatus finalStatus = new AppFinalStatus(
        FinalApplicationStatus.SUCCEEDED, "", "");
    when(appClient.getFinalAppStatus()).thenReturn(finalStatus);
    taskScheduler.shutdown();
  }
  
  @Test (timeout=5000)
  public void testScaleDownPercentage() {
    Assert.assertEquals(100, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 100));
    Assert.assertEquals(70, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 70));
    Assert.assertEquals(50, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 50));
    Assert.assertEquals(10, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 10));
    Assert.assertEquals(5, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 5));
    Assert.assertEquals(1, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(100, 1));
    Assert.assertEquals(1, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(5, 5));
    Assert.assertEquals(1, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(1, 10));
    Assert.assertEquals(1, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(1, 70));
    Assert.assertEquals(1, YarnTaskSchedulerService.scaleDownByPreemptionPercentage(1, 1));
  }

  @Test
  public void testContainerExpired() throws Exception {
    TezAMRMClientAsync<CookieContainerRequest> mockRMClient = spy(
        new AMRMClientAsyncForTest(new AMRMClientForTest(), 100));

    Configuration conf = new Configuration();
    // to match all in the same pass
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    // to release immediately after deallocate
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);

    TaskSchedulerContext mockApp = setupMockTaskSchedulerContext(DEFAULT_APP_HOST, DEFAULT_APP_PORT, DEFAULT_APP_URL, conf);
    final TaskSchedulerContextDrainable drainableAppCallback = createDrainableContext(mockApp);
    TaskSchedulerWithDrainableContext scheduler =
        new TaskSchedulerWithDrainableContext(drainableAppCallback, mockRMClient);

    scheduler.initialize();
    scheduler.start();
    drainableAppCallback.drain();

    Object mockTask1 = new MockTask("task1");
    Object mockCookie1 = new Object();
    Resource mockCapability = Resource.newInstance(1024, 1);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    final Priority mockPriority1 = Priority.newInstance(1);
    final Priority mockPriority2 = Priority.newInstance(2);
    Object mockTask2 = new MockTask("task2");
    Object mockCookie2 = new Object();
    ArgumentCaptor<CookieContainerRequest> requestCaptor =
        ArgumentCaptor.forClass(CookieContainerRequest.class);

    scheduler.allocateTask(mockTask2, mockCapability, hosts,
        racks, mockPriority2, null, mockCookie2);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
                                addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request2 = requestCaptor.getValue();

    scheduler.allocateTask(mockTask1, mockCapability, hosts,
        racks, mockPriority1, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).
                                addContainerRequest(requestCaptor.capture());

    List<Container> containers = new ArrayList<Container>();
    // sending only lower priority container to make sure its not matched
    NodeId host2 = NodeId.newInstance("host2", 2);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1);
    ContainerId mockCId2 = ContainerId.newContainerId(attemptId, 2);
    Container mockContainer2 = Container.newInstance(mockCId2, host2, null, mockCapability, mockPriority2, null);
    containers.add(mockContainer2);

    scheduler.onContainersAllocated(containers);

    List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
    ContainerStatus mockStatus2 = mock(ContainerStatus.class);
    when(mockStatus2.getContainerId()).thenReturn(mockCId2);
    statuses.add(mockStatus2);
    scheduler.onContainersCompleted(statuses);

    verify(mockApp, times(0)).taskAllocated(any(), any(), any(Container.class));
    verify(mockRMClient, times(3)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest resubmitRequest = requestCaptor.getValue();
    assertEquals(request2.getCookie().getTask(), resubmitRequest.getCookie().getTask());
    assertEquals(request2.getCookie().getAppCookie(), resubmitRequest.getCookie().getAppCookie());
    assertEquals(request2.getCookie().getContainerSignature(), resubmitRequest.getCookie().getContainerSignature());
    assertEquals(request2.getCapability(), resubmitRequest.getCapability());
    assertEquals(request2.getPriority(), resubmitRequest.getPriority());

    // verify container is not re-requested when nothing at that priority
    assertFalse(scheduler.deallocateTask(mockTask2, true, null, null));
    scheduler.onContainersAllocated(containers);
    scheduler.onContainersCompleted(statuses);
    verify(mockApp, times(0)).taskAllocated(any(), any(), any(Container.class));
    verify(mockRMClient, times(3)).addContainerRequest(requestCaptor.capture());
  }

  private Container createContainer(int id, String host, Resource resource,
      Priority priority) {
    ContainerId containerID = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 1), 1),
        id);
    NodeId nodeID = NodeId.newInstance(host, 0);
    Container container = Container.newInstance(containerID, nodeID, host
        + ":0", resource, priority, null);
    return container;
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
}

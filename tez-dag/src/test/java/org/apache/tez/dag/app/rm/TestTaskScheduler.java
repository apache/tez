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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.CookieContainerRequest;
import org.apache.tez.dag.app.rm.YarnTaskSchedulerService.HeldContainer;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerAppCallbackDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableAppCallback;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AlwaysMatchesContainerMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.PreemptionMatcher;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Sets;


public class TestTaskScheduler {

  RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  static ContainerSignatureMatcher containerSignatureMatcher = new AlwaysMatchesContainerMatcher();

  @BeforeClass
  public static void beforeClass() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @SuppressWarnings({ "unchecked" })
  @Test(timeout=10000)
  public void testTaskSchedulerNoReuse() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskSchedulerWithDrainableAppCallback scheduler =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    TaskSchedulerAppCallbackDrainable drainableAppCallback = scheduler
        .getDrainableAppCallback();

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    int interval = 100;
    conf.setInt(TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX, interval);
    scheduler.init(conf);
    drainableAppCallback.drain();
    verify(mockRMClient).init(conf);
    verify(mockRMClient).setHeartbeatInterval(interval);

    RegisterApplicationMasterResponse mockRegResponse =
                                mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).
                                                   thenReturn(mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    ByteBuffer mockKey = mock(ByteBuffer.class);
    when(mockRegResponse.getClientToAMTokenMasterKey()).thenReturn(mockKey);
    when(mockRMClient.
          registerApplicationMaster(anyString(), anyInt(), anyString())).
                                                   thenReturn(mockRegResponse);
    scheduler.start();
    drainableAppCallback.drain();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    verify(mockApp).setApplicationRegistrationData(mockMaxResource,
                                                   mockAcls, mockKey);

    when(mockRMClient.getClusterNodeCount()).thenReturn(5);
    Assert.assertEquals(5, scheduler.getClusterNodeCount());

    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getAvailableResources()).
                                              thenReturn(mockClusterResource);
    Assert.assertEquals(mockClusterResource,
                        mockRMClient.getAvailableResources());

    Object mockTask1 = mock(Object.class);
    Object mockCookie1 = mock(Object.class);
    Resource mockCapability = mock(Resource.class);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    Priority mockPriority = mock(Priority.class);
    ArgumentCaptor<CookieContainerRequest> requestCaptor =
                        ArgumentCaptor.forClass(CookieContainerRequest.class);
    // allocate task
    scheduler.allocateTask(mockTask1, mockCapability, hosts,
                           racks, mockPriority, null, mockCookie1);
    drainableAppCallback.drain();
    verify(mockRMClient, times(1)).
                           addContainerRequest((CookieContainerRequest) any());

    // returned from task requests before allocation happens
    assertFalse(scheduler.deallocateTask(mockTask1, true));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((CookieContainerRequest) any());
    verify(mockRMClient, times(0)).
                                 releaseAssignedContainer((ContainerId) any());

    // deallocating unknown task
    assertFalse(scheduler.deallocateTask(mockTask1, true));
    verify(mockApp, times(0)).containerBeingReleased(any(ContainerId.class));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((CookieContainerRequest) any());
    verify(mockRMClient, times(0)).
                                 releaseAssignedContainer((ContainerId) any());

    // allocate tasks
    Object mockTask2 = mock(Object.class);
    Object mockCookie2 = mock(Object.class);
    Object mockTask3 = mock(Object.class);
    Object mockCookie3 = mock(Object.class);
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

    List<Container> containers = new ArrayList<Container>();
    Container mockContainer1 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer1.getNodeId().getHost()).thenReturn("host1");
    ContainerId mockCId1 = mock(ContainerId.class);
    when(mockContainer1.getId()).thenReturn(mockCId1);
    containers.add(mockContainer1);
    Container mockContainer2 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer2.getNodeId().getHost()).thenReturn("host2");
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    containers.add(mockContainer2);
    Container mockContainer3 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer3.getNodeId().getHost()).thenReturn("host3");
    ContainerId mockCId3 = mock(ContainerId.class);
    when(mockContainer3.getId()).thenReturn(mockCId3);
    containers.add(mockContainer3);
    Container mockContainer4 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer4.getNodeId().getHost()).thenReturn("host4");
    ContainerId mockCId4 = mock(ContainerId.class);
    when(mockContainer4.getId()).thenReturn(mockCId4);
    containers.add(mockContainer4);
    ArrayList<CookieContainerRequest> hostContainers =
                             new ArrayList<CookieContainerRequest>();
    hostContainers.add(request1);
    hostContainers.add(request2);
    hostContainers.add(request3);
    ArrayList<CookieContainerRequest> rackContainers =
                             new ArrayList<CookieContainerRequest>();
    rackContainers.add(request2);
    rackContainers.add(request3);
    ArrayList<CookieContainerRequest> anyContainers =
                             new ArrayList<CookieContainerRequest>();
    anyContainers.add(request3);

    final List<ArrayList<CookieContainerRequest>> hostList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    hostList.add(hostContainers);
    final List<ArrayList<CookieContainerRequest>> rackList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    rackList.add(rackContainers);
    final List<ArrayList<CookieContainerRequest>> anyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    anyList.add(anyContainers);
    final List<ArrayList<CookieContainerRequest>> emptyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    // return all requests for host1
    when(
        mockRMClient.getMatchingRequests((Priority) any(), eq("host1"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return hostList;
          }

        });
    // first request matched by host
    // second request matched to rack. RackResolver by default puts hosts in
    // /default-rack. We need to workaround by returning rack matches only once
    when(
        mockRMClient.getMatchingRequests((Priority) any(), eq("/default-rack"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return rackList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    // third request matched to ANY
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
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
    assertTrue(scheduler.deallocateTask(mockTask1, true));
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
    NodeId badNodeId = mock(NodeId.class);
    when(badNodeId.getHost()).thenReturn(badHost);
    scheduler.blacklistNode(badNodeId);
    verify(mockRMClient, times(1)).addNodeToBlacklist(badNodeId);
    Object mockTask4 = mock(Object.class);
    Object mockCookie4 = mock(Object.class);
    scheduler.allocateTask(mockTask4, mockCapability, null,
        null, mockPriority, null, mockCookie4);
    drainableAppCallback.drain();
    verify(mockRMClient, times(5)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request4 = requestCaptor.getValue();
    anyContainers.clear();
    anyContainers.add(request4);
    Container mockContainer5 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer5.getNodeId().getHost()).thenReturn(badHost);
    when(mockContainer5.getNodeId()).thenReturn(badNodeId);
    ContainerId mockCId5 = mock(ContainerId.class);
    when(mockContainer5.getId()).thenReturn(mockCId5);
    containers.clear();
    containers.add(mockContainer5);
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    // no new allocation
    verify(mockApp, times(3)).taskAllocated(any(), any(), (Container) any());
    // verify blacklisted container released
    verify(mockRMClient).releaseAssignedContainer(mockCId5);
    verify(mockRMClient, times(4)).releaseAssignedContainer((ContainerId) any());
    // verify request added back
    verify(mockRMClient, times(6)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request5 = requestCaptor.getValue();
    anyContainers.clear();
    anyContainers.add(request5);
    Container mockContainer6 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer6.getNodeId().getHost()).thenReturn("host7");
    ContainerId mockCId6 = mock(ContainerId.class);
    when(mockContainer6.getId()).thenReturn(mockCId6);
    containers.clear();
    containers.add(mockContainer6);
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    // new allocation
    verify(mockApp, times(4)).taskAllocated(any(), any(), (Container) any());
    verify(mockApp).taskAllocated(mockTask4, mockCookie4, mockContainer6);
    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask4, true));
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
    assertFalse(scheduler.deallocateTask(mockTask1, true));

    List<NodeReport> mockUpdatedNodes = mock(List.class);
    scheduler.onNodesUpdated(mockUpdatedNodes);
    drainableAppCallback.drain();
    verify(mockApp).nodesUpdated(mockUpdatedNodes);

    Exception mockException = mock(Exception.class);
    scheduler.onError(mockException);
    drainableAppCallback.drain();
    verify(mockApp).onError(mockException);

    scheduler.onShutdownRequest();
    drainableAppCallback.drain();
    verify(mockApp).appShutdownRequested();

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    drainableAppCallback.drain();
    verify(mockRMClient).
                  unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                                              appMsg, appUrl);
    verify(mockRMClient).stop();
    scheduler.close();
  }

  @SuppressWarnings({ "unchecked" })
  @Test(timeout=10000)
  public void testTaskSchedulerInitiateStop() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskSchedulerWithDrainableAppCallback scheduler =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    final TaskSchedulerAppCallbackDrainable drainableAppCallback = scheduler
        .getDrainableAppCallback();

    Configuration conf = new Configuration();
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    // keep containers held for 10 seconds
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 10000);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 10000);
    scheduler.init(conf);
    drainableAppCallback.drain();

    RegisterApplicationMasterResponse mockRegResponse =
                                mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).
                                                   thenReturn(mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    when(mockRMClient.
          registerApplicationMaster(anyString(), anyInt(), anyString())).
                                                   thenReturn(mockRegResponse);
    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getAvailableResources()).
                                              thenReturn(mockClusterResource);

    scheduler.start();
    drainableAppCallback.drain();

    Object mockTask1 = mock(Object.class);
    when(mockTask1.toString()).thenReturn("task1");
    Object mockCookie1 = mock(Object.class);
    Resource mockCapability = mock(Resource.class);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    final Priority mockPriority1 = Priority.newInstance(1);
    final Priority mockPriority2 = Priority.newInstance(2);
    final Priority mockPriority3 = Priority.newInstance(3);
    final Priority mockPriority4 = Priority.newInstance(4);
    final Priority mockPriority5 = Priority.newInstance(5);
    Object mockTask2 = mock(Object.class);
    when(mockTask2.toString()).thenReturn("task2");
    Object mockCookie2 = mock(Object.class);
    Object mockTask3 = mock(Object.class);
    when(mockTask3.toString()).thenReturn("task3");
    Object mockCookie3 = mock(Object.class);
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
    Container mockContainer1 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer1.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer1.getPriority()).thenReturn(mockPriority1);
    when(mockContainer1.toString()).thenReturn("container1");
    ContainerId mockCId1 = mock(ContainerId.class);
    when(mockContainer1.getId()).thenReturn(mockCId1);
    when(mockCId1.toString()).thenReturn("container1");
    containers.add(mockContainer1);
    Container mockContainer2 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer2.getNodeId().getHost()).thenReturn("host2");
    when(mockContainer2.getPriority()).thenReturn(mockPriority2);
    when(mockContainer2.toString()).thenReturn("container2");
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    when(mockCId2.toString()).thenReturn("container2");
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

    final List<ArrayList<CookieContainerRequest>> hostList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    hostList.add(hostContainers);
    final List<ArrayList<CookieContainerRequest>> rackList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    rackList.add(rackContainers);
    final List<ArrayList<CookieContainerRequest>> anyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    anyList.add(anyContainers);
    final List<ArrayList<CookieContainerRequest>> emptyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    // return pri1 requests for host1
    when(
        mockRMClient.getMatchingRequestsForTopPriority(eq("host1"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return hostList;
          }

        });
    // second request matched to rack. RackResolver by default puts hosts in
    // /default-rack. We need to workaround by returning rack matches only once
    when(
        mockRMClient.getMatchingRequestsForTopPriority(eq("/default-rack"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return rackList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    // third request matched to ANY
    when(
        mockRMClient.getMatchingRequestsForTopPriority(
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });

    when(mockRMClient.getTopPriority()).then(
        new Answer<Priority>() {
          @Override
          public Priority answer(
              InvocationOnMock invocation) throws Throwable {
            int allocations = drainableAppCallback.count.get();
            if (allocations == 0) {
              return mockPriority1;
            }
            if (allocations == 1) {
              return mockPriority2;
            }
            if (allocations == 2) {
              return mockPriority3;
            }
            if (allocations == 3) {
              return mockPriority4;
            }
            return null;
          }
        });

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
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskSchedulerWithDrainableAppCallback scheduler =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    final TaskSchedulerAppCallbackDrainable drainableAppCallback = scheduler
        .getDrainableAppCallback();

    Configuration conf = new Configuration();
    // to match all in the same pass
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    // to release immediately after deallocate
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, 0);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, 0);
    scheduler.init(conf);
    drainableAppCallback.drain();

    RegisterApplicationMasterResponse mockRegResponse =
                                mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).
                                                   thenReturn(mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    when(mockRMClient.
          registerApplicationMaster(anyString(), anyInt(), anyString())).
                                                   thenReturn(mockRegResponse);
    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getAvailableResources()).
                                              thenReturn(mockClusterResource);

    scheduler.start();
    drainableAppCallback.drain();

    Object mockTask1 = mock(Object.class);
    when(mockTask1.toString()).thenReturn("task1");
    Object mockCookie1 = mock(Object.class);
    Resource mockCapability = mock(Resource.class);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    final Priority mockPriority1 = Priority.newInstance(1);
    final Priority mockPriority2 = Priority.newInstance(2);
    final Priority mockPriority3 = Priority.newInstance(3);
    final Priority mockPriority4 = Priority.newInstance(4);
    final Priority mockPriority5 = Priority.newInstance(5);
    Object mockTask2 = mock(Object.class);
    when(mockTask2.toString()).thenReturn("task2");
    Object mockCookie2 = mock(Object.class);
    Object mockTask3 = mock(Object.class);
    when(mockTask3.toString()).thenReturn("task3");
    Object mockCookie3 = mock(Object.class);
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
    Container mockContainer4 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer4.getNodeId().getHost()).thenReturn("host4");
    when(mockContainer4.toString()).thenReturn("container4");
    when(mockContainer4.getPriority()).thenReturn(mockPriority4);
    ContainerId mockCId4 = mock(ContainerId.class);
    when(mockContainer4.getId()).thenReturn(mockCId4);
    when(mockCId4.toString()).thenReturn("container4");
    containers.add(mockContainer4);
    Container mockContainer1 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer1.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer1.getPriority()).thenReturn(mockPriority1);
    when(mockContainer1.toString()).thenReturn("container1");
    ContainerId mockCId1 = mock(ContainerId.class);
    when(mockContainer1.getId()).thenReturn(mockCId1);
    when(mockCId1.toString()).thenReturn("container1");
    containers.add(mockContainer1);
    Container mockContainer2 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer2.getNodeId().getHost()).thenReturn("host2");
    when(mockContainer2.getPriority()).thenReturn(mockPriority2);
    when(mockContainer2.toString()).thenReturn("container2");
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    when(mockCId2.toString()).thenReturn("container2");
    containers.add(mockContainer2);
    Container mockContainer3 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer3.getNodeId().getHost()).thenReturn("host3");
    when(mockContainer3.getPriority()).thenReturn(mockPriority3);
    when(mockContainer3.toString()).thenReturn("container3");
    ContainerId mockCId3 = mock(ContainerId.class);
    when(mockContainer3.getId()).thenReturn(mockCId3);
    when(mockCId3.toString()).thenReturn("container3");
    containers.add(mockContainer3);

    ArrayList<CookieContainerRequest> hostContainers =
                             new ArrayList<CookieContainerRequest>();
    hostContainers.add(request1);
    ArrayList<CookieContainerRequest> rackContainers =
                             new ArrayList<CookieContainerRequest>();
    rackContainers.add(request2);
    ArrayList<CookieContainerRequest> anyContainers =
                             new ArrayList<CookieContainerRequest>();
    anyContainers.add(request3);

    final List<ArrayList<CookieContainerRequest>> hostList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    hostList.add(hostContainers);
    final List<ArrayList<CookieContainerRequest>> rackList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    rackList.add(rackContainers);
    final List<ArrayList<CookieContainerRequest>> anyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    anyList.add(anyContainers);
    final List<ArrayList<CookieContainerRequest>> emptyList =
                        new LinkedList<ArrayList<CookieContainerRequest>>();
    // return pri1 requests for host1
    when(
        mockRMClient.getMatchingRequestsForTopPriority(eq("host1"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return hostList;
          }

        });
    // second request matched to rack. RackResolver by default puts hosts in
    // /default-rack. We need to workaround by returning rack matches only once
    when(
        mockRMClient.getMatchingRequestsForTopPriority(eq("/default-rack"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return rackList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    // third request matched to ANY
    when(
        mockRMClient.getMatchingRequestsForTopPriority(
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    
    when(mockRMClient.getTopPriority()).then(        
        new Answer<Priority>() {
          @Override
          public Priority answer(
              InvocationOnMock invocation) throws Throwable {
            int allocations = drainableAppCallback.count.get();
            if (allocations == 0) {
              return mockPriority1;
            }
            if (allocations == 1) {
              return mockPriority2;
            }
            if (allocations == 2) {
              return mockPriority3;
            }
            if (allocations == 3) {
              return mockPriority4;
            }
            return null;
          }
        });
    
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
    assertTrue(scheduler.deallocateTask(mockTask1, true));
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
    NodeId badNodeId = mock(NodeId.class);
    when(badNodeId.getHost()).thenReturn(badHost);
    scheduler.blacklistNode(badNodeId);
    verify(mockRMClient, times(1)).addNodeToBlacklist(badNodeId);
    Object mockTask4 = mock(Object.class);
    when(mockTask4.toString()).thenReturn("task4");
    Object mockCookie4 = mock(Object.class);
    scheduler.allocateTask(mockTask4, mockCapability, null,
        null, mockPriority4, null, mockCookie4);
    drainableAppCallback.drain();
    verify(mockRMClient, times(4)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request4 = requestCaptor.getValue();
    anyContainers.clear();
    anyContainers.add(request4);
    Container mockContainer5 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer5.getNodeId().getHost()).thenReturn(badHost);
    when(mockContainer5.getNodeId()).thenReturn(badNodeId);
    ContainerId mockCId5 = mock(ContainerId.class);
    when(mockContainer5.toString()).thenReturn("container5");
    when(mockCId5.toString()).thenReturn("container5");
    when(mockContainer5.getId()).thenReturn(mockCId5);
    when(mockContainer5.getPriority()).thenReturn(mockPriority4);
    containers.clear();
    containers.add(mockContainer5);
    when(
        mockRMClient.getMatchingRequestsForTopPriority(
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
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
    CookieContainerRequest request5 = requestCaptor.getValue();
    anyContainers.clear();
    anyContainers.add(request5);
    Container mockContainer6 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer6.getNodeId().getHost()).thenReturn("host7");
    ContainerId mockCId6 = mock(ContainerId.class);
    when(mockContainer6.getId()).thenReturn(mockCId6);
    when(mockContainer6.toString()).thenReturn("container6");
    when(mockCId6.toString()).thenReturn("container6");
    containers.clear();
    containers.add(mockContainer6);
    when(
        mockRMClient.getMatchingRequestsForTopPriority(
            eq(ResourceRequest.ANY), (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return anyList;
          }

        }).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return emptyList;
          }

        });
    drainNotifier.set(false);
    scheduler.onContainersAllocated(containers);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    // new allocation
    verify(mockApp, times(4)).taskAllocated(any(), any(), (Container) any());
    verify(mockApp).taskAllocated(mockTask4, mockCookie4, mockContainer6);
    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask4, true));
    drainableAppCallback.drain();
    verify(mockApp).containerBeingReleased(mockCId6);
    verify(mockRMClient).releaseAssignedContainer(mockCId6);
    verify(mockRMClient, times(5)).releaseAssignedContainer((ContainerId) any());
    // test unblacklist
    scheduler.unblacklistNode(badNodeId);
    verify(mockRMClient, times(1)).removeNodeFromBlacklist(badNodeId);
    assertEquals(0, scheduler.blacklistedNodes.size());
    
    // verify container level matching
    // fudge the top level priority to prevent containers from being released
    // if top level priority is higher than newly allocated containers then 
    // they will not be released
    final AtomicBoolean fudgePriority = new AtomicBoolean(true);
    when(mockRMClient.getTopPriority()).then(        
        new Answer<Priority>() {
          @Override
          public Priority answer(
              InvocationOnMock invocation) throws Throwable {
            if (fudgePriority.get()) {
              return mockPriority4;
            }
            return mockPriority5;
          }
        });
    // add a dummy task to prevent release of allocated containers
    Object mockTask5 = mock(Object.class);
    when(mockTask5.toString()).thenReturn("task5");
    Object mockCookie5 = mock(Object.class);
    scheduler.allocateTask(mockTask5, mockCapability, hosts,
        racks, mockPriority5, null, mockCookie5);
    verify(mockRMClient, times(6)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request6 = requestCaptor.getValue();
    drainableAppCallback.drain();
    // add containers so that we can reference one of them for container specific
    // allocation
    containers.clear();
    Container mockContainer7 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer7.getNodeId().getHost()).thenReturn("host5");
    ContainerId mockCId7 = mock(ContainerId.class);
    when(mockContainer7.toString()).thenReturn("container7");
    when(mockCId7.toString()).thenReturn("container7");
    when(mockContainer7.getId()).thenReturn(mockCId7);
    when(mockContainer7.getPriority()).thenReturn(mockPriority5);
    containers.add(mockContainer7);
    Container mockContainer8 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer8.getNodeId().getHost()).thenReturn("host5");
    ContainerId mockCId8 = mock(ContainerId.class);
    when(mockContainer8.toString()).thenReturn("container8");
    when(mockCId8.toString()).thenReturn("container8");
    when(mockContainer8.getId()).thenReturn(mockCId8);
    when(mockContainer8.getPriority()).thenReturn(mockPriority5);
    containers.add(mockContainer8);
    drainNotifier.set(false);
    scheduler.onContainersAllocated(containers);
    drainableAppCallback.drain();
    verify(mockRMClient, times(5)).releaseAssignedContainer((ContainerId) any());    
    Object mockTask6 = mock(Object.class);
    when(mockTask6.toString()).thenReturn("task6");
    Object mockCookie6 = mock(Object.class);
    // allocate request with container affinity
    scheduler.allocateTask(mockTask6, mockCapability, mockCId7, mockPriority5, null, mockCookie6);
    drainableAppCallback.drain();
    verify(mockRMClient, times(7)).addContainerRequest(requestCaptor.capture());
    CookieContainerRequest request7 = requestCaptor.getValue();
    hostContainers.clear();
    hostContainers.add(request6);
    hostContainers.add(request7);
    
    when(
        mockRMClient.getMatchingRequestsForTopPriority(eq("host5"),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {
          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            return hostList;
          }

        });
    // stop fudging top priority
    fudgePriority.set(false);
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(mockApp, times(6)).taskAllocated(any(), any(), (Container) any());
    // container7 allocated to the task with affinity for it
    verify(mockApp).taskAllocated(mockTask6, mockCookie6, mockContainer7);
    // deallocate allocated task
    assertTrue(scheduler.deallocateTask(mockTask5, true));
    assertTrue(scheduler.deallocateTask(mockTask6, true));
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

    Exception mockException = mock(Exception.class);
    scheduler.onError(mockException);
    drainableAppCallback.drain();
    verify(mockApp).onError(mockException);

    scheduler.onShutdownRequest();
    drainableAppCallback.drain();
    verify(mockApp).appShutdownRequested();

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    drainableAppCallback.drain();
    verify(mockRMClient).
                  unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                                              appMsg, appUrl);
    verify(mockRMClient).stop();
    scheduler.close();
  }
  
  @SuppressWarnings("unchecked")
  @Test (timeout=5000)
  public void testTaskSchedulerDetermineMinHeldContainers() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);
    when(mockAppContext.isSession()).thenReturn(true);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskSchedulerWithDrainableAppCallback scheduler =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    TaskSchedulerAppCallbackDrainable drainableAppCallback = scheduler
        .getDrainableAppCallback();

    Configuration conf = new Configuration();
    scheduler.init(conf);
    RegisterApplicationMasterResponse mockRegResponse = mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).thenReturn(
        mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    when(
        mockRMClient.registerApplicationMaster(anyString(), anyInt(),
            anyString())).thenReturn(mockRegResponse);
    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getAvailableResources()).thenReturn(mockClusterResource);

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

    Resource r = Resource.newInstance(0, 0);
    ContainerId mockCId1 = ContainerId.newInstance(appId, 0);
    Container c1 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c1.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc1 = Mockito.spy(new HeldContainer(c1, 0, 0, null, containerSignatureMatcher));
    when(hc1.getNode()).thenReturn(node1Rack1);
    when(hc1.getRack()).thenReturn(rack1);
    when(c1.getId()).thenReturn(mockCId1);
    when(c1.getResource()).thenReturn(r);
    when(hc1.getContainer()).thenReturn(c1);
    ContainerId mockCId2 = ContainerId.newInstance(appId, 1);
    Container c2 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c2.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc2 = Mockito.spy(new HeldContainer(c2, 0, 0, null, containerSignatureMatcher));
    when(hc2.getNode()).thenReturn(node2Rack1);
    when(hc2.getRack()).thenReturn(rack1);
    when(c2.getId()).thenReturn(mockCId2);
    when(c2.getResource()).thenReturn(r);
    when(hc2.getContainer()).thenReturn(c2);
    ContainerId mockCId3 = ContainerId.newInstance(appId, 2);
    Container c3 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c3.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc3 = Mockito.spy(new HeldContainer(c3, 0, 0, null, containerSignatureMatcher));
    when(hc3.getNode()).thenReturn(node1Rack1);
    when(hc3.getRack()).thenReturn(rack1);
    when(c3.getId()).thenReturn(mockCId3);
    when(c3.getResource()).thenReturn(r);
    when(hc3.getContainer()).thenReturn(c3);
    ContainerId mockCId4 = ContainerId.newInstance(appId, 3);
    Container c4 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c4.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc4 = Mockito.spy(new HeldContainer(c4, 0, 0, null, containerSignatureMatcher));
    when(hc4.getNode()).thenReturn(node2Rack1);
    when(hc4.getRack()).thenReturn(rack1);
    when(c4.getId()).thenReturn(mockCId4);
    when(c4.getResource()).thenReturn(r);
    when(hc4.getContainer()).thenReturn(c4);
    ContainerId mockCId5 = ContainerId.newInstance(appId, 4);
    Container c5 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c5.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc5 = Mockito.spy(new HeldContainer(c5, 0, 0, null, containerSignatureMatcher));
    when(hc5.getNode()).thenReturn(node1Rack2);
    when(hc5.getRack()).thenReturn(rack2);
    when(c5.getId()).thenReturn(mockCId5);
    when(c5.getResource()).thenReturn(r);
    when(hc5.getContainer()).thenReturn(c5);
    ContainerId mockCId6 = ContainerId.newInstance(appId, 5);
    Container c6 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c6.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc6 = Mockito.spy(new HeldContainer(c6, 0, 0, null, containerSignatureMatcher));
    when(hc6.getNode()).thenReturn(node2Rack2);
    when(hc6.getRack()).thenReturn(rack2);
    when(c6.getId()).thenReturn(mockCId6);
    when(c6.getResource()).thenReturn(r);
    when(hc6.getContainer()).thenReturn(c6);
    ContainerId mockCId7 = ContainerId.newInstance(appId, 6);
    Container c7 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(c7.getNodeId().getHost()).thenReturn(""); // we are mocking directly
    HeldContainer hc7 = Mockito.spy(new HeldContainer(c7, 0, 0, null, containerSignatureMatcher));
    when(hc7.getNode()).thenReturn(node1Rack3);
    when(hc7.getRack()).thenReturn(rack3);
    when(c7.getId()).thenReturn(mockCId7);
    when(c7.getResource()).thenReturn(r);
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

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    scheduler.close();
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testTaskSchedulerRandomReuseExpireTime() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskSchedulerWithDrainableAppCallback scheduler1 =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    TaskSchedulerWithDrainableAppCallback scheduler2 =
        new TaskSchedulerWithDrainableAppCallback(
          mockApp, new AlwaysMatchesContainerMatcher(), appHost, appPort,
          appUrl, mockRMClient, mockAppContext);

    long minTime = 1000l;
    long maxTime = 100000l;
    Configuration conf1 = new Configuration();
    conf1.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, minTime);
    conf1.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, minTime);
    scheduler1.init(conf1);
    Configuration conf2 = new Configuration();
    conf2.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS, minTime);
    conf2.setLong(TezConfiguration.TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS, maxTime);
    scheduler2.init(conf2);

    RegisterApplicationMasterResponse mockRegResponse =
                                mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).
                                                   thenReturn(mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    when(mockRMClient.
          registerApplicationMaster(anyString(), anyInt(), anyString())).
                                                   thenReturn(mockRegResponse);
    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getAvailableResources()).
                                              thenReturn(mockClusterResource);

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

    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler1.stop();
    scheduler1.close();
    scheduler2.stop();
    scheduler2.close();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test (timeout=5000)
  public void testTaskSchedulerPreemption() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TezAMRMClientAsync<CookieContainerRequest> mockRMClient =
                                                  mock(TezAMRMClientAsync.class);

    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    final TaskSchedulerWithDrainableAppCallback scheduler =
      new TaskSchedulerWithDrainableAppCallback(
        mockApp, new PreemptionMatcher(), appHost, appPort,
        appUrl, mockRMClient, mockAppContext);
    TaskSchedulerAppCallbackDrainable drainableAppCallback = scheduler
        .getDrainableAppCallback();

    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    scheduler.init(conf);

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
    Object mockTask1 = mock(Object.class);
    Object mockTask2 = mock(Object.class);
    Object mockTask3 = mock(Object.class);
    Object mockTask3Wait = mock(Object.class);
    Object mockTask3Retry = mock(Object.class);
    Object mockTask3KillA = mock(Object.class);
    Object mockTask3KillB = mock(Object.class);
    Object obj3 = new Object();
    Priority pri2 = Priority.newInstance(2);
    Priority pri4 = Priority.newInstance(4);
    Priority pri5 = Priority.newInstance(5);
    Priority pri6 = Priority.newInstance(6);

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
    List<Container> containers = new ArrayList<Container>();
    Container mockContainer1 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer1.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer1.getResource()).thenReturn(taskAsk);
    when(mockContainer1.getPriority()).thenReturn(pri2);
    ContainerId mockCId1 = mock(ContainerId.class);
    when(mockContainer1.getId()).thenReturn(mockCId1);
    containers.add(mockContainer1);
    Container mockContainer2 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer2.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer2.getResource()).thenReturn(taskAsk);
    when(mockContainer2.getPriority()).thenReturn(pri6);
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    containers.add(mockContainer2);
    Container mockContainer3A = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer3A.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer3A.getResource()).thenReturn(taskAsk);
    when(mockContainer3A.getPriority()).thenReturn(pri6);
    ContainerId mockCId3A = mock(ContainerId.class);
    when(mockContainer3A.getId()).thenReturn(mockCId3A);
    containers.add(mockContainer3A);
    Container mockContainer3B = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer3B.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer3B.getResource()).thenReturn(taskAsk);
    when(mockContainer3B.getPriority()).thenReturn(pri2); // high priority container 
    ContainerId mockCId3B = mock(ContainerId.class);
    when(mockContainer3B.getId()).thenReturn(mockCId3B);
    containers.add(mockContainer3B);
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
    Assert.assertEquals(mockCId3A,
        scheduler.taskAllocations.get(mockTask3KillA).getId());
    // high priority container assigned to lower pri task. This task should still be preempted 
    // because the task priority is relevant for preemption and not the container priority
    Assert.assertEquals(mockCId3B,
        scheduler.taskAllocations.get(mockTask3KillB).getId());

    // no preemption
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());

    Object mockTask3WaitCookie = new Object();
    scheduler.allocateTask(mockTask3Wait, taskAsk, null,
                           null, pri6, obj3, mockTask3WaitCookie);
    // no preemption - same pri
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
    
    Priority pri8 = Priority.newInstance(8);
    Container mockContainer4 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer4.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer4.getResource()).thenReturn(taskAsk);
    when(mockContainer4.getPriority()).thenReturn(pri8);
    ContainerId mockCId4 = mock(ContainerId.class);
    when(mockContainer4.getId()).thenReturn(mockCId4);
    containers.clear();
    containers.add(mockContainer4);
    
    // Fudge new container being present in delayed allocation list due to race
    HeldContainer heldContainer = new HeldContainer(mockContainer4, -1, -1, null,
        containerSignatureMatcher);
    scheduler.delayedContainerManager.delayedContainers.add(heldContainer);
    // no preemption - container assignment attempts < 3
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(0)).releaseAssignedContainer((ContainerId)any());
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
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId4);
    verify(mockRMClient, times(5)).
    addContainerRequest(requestCaptor.capture());
    CookieContainerRequest reAdded = requestCaptor.getValue();
    Assert.assertEquals(pri6, reAdded.getPriority());
    Assert.assertEquals(taskAsk, reAdded.getCapability());
    Assert.assertEquals(mockTask3WaitCookie, reAdded.getCookie().getAppCookie());
    
    // remove fudging.
    scheduler.delayedContainerManager.delayedContainers.clear();
    
    scheduler.allocateTask(mockTask3Retry, taskAsk, null,
                           null, pri5, obj3, null);
    // no preemption - higher pri. exact match
    scheduler.getProgress();
    drainableAppCallback.drain();
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
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId3B);
    // next 3 heartbeats do nothing, waiting for the RM to act on the last released resources
    scheduler.getProgress();
    scheduler.getProgress();
    scheduler.getProgress();
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId)any());
    scheduler.getProgress();
    drainableAppCallback.drain();
    // Next oldest mockTaskPri3KillA gets preempted to clear 10% of outstanding running preemptable tasks
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId3A);

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, "", appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    drainableAppCallback.drain();
    scheduler.close();
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testLocalityMatching() throws Exception {

    RackResolver.init(new Configuration());
    TaskSchedulerAppCallback appClient = mock(TaskSchedulerAppCallback.class);
    TezAMRMClientAsync<CookieContainerRequest> amrmClient =
      mock(TezAMRMClientAsync.class);
    AppContext mockAppContext = mock(AppContext.class);
    when(mockAppContext.getAMState()).thenReturn(DAGAppMasterState.RUNNING);

    TaskSchedulerWithDrainableAppCallback taskScheduler =
      new TaskSchedulerWithDrainableAppCallback(
        appClient, new AlwaysMatchesContainerMatcher(), "host", 0, "",
        amrmClient, mockAppContext);
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler
        .getDrainableAppCallback();
    
    Configuration conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    taskScheduler.init(conf);
    
    RegisterApplicationMasterResponse mockRegResponse = mock(RegisterApplicationMasterResponse.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMaximumResourceCapability()).thenReturn(
        mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);
    when(amrmClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegResponse);
    
    taskScheduler.start();
    
    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(1);

    String hostsTask1[] = { "host1" };
    String hostsTask2[] = { "non-allocated-host" };

    String defaultRack[] = { "/default-rack" };
    String otherRack[] = { "/other-rack" };

    Object mockTask1 = mock(Object.class);
    CookieContainerRequest mockCookie1 = mock(CookieContainerRequest.class,
        RETURNS_DEEP_STUBS);
    when(mockCookie1.getCookie().getTask()).thenReturn(mockTask1);

    Object mockTask2 = mock(Object.class);
    CookieContainerRequest mockCookie2 = mock(CookieContainerRequest.class,
        RETURNS_DEEP_STUBS);
    when(mockCookie2.getCookie().getTask()).thenReturn(mockTask2);

    Container containerHost1 = createContainer(1, "host1", resource, priority);
    Container containerHost3 = createContainer(2, "host3", resource, priority);
    List<Container> allocatedContainers = new LinkedList<Container>();

    allocatedContainers.add(containerHost3);
    allocatedContainers.add(containerHost1);

    final Map<String, List<CookieContainerRequest>> matchingMap = new HashMap<String, List<CookieContainerRequest>>();
    taskScheduler.allocateTask(mockTask1, resource, hostsTask1, defaultRack,
        priority, null, mockCookie1);
    drainableAppCallback.drain();

    List<CookieContainerRequest> host1List = new ArrayList<CookieContainerRequest>();
    host1List.add(mockCookie1);
    List<CookieContainerRequest> defaultRackList = new ArrayList<CookieContainerRequest>();
    defaultRackList.add(mockCookie1);
    matchingMap.put(hostsTask1[0], host1List);
    matchingMap.put(defaultRack[0], defaultRackList);

    List<CookieContainerRequest> nonAllocatedHostList = new ArrayList<YarnTaskSchedulerService.CookieContainerRequest>();
    nonAllocatedHostList.add(mockCookie2);
    List<CookieContainerRequest> otherRackList = new ArrayList<YarnTaskSchedulerService.CookieContainerRequest>();
    otherRackList.add(mockCookie2);
    taskScheduler.allocateTask(mockTask2, resource, hostsTask2, otherRack,
        priority, null, mockCookie2);
    drainableAppCallback.drain();
    matchingMap.put(hostsTask2[0], nonAllocatedHostList);
    matchingMap.put(otherRack[0], otherRackList);

    List<CookieContainerRequest> anyList = new LinkedList<YarnTaskSchedulerService.CookieContainerRequest>();
    anyList.add(mockCookie1);
    anyList.add(mockCookie2);

    matchingMap.put(ResourceRequest.ANY, anyList);

    final List<ArrayList<CookieContainerRequest>> emptyList = new LinkedList<ArrayList<CookieContainerRequest>>();

    when(
        amrmClient.getMatchingRequests((Priority) any(), anyString(),
            (Resource) any())).thenAnswer(
        new Answer<List<? extends Collection<CookieContainerRequest>>>() {

          @Override
          public List<? extends Collection<CookieContainerRequest>> answer(
              InvocationOnMock invocation) throws Throwable {
            String location = (String) invocation.getArguments()[1];
            if (matchingMap.get(location) != null) {
              CookieContainerRequest matched = matchingMap.get(location).get(0);
              // Remove matched from matchingMap - assuming TaskScheduler will
              // pick the first entry.
              Iterator<Entry<String, List<CookieContainerRequest>>> iter = matchingMap
                  .entrySet().iterator();
              while (iter.hasNext()) {
                Entry<String, List<CookieContainerRequest>> entry = iter.next();
                if (entry.getValue().remove(matched)) {
                  if (entry.getValue().size() == 0) {
                    iter.remove();
                  }
                }
              }
              return Collections.singletonList(Collections
                  .singletonList(matched));
            } else {
              return emptyList;
            }
          }
        });

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
    taskScheduler.close();
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
}

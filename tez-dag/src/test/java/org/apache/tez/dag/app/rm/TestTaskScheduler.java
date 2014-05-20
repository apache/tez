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
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.rm.TaskScheduler.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TaskScheduler.HeldContainer;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerAppCallbackDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableAppCallback;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AlwaysMatchesContainerMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.PreemptionMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class TestTaskScheduler {

  RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

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
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS, 0);
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

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
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
    Object mockTask3Kill = mock(Object.class);
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
    scheduler.allocateTask(mockTask3Kill, taskAsk, null,
                           null, pri6, obj3, null);
    drainableAppCallback.drain();
    verify(mockRMClient, times(3)).
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
    Container mockContainer3 = mock(Container.class, RETURNS_DEEP_STUBS);
    when(mockContainer3.getNodeId().getHost()).thenReturn("host1");
    when(mockContainer3.getResource()).thenReturn(taskAsk);
    when(mockContainer3.getPriority()).thenReturn(pri6);
    ContainerId mockCId3 = mock(ContainerId.class);
    when(mockContainer3.getId()).thenReturn(mockCId3);
    containers.add(mockContainer3);
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
    Assert.assertEquals(3, scheduler.taskAllocations.size());
    Assert.assertEquals(3072, scheduler.allocatedResources.getMemory());
    Assert.assertEquals(mockCId1,
        scheduler.taskAllocations.get(mockTask1).getId());
    Assert.assertEquals(mockCId2,
        scheduler.taskAllocations.get(mockTask3).getId());
    Assert.assertEquals(mockCId3,
        scheduler.taskAllocations.get(mockTask3Kill).getId());

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
    HeldContainer heldContainer = new HeldContainer(mockContainer4, -1, -1, null);
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
    verify(mockRMClient, times(4)).
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

    scheduler.allocateTask(mockTask2, taskAsk, null,
                           null, pri4, null, null);
    drainableAppCallback.drain();

    // mockTaskPri3Kill gets preempted
    scheduler.getProgress();
    drainableAppCallback.drain();
    verify(mockRMClient, times(2)).releaseAssignedContainer((ContainerId)any());
    verify(mockRMClient, times(1)).releaseAssignedContainer(mockCId3);

    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, "", appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    drainableAppCallback.drain();
    scheduler.close();
  }

  @SuppressWarnings("unchecked")
  @Test
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

    List<CookieContainerRequest> nonAllocatedHostList = new ArrayList<TaskScheduler.CookieContainerRequest>();
    nonAllocatedHostList.add(mockCookie2);
    List<CookieContainerRequest> otherRackList = new ArrayList<TaskScheduler.CookieContainerRequest>();
    otherRackList.add(mockCookie2);
    taskScheduler.allocateTask(mockTask2, resource, hostsTask2, otherRack,
        priority, null, mockCookie2);
    drainableAppCallback.drain();
    matchingMap.put(hostsTask2[0], nonAllocatedHostList);
    matchingMap.put(otherRack[0], otherRackList);

    List<CookieContainerRequest> anyList = new LinkedList<TaskScheduler.CookieContainerRequest>();
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

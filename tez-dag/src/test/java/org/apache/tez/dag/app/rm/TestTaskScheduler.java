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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.app.rm.AMRMClient.StoredContainerRequest;
import org.apache.tez.dag.app.rm.TaskScheduler.CRCookie;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;

public class TestTaskScheduler {
  
  RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
    
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testTaskScheduler() throws Exception {
    RackResolver.init(new YarnConfiguration());
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);
    
    AMRMClientAsync<StoredContainerRequest<CRCookie>> mockRMClient = 
                                                  mock(AMRMClientAsync.class);
    
    ApplicationAttemptId attemptId = 
        BuilderUtils.newApplicationAttemptId(
                                  BuilderUtils.newApplicationId(1234, 0), 0);
    String appHost = "host";
    int appPort = 0;
    String appUrl = "url";
    TaskScheduler scheduler = new TaskScheduler(attemptId, mockApp, appHost, 
                                                appPort, appUrl, mockRMClient);
    
    Configuration conf = new Configuration(); 
    scheduler.init(conf);
    verify(mockRMClient).init(conf);
    
    RegisterApplicationMasterResponse mockRegResponse = 
                                mock(RegisterApplicationMasterResponse.class);
    Resource mockMinResource = mock(Resource.class);
    Resource mockMaxResource = mock(Resource.class);
    Map<ApplicationAccessType, String> mockAcls = mock(Map.class);
    when(mockRegResponse.getMinimumResourceCapability()).
                                                   thenReturn(mockMinResource);
    when(mockRegResponse.getMaximumResourceCapability()).
                                                   thenReturn(mockMaxResource);
    when(mockRegResponse.getApplicationACLs()).thenReturn(mockAcls);    
    when(mockRMClient.
          registerApplicationMaster(anyString(), anyInt(), anyString())).
                                                   thenReturn(mockRegResponse);
    scheduler.start();
    verify(mockRMClient).start();
    verify(mockRMClient).registerApplicationMaster(appHost, appPort, appUrl);
    verify(mockApp).setApplicationRegistrationData(mockMinResource, 
                                                   mockMaxResource, 
                                                   mockAcls);
    
    when(mockRMClient.getClusterNodeCount()).thenReturn(5);
    Assert.assertEquals(5, scheduler.getClusterNodeCount());
    
    Resource mockClusterResource = mock(Resource.class);
    when(mockRMClient.getClusterAvailableResources()).
                                              thenReturn(mockClusterResource);
    Assert.assertEquals(mockClusterResource, 
                        mockRMClient.getClusterAvailableResources());
    
    Object mockTask1 = mock(Object.class);
    Object mockCookie1 = mock(Object.class);
    Resource mockCapability = mock(Resource.class);
    String[] hosts = {"host1", "host5"};
    String[] racks = {"/default-rack", "/default-rack"};
    Priority mockPriority = mock(Priority.class);
    ArgumentCaptor<StoredContainerRequest> requestCaptor = 
                        ArgumentCaptor.forClass(StoredContainerRequest.class);
    // allocate task
    scheduler.allocateTask(mockTask1, mockCapability, hosts, 
                           racks, mockPriority, mockCookie1);
    verify(mockRMClient, times(1)).
                           addContainerRequest((StoredContainerRequest) any());

    // returned from task requests before allocation happens
    Assert.assertNull(scheduler.deallocateTask(mockTask1));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((StoredContainerRequest) any());
    verify(mockRMClient, times(0)).
                                 releaseAssignedContainer((ContainerId) any());
    
    // deallocating unknown task
    Assert.assertNull(scheduler.deallocateTask(mockTask1));
    verify(mockRMClient, times(1)).
                        removeContainerRequest((StoredContainerRequest) any());
    verify(mockRMClient, times(0)).
                                 releaseAssignedContainer((ContainerId) any());

    // allocate tasks
    Object mockTask2 = mock(Object.class);
    Object mockCookie2 = mock(Object.class);
    Object mockTask3 = mock(Object.class);
    Object mockCookie3 = mock(Object.class);
    scheduler.allocateTask(mockTask1, mockCapability, hosts, 
        racks, mockPriority, mockCookie1);
    verify(mockRMClient, times(2)).
                                addContainerRequest(requestCaptor.capture());
    StoredContainerRequest<CRCookie> request1 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask2, mockCapability, hosts, 
        racks, mockPriority, mockCookie2);
    verify(mockRMClient, times(3)).
                                addContainerRequest(requestCaptor.capture());
    StoredContainerRequest<CRCookie> request2 = requestCaptor.getValue();
    scheduler.allocateTask(mockTask3, mockCapability, hosts, 
        racks, mockPriority, mockCookie3);
    verify(mockRMClient, times(4)).
                                addContainerRequest(requestCaptor.capture());
    StoredContainerRequest<CRCookie> request3 = requestCaptor.getValue();
    
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
    ArrayList<StoredContainerRequest<CRCookie>> hostContainers = 
                             new ArrayList<StoredContainerRequest<CRCookie>>();
    hostContainers.add(request1);
    hostContainers.add(request2);
    hostContainers.add(request3);
    ArrayList<StoredContainerRequest<CRCookie>> rackContainers = 
                             new ArrayList<StoredContainerRequest<CRCookie>>();
    rackContainers.add(request2);
    rackContainers.add(request3);
    ArrayList<StoredContainerRequest<CRCookie>> anyContainers = 
                             new ArrayList<StoredContainerRequest<CRCookie>>();
    anyContainers.add(request3);

    // return all requests for host1
    when(
        mockRMClient.getMatchingRequests((Priority) any(), eq("host1"),
            (Resource) any())).thenReturn(hostContainers);
    // first request matched by host
    // second request matched to rack. RackResolver by default puts hosts in 
    // /default-rack. We need to workaround by returning rack matches only once
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq("/default-rack"), (Resource) any())).thenReturn(
        rackContainers).thenReturn(null);    
    // third request matched to ANY
    when(
        mockRMClient.getMatchingRequests((Priority) any(),
            eq(ResourceRequest.ANY), (Resource) any())).thenReturn(
        anyContainers).thenReturn(null);
    scheduler.onContainersAllocated(containers);
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
    Assert.assertEquals(mockContainer1, scheduler.deallocateTask(mockTask1));
    verify(mockRMClient).releaseAssignedContainer(mockCId1);
    // deallocate allocated container
    Assert.assertEquals(mockTask2, scheduler.deallocateContainer(mockCId2));
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
    // released container status returned
    verify(mockApp).containerCompleted(mockTask1, mockStatus1);
    verify(mockApp).containerCompleted(mockTask2, mockStatus2);
    // currently allocated container status returned and not released
    verify(mockApp).containerCompleted(mockTask3, mockStatus3);
    // no other statuses returned
    verify(mockApp, times(3)).containerCompleted(any(), (ContainerStatus) any());
    verify(mockRMClient, times(3)).releaseAssignedContainer((ContainerId) any());
    
    
    float progress = 0.5f;
    when(mockApp.getProgress()).thenReturn(progress);
    Assert.assertEquals(progress, scheduler.getProgress(), 0);
    
    List<NodeReport> mockUpdatedNodes = mock(List.class);
    scheduler.onNodesUpdated(mockUpdatedNodes);
    verify(mockApp).nodesUpdated(mockUpdatedNodes);
    
    Exception mockException = mock(Exception.class);
    scheduler.onError(mockException);
    verify(mockApp).onError(mockException);
    
    scheduler.onRebootRequest();
    verify(mockApp).appRebootRequested();
    
    String appMsg = "success";
    AppFinalStatus finalStatus = 
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);
    when(mockApp.getFinalAppStatus()).thenReturn(finalStatus);
    scheduler.stop();
    verify(mockRMClient).
                  unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, 
                                              appMsg, appUrl);
    verify(mockRMClient).stop();
  }
  
}

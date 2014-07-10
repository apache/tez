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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings("rawtypes")
public class TestTaskSchedulerEventHandler {
  
  class TestEventHandler implements EventHandler{
    List<Event> events = Lists.newLinkedList();
    @Override
    public void handle(Event event) {
      events.add(event);
    }
  }
  
  class MockTaskSchedulerEventHandler extends TaskSchedulerEventHandler {

    public MockTaskSchedulerEventHandler(AppContext appContext,
        DAGClientServer clientService, EventHandler eventHandler,
        ContainerSignatureMatcher containerSignatureMatcher) {
      super(appContext, clientService, eventHandler, containerSignatureMatcher);
    }
    
    @Override
    protected TaskSchedulerService createTaskScheduler(String host, int port,
        String trackingUrl, AppContext appContext) {
      return mockTaskScheduler;
    }
    
  }

  AppContext mockAppContext;
  DAGClientServer mockClientService;
  TestEventHandler mockEventHandler;
  ContainerSignatureMatcher mockSigMatcher;
  MockTaskSchedulerEventHandler schedulerHandler;
  TaskSchedulerService mockTaskScheduler;
  AMContainerMap mockAMContainerMap;

  @Before
  public void setup() {
    mockAppContext = mock(AppContext.class);
    mockClientService = mock(DAGClientServer.class);
    mockEventHandler = new TestEventHandler();
    mockSigMatcher = mock(ContainerSignatureMatcher.class);
    mockTaskScheduler = mock(TaskSchedulerService.class);
    mockAMContainerMap = mock(AMContainerMap.class);
    when(mockAppContext.getAllContainers()).thenReturn(mockAMContainerMap);
    when(mockClientService.getBindAddress()).thenReturn(new InetSocketAddress(10000));
    schedulerHandler = new MockTaskSchedulerEventHandler(
        mockAppContext, mockClientService, mockEventHandler, mockSigMatcher);
  }
  
  @Test (timeout = 5000)
  public void testContainerPreempted() throws IOException {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();
    
    String diagnostics = "Container preempted by RM.";
    TaskAttemptImpl mockTask = mock(TaskAttemptImpl.class);
    ContainerStatus mockStatus = mock(ContainerStatus.class);
    ContainerId mockCId = mock(ContainerId.class);
    AMContainer mockAMContainer = mock(AMContainer.class);
    when(mockAMContainerMap.get(mockCId)).thenReturn(mockAMContainer);
    when(mockAMContainer.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getDiagnostics()).thenReturn(diagnostics);
    when(mockStatus.getExitStatus()).thenReturn(ContainerExitStatus.PREEMPTED);
    schedulerHandler.containerCompleted(mockTask, mockStatus);
    Assert.assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    Assert.assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    Assert.assertEquals(mockCId, completedEvent.getContainerId());
    Assert.assertEquals("Container preempted externally. Container preempted by RM.", 
        completedEvent.getDiagnostics());
    Assert.assertTrue(completedEvent.isPreempted());
    Assert.assertFalse(completedEvent.isDiskFailed());

    schedulerHandler.stop();
    schedulerHandler.close();
  }
  
  @Test (timeout = 5000)
  public void testContainerDiskFailed() throws IOException {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();
    
    String diagnostics = "NM disk failed.";
    TaskAttemptImpl mockTask = mock(TaskAttemptImpl.class);
    ContainerStatus mockStatus = mock(ContainerStatus.class);
    ContainerId mockCId = mock(ContainerId.class);
    AMContainer mockAMContainer = mock(AMContainer.class);
    when(mockAMContainerMap.get(mockCId)).thenReturn(mockAMContainer);
    when(mockAMContainer.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getDiagnostics()).thenReturn(diagnostics);
    when(mockStatus.getExitStatus()).thenReturn(ContainerExitStatus.DISKS_FAILED);
    schedulerHandler.containerCompleted(mockTask, mockStatus);
    Assert.assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    Assert.assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    Assert.assertEquals(mockCId, completedEvent.getContainerId());
    Assert.assertEquals("Container disk failed. NM disk failed.", 
        completedEvent.getDiagnostics());
    Assert.assertFalse(completedEvent.isPreempted());
    Assert.assertTrue(completedEvent.isDiskFailed());

    schedulerHandler.stop();
    schedulerHandler.close();
  }

}

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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.app.dag.impl.TaskImpl;
import org.apache.tez.dag.app.dag.impl.VertexImpl;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
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

    AtomicBoolean notify = new AtomicBoolean(false);
    
    public MockTaskSchedulerEventHandler(AppContext appContext,
        DAGClientServer clientService, EventHandler eventHandler,
        ContainerSignatureMatcher containerSignatureMatcher, WebUIService webUI) {
      super(appContext, clientService, eventHandler, containerSignatureMatcher, webUI);
    }
    
    @Override
    protected TaskSchedulerService createTaskScheduler(String host, int port,
        String trackingUrl, AppContext appContext) {
      return mockTaskScheduler;
    }
    
    @Override
    protected void notifyForTest() {
      synchronized (notify) {
        notify.set(true);
        notify.notifyAll();
      }
    }
    
  }

  AppContext mockAppContext;
  DAGClientServer mockClientService;
  TestEventHandler mockEventHandler;
  ContainerSignatureMatcher mockSigMatcher;
  MockTaskSchedulerEventHandler schedulerHandler;
  TaskSchedulerService mockTaskScheduler;
  AMContainerMap mockAMContainerMap;
  WebUIService mockWebUIService;

  @Before
  public void setup() {
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    doReturn(new Configuration(false)).when(mockAppContext).getAMConf();
    mockClientService = mock(DAGClientServer.class);
    mockEventHandler = new TestEventHandler();
    mockSigMatcher = mock(ContainerSignatureMatcher.class);
    mockTaskScheduler = mock(TaskSchedulerService.class);
    mockAMContainerMap = mock(AMContainerMap.class);
    mockWebUIService = mock(WebUIService.class);
    when(mockAppContext.getAllContainers()).thenReturn(mockAMContainerMap);
    when(mockClientService.getBindAddress()).thenReturn(new InetSocketAddress(10000));
    schedulerHandler = new MockTaskSchedulerEventHandler(
        mockAppContext, mockClientService, mockEventHandler, mockSigMatcher, mockWebUIService);
  }

  @Test(timeout = 5000)
  public void testSimpleAllocate() throws Exception {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();

    TaskAttemptImpl mockTaskAttempt = mock(TaskAttemptImpl.class);
    TezTaskAttemptID mockAttemptId = mock(TezTaskAttemptID.class);
    when(mockAttemptId.getId()).thenReturn(0);
    when(mockTaskAttempt.getID()).thenReturn(mockAttemptId);
    Resource resource = Resource.newInstance(1024, 1);
    ContainerContext containerContext =
        new ContainerContext(new HashMap<String, LocalResource>(), new Credentials(),
            new HashMap<String, String>(), "");
    int priority = 10;
    TaskLocationHint locHint = TaskLocationHint.createTaskLocationHint(new HashSet<String>(), null);

    ContainerId mockCId = mock(ContainerId.class);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(mockCId);

    AMContainer mockAMContainer = mock(AMContainer.class);
    when(mockAMContainer.getContainerId()).thenReturn(mockCId);
    when(mockAMContainer.getState()).thenReturn(AMContainerState.IDLE);

    when(mockAMContainerMap.get(mockCId)).thenReturn(mockAMContainer);

    AMSchedulerEventTALaunchRequest lr =
        new AMSchedulerEventTALaunchRequest(mockAttemptId, resource, null, mockTaskAttempt, locHint,
            priority, containerContext);
    schedulerHandler.taskAllocated(mockTaskAttempt, lr, container);
    assertEquals(2, mockEventHandler.events.size());
    assertTrue(mockEventHandler.events.get(1) instanceof AMContainerEventAssignTA);
    AMContainerEventAssignTA assignEvent =
        (AMContainerEventAssignTA) mockEventHandler.events.get(1);
    assertEquals(priority, assignEvent.getPriority());
    assertEquals(mockAttemptId, assignEvent.getTaskAttemptId());
  }

  @Test (timeout = 5000)
  public void testTaskBasedAffinity() throws Exception {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();

    TaskAttemptImpl mockTaskAttempt = mock(TaskAttemptImpl.class);
    TezTaskAttemptID taId = mock(TezTaskAttemptID.class);
    String affVertexName = "srcVertex";
    int affTaskIndex = 1;
    TaskLocationHint locHint = TaskLocationHint.createTaskLocationHint(affVertexName, affTaskIndex);
    VertexImpl affVertex = mock(VertexImpl.class);
    TaskImpl affTask = mock(TaskImpl.class);
    TaskAttemptImpl affAttempt = mock(TaskAttemptImpl.class);
    ContainerId affCId = mock(ContainerId.class);
    when(affVertex.getTotalTasks()).thenReturn(2);
    when(affVertex.getTask(affTaskIndex)).thenReturn(affTask);
    when(affTask.getSuccessfulAttempt()).thenReturn(affAttempt);
    when(affAttempt.getAssignedContainerID()).thenReturn(affCId);
    when(mockAppContext.getCurrentDAG().getVertex(affVertexName)).thenReturn(affVertex);
    Resource resource = Resource.newInstance(100, 1);
    AMSchedulerEventTALaunchRequest event = new AMSchedulerEventTALaunchRequest
        (taId, resource, null, mockTaskAttempt, locHint, 3, null);
    schedulerHandler.notify.set(false);
    schedulerHandler.handle(event);
    synchronized (schedulerHandler.notify) {
      while (!schedulerHandler.notify.get()) {
        schedulerHandler.notify.wait();
      }
    }
    
    // verify mockTaskAttempt affinitized to expected affCId
    verify(mockTaskScheduler, times(1)).allocateTask(mockTaskAttempt, resource, affCId,
        Priority.newInstance(3), null, event);
    
    schedulerHandler.stop();
    schedulerHandler.close();
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
    assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    assertEquals(mockCId, completedEvent.getContainerId());
    assertEquals("Container preempted externally. Container preempted by RM.",
        completedEvent.getDiagnostics());
    assertTrue(completedEvent.isPreempted());
    assertEquals(TaskAttemptTerminationCause.EXTERNAL_PREEMPTION,
        completedEvent.getTerminationCause());
    Assert.assertFalse(completedEvent.isDiskFailed());

    schedulerHandler.stop();
    schedulerHandler.close();
  }
  
  @Test (timeout = 5000)
  public void testContainerInternalPreempted() throws IOException {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();
    
    ContainerId mockCId = mock(ContainerId.class);
    verify(mockTaskScheduler, times(0)).deallocateContainer((ContainerId)any());
    schedulerHandler.preemptContainer(mockCId);
    verify(mockTaskScheduler, times(1)).deallocateContainer(mockCId);
    assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    assertEquals(mockCId, completedEvent.getContainerId());
    assertEquals("Container preempted internally", completedEvent.getDiagnostics());
    assertTrue(completedEvent.isPreempted());
    Assert.assertFalse(completedEvent.isDiskFailed());
    assertEquals(TaskAttemptTerminationCause.INTERNAL_PREEMPTION,
        completedEvent.getTerminationCause());

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
    assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    assertEquals(mockCId, completedEvent.getContainerId());
    assertEquals("Container disk failed. NM disk failed.",
        completedEvent.getDiagnostics());
    Assert.assertFalse(completedEvent.isPreempted());
    assertTrue(completedEvent.isDiskFailed());
    assertEquals(TaskAttemptTerminationCause.NODE_DISK_ERROR,
        completedEvent.getTerminationCause());

    schedulerHandler.stop();
    schedulerHandler.close();
  }

  @Test (timeout = 5000)
  public void testContainerExceededPMem() throws IOException {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();

    String diagnostics = "Exceeded Physical Memory";
    TaskAttemptImpl mockTask = mock(TaskAttemptImpl.class);
    ContainerStatus mockStatus = mock(ContainerStatus.class);
    ContainerId mockCId = mock(ContainerId.class);
    AMContainer mockAMContainer = mock(AMContainer.class);
    when(mockAMContainerMap.get(mockCId)).thenReturn(mockAMContainer);
    when(mockAMContainer.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getContainerId()).thenReturn(mockCId);
    when(mockStatus.getDiagnostics()).thenReturn(diagnostics);
    // use -104 rather than ContainerExitStatus.KILLED_EXCEEDED_PMEM because
    // ContainerExitStatus.KILLED_EXCEEDED_PMEM is only available after hadoop-2.5
    when(mockStatus.getExitStatus()).thenReturn(-104);
    schedulerHandler.containerCompleted(mockTask, mockStatus);
    assertEquals(1, mockEventHandler.events.size());
    Event event = mockEventHandler.events.get(0);
    assertEquals(AMContainerEventType.C_COMPLETED, event.getType());
    AMContainerEventCompleted completedEvent = (AMContainerEventCompleted) event;
    assertEquals(mockCId, completedEvent.getContainerId());
    assertEquals("Container failed, exitCode=-104. Exceeded Physical Memory",
        completedEvent.getDiagnostics());
    Assert.assertFalse(completedEvent.isPreempted());
    Assert.assertFalse(completedEvent.isDiskFailed());
    assertEquals(TaskAttemptTerminationCause.CONTAINER_EXITED,
        completedEvent.getTerminationCause());

    schedulerHandler.stop();
    schedulerHandler.close();
  }

  @Test (timeout = 5000)
  public void testHistoryUrlConf() throws Exception {
    Configuration conf = schedulerHandler.appContext.getAMConf();

    // ensure history url is empty when timeline server is not the logging class
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "http://ui-host:9999");
    assertTrue("".equals(schedulerHandler.getHistoryUrl()));

    // ensure expansion of url happens
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService");
    final ApplicationId mockApplicationId = mock(ApplicationId.class);
    doReturn("TEST_APP_ID").when(mockApplicationId).toString();
    doReturn(mockApplicationId).when(mockAppContext).getApplicationID();
    assertTrue("http://ui-host:9999/#/tez-app/TEST_APP_ID"
        .equals(schedulerHandler.getHistoryUrl()));

    // ensure the trailing / in history url is handled
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "http://ui-host:9998/");
    assertTrue("http://ui-host:9998/#/tez-app/TEST_APP_ID"
        .equals(schedulerHandler.getHistoryUrl()));

    // ensure missing scheme in history url is handled
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "ui-host:9998/");
    Assert.assertTrue("http://ui-host:9998/#/tez-app/TEST_APP_ID"
        .equals(schedulerHandler.getHistoryUrl()));

    // handle bad template ex without begining /
    conf.set(TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE,
        "__HISTORY_URL_BASE__#/somepath");
    assertTrue("http://ui-host:9998/#/somepath"
        .equals(schedulerHandler.getHistoryUrl()));

    conf.set(TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE,
        "__HISTORY_URL_BASE__?viewPath=tez-app/__APPLICATION_ID__");
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "http://localhost/ui/tez");
    assertTrue("http://localhost/ui/tez?viewPath=tez-app/TEST_APP_ID"
        .equals(schedulerHandler.getHistoryUrl()));

  }

}

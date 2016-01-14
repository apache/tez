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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.app.dag.impl.TaskImpl;
import org.apache.tez.dag.app.dag.impl.VertexImpl;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings("rawtypes")
public class TestTaskSchedulerManager {
  
  class TestEventHandler implements EventHandler{
    List<Event> events = Lists.newLinkedList();
    @Override
    public void handle(Event event) {
      events.add(event);
    }
  }
  
  class MockTaskSchedulerManager extends TaskSchedulerManager {

    final AtomicBoolean notify = new AtomicBoolean(false);
    
    public MockTaskSchedulerManager(AppContext appContext,
                                    DAGClientServer clientService, EventHandler eventHandler,
                                    ContainerSignatureMatcher containerSignatureMatcher,
                                    WebUIService webUI) {
      super(appContext, clientService, eventHandler, containerSignatureMatcher, webUI,
          Lists.newArrayList(new NamedEntityDescriptor("FakeDescriptor", null)), false);
    }

    @Override
    protected void instantiateSchedulers(String host, int port, String trackingUrl,
                                         AppContext appContext) {
      taskSchedulers[0] = new TaskSchedulerWrapper(mockTaskScheduler);
      taskSchedulerServiceWrappers[0] =
          new ServicePluginLifecycleAbstractService<>(taskSchedulers[0].getTaskScheduler());
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
  MockTaskSchedulerManager schedulerHandler;
  TaskScheduler mockTaskScheduler;
  AMContainerMap mockAMContainerMap;
  WebUIService mockWebUIService;

  @Before
  public void setup() {
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    doReturn(new Configuration(false)).when(mockAppContext).getAMConf();
    mockClientService = mock(DAGClientServer.class);
    mockEventHandler = new TestEventHandler();
    mockSigMatcher = mock(ContainerSignatureMatcher.class);
    mockTaskScheduler = mock(TaskScheduler.class);
    mockAMContainerMap = mock(AMContainerMap.class);
    mockWebUIService = mock(WebUIService.class);
    when(mockAppContext.getAllContainers()).thenReturn(mockAMContainerMap);
    when(mockClientService.getBindAddress()).thenReturn(new InetSocketAddress(10000));
    schedulerHandler = new MockTaskSchedulerManager(
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
            priority, containerContext, 0, 0, 0);
    schedulerHandler.taskAllocated(0, mockTaskAttempt, lr, container);
    assertEquals(1, mockEventHandler.events.size());
    assertTrue(mockEventHandler.events.get(0) instanceof AMContainerEventAssignTA);
    AMContainerEventAssignTA assignEvent =
        (AMContainerEventAssignTA) mockEventHandler.events.get(0);
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
        (taId, resource, null, mockTaskAttempt, locHint, 3, null, 0, 0, 0);
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
    schedulerHandler.containerCompleted(0, mockTask, mockStatus);
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
  public void testContainerInternalPreempted() throws IOException, ServicePluginException {
    Configuration conf = new Configuration(false);
    schedulerHandler.init(conf);
    schedulerHandler.start();

    AMContainer mockAmContainer = mock(AMContainer.class);
    when(mockAmContainer.getTaskSchedulerIdentifier()).thenReturn(0);
    when(mockAmContainer.getContainerLauncherIdentifier()).thenReturn(0);
    when(mockAmContainer.getTaskCommunicatorIdentifier()).thenReturn(0);
    ContainerId mockCId = mock(ContainerId.class);
    verify(mockTaskScheduler, times(0)).deallocateContainer((ContainerId) any());
    when(mockAMContainerMap.get(mockCId)).thenReturn(mockAmContainer);
    schedulerHandler.preemptContainer(0, mockCId);
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
    schedulerHandler.containerCompleted(0, mockTask, mockStatus);
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
    schedulerHandler.containerCompleted(0, mockTask, mockStatus);
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
    final ApplicationId mockApplicationId = mock(ApplicationId.class);
    doReturn("TEST_APP_ID").when(mockApplicationId).toString();
    doReturn(mockApplicationId).when(mockAppContext).getApplicationID();

    // ensure history url is empty when timeline server is not the logging class
    conf.set(TezConfiguration.TEZ_HISTORY_URL_BASE, "http://ui-host:9999");
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

  @Test(timeout = 5000)
  public void testNoSchedulerSpecified() throws IOException {
    try {
      new TSEHForMultipleSchedulersTest(mockAppContext, mockClientService, mockEventHandler,
          mockSigMatcher, mockWebUIService, null, false);
      fail("Expecting an IllegalStateException with no schedulers specified");
    } catch (IllegalArgumentException e) {
    }
  }

  // Verified via statics
  @Test(timeout = 5000)
  public void testCustomTaskSchedulerSetup() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testval");
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    String customSchedulerName = "fakeScheduler";
    List<NamedEntityDescriptor> taskSchedulers = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload userPayload = UserPayload.create(bb);
    taskSchedulers.add(
        new NamedEntityDescriptor(customSchedulerName, FakeTaskScheduler.class.getName())
            .setUserPayload(userPayload));
    taskSchedulers.add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
        .setUserPayload(defaultPayload));

    TSEHForMultipleSchedulersTest tseh =
        new TSEHForMultipleSchedulersTest(mockAppContext, mockClientService, mockEventHandler,
            mockSigMatcher, mockWebUIService, taskSchedulers, false);

    tseh.init(conf);
    tseh.start();

    // Verify that the YARN task scheduler is installed by default
    assertTrue(tseh.getYarnSchedulerCreated());
    assertFalse(tseh.getUberSchedulerCreated());
    assertEquals(2, tseh.getNumCreateInvocations());

    // Verify the order of the schedulers
    assertEquals(customSchedulerName, tseh.getTaskSchedulerName(0));
    assertEquals(TezConstants.getTezYarnServicePluginName(), tseh.getTaskSchedulerName(1));

    // Verify the payload setup for the custom task scheduler
    assertNotNull(tseh.getTaskSchedulerContext(0));
    assertEquals(bb, tseh.getTaskSchedulerContext(0).getInitialUserPayload().getPayload());

    // Verify the payload on the yarn scheduler
    assertNotNull(tseh.getTaskSchedulerContext(1));
    Configuration parsed = TezUtils.createConfFromUserPayload(tseh.getTaskSchedulerContext(1).getInitialUserPayload());
    assertEquals("testval", parsed.get("testkey"));
  }

  @Test(timeout = 5000)
  public void testTaskSchedulerRouting() throws Exception {
    Configuration conf = new Configuration(false);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    String customSchedulerName = "fakeScheduler";
    List<NamedEntityDescriptor> taskSchedulers = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload userPayload = UserPayload.create(bb);
    taskSchedulers.add(
        new NamedEntityDescriptor(customSchedulerName, FakeTaskScheduler.class.getName())
            .setUserPayload(userPayload));
    taskSchedulers.add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
        .setUserPayload(defaultPayload));

    TSEHForMultipleSchedulersTest tseh =
        new TSEHForMultipleSchedulersTest(mockAppContext, mockClientService, mockEventHandler,
            mockSigMatcher, mockWebUIService, taskSchedulers, false);

    tseh.init(conf);
    tseh.start();

    // Verify that the YARN task scheduler is installed by default
    assertTrue(tseh.getYarnSchedulerCreated());
    assertFalse(tseh.getUberSchedulerCreated());
    assertEquals(2, tseh.getNumCreateInvocations());

    // Verify the order of the schedulers
    assertEquals(customSchedulerName, tseh.getTaskSchedulerName(0));
    assertEquals(TezConstants.getTezYarnServicePluginName(), tseh.getTaskSchedulerName(1));

    verify(tseh.getTestTaskScheduler(0)).initialize();
    verify(tseh.getTestTaskScheduler(0)).start();

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagId, 1);
    TezTaskID taskId1 = TezTaskID.getInstance(vertexID, 1);
    TezTaskAttemptID attemptId11 = TezTaskAttemptID.getInstance(taskId1, 1);
    TezTaskID taskId2 = TezTaskID.getInstance(vertexID, 2);
    TezTaskAttemptID attemptId21 = TezTaskAttemptID.getInstance(taskId2, 1);

    Resource resource = Resource.newInstance(1024, 1);

    TaskAttempt mockTaskAttempt1 = mock(TaskAttempt.class);
    TaskAttempt mockTaskAttempt2 = mock(TaskAttempt.class);

    AMSchedulerEventTALaunchRequest launchRequest1 =
        new AMSchedulerEventTALaunchRequest(attemptId11, resource, mock(TaskSpec.class),
            mockTaskAttempt1, mock(TaskLocationHint.class), 1, mock(ContainerContext.class), 0, 0,
            0);

    tseh.handle(launchRequest1);

    verify(tseh.getTestTaskScheduler(0)).allocateTask(eq(mockTaskAttempt1), eq(resource),
        any(String[].class), any(String[].class), any(Priority.class), any(Object.class),
        eq(launchRequest1));

    AMSchedulerEventTALaunchRequest launchRequest2 =
        new AMSchedulerEventTALaunchRequest(attemptId21, resource, mock(TaskSpec.class),
            mockTaskAttempt2, mock(TaskLocationHint.class), 1, mock(ContainerContext.class), 1, 0,
            0);
    tseh.handle(launchRequest2);
    verify(tseh.getTestTaskScheduler(1)).allocateTask(eq(mockTaskAttempt2), eq(resource),
        any(String[].class), any(String[].class), any(Priority.class), any(Object.class),
        eq(launchRequest2));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskSchedulerUserError() {
    TaskScheduler taskScheduler = mock(TaskScheduler.class, new ExceptionAnswer());

    EventHandler eventHandler = mock(EventHandler.class);
    AppContext appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);

    when(appContext.getEventHandler()).thenReturn(eventHandler);
    doReturn("testTaskScheduler").when(appContext).getTaskSchedulerName(0);
    String expectedId = "[0:testTaskScheduler]";

    Configuration conf = new Configuration(false);

    InetSocketAddress address = new InetSocketAddress(15222);
    DAGClientServer mockClientService = mock(DAGClientServer.class);
    doReturn(address).when(mockClientService).getBindAddress();
    TaskSchedulerManager taskSchedulerManager =
        new TaskSchedulerManager(taskScheduler, appContext, mock(ContainerSignatureMatcher.class),
            mockClientService,
            Executors.newFixedThreadPool(1)) {
          @Override
          protected void instantiateSchedulers(String host, int port, String trackingUrl,
                                               AppContext appContext) throws TezException {
            // Stubbed out since these are setup up front in the constructor used for testing
          }
        };

    try {
      taskSchedulerManager.init(conf);
      taskSchedulerManager.start();

      // Invoking a couple of random methods

      AMSchedulerEventTALaunchRequest launchRequest =
          new AMSchedulerEventTALaunchRequest(mock(TezTaskAttemptID.class), mock(Resource.class),
              mock(TaskSpec.class), mock(TaskAttempt.class), mock(TaskLocationHint.class), 0,
              mock(ContainerContext.class), 0, 0, 0);
      taskSchedulerManager.handleEvent(launchRequest);

      ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);

      verify(eventHandler, times(1)).handle(argumentCaptor.capture());

      Event rawEvent = argumentCaptor.getValue();
      assertTrue(rawEvent instanceof DAGAppMasterEventUserServiceFatalError);
      DAGAppMasterEventUserServiceFatalError event =
          (DAGAppMasterEventUserServiceFatalError) rawEvent;

      assertEquals(DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR, event.getType());
      assertTrue(event.getError().getMessage().contains("TestException_" + "allocateTask"));
      assertTrue(event.getDiagnosticInfo().contains("Task Allocation"));
      assertTrue(event.getDiagnosticInfo().contains(expectedId));


      taskSchedulerManager.dagCompleted();
      argumentCaptor = ArgumentCaptor.forClass(Event.class);
      verify(eventHandler, times(2)).handle(argumentCaptor.capture());

      rawEvent = argumentCaptor.getAllValues().get(1);
      assertTrue(rawEvent instanceof DAGAppMasterEventUserServiceFatalError);
      event = (DAGAppMasterEventUserServiceFatalError) rawEvent;

      assertEquals(DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR, event.getType());
      assertTrue(event.getError().getMessage().contains("TestException_" + "dagComplete"));
      assertTrue(event.getDiagnosticInfo().contains("Dag Completion"));
      assertTrue(event.getDiagnosticInfo().contains(expectedId));

    } finally {
      taskSchedulerManager.stop();
    }
  }

  private static class ExceptionAnswer implements Answer {
    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      Method method = invocation.getMethod();
      if (method.getDeclaringClass().equals(TaskScheduler.class) &&
          !method.getName().equals("getContext") && !method.getName().equals("initialize") &&
          !method.getName().equals("start") && !method.getName().equals("shutdown")) {
        throw new RuntimeException("TestException_" + method.getName());
      } else {
        return invocation.callRealMethod();
      }
    }
  }

  public static class TSEHForMultipleSchedulersTest extends TaskSchedulerManager {

    private final TaskScheduler yarnTaskScheduler;
    private final TaskScheduler uberTaskScheduler;
    private final AtomicBoolean uberSchedulerCreated = new AtomicBoolean(false);
    private final AtomicBoolean yarnSchedulerCreated = new AtomicBoolean(false);
    private final AtomicInteger numCreateInvocations = new AtomicInteger(0);
    private final Set<Integer> seenSchedulers = new HashSet<>();
    private final List<TaskSchedulerContext> taskSchedulerContexts = new LinkedList<>();
    private final List<String> taskSchedulerNames = new LinkedList<>();
    private final List<TaskScheduler> testTaskSchedulers = new LinkedList<>();

    public TSEHForMultipleSchedulersTest(AppContext appContext,
                                         DAGClientServer clientService,
                                         EventHandler eventHandler,
                                         ContainerSignatureMatcher containerSignatureMatcher,
                                         WebUIService webUI,
                                         List<NamedEntityDescriptor> schedulerDescriptors,
                                         boolean isPureLocalMode) {
      super(appContext, clientService, eventHandler, containerSignatureMatcher, webUI,
          schedulerDescriptors, isPureLocalMode);
      yarnTaskScheduler = mock(TaskScheduler.class);
      uberTaskScheduler = mock(TaskScheduler.class);
    }

    @Override
    TaskScheduler createTaskScheduler(String host, int port, String trackingUrl,
                                      AppContext appContext,
                                      NamedEntityDescriptor taskSchedulerDescriptor,
                                      long customAppIdIdentifier,
                                      int schedulerId) throws TezException {

      numCreateInvocations.incrementAndGet();
      boolean added = seenSchedulers.add(schedulerId);
      assertTrue("Cannot add multiple schedulers with the same schedulerId", added);
      taskSchedulerNames.add(taskSchedulerDescriptor.getEntityName());
      return super.createTaskScheduler(host, port, trackingUrl, appContext, taskSchedulerDescriptor,
          customAppIdIdentifier, schedulerId);
    }

    @Override
    TaskSchedulerContext wrapTaskSchedulerContext(TaskSchedulerContext rawContext) {
      // Avoid wrapping in threads
      return rawContext;
    }

    @Override
    TaskScheduler createYarnTaskScheduler(TaskSchedulerContext taskSchedulerContext, int schedulerId) {
      taskSchedulerContexts.add(taskSchedulerContext);
      testTaskSchedulers.add(yarnTaskScheduler);
      yarnSchedulerCreated.set(true);
      return yarnTaskScheduler;
    }

    @Override
    TaskScheduler createUberTaskScheduler(TaskSchedulerContext taskSchedulerContext, int schedulerId) {
      taskSchedulerContexts.add(taskSchedulerContext);
      uberSchedulerCreated.set(true);
      testTaskSchedulers.add(yarnTaskScheduler);
      return uberTaskScheduler;
    }

    @Override
    TaskScheduler createCustomTaskScheduler(TaskSchedulerContext taskSchedulerContext,
                                            NamedEntityDescriptor taskSchedulerDescriptor, int schedulerId)
                                                throws TezException {
      taskSchedulerContexts.add(taskSchedulerContext);
      TaskScheduler taskScheduler = spy(super.createCustomTaskScheduler(taskSchedulerContext, taskSchedulerDescriptor, schedulerId));
      testTaskSchedulers.add(taskScheduler);
      return taskScheduler;
    }

    @Override
    // Inline handling of events.
    public void handle(AMSchedulerEvent event) {
      handleEvent(event);
    }

    public boolean getUberSchedulerCreated() {
      return uberSchedulerCreated.get();
    }

    public boolean getYarnSchedulerCreated() {
      return yarnSchedulerCreated.get();
    }

    public int getNumCreateInvocations() {
      return numCreateInvocations.get();
    }

    public TaskSchedulerContext getTaskSchedulerContext(int schedulerId) {
      return taskSchedulerContexts.get(schedulerId);
    }

    public String getTaskSchedulerName(int schedulerId) {
      return taskSchedulerNames.get(schedulerId);
    }

    public TaskScheduler getTestTaskScheduler(int schedulerId) {
      return testTaskSchedulers.get(schedulerId);
    }
  }

  public static class FakeTaskScheduler extends TaskScheduler {

    public FakeTaskScheduler(
        TaskSchedulerContext taskSchedulerContext) {
      super(taskSchedulerContext);
    }

    @Override
    public Resource getAvailableResources() {
      return null;
    }

    @Override
    public int getClusterNodeCount() {
      return 0;
    }

    @Override
    public void dagComplete() {

    }

    @Override
    public Resource getTotalResources() {
      return null;
    }

    @Override
    public void blacklistNode(NodeId nodeId) {

    }

    @Override
    public void unblacklistNode(NodeId nodeId) {

    }

    @Override
    public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                             Priority priority, Object containerSignature, Object clientCookie) {

    }

    @Override
    public void allocateTask(Object task, Resource capability, ContainerId containerId,
                             Priority priority, Object containerSignature, Object clientCookie) {

    }

    @Override
    public boolean deallocateTask(Object task, boolean taskSucceeded,
                                  TaskAttemptEndReason endReason,
                                  String diagnostics) {
      return false;
    }

    @Override
    public Object deallocateContainer(ContainerId containerId) {
      return null;
    }

    @Override
    public void setShouldUnregister() {

    }

    @Override
    public boolean hasUnregistered() {
      return false;
    }
  }
}

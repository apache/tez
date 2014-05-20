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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.TaskScheduler.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientAsyncForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AlwaysMatchesContainerMatcher;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.CapturingEventHandler;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerAppCallbackDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerEventHandlerForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableAppCallback;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TestContainerReuse {
  private static final Log LOG = LogFactory.getLog(TestContainerReuse.class);

  @Test(timeout = 15000l)
  public void testDelayedReuseContainerBecomesAvailable()
      throws IOException, InterruptedException, ExecutionException {
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 3000l);
    RackResolver.init(conf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskAttemptListener.class),
      new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal =
        new TaskSchedulerEventHandlerForTest(
          appContext, eventHandler, rmClient,
          new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler =
        spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(conf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler =
      (TaskSchedulerWithDrainableAppCallback)
        ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback =
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

    taskSchedulerEventHandler.handleEvent(lrTa11);
    taskSchedulerEventHandler.handleEvent(lrTa21);

    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(
      Lists.newArrayList(containerHost1, containerHost2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta21), any(Object.class), eq(containerHost2));

    // Adding the event later so that task1 assigned to containerHost1
    // is deterministic.
    taskSchedulerEventHandler.handleEvent(lrTa31);

    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(
        ta11, containerHost1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler, times(1)).taskAllocated(
      eq(ta31), any(Object.class), eq(containerHost1));
    verify(rmClient, times(0)).releaseAssignedContainer(
      eq(containerHost1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta21, containerHost2.getId(),
        TaskAttemptState.SUCCEEDED));

    long currentTs = System.currentTimeMillis();
    Throwable exception = null;
    while (System.currentTimeMillis() < currentTs + 5000l) {
      try {
        verify(taskSchedulerEventHandler,
          times(1)).containerBeingReleased(eq(containerHost2.getId()));
        exception = null;
        break;
      } catch (Throwable e) {
        exception = e;
      }
    }
    assertTrue("containerHost2 was not released", exception == null);
    taskScheduler.stop();
    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  @Test(timeout = 15000l)
  public void testDelayedReuseContainerNotAvailable()
      throws IOException, InterruptedException, ExecutionException {
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 1000l);
    RackResolver.init(conf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskAttemptListener.class),
      new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal =
      new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient,
        new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler =
      spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(conf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler =
      (TaskSchedulerWithDrainableAppCallback)
        ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
          .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback =
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

    taskSchedulerEventHandler.handleEvent(lrTa11);
    taskSchedulerEventHandler.handleEvent(lrTa21);

    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Lists.newArrayList(containerHost1, containerHost2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta21), any(Object.class), eq(containerHost2));

    // Adding the event later so that task1 assigned to containerHost1 is deterministic.
    taskSchedulerEventHandler.handleEvent(lrTa31);

    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta21, containerHost2.getId(),
        TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta21), eq(true));
    verify(taskSchedulerEventHandler, times(0)).taskAllocated(
      eq(ta31), any(Object.class), eq(containerHost2));
    verify(rmClient, times(1)).releaseAssignedContainer(
      eq(containerHost2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.stop();
    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  @Test(timeout = 10000l)
  public void testSimpleReuse() throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS, 0);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskAttemptListener.class), new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient, new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
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

    taskSchedulerEventHandler.handleEvent(lrEvent1);
    taskSchedulerEventHandler.handleEvent(lrEvent2);
    taskSchedulerEventHandler.handleEvent(lrEvent3);
    taskSchedulerEventHandler.handleEvent(lrEvent4);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(container1));

    // Task assigned to container completed successfully. Container should be re-used.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta11, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta12, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta12), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta13), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Verify no re-use if a previous task fails.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta13, container1.getId(), TaskAttemptState.FAILED));
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler, times(0)).taskAllocated(eq(ta14), any(Object.class), eq(container1));
    verify(taskScheduler).deallocateTask(eq(ta13), eq(false));
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    Container container2 = createContainer(2, "host2", resource1, priority1);

    // Second container allocated. Should be allocated to the last task.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta14), any(Object.class), eq(container2));

    // Task assigned to container completed successfully. No pending requests. Container should be released.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta14, container2.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta14), eq(true));
    verify(rmClient).releaseAssignedContainer(eq(container2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();


    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  @Test(timeout = 30000l)
  public void testReuseNonLocalRequest()
      throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 100l);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS, 10000l);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
        spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class),
        mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal =
        new TaskSchedulerEventHandlerForTest(
          appContext, eventHandler, rmClient,
          new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler =
        spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler =
      (TaskSchedulerWithDrainableAppCallback)
        ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback =
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
    taskSchedulerEventHandler.handleEvent(lrEvent11);

    Container container1 = createContainer(1, "randomHost", resource1, priority);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerEventHandler.handleEvent(lrEvent12);

    // Task assigned to container completed successfully.
    // Container should not be immediately assigned to task 2
    // until delay expires.
    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta11, container1.getId(),
        TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler, times(0)).taskAllocated(
      eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(6000l);
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta12), any(Object.class), eq(container1));

    // TA12 completed.
    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta12, container1.getId(),
        TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  @Test(timeout = 30000l)
  public void testReuseAcrossVertices()
      throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setLong(
      TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 1l);
    tezConf.setLong(
      TezConfiguration.TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS, 2000l);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = TezDAGID.getInstance("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient =
      spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(
      mock(ContainerHeartbeatHandler.class),
      mock(TaskAttemptListener.class),
      new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(true).when(appContext).isSession();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal =
      new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient,
        new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler =
      spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler =
      (TaskSchedulerWithDrainableAppCallback)
        ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
          .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

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
    taskSchedulerEventHandler.handleEvent(lrEvent11);

    Container container1 = createContainer(1, host1[0], resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerEventHandler.handleEvent(lrEvent21);

    // Task assigned to container completed successfully.
    // Container should  be assigned to task21.
    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta11, container1.getId(),
        TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(
      eq(ta21), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));

    // Task 2 completes.
    taskSchedulerEventHandler.handleEvent(
      new AMSchedulerEventTAEnded(ta21, container1.getId(),
        TaskAttemptState.SUCCEEDED));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));

    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(6000l);
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }
  
  @Test(timeout = 30000l)
  public void testReuseLocalResourcesChanged() throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS, 0);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS, -1);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID1 = TezDAGID.getInstance("0", 1, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    TezAMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    ChangingDAGIDAnswer dagIDAnswer = new ChangingDAGIDAnswer(dagID1);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class),
        mock(TaskAttemptListener.class), new ContainerContextMatcher(), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(DAGAppMasterState.RUNNING).when(appContext).getAMState();
    doReturn(true).when(appContext).isSession();
    doAnswer(dagIDAnswer).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();
    
    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient, new AlwaysMatchesContainerMatcher());
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
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

    taskSchedulerEventHandler.handleEvent(lrEvent11);
    taskSchedulerEventHandler.handleEvent(lrEvent12);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    drainNotifier.set(false);
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    TestTaskSchedulerHelpers.waitForDelayedDrainNotify(drainNotifier);
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta111), any(Object.class), eq(container1));
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());
    
    // Task assigned to container completed successfully. Container should be re-used.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta111, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta111), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta112), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(1, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta112, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta112), eq(true));
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

    taskSchedulerEventHandler.handleEvent(lrEvent21);
    taskSchedulerEventHandler.handleEvent(lrEvent22);
    drainableAppCallback.drain();

    // TODO This is terrible, need a better way to ensure the scheduling loop has run
    LOG.info("Sleeping to ensure that the scheduling loop runs");
    Thread.sleep(6000l);
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta211), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(2, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta211, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta211), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta212), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    assignEvent = (AMContainerEventAssignTA) eventHandler.verifyInvocation(AMContainerEventAssignTA.class);
    assertEquals(2, assignEvent.getRemoteTaskLocalResources().size());
    eventHandler.reset();

    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  private Container createContainer(int id, String host, Resource resource, Priority priority) {
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
      locationHint = new TaskLocationHint(hostsSet, racksSet);
    }
    AMSchedulerEventTALaunchRequest lr = new AMSchedulerEventTALaunchRequest(
      taID, capability, new TaskSpec(taID, "dagName", "vertexName",
      new ProcessorDescriptor("processorClassName"),
      Collections.singletonList(new InputSpec("vertexName",
        new InputDescriptor("inputClassName"), 1)),
      Collections.singletonList(new OutputSpec("vertexName",
        new OutputDescriptor("outputClassName"), 1)), null), ta, locationHint,
      priority, containerContext);
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
        new ContainerContext(localResources, new Credentials(), new HashMap<String, String>(), ""));
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
}
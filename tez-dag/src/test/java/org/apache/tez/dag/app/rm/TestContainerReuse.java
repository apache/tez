package org.apache.tez.dag.app.rm;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

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
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.TaskScheduler.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientAsyncForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.AMRMClientForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.CapturingEventHandler;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerAppCallbackDrainable;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerEventHandlerForTest;
import org.apache.tez.dag.app.rm.TestTaskSchedulerHelpers.TaskSchedulerWithDrainableAppCallback;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestContainerReuse {
  

  @Test(timeout = 15000l)
  public void testDelayedReuseContainerBecomesAvailable() throws IOException, InterruptedException, ExecutionException {
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_DELAY_ALLOCATION_MILLIS, 3000l);
    RackResolver.init(conf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = new TezDAGID("0", 0, 0);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    
    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    AMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(conf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(5);
    String [] host1 = {"host1"};
    String [] host2 = {"host2"};

    String [] defaultRack = {"/default-rack"};

    TezTaskAttemptID taID11 = new TezTaskAttemptID(new TezTaskID(vertexID, 1), 1);
    TezTaskAttemptID taID21 = new TezTaskAttemptID(new TezTaskID(vertexID, 2), 1);
    TezTaskAttemptID taID31 = new TezTaskAttemptID(new TezTaskID(vertexID, 3), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    TaskAttempt ta31 = mock(TaskAttempt.class);

    AMSchedulerEventTALaunchRequest lrTa11 = createLaunchRequestEvent(taID11, ta11, resource, host1, defaultRack, priority, conf);
    AMSchedulerEventTALaunchRequest lrTa21 = createLaunchRequestEvent(taID21, ta21, resource, host2, defaultRack, priority, conf);
    AMSchedulerEventTALaunchRequest lrTa31 = createLaunchRequestEvent(taID31, ta31, resource, host1, defaultRack, priority, conf);

    taskSchedulerEventHandler.handleEvent(lrTa11);
    taskSchedulerEventHandler.handleEvent(lrTa21);

    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    taskScheduler.onContainersAllocated(Lists.newArrayList(containerHost1, containerHost2));
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta21), any(Object.class), eq(containerHost2));

    // Adding the event later so that task1 assigned to containerHost1 is deterministic.
    taskSchedulerEventHandler.handleEvent(lrTa31);

    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta11, containerHost1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler, times(1)).taskAllocated(eq(ta31), any(Object.class), eq(containerHost1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(containerHost1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();
    
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta21, containerHost2.getId(), TaskAttemptState.SUCCEEDED));
    
    long currentTs = System.currentTimeMillis();
    Throwable exception = null;
    while (System.currentTimeMillis() < currentTs + 5000l) {
      try {
        verify(taskSchedulerEventHandler, times(1)).containerBeingReleased(eq(containerHost2.getId()));
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
  public void testDelayedReuseContainerNotAvailable() throws IOException, InterruptedException, ExecutionException {
    Configuration conf = new Configuration(new YarnConfiguration());
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    conf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_DELAY_ALLOCATION_MILLIS, 3000l);
    RackResolver.init(conf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = new TezDAGID("0", 0, 0);
    TezVertexID vertexID = new TezVertexID(dagID, 1);
    
    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    AMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(conf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();
    
    Resource resource = Resource.newInstance(1024, 1);
    Priority priority = Priority.newInstance(5);
    String [] host1 = {"host1"};
    String [] host2 = {"host2"};
    
    String [] defaultRack = {"/default-rack"};
    
    TezTaskAttemptID taID11 = new TezTaskAttemptID(new TezTaskID(vertexID, 1), 1);
    TezTaskAttemptID taID21 = new TezTaskAttemptID(new TezTaskID(vertexID, 2), 1);
    TezTaskAttemptID taID31 = new TezTaskAttemptID(new TezTaskID(vertexID, 3), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    TaskAttempt ta31 = mock(TaskAttempt.class);
    
    AMSchedulerEventTALaunchRequest lrTa11 = createLaunchRequestEvent(taID11, ta11, resource, host1, defaultRack, priority, conf);
    AMSchedulerEventTALaunchRequest lrTa21 = createLaunchRequestEvent(taID21, ta21, resource, host2, defaultRack, priority, conf);
    AMSchedulerEventTALaunchRequest lrTa31 = createLaunchRequestEvent(taID31, ta31, resource, host1, defaultRack, priority, conf);
    
    taskSchedulerEventHandler.handleEvent(lrTa11);
    taskSchedulerEventHandler.handleEvent(lrTa21);
    
    Container containerHost1 = createContainer(1, host1[0], resource, priority);
    Container containerHost2 = createContainer(2, host2[0], resource, priority);

    taskScheduler.onContainersAllocated(Lists.newArrayList(containerHost1, containerHost2));
    drainableAppCallback.drain();
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(containerHost1));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta21), any(Object.class), eq(containerHost2));
    
    // Adding the event later so that task1 assigned to containerHost1 is deterministic.
    taskSchedulerEventHandler.handleEvent(lrTa31);
   
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta21, containerHost2.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta21), eq(true));
    verify(taskSchedulerEventHandler, times(0)).taskAllocated(eq(ta31), any(Object.class), eq(containerHost2));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(containerHost2.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();
    
    long currentTs = System.currentTimeMillis();
    Throwable exception = null;
    while (System.currentTimeMillis() < currentTs + 5000l) {
      try {
      verify(taskSchedulerEventHandler, times(1)).taskAllocated(eq(ta31), any(Object.class), eq(containerHost2));
      exception = null;
      break;
      } catch (Throwable e) {
        exception = e;
      }
    }
    assertTrue("containerHost2 not assigned to ta31", exception == null);
    taskScheduler.stop();
    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }

  @Test(timeout = 10000l)
  public void testSimpleReuse() throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, true);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = new TezDAGID("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    AMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};
    String[] host2 = {"host2"};

    String []racks = {"/default-rack"};
    Priority priority1 = Priority.newInstance(1);

    TezVertexID vertexID1 = new TezVertexID(dagID, 1);

    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = new TezTaskAttemptID(new TezTaskID(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent1 = createLaunchRequestEvent(taID11, ta11, resource1, host1, racks, priority1, tezConf);

    //Vertex 1, Task 2, Attempt 1, host1
    TezTaskAttemptID taID12 = new TezTaskAttemptID(new TezTaskID(vertexID1, 2), 1);
    TaskAttempt ta12 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent2 = createLaunchRequestEvent(taID12, ta12, resource1, host1, racks, priority1, tezConf);

    //Vertex 1, Task 3, Attempt 1, host2
    TezTaskAttemptID taID13 = new TezTaskAttemptID(new TezTaskID(vertexID1, 3), 1);
    TaskAttempt ta13 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent3 = createLaunchRequestEvent(taID13, ta13, resource1, host2, racks, priority1, tezConf);

    //Vertex 1, Task 4, Attempt 1, host2
    TezTaskAttemptID taID14 = new TezTaskAttemptID(new TezTaskID(vertexID1, 4), 1);
    TaskAttempt ta14 = mock(TaskAttempt.class);
    AMSchedulerEventTALaunchRequest lrEvent4 = createLaunchRequestEvent(taID14, ta14, resource1, host2, racks, priority1, tezConf);

    taskSchedulerEventHandler.handleEvent(lrEvent1);
    taskSchedulerEventHandler.handleEvent(lrEvent2);
    taskSchedulerEventHandler.handleEvent(lrEvent3);
    taskSchedulerEventHandler.handleEvent(lrEvent4);

    Container container1 = createContainer(1, "host1", resource1, priority1);

    // One container allocated.
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
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
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));
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
  
  @Test(timeout = 10000l)
  public void testReuseNonLocalRequest() throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED, false);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED, false);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_DELAY_ALLOCATION_MILLIS, 10000l);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = new TezDAGID("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    AMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

    Resource resource1 = Resource.newInstance(1024, 1);
    String [] emptyHosts = new String[0];
    String [] emptyRacks = new String[0];

    Priority priority = Priority.newInstance(3);

    TezVertexID vertexID = new TezVertexID(dagID, 1);

    //Vertex 1, Task 1, Attempt 1, no locality information.
    TezTaskAttemptID taID11 = new TezTaskAttemptID(new TezTaskID(vertexID, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    doReturn(vertexID).when(ta11).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(taID11, ta11, resource1, emptyHosts, emptyRacks, priority, tezConf);

    //Vertex1, Task2, Attempt 1,  nolocality information.
    TezTaskAttemptID taID12 = new TezTaskAttemptID(new TezTaskID(vertexID, 2), 1);
    TaskAttempt ta12 = mock(TaskAttempt.class);
    doReturn(vertexID).when(ta12).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent12 = createLaunchRequestEvent(taID12, ta12, resource1, emptyHosts, emptyRacks, priority, tezConf);
    
    // Send launch request for task 1 onle, deterministic assignment to this task.
    taskSchedulerEventHandler.handleEvent(lrEvent11);

    Container container1 = createContainer(1, "randomHost", resource1, priority);

    // One container allocated.
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerEventHandler.handleEvent(lrEvent12);
    
    // Task assigned to container completed successfully. Container should be
    // assigned immediately to ta12, since there's no local requests (instead of
    // waiting for the re-use delay)
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta11, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // TA12 completed.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta12, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

    taskScheduler.close();
    taskSchedulerEventHandler.close();
  }
  
  @Test(timeout = 10000l)
  public void testNoReuseAcrossVertices() throws IOException, InterruptedException, ExecutionException {
    Configuration tezConf = new Configuration(new YarnConfiguration());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, true);
    tezConf.setLong(TezConfiguration.TEZ_AM_CONTAINER_REUSE_DELAY_ALLOCATION_MILLIS, 0l);
    RackResolver.init(tezConf);
    TaskSchedulerAppCallback mockApp = mock(TaskSchedulerAppCallback.class);

    CapturingEventHandler eventHandler = new CapturingEventHandler();
    TezDAGID dagID = new TezDAGID("0", 0, 0);

    AMRMClient<CookieContainerRequest> rmClientCore = new AMRMClientForTest();
    AMRMClientAsync<CookieContainerRequest> rmClient = spy(new AMRMClientAsyncForTest(rmClientCore, 100));
    String appUrl = "url";
    String appMsg = "success";
    AppFinalStatus finalStatus =
        new AppFinalStatus(FinalApplicationStatus.SUCCEEDED, appMsg, appUrl);

    doReturn(finalStatus).when(mockApp).getFinalAppStatus();

    AppContext appContext = mock(AppContext.class);
    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class), appContext);
    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(amNodeMap).when(appContext).getAllNodes();
    doReturn(dagID).when(appContext).getCurrentDAGID();
    doReturn(mock(ClusterInfo.class)).when(appContext).getClusterInfo();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskSchedulerWithDrainableAppCallback taskScheduler = (TaskSchedulerWithDrainableAppCallback) ((TaskSchedulerEventHandlerForTest) taskSchedulerEventHandler)
        .getSpyTaskScheduler();
    TaskSchedulerAppCallbackDrainable drainableAppCallback = taskScheduler.getDrainableAppCallback();

    Resource resource1 = Resource.newInstance(1024, 1);
    String[] host1 = {"host1"};

    String []racks = {"/default-rack"};
    Priority priority = Priority.newInstance(3);

    TezVertexID vertexID1 = new TezVertexID(dagID, 1);
    TezVertexID vertexID2 = new TezVertexID(dagID, 2);

    //Vertex 1, Task 1, Attempt 1, host1
    TezTaskAttemptID taID11 = new TezTaskAttemptID(new TezTaskID(vertexID1, 1), 1);
    TaskAttempt ta11 = mock(TaskAttempt.class);
    doReturn(vertexID1).when(ta11).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent11 = createLaunchRequestEvent(taID11, ta11, resource1, host1, racks, priority, tezConf);

    //Vertex2, Task1, Attempt 1, host1
    TezTaskAttemptID taID21 = new TezTaskAttemptID(new TezTaskID(vertexID2, 1), 1);
    TaskAttempt ta21 = mock(TaskAttempt.class);
    doReturn(vertexID2).when(ta21).getVertexID();
    AMSchedulerEventTALaunchRequest lrEvent21 = createLaunchRequestEvent(taID21, ta21, resource1, host1, racks, priority, tezConf);
    
    // Send launch request for task 1 onle, deterministic assignment to this task.
    taskSchedulerEventHandler.handleEvent(lrEvent11);

    Container container1 = createContainer(1, host1[0], resource1, priority);

    // One container allocated.
    taskScheduler.onContainersAllocated(Collections.singletonList(container1));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta11), any(Object.class), eq(container1));

    // Send launch request for task2 (vertex2)
    taskSchedulerEventHandler.handleEvent(lrEvent21);
    
    // Task assigned to container completed successfully. Container should not be assigned to task21. Released since delay is 0.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta11, container1.getId(), TaskAttemptState.SUCCEEDED));
    drainableAppCallback.drain();
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);

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
      TezTaskAttemptID taID, TaskAttempt ta, Resource capability, String[] hosts,
      String[] racks, Priority priority, Configuration conf) {

    ContainerContext containerContext = 
        new ContainerContext(new HashMap<String, LocalResource>(),
            new Credentials(), new HashMap<String, String>(), "");
    AMSchedulerEventTALaunchRequest lr = new AMSchedulerEventTALaunchRequest(taID, capability, new TezEngineTaskContext(taID, "user", "jobName", "vertexName",
        new ProcessorDescriptor("processorClassName"),
        Collections.singletonList(new InputSpec("vertexName", 1,
            "inputClassName")), Collections.singletonList(new OutputSpec(
            "vertexName", 1, "outputClassName"))), ta, hosts, racks, priority, containerContext);
    return lr;
  }


}

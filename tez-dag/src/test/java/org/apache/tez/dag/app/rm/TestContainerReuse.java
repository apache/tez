package org.apache.tez.dag.app.rm;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
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
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
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
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.rm.TaskScheduler.CookieContainerRequest;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback.AppFinalStatus;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Test;

public class TestContainerReuse {

  @Test
  public void test() throws IOException {
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
    doReturn(dagID).when(appContext).getDAGID();

    TaskSchedulerEventHandler taskSchedulerEventHandlerReal = new TaskSchedulerEventHandlerForTest(appContext, eventHandler, rmClient);
    TaskSchedulerEventHandler taskSchedulerEventHandler = spy(taskSchedulerEventHandlerReal);
    taskSchedulerEventHandler.init(tezConf);
    taskSchedulerEventHandler.start();

    TaskScheduler taskScheduler = ((TaskSchedulerEventHandlerForTest)taskSchedulerEventHandler).getSpyTaskScheduler();

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
    verify(taskScheduler).deallocateTask(eq(ta11), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta12), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Task assigned to container completed successfully.
    // Verify reuse across hosts.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta12, container1.getId(), TaskAttemptState.SUCCEEDED));
    verify(taskScheduler).deallocateTask(eq(ta12), eq(true));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta13), any(Object.class), eq(container1));
    verify(rmClient, times(0)).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyNoInvocations(AMContainerEventStopRequest.class);
    eventHandler.reset();

    // Verify no re-use if a previous task fails.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta13, container1.getId(), TaskAttemptState.FAILED));
    verify(taskSchedulerEventHandler, times(0)).taskAllocated(eq(ta14), any(Object.class), eq(container1));
    verify(taskScheduler).deallocateTask(eq(ta13), eq(false));
    verify(rmClient).releaseAssignedContainer(eq(container1.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
    eventHandler.reset();

    Container container2 = createContainer(2, "host2", resource1, priority1);

    // Second container allocated. Should be allocated to the last task.
    taskScheduler.onContainersAllocated(Collections.singletonList(container2));
    verify(taskSchedulerEventHandler).taskAllocated(eq(ta14), any(Object.class), eq(container2));

    // Task assigned to container completed successfully. No pending requests. Container should be released.
    taskSchedulerEventHandler.handleEvent(new AMSchedulerEventTAEnded(ta14, container2.getId(), TaskAttemptState.SUCCEEDED));
    verify(taskScheduler).deallocateTask(eq(ta14), eq(true));
    verify(rmClient).releaseAssignedContainer(eq(container2.getId()));
    eventHandler.verifyInvocation(AMContainerEventStopRequest.class);
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
      TezTaskAttemptID taID, TaskAttempt ta, Resource capability, String[] hosts,
      String[] racks, Priority priority, Configuration conf) {
    AMSchedulerEventTALaunchRequest lr = new AMSchedulerEventTALaunchRequest(
        taID, capability, new HashMap<String, LocalResource>(),
        new TezEngineTaskContext(taID, "user", "jobName", "vertexName",
            new ProcessorDescriptor("processorClassName"),
            Collections.singletonList(new InputSpec("vertexName", 1,
                "inputClassName")), Collections.singletonList(new OutputSpec(
                "vertexName", 1, "outputClassName"))), ta,
        new Credentials(), null, hosts, racks, priority,
        new HashMap<String, String>(), conf);
    return lr;
  }

  // Mocking AMRMClientImpl to make use of getMatchingRequest
  public static class AMRMClientForTest extends AMRMClientImpl<CookieContainerRequest> {

    @Override
    protected void serviceStart() {
    }

    @Override
    protected void serviceStop() {
    }
  }


  // Mocking AMRMClientAsyncImpl to make use of getMatchingRequest
  public static class AMRMClientAsyncForTest extends
      AMRMClientAsyncImpl<CookieContainerRequest> {

    public AMRMClientAsyncForTest(
        AMRMClient<CookieContainerRequest> client,
        int intervalMs) {
      // CallbackHandler is not needed - will be called independently in the test.
      super(client, intervalMs, null);
    }

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        String appHostName, int appHostPort, String appTrackingUrl) {
      return null;
    }

    @Override
    public void unregisterApplicationMaster(FinalApplicationStatus appStatus,
        String appMessage, String appTrackingUrl) {
    }

    @Override
    protected void serviceStart() {
    }

    @Override
    protected void serviceStop() {
    }
  }

  // Overrides start / stop. Will be controlled without the extra event handling thread.
  public static class TaskSchedulerEventHandlerForTest extends TaskSchedulerEventHandler {

    private AMRMClientAsync<CookieContainerRequest> amrmClientAsync;

    @SuppressWarnings("rawtypes")
    public TaskSchedulerEventHandlerForTest(AppContext appContext,
        EventHandler eventHandler, AMRMClientAsync<CookieContainerRequest> amrmClientAsync) {
      super(appContext, null, eventHandler);
      this.amrmClientAsync = amrmClientAsync;
    }

    @Override
    public TaskScheduler createTaskScheduler(String host, int port, String trackingUrl) {
      return new TaskScheduler(this, host, port, trackingUrl, amrmClientAsync);
    }

    public TaskScheduler getSpyTaskScheduler() {
      return this.taskScheduler;
    }

    @Override
    public void serviceStart() {
      TaskScheduler taskSchedulerReal = createTaskScheduler("host", 0, "");
      // Init the service so that reuse configuration is picked up.
      taskSchedulerReal.serviceInit(getConfig());
      taskScheduler = spy(taskSchedulerReal);
    }

    @Override
    public void serviceStop() {
    }
  }

  @SuppressWarnings("rawtypes")
  static class CapturingEventHandler implements EventHandler {

    private List<Event> events = new LinkedList<Event>();

    public void handle(Event event) {
      events.add(event);
    }

    public void reset() {
      events.clear();
    }

    public void verifyNoInvocations(Class<? extends Event> eventClass) {
      for (Event e : events) {
        assertFalse(e.getClass().getName().equals(eventClass.getName()));
      }
    }

    public void verifyInvocation(Class<? extends Event> eventClass) {
      for (Event e : events) {
        if (e.getClass().getName().equals(eventClass.getName())) {
          return;
        }
      }
      fail("Expected Event: " + eventClass.getName() + " not sent");
    }
  }
}

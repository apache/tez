/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.tez.dag.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskHeartbeatRequest;
import org.apache.tez.serviceplugins.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezException;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTezEventUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class TestTaskCommunicatorManager1 {
  private ApplicationId appId;
  private ApplicationAttemptId appAttemptId;
  private AppContext appContext;
  Credentials credentials;
  AMContainerMap amContainerMap;
  EventHandler eventHandler;
  DAG dag;
  TaskCommunicatorManager taskAttemptListener;
  ContainerTask containerTask;
  AMContainerTask amContainerTask;
  TaskSpec taskSpec;

  TezVertexID vertexID;
  TezTaskID taskID;
  TezTaskAttemptID taskAttemptID;

  @Before
  public void setUp() throws TezException {
    appId = ApplicationId.newInstance(1000, 1);
    appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    dag = mock(DAG.class);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    vertexID = TezVertexID.getInstance(dagID, 1);
    taskID = TezTaskID.getInstance(vertexID, 1);
    taskAttemptID = TezTaskAttemptID.getInstance(taskID, 1);
    credentials = new Credentials();

    amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();

    eventHandler = mock(EventHandler.class);

    MockClock clock = new MockClock();
    
    appContext = mock(AppContext.class);
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(appAcls).when(appContext).getApplicationACLs();
    doReturn(amContainerMap).when(appContext).getAllContainers();
    doReturn(clock).when(appContext).getClock();
    
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(credentials).when(appContext).getAppCredentials();
    NodeId nodeId = NodeId.newInstance("localhost", 0);
    AMContainer amContainer = mock(AMContainer.class);
    Container container = mock(Container.class);
    doReturn(nodeId).when(container).getNodeId();
    doReturn(amContainer).when(amContainerMap).get(any(ContainerId.class));
    doReturn(container).when(amContainer).getContainer();

    Configuration conf = new TezConfiguration();
    UserPayload defaultPayload;
    try {
      defaultPayload = TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    taskAttemptListener = new TaskCommunicatorManagerInterfaceImplForTest(appContext,
        mock(TaskHeartbeatHandler.class), mock(ContainerHeartbeatHandler.class),
        Lists.newArrayList(
            new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
                .setUserPayload(defaultPayload)));

    taskSpec = mock(TaskSpec.class);
    doReturn(taskAttemptID).when(taskSpec).getTaskAttemptID();
    amContainerTask = new AMContainerTask(taskSpec, null, null, false, 0);
    containerTask = null;
  }

  @Test(timeout = 5000)
  public void testGetTask() throws IOException {

    TezTaskCommunicatorImpl taskCommunicator =
        (TezTaskCommunicatorImpl) taskAttemptListener.getTaskCommunicator(0).getTaskCommunicator();
    TezTaskUmbilicalProtocol tezUmbilical = taskCommunicator.getUmbilical();

    ContainerId containerId1 = createContainerId(appId, 1);
    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    containerTask = tezUmbilical.getTask(containerContext1);
    assertTrue(containerTask.shouldDie());

    ContainerId containerId2 = createContainerId(appId, 2);
    ContainerContext containerContext2 = new ContainerContext(containerId2.toString());
    taskAttemptListener.registerRunningContainer(containerId2, 0);
    containerTask = tezUmbilical.getTask(containerContext2);
    assertNull(containerTask);

    // Valid task registered
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId2, 0);
    containerTask = tezUmbilical.getTask(containerContext2);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Task unregistered. Should respond to heartbeats
    taskAttemptListener.unregisterTaskAttempt(taskAttemptID, 0, TaskAttemptEndReason.OTHER, null);
    containerTask = tezUmbilical.getTask(containerContext2);
    assertNull(containerTask);

    // Container unregistered. Should send a shouldDie = true
    taskAttemptListener.unregisterRunningContainer(containerId2, 0, ContainerEndReason.OTHER, null);
    containerTask = tezUmbilical.getTask(containerContext2);
    assertTrue(containerTask.shouldDie());

    ContainerId containerId3 = createContainerId(appId, 3);
    ContainerContext containerContext3 = new ContainerContext(containerId3.toString());
    taskAttemptListener.registerRunningContainer(containerId3, 0);

    // Register task to container3, followed by unregistering container 3 all together
    TaskSpec taskSpec2 = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId2 = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId2).when(taskSpec2).getTaskAttemptID();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec, null, null, false, 0);
    taskAttemptListener.registerTaskAttempt(amContainerTask2, containerId3, 0);
    taskAttemptListener.unregisterRunningContainer(containerId3, 0, ContainerEndReason.OTHER, null);
    containerTask = tezUmbilical.getTask(containerContext3);
    assertTrue(containerTask.shouldDie());
  }

  @Test(timeout = 5000)
  public void testGetTaskMultiplePulls() throws IOException {
    TezTaskCommunicatorImpl taskCommunicator =
        (TezTaskCommunicatorImpl) taskAttemptListener.getTaskCommunicator(0).getTaskCommunicator();
    TezTaskUmbilicalProtocol tezUmbilical = taskCommunicator.getUmbilical();

    ContainerId containerId1 = createContainerId(appId, 1);

    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    taskAttemptListener.registerRunningContainer(containerId1, 0);
    containerTask = tezUmbilical.getTask(containerContext1);
    assertNull(containerTask);

    // Register task
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId1, 0);
    containerTask = tezUmbilical.getTask(containerContext1);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Try pulling again - simulates re-use pull
    containerTask = tezUmbilical.getTask(containerContext1);
    assertNull(containerTask);
  }

  @Test (timeout = 5000)
  public void testTaskEventRouting() throws Exception {
    List<TezEvent> events =  Arrays.asList(
      new TezEvent(new TaskStatusUpdateEvent(null, 0.0f, null, false), new EventMetaData(EventProducerConsumerType.PROCESSOR,
          "v1", "v2", taskAttemptID)),
      new TezEvent(DataMovementEvent.create(0, ByteBuffer.wrap(new byte[0])), new EventMetaData(EventProducerConsumerType.OUTPUT,
          "v1", "v2", taskAttemptID)),
      new TezEvent(new TaskAttemptCompletedEvent(), new EventMetaData(EventProducerConsumerType.SYSTEM,
          "v1", "v2", taskAttemptID))
    );

    generateHeartbeat(events, 0, 1, 0, new ArrayList<TezEvent>());

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(4)).handle(arg.capture());
    final List<Event> argAllValues = arg.getAllValues();

    final Event statusUpdateEvent = argAllValues.get(0);
    assertEquals("First event should be status update", TaskAttemptEventType.TA_STATUS_UPDATE,
        statusUpdateEvent.getType());
    assertEquals(false, ((TaskAttemptEventStatusUpdate)statusUpdateEvent).getReadErrorReported());

    final TaskAttemptEventTezEventUpdate taEvent = (TaskAttemptEventTezEventUpdate)argAllValues.get(1);
    assertEquals(1, taEvent.getTezEvents().size());
    assertEquals(EventType.DATA_MOVEMENT_EVENT,
        taEvent.getTezEvents().get(0).getEventType());
    
    final TaskAttemptEvent taCompleteEvent = (TaskAttemptEvent)argAllValues.get(2);
    assertEquals(TaskAttemptEventType.TA_DONE, taCompleteEvent.getType());
    final VertexEventRouteEvent vertexRouteEvent = (VertexEventRouteEvent)argAllValues.get(3);
    assertEquals(1, vertexRouteEvent.getEvents().size());
    assertEquals(EventType.DATA_MOVEMENT_EVENT,
        vertexRouteEvent.getEvents().get(0).getEventType());
  }
  
  @Test (timeout = 5000)
  public void testTaskEventRoutingWithReadError() throws Exception {
    List<TezEvent> events =  Arrays.asList(
      new TezEvent(new TaskStatusUpdateEvent(null, 0.0f, null, false), null),
      new TezEvent(InputReadErrorEvent.create("", 0, 0), new EventMetaData(EventProducerConsumerType.INPUT,
          "v2", "v1", taskAttemptID)),
      new TezEvent(new TaskAttemptCompletedEvent(), new EventMetaData(EventProducerConsumerType.SYSTEM,
          "v1", "v2", taskAttemptID))
    );

    generateHeartbeat(events, 0, 1, 0, new ArrayList<TezEvent>());

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(3)).handle(arg.capture());
    final List<Event> argAllValues = arg.getAllValues();

    final Event statusUpdateEvent = argAllValues.get(0);
    assertEquals("First event should be status update", TaskAttemptEventType.TA_STATUS_UPDATE,
        statusUpdateEvent.getType());
    assertEquals(true, ((TaskAttemptEventStatusUpdate)statusUpdateEvent).getReadErrorReported());

    final Event taFinishedEvent = argAllValues.get(1);
    assertEquals("Second event should be TA_DONE", TaskAttemptEventType.TA_DONE,
        taFinishedEvent.getType());

    final Event vertexEvent = argAllValues.get(2);
    final VertexEventRouteEvent vertexRouteEvent = (VertexEventRouteEvent)vertexEvent;
    assertEquals("Third event should be routed to vertex", VertexEventType.V_ROUTE_EVENT,
        vertexEvent.getType());
    assertEquals(EventType.INPUT_READ_ERROR_EVENT,
        vertexRouteEvent.getEvents().get(0).getEventType());
  }


  @Test (timeout = 5000)
  public void testTaskEventRoutingTaskAttemptOnly() throws Exception {
    List<TezEvent> events = Arrays.asList(
      new TezEvent(new TaskAttemptCompletedEvent(), new EventMetaData(EventProducerConsumerType.SYSTEM,
          "v1", "v2", taskAttemptID))
    );
    generateHeartbeat(events, 0, 1, 0, new ArrayList<TezEvent>());

    ArgumentCaptor<Event> arg = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(1)).handle(arg.capture());
    final List<Event> argAllValues = arg.getAllValues();

    final Event event = argAllValues.get(0);
    // Route to TaskAttempt directly rather than through Vertex
    assertEquals("only event should be route event", TaskAttemptEventType.TA_DONE,
        event.getType());
  }
  
  @Test (timeout = 5000)
  public void testTaskHeartbeatResponse() throws Exception {
    List<TezEvent> events = new ArrayList<TezEvent>();
    List<TezEvent> eventsToSend = new ArrayList<TezEvent>();
    TaskHeartbeatResponse response = generateHeartbeat(events, 0, 1, 2, eventsToSend);
    
    assertEquals(2, response.getNextFromEventId());
    assertEquals(eventsToSend, response.getEvents());
  }

  //try 10 times to allocate random port, fail it if no one is succeed.
  @Test (timeout = 5000)
  public void testPortRange() {
    boolean succeedToAllocate = false;
    Random rand = new Random();
    for (int i = 0; i < 10; ++i) {
      int nextPort = 1024 + rand.nextInt(65535 - 1024);
      if (testPortRange(nextPort)) {
        succeedToAllocate = true;
        break;
      }
    }
    if (!succeedToAllocate) {
      fail("Can not allocate free port even in 10 iterations for TaskAttemptListener");
    }
  }

  // TODO TEZ-2003 Move this into TestTezTaskCommunicator. Potentially other tests as well.
  @Test (timeout= 5000)
  public void testPortRange_NotSpecified() throws IOException, TezException {
    Configuration conf = new Configuration();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(
        "fakeIdentifier"));
    Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(identifier,
        new JobTokenSecretManager());
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, credentials);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);
    taskAttemptListener = new TaskCommunicatorManager(appContext,
        mock(TaskHeartbeatHandler.class), mock(ContainerHeartbeatHandler.class), Lists.newArrayList(
        new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
            .setUserPayload(userPayload)));
    // no exception happen, should started properly
    taskAttemptListener.init(conf);
    taskAttemptListener.start();
  }

  private boolean testPortRange(int port) {
    boolean succeedToAllocate = true;
    try {
      Configuration conf = new Configuration();
      
      JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(
          "fakeIdentifier"));
      Token<JobTokenIdentifier> sessionToken = new Token<JobTokenIdentifier>(identifier,
          new JobTokenSecretManager());
      sessionToken.setService(identifier.getJobId());
      TokenCache.setSessionToken(sessionToken, credentials);

      conf.set(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE, port + "-" + port);
      UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);

      taskAttemptListener = new TaskCommunicatorManager(appContext,
          mock(TaskHeartbeatHandler.class), mock(ContainerHeartbeatHandler.class), Lists
          .newArrayList(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
              .setUserPayload(userPayload)));
      taskAttemptListener.init(conf);
      taskAttemptListener.start();
      int resultedPort = taskAttemptListener.getTaskCommunicator(0).getAddress().getPort();
      assertEquals(port, resultedPort);
    } catch (Exception e) {
      succeedToAllocate = false;
    } finally {
      if (taskAttemptListener != null) {
        try {
          taskAttemptListener.close();
        } catch (IOException e) {
          e.printStackTrace();
          fail("fail to stop TaskAttemptListener");
        }
      }
    }
    return succeedToAllocate;
  }

  private TaskHeartbeatResponse generateHeartbeat(List<TezEvent> events,
      int fromEventId, int maxEvents, int nextFromEventId,
      List<TezEvent> sendEvents) throws IOException, TezException {
    ContainerId containerId = createContainerId(appId, 1);
    Vertex vertex = mock(Vertex.class);

    doReturn(vertex).when(dag).getVertex(vertexID);
    doReturn("test_vertex").when(vertex).getName();
    TaskAttemptEventInfo eventInfo = new TaskAttemptEventInfo(nextFromEventId, sendEvents, 0);
    doReturn(eventInfo).when(vertex).getTaskAttemptTezEvents(taskAttemptID, fromEventId, 0, maxEvents);

    taskAttemptListener.registerRunningContainer(containerId, 0);
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId, 0);

    TaskHeartbeatRequest request = mock(TaskHeartbeatRequest.class);
    doReturn(containerId.toString()).when(request).getContainerIdentifier();
    doReturn(containerId.toString()).when(request).getContainerIdentifier();
    doReturn(taskAttemptID).when(request).getTaskAttemptId();
    doReturn(events).when(request).getEvents();
    doReturn(maxEvents).when(request).getMaxEvents();
    doReturn(fromEventId).when(request).getStartIndex();

    return taskAttemptListener.heartbeat(request);
  }

  @SuppressWarnings("deprecation")
  private ContainerId createContainerId(ApplicationId applicationId, int containerIdx) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    return ContainerId.newInstance(appAttemptId, containerIdx);
  }

  private static class TaskCommunicatorManagerInterfaceImplForTest extends TaskCommunicatorManager {

    public TaskCommunicatorManagerInterfaceImplForTest(AppContext context,
                                                       TaskHeartbeatHandler thh,
                                                       ContainerHeartbeatHandler chh,
                                                       List<NamedEntityDescriptor> taskCommDescriptors) throws TezException {
      super(context, thh, chh, taskCommDescriptors);
    }

    @Override
    TaskCommunicator createDefaultTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) {
      return new TezTaskCommunicatorImplForTest(taskCommunicatorContext);
    }

  }

  private static class TezTaskCommunicatorImplForTest extends TezTaskCommunicatorImpl {

    public TezTaskCommunicatorImplForTest(
        TaskCommunicatorContext taskCommunicatorContext) {
      super(taskCommunicatorContext);
    }

    @Override
    protected void startRpcServer() {
    }

    @Override
    protected void stopRpcServer() {
    }
  }
}

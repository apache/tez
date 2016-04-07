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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptKilled;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.TaskHeartbeatRequest;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestTaskCommunicatorManager2 {

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskAttemptFailedKilled() throws IOException, TezException {

    TaskCommunicatorManagerWrapperForTest wrapper = new TaskCommunicatorManagerWrapperForTest();

    TaskSpec taskSpec1 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask1 = new AMContainerTask(taskSpec1, null, null, false, 10);

    TaskSpec taskSpec2 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec2, null, null, false, 10);

    ContainerId containerId1 = wrapper.createContainerId(1);
    wrapper.registerRunningContainer(containerId1);
    wrapper.registerTaskAttempt(containerId1, amContainerTask1);

    ContainerId containerId2 = wrapper.createContainerId(2);
    wrapper.registerRunningContainer(containerId2);
    wrapper.registerTaskAttempt(containerId2, amContainerTask2);

    wrapper.getTaskCommunicatorManager().taskFailed(amContainerTask1.getTask().getTaskAttemptID(),
        TaskFailureType.NON_FATAL, TaskAttemptEndReason.COMMUNICATION_ERROR, "Diagnostics1");
    wrapper.getTaskCommunicatorManager().taskKilled(amContainerTask2.getTask().getTaskAttemptID(),
        TaskAttemptEndReason.EXECUTOR_BUSY, "Diagnostics2");

    ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(wrapper.getEventHandler(), times(2)).handle(argumentCaptor.capture());
    assertTrue(argumentCaptor.getAllValues().get(0) instanceof TaskAttemptEventAttemptFailed);
    assertTrue(argumentCaptor.getAllValues().get(1) instanceof TaskAttemptEventAttemptKilled);
    TaskAttemptEventAttemptFailed failedEvent =
        (TaskAttemptEventAttemptFailed) argumentCaptor.getAllValues().get(0);
    TaskAttemptEventAttemptKilled killedEvent =
        (TaskAttemptEventAttemptKilled) argumentCaptor.getAllValues().get(1);

    assertEquals("Diagnostics1", failedEvent.getDiagnosticInfo());
    assertEquals(TaskAttemptTerminationCause.COMMUNICATION_ERROR,
        failedEvent.getTerminationCause());

    assertEquals("Diagnostics2", killedEvent.getDiagnosticInfo());
    assertEquals(TaskAttemptTerminationCause.SERVICE_BUSY, killedEvent.getTerminationCause());
//   TODO TEZ-2003. Verify unregistration from the registered list
  }

  // Tests fatal and non fatal
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskAttemptFailureViaHeartbeat() throws IOException, TezException {

    TaskCommunicatorManagerWrapperForTest wrapper = new TaskCommunicatorManagerWrapperForTest();

    TaskSpec taskSpec1 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask1 = new AMContainerTask(taskSpec1, null, null, false, 10);

    TaskSpec taskSpec2 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec2, null, null, false, 10);

    ContainerId containerId1 = wrapper.createContainerId(1);
    wrapper.registerRunningContainer(containerId1);
    wrapper.registerTaskAttempt(containerId1, amContainerTask1);

    ContainerId containerId2 = wrapper.createContainerId(2);
    wrapper.registerRunningContainer(containerId2);
    wrapper.registerTaskAttempt(containerId2, amContainerTask2);

    List<TezEvent> events = new LinkedList<>();

    EventMetaData sourceInfo1 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, "testVertex", null,
            taskSpec1.getTaskAttemptID());
    TaskAttemptFailedEvent failedEvent1 = new TaskAttemptFailedEvent("non-fatal test error",
        TaskFailureType.NON_FATAL);
    TezEvent failedEventT1 = new TezEvent(failedEvent1, sourceInfo1);
    events.add(failedEventT1);
    TaskHeartbeatRequest taskHeartbeatRequest1 =
        new TaskHeartbeatRequest(containerId1.toString(), taskSpec1.getTaskAttemptID(), events, 0,
            0, 0);
    wrapper.getTaskCommunicatorManager().heartbeat(taskHeartbeatRequest1);

    ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(wrapper.getEventHandler(), times(1)).handle(argumentCaptor.capture());
    assertTrue(argumentCaptor.getAllValues().get(0) instanceof TaskAttemptEventAttemptFailed);
    TaskAttemptEventAttemptFailed failedEvent =
        (TaskAttemptEventAttemptFailed) argumentCaptor.getAllValues().get(0);
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());
    assertTrue(failedEvent.getDiagnosticInfo().contains("non-fatal"));

    events.clear();
    reset(wrapper.getEventHandler());

    EventMetaData sourceInfo2 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, "testVertex", null,
            taskSpec2.getTaskAttemptID());
    TaskAttemptFailedEvent failedEvent2 = new TaskAttemptFailedEvent("-fatal- test error",
        TaskFailureType.FATAL);
    TezEvent failedEventT2 = new TezEvent(failedEvent2, sourceInfo2);
    events.add(failedEventT2);
    TaskHeartbeatRequest taskHeartbeatRequest2 =
        new TaskHeartbeatRequest(containerId2.toString(), taskSpec2.getTaskAttemptID(), events, 0,
            0, 0);
    wrapper.getTaskCommunicatorManager().heartbeat(taskHeartbeatRequest2);

    argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(wrapper.getEventHandler(), times(1)).handle(argumentCaptor.capture());
    assertTrue(argumentCaptor.getAllValues().get(0) instanceof TaskAttemptEventAttemptFailed);
    failedEvent = (TaskAttemptEventAttemptFailed) argumentCaptor.getAllValues().get(0);
    assertEquals(TaskFailureType.FATAL, failedEvent.getTaskFailureType());
    assertTrue(failedEvent.getDiagnosticInfo().contains("-fatal-"));
  }

  // Tests fatal and non fatal
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskAttemptFailureViaContext() throws IOException, TezException {
    TaskCommunicatorManagerWrapperForTest wrapper = new TaskCommunicatorManagerWrapperForTest();

    TaskSpec taskSpec1 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask1 = new AMContainerTask(taskSpec1, null, null, false, 10);

    TaskSpec taskSpec2 = wrapper.createTaskSpec();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec2, null, null, false, 10);

    ContainerId containerId1 = wrapper.createContainerId(1);
    wrapper.registerRunningContainer(containerId1);
    wrapper.registerTaskAttempt(containerId1, amContainerTask1);

    ContainerId containerId2 = wrapper.createContainerId(2);
    wrapper.registerRunningContainer(containerId2);
    wrapper.registerTaskAttempt(containerId2, amContainerTask2);


    // non-fatal
    wrapper.getTaskCommunicatorManager()
        .taskFailed(taskSpec1.getTaskAttemptID(), TaskFailureType.NON_FATAL,
            TaskAttemptEndReason.CONTAINER_EXITED, "--non-fatal--");
    ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(wrapper.getEventHandler(), times(1)).handle(argumentCaptor.capture());
    assertTrue(argumentCaptor.getAllValues().get(0) instanceof TaskAttemptEventAttemptFailed);
    TaskAttemptEventAttemptFailed failedEvent =
        (TaskAttemptEventAttemptFailed) argumentCaptor.getAllValues().get(0);
    assertEquals(TaskFailureType.NON_FATAL, failedEvent.getTaskFailureType());
    assertTrue(failedEvent.getDiagnosticInfo().contains("--non-fatal--"));

    reset(wrapper.getEventHandler());

    // fatal
    wrapper.getTaskCommunicatorManager()
        .taskFailed(taskSpec2.getTaskAttemptID(), TaskFailureType.FATAL, TaskAttemptEndReason.OTHER,
            "--fatal--");
    argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(wrapper.getEventHandler(), times(1)).handle(argumentCaptor.capture());
    assertTrue(argumentCaptor.getAllValues().get(0) instanceof TaskAttemptEventAttemptFailed);
    failedEvent = (TaskAttemptEventAttemptFailed) argumentCaptor.getAllValues().get(0);
    assertEquals(TaskFailureType.FATAL, failedEvent.getTaskFailureType());
    assertTrue(failedEvent.getDiagnosticInfo().contains("--fatal--"));
  }

  @SuppressWarnings("unchecked")
  private static class TaskCommunicatorManagerWrapperForTest {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    Credentials credentials = new Credentials();
    AppContext appContext = mock(AppContext.class);
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = mock(DAG.class);
    Vertex vertex = mock(Vertex.class);
    TezDAGID dagId;
    TezVertexID vertexId;
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    Configuration conf = new TezConfiguration();
    UserPayload userPayload;
    TaskCommunicatorManager taskCommunicatorManager;
    private AtomicInteger taskIdCounter = new AtomicInteger(0);

    TaskCommunicatorManagerWrapperForTest() throws IOException, TezException {
      dagId = TezDAGID.getInstance(appId, 1);
      vertexId = TezVertexID.getInstance(dagId, 100);
      doReturn(eventHandler).when(appContext).getEventHandler();
      doReturn(dag).when(appContext).getCurrentDAG();
      doReturn(vertex).when(dag).getVertex(eq(vertexId));
      doReturn(new TaskAttemptEventInfo(0, new LinkedList<TezEvent>(), 0)).when(vertex)
          .getTaskAttemptTezEvents(any(TezTaskAttemptID.class), anyInt(), anyInt(), anyInt());
      doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
      doReturn(credentials).when(appContext).getAppCredentials();
      doReturn(appAcls).when(appContext).getApplicationACLs();
      doReturn(amContainerMap).when(appContext).getAllContainers();
      doReturn(new SystemClock()).when(appContext).getClock();

      NodeId nodeId = NodeId.newInstance("localhost", 0);
      AMContainer amContainer = mock(AMContainer.class);
      Container container = mock(Container.class);
      doReturn(nodeId).when(container).getNodeId();
      doReturn(amContainer).when(amContainerMap).get(any(ContainerId.class));
      doReturn(container).when(amContainer).getContainer();

      userPayload = TezUtils.createUserPayloadFromConf(conf);

      taskCommunicatorManager =
          new TaskCommunicatorManager(appContext, mock(TaskHeartbeatHandler.class),
              mock(ContainerHeartbeatHandler.class), Lists.newArrayList(new NamedEntityDescriptor(
              TezConstants.getTezYarnServicePluginName(), null).setUserPayload(userPayload)));
    }


    TaskCommunicatorManager getTaskCommunicatorManager() {
      return taskCommunicatorManager;
    }

    EventHandler getEventHandler() {
      return eventHandler;
    }

    private void registerRunningContainer(ContainerId containerId) {
      taskCommunicatorManager.registerRunningContainer(containerId, 0);
    }

    private void registerTaskAttempt(ContainerId containerId, AMContainerTask amContainerTask) {
      taskCommunicatorManager.registerTaskAttempt(amContainerTask, containerId, 0);
    }

    private TaskSpec createTaskSpec() {
      TaskSpec taskSpec = mock(TaskSpec.class);
      TezTaskID taskId = TezTaskID.getInstance(vertexId, taskIdCounter.incrementAndGet());
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      doReturn(taskAttemptId).when(taskSpec).getTaskAttemptID();
      return taskSpec;
    }


    @SuppressWarnings("deprecation")
    private ContainerId createContainerId(int id) {
      ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
      ContainerId containerId = ContainerId.newInstance(appAttemptId, id);
      return containerId;
    }

  }
}

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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
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
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestTaskCommunicatorManager2 {

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskAttemptFailedKilled() throws IOException, TezException {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    Credentials credentials = new Credentials();
    AppContext appContext = mock(AppContext.class);
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = mock(DAG.class);
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(credentials).when(appContext).getAppCredentials();
    doReturn(appAcls).when(appContext).getApplicationACLs();
    doReturn(amContainerMap).when(appContext).getAllContainers();
    NodeId nodeId = NodeId.newInstance("localhost", 0);
    AMContainer amContainer = mock(AMContainer.class);
    Container container = mock(Container.class);
    doReturn(nodeId).when(container).getNodeId();
    doReturn(amContainer).when(amContainerMap).get(any(ContainerId.class));
    doReturn(container).when(amContainer).getContainer();

    Configuration conf = new TezConfiguration();
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);
    TaskCommunicatorManager taskAttemptListener =
        new TaskCommunicatorManager(appContext, mock(TaskHeartbeatHandler.class),
            mock(ContainerHeartbeatHandler.class), Lists.newArrayList(new NamedEntityDescriptor(
            TezConstants.getTezYarnServicePluginName(), null).setUserPayload(userPayload)));

    TaskSpec taskSpec1 = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId1 = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId1).when(taskSpec1).getTaskAttemptID();
    AMContainerTask amContainerTask1 = new AMContainerTask(taskSpec1, null, null, false, 10);

    TaskSpec taskSpec2 = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId2 = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId2).when(taskSpec2).getTaskAttemptID();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec2, null, null, false, 10);

    ContainerId containerId1 = createContainerId(appId, 1);
    taskAttemptListener.registerRunningContainer(containerId1, 0);
    taskAttemptListener.registerTaskAttempt(amContainerTask1, containerId1, 0);
    ContainerId containerId2 = createContainerId(appId, 2);
    taskAttemptListener.registerRunningContainer(containerId2, 0);
    taskAttemptListener.registerTaskAttempt(amContainerTask2, containerId2, 0);


    taskAttemptListener
        .taskFailed(taskAttemptId1, TaskAttemptEndReason.COMMUNICATION_ERROR, "Diagnostics1");
    taskAttemptListener
        .taskKilled(taskAttemptId2, TaskAttemptEndReason.EXECUTOR_BUSY, "Diagnostics2");

    ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
    verify(eventHandler, times(2)).handle(argumentCaptor.capture());
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
    // TODO TEZ-2003. Verify unregistration from the registered list
  }

  @SuppressWarnings("deprecation")
  private ContainerId createContainerId(ApplicationId applicationId, int containerIdx) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, containerIdx);
    return containerId;
  }
}

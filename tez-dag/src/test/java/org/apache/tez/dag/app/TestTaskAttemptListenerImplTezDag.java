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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.Test;

public class TestTaskAttemptListenerImplTezDag {

  @Test(timeout = 5000)
  public void testGetTask() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    AppContext appContext = mock(AppContext.class);
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = mock(DAG.class);
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(appAcls).when(appContext).getApplicationACLs();
    doReturn(amContainerMap).when(appContext).getAllContainers();

    TaskAttemptListenerImpTezDag taskAttemptListener =
        new TaskAttemptListenerImplForTest(appContext, mock(TaskHeartbeatHandler.class),
            mock(ContainerHeartbeatHandler.class), null);


    TaskSpec taskSpec = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId).when(taskSpec).getTaskAttemptID();
    AMContainerTask amContainerTask = new AMContainerTask(taskSpec, null, null, false);
    ContainerTask containerTask = null;


    ContainerId containerId1 = createContainerId(appId, 1);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId1);
    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertTrue(containerTask.shouldDie());


    ContainerId containerId2 = createContainerId(appId, 2);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId2);
    ContainerContext containerContext2 = new ContainerContext(containerId2.toString());
    taskAttemptListener.registerRunningContainer(containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertNull(containerTask);

    // Valid task registered
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Task unregistered. Should respond to heartbeats
    taskAttemptListener.unregisterTaskAttempt(taskAttemptId);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertNull(containerTask);

    // Container unregistered. Should send a shouldDie = true
    taskAttemptListener.unregisterRunningContainer(containerId2);
    containerTask = taskAttemptListener.getTask(containerContext2);
    assertTrue(containerTask.shouldDie());

    ContainerId containerId3 = createContainerId(appId, 3);
    ContainerContext containerContext3 = new ContainerContext(containerId3.toString());
    taskAttemptListener.registerRunningContainer(containerId3);

    // Register task to container3, followed by unregistering container 3 all together
    TaskSpec taskSpec2 = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId2 = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId2).when(taskSpec2).getTaskAttemptID();
    AMContainerTask amContainerTask2 = new AMContainerTask(taskSpec, null, null, false);
    taskAttemptListener.registerTaskAttempt(amContainerTask2, containerId3);
    taskAttemptListener.unregisterRunningContainer(containerId3);
    containerTask = taskAttemptListener.getTask(containerContext3);
    assertTrue(containerTask.shouldDie());
  }

  @Test(timeout = 5000)
  public void testGetTaskMultiplePulls() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    AppContext appContext = mock(AppContext.class);
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = mock(DAG.class);
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(appAcls).when(appContext).getApplicationACLs();
    doReturn(amContainerMap).when(appContext).getAllContainers();

    TaskAttemptListenerImpTezDag taskAttemptListener =
        new TaskAttemptListenerImplForTest(appContext, mock(TaskHeartbeatHandler.class),
            mock(ContainerHeartbeatHandler.class), null);


    TaskSpec taskSpec = mock(TaskSpec.class);
    TezTaskAttemptID taskAttemptId = mock(TezTaskAttemptID.class);
    doReturn(taskAttemptId).when(taskSpec).getTaskAttemptID();
    AMContainerTask amContainerTask = new AMContainerTask(taskSpec, null, null, false);
    ContainerTask containerTask = null;


    ContainerId containerId1 = createContainerId(appId, 1);
    doReturn(mock(AMContainer.class)).when(amContainerMap).get(containerId1);
    ContainerContext containerContext1 = new ContainerContext(containerId1.toString());
    taskAttemptListener.registerRunningContainer(containerId1);
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertNull(containerTask);

    // Register task
    taskAttemptListener.registerTaskAttempt(amContainerTask, containerId1);
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertFalse(containerTask.shouldDie());
    assertEquals(taskSpec, containerTask.getTaskSpec());

    // Try pulling again - simulates re-use pull
    containerTask = taskAttemptListener.getTask(containerContext1);
    assertNull(containerTask);
  }

  private ContainerId createContainerId(ApplicationId applicationId, long containerIdLong) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, containerIdLong);
    return containerId;
  }

  private static class TaskAttemptListenerImplForTest extends TaskAttemptListenerImpTezDag {

    public TaskAttemptListenerImplForTest(AppContext context,
                                          TaskHeartbeatHandler thh,
                                          ContainerHeartbeatHandler chh,
                                          JobTokenSecretManager jobTokenSecretManager) {
      super(context, thh, chh, jobTokenSecretManager);
    }

    @Override
    protected void startRpcServer() {
    }

    @Override
    protected void stopRpcServer() {
    }
  }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskCommunicatorContextImpl {

  @Test(timeout = 5000)
  public void testIsKnownContainer() {
    AppContext appContext = mock(AppContext.class);
    when(appContext.getAMConf()).thenReturn(new Configuration());
    TaskCommunicatorManager tal = mock(TaskCommunicatorManager.class);

    AMContainerMap amContainerMap = new AMContainerMap(mock(ContainerHeartbeatHandler.class), tal, mock(
        ContainerSignatureMatcher.class), appContext);

    doReturn(amContainerMap).when(appContext).getAllContainers();

    ContainerId containerId01 = mock(ContainerId.class);
    Container container01 = mock(Container.class);
    doReturn(containerId01).when(container01).getId();

    ContainerId containerId11 = mock(ContainerId.class);
    Container container11 = mock(Container.class);
    doReturn(containerId11).when(container11).getId();

    amContainerMap.addContainerIfNew(container01, 0, 0, 0);
    amContainerMap.addContainerIfNew(container11, 1, 1, 1);

    TaskCommunicatorContext taskCommContext0 = new TaskCommunicatorContextImpl(appContext, tal, null, 0);
    TaskCommunicatorContext taskCommContext1 = new TaskCommunicatorContextImpl(appContext, tal, null, 1);

    assertTrue(taskCommContext0.isKnownContainer(containerId01));
    assertFalse(taskCommContext0.isKnownContainer(containerId11));

    assertFalse(taskCommContext1.isKnownContainer(containerId01));
    assertTrue(taskCommContext1.isKnownContainer(containerId11));

    taskCommContext0.containerAlive(containerId01);
    verify(tal).containerAlive(containerId01);
    reset(tal);

    taskCommContext0.containerAlive(containerId11);
    verify(tal, never()).containerAlive(containerId11);
    reset(tal);

    taskCommContext1.containerAlive(containerId01);
    verify(tal, never()).containerAlive(containerId01);
    reset(tal);

    taskCommContext1.containerAlive(containerId11);
    verify(tal).containerAlive(containerId11);
    reset(tal);

    taskCommContext1.containerAlive(containerId01);
    verify(tal, never()).containerAlive(containerId01);
    reset(tal);
  }

  @Test
  public void testTaskCommContextReachesDAGConf() {
    Configuration conf = new Configuration();
    conf.set("dagkey", "dagvalue");

    DAG dag = mock(DAG.class);
    when(dag.getConf()).thenReturn(conf);

    // TaskCommunicatorContextImpl.dag is present
    TaskCommunicatorContextImpl commContext = new TaskCommunicatorContextImpl(null, null, null, 0);
    commContext.dag = dag;

    Assert.assertEquals("DAG config should be exposed via context.dag.getConf()",
        commContext.getCurrentDagInfo().getConf().get("dagkey"), "dagvalue");

    // TaskCommunicatorContextImpl.appContext.getCurrentDAG() is present
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentDAG()).thenReturn(dag);
    commContext = new TaskCommunicatorContextImpl(appContext, null, null, 0);

    Assert.assertEquals(
        "DAG config should be exposed via context.appContext.getCurrentDAG().getConf()",
        commContext.getCurrentDagInfo().getConf().get("dagkey"), "dagvalue");
  }
}

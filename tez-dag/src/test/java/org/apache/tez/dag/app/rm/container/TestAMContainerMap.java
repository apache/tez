/*
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

package org.apache.tez.dag.app.rm.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.app.TaskCommunicatorWrapper;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.container.TestAMContainer.WrappedContainer;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.junit.Test;
import static org.mockito.Mockito.when;

public class TestAMContainerMap {


  @Test (timeout = 10000)
  public void testCleanupOnDagComplete() {

    ContainerHeartbeatHandler chh = mock(ContainerHeartbeatHandler.class);
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getAMConf()).thenReturn(new Configuration());



    int numContainers = 7;
    WrappedContainer[] wContainers = new WrappedContainer[numContainers];
    for (int i = 0 ; i < numContainers ; i++) {
      WrappedContainer wc =
          new WrappedContainer(false, null, i);
      wContainers[i] = wc;
    }

    AMContainerMap amContainerMap = new AMContainerMapForTest(chh, tal, mock(
        ContainerSignatureMatcher.class), appContext, wContainers);

    for (int i = 0 ; i < numContainers ; i++) {
      amContainerMap.addContainerIfNew(wContainers[i].container, 0, 0, 0);
    }


    // Container 1 in LAUNCHING state
    wContainers[0].launchContainer();
    wContainers[0].verifyState(AMContainerState.LAUNCHING);

    // Container 2 in IDLE state
    wContainers[1].launchContainer();
    wContainers[1].containerLaunched();
    wContainers[1].verifyState(AMContainerState.IDLE);

    // Container 3 RUNNING state
    wContainers[2].launchContainer();
    wContainers[2].containerLaunched();
    wContainers[2].assignTaskAttempt(wContainers[2].taskAttemptID);
    wContainers[2].verifyState(AMContainerState.RUNNING);

    // Cointainer 4 STOP_REQUESTED
    wContainers[3].launchContainer();
    wContainers[3].containerLaunched();
    wContainers[3].stopRequest();
    wContainers[3].verifyState(AMContainerState.STOP_REQUESTED);

    // Container 5 STOPPING
    wContainers[4].launchContainer();
    wContainers[4].containerLaunched();
    wContainers[4].stopRequest();
    wContainers[4].nmStopSent();
    wContainers[4].verifyState(AMContainerState.STOPPING);

    // Container 6 COMPLETED
    wContainers[5].launchContainer();
    wContainers[5].containerLaunched();
    wContainers[5].stopRequest();
    wContainers[5].nmStopSent();
    wContainers[5].containerCompleted();
    wContainers[5].verifyState(AMContainerState.COMPLETED);

    // Container 7 STOP_REQUESTED + ERROR
    wContainers[6].launchContainer();
    wContainers[6].containerLaunched();
    wContainers[6].containerLaunched();
    assertTrue(wContainers[6].amContainer.isInErrorState());
    wContainers[6].verifyState(AMContainerState.STOP_REQUESTED);

    // 7 containers present, and registered with AMContainerMap at this point.

    assertEquals(7, amContainerMap.containerMap.size());
    amContainerMap.dagComplete(mock(DAG.class));
    assertEquals(5, amContainerMap.containerMap.size());
  }

  private static class AMContainerMapForTest extends AMContainerMap {


    private WrappedContainer[] wrappedContainers;

    public AMContainerMapForTest(ContainerHeartbeatHandler chh,
                                 TaskCommunicatorManagerInterface tal,
                                 ContainerSignatureMatcher containerSignatureMatcher,
                                 AppContext context, WrappedContainer[] wrappedContainers) {
      super(chh, tal, containerSignatureMatcher, context);
      this.wrappedContainers = wrappedContainers;
    }

    @Override
    AMContainer createAmContainer(Container container,
                                  ContainerHeartbeatHandler chh,
                                  TaskCommunicatorManagerInterface tal,
                                  ContainerSignatureMatcher signatureMatcher,
                                  AppContext appContext, int schedulerId,
                                  int launcherId, int taskCommId, String auxiliaryService) {
      return wrappedContainers[container.getId().getId()].amContainer;
    }

  }
}

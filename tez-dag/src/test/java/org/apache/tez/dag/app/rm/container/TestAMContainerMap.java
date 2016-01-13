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

package org.apache.tez.dag.app.rm.container;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.serviceplugins.api.ServicePluginException;

public class TestAMContainerMap {

  private ContainerHeartbeatHandler mockContainerHeartBeatHandler() {
    return mock(ContainerHeartbeatHandler.class);
  }

  private TaskCommunicatorManagerInterface mockTaskAttemptListener() throws ServicePluginException {
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);
    TaskCommunicator taskComm = mock(TaskCommunicator.class);
    doReturn(new InetSocketAddress("localhost", 21000)).when(taskComm).getAddress();
    doReturn(taskComm).when(tal).getTaskCommunicator(0);
    return tal;
  }

  private AppContext mockAppContext() {
    AppContext appContext = mock(AppContext.class);
    return appContext;
  }

  @SuppressWarnings("deprecation")
  private ContainerId mockContainerId(int cId) {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, cId);
    return containerId;
  }

  private Container mockContainer(ContainerId containerId) {
    NodeId nodeId = NodeId.newInstance("localhost", 43255);
    Container container = Container.newInstance(containerId, nodeId, "localhost:33333",
        Resource.newInstance(1024, 1), Priority.newInstance(1), mock(Token.class));
    return container;
  }
}

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

package org.apache.tez.dag.app.dag.app;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.TezTaskCommunicatorImpl;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.junit.Test;

public class TestTezTaskCommunicatorManager {

  @Test (timeout = 5000)
  public void testContainerAliveOnGetTask() throws IOException {

    TaskCommunicatorContext context = mock(TaskCommunicatorContext.class);
    Configuration conf = new Configuration(false);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);




    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = createContainerId(appId, 1);

    doReturn(appAttemptId).when(context).getApplicationAttemptId();
    doReturn(userPayload).when(context).getInitialUserPayload();
    doReturn(new Credentials()).when(context).getCredentials();

    TezTaskCommunicatorImpl taskComm = new TezTaskCommunicatorImpl(context);

    ContainerContext containerContext = new ContainerContext(containerId.toString());
    taskComm.registerRunningContainer(containerId, "fakehost", 0);
    ContainerTask containerTask = taskComm.getUmbilical().getTask(containerContext);
    assertNull(containerTask);

    verify(context).containerAlive(containerId);
  }

  @SuppressWarnings("deprecation")
  private ContainerId createContainerId(ApplicationId applicationId, int containerIdx) {
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    return ContainerId.newInstance(appAttemptId, containerIdx);
  }
}

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

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;

public class TaskCommunicatorWrapper {

  private final TaskCommunicator real;

  public TaskCommunicatorWrapper(TaskCommunicator real) {
    this.real = real;
  }


  public void registerRunningContainer(ContainerId containerId, String hostname, int port) throws
      Exception {
    real.registerRunningContainer(containerId, hostname, port);
  }

  public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason,
                                   @Nullable String diagnostics) throws Exception {
    real.registerContainerEnd(containerId, endReason, diagnostics);

  }

  public void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                         Map<String, LocalResource> additionalResources,
                                         Credentials credentials, boolean credentialsChanged,
                                         int priority) throws Exception {
    real.registerRunningTaskAttempt(containerId, taskSpec, additionalResources, credentials, credentialsChanged, priority);
  }

  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID,
                                           TaskAttemptEndReason endReason,
                                           @Nullable String diagnostics) throws Exception {
    real.unregisterRunningTaskAttempt(taskAttemptID, endReason, diagnostics);
  }

  public InetSocketAddress getAddress() throws Exception {
    return real.getAddress();
  }

  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception {
    real.onVertexStateUpdated(stateUpdate);
  }

  public void dagComplete(int dagIdentifier) throws Exception {
    real.dagComplete(dagIdentifier);
  }

  public Object getMetaInfo() throws Exception {
    return real.getMetaInfo();
  }

  public TaskCommunicator getTaskCommunicator() {
    return real;
  }
}

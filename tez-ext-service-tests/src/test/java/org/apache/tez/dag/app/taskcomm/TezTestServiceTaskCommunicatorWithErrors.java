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

package org.apache.tez.dag.app.taskcomm;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;

public class TezTestServiceTaskCommunicatorWithErrors extends TaskCommunicator {
  public TezTestServiceTaskCommunicatorWithErrors(
      TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, String hostname, int port) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason,
                                   @Nullable String diagnostics) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                         Map<String, LocalResource> additionalResources,
                                         Credentials credentials, boolean credentialsChanged,
                                         int priority) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID,
                                           TaskAttemptEndReason endReason,
                                           @Nullable String diagnostics) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public InetSocketAddress getAddress() {
    return NetUtils.createSocketAddrForHost("localhost", 0);
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void dagComplete(int dagIdentifier) {
  }

  @Override
  public Object getMetaInfo() {
    throw new RuntimeException("Simulated Error");
  }
}

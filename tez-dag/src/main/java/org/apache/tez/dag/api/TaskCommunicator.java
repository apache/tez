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

package org.apache.tez.dag.api;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.ServicePluginLifecycle;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

// TODO TEZ-2003 (post) TEZ-2665. Move to the tez-api module
// TODO TEZ-2003 (post) TEZ-2664. Ideally, don't expose YARN containerId; instead expose a Tez specific construct.
public abstract class TaskCommunicator implements ServicePluginLifecycle {

  // TODO TEZ-2003 (post) TEZ-2666 Enhancements to interface
  // - registerContainerEnd should provide the end reason / possible rename
  // - get rid of getAddress
  // - Add methods to support task preemption
  // - Add a dagStarted notification, along with a payload
  // - taskSpec breakup into a clean interface
  // - Add methods to report task / container completion

  private final TaskCommunicatorContext taskCommunicatorContext;

  public TaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) {
    this.taskCommunicatorContext = taskCommunicatorContext;
  }

  public TaskCommunicatorContext getContext() {
    return taskCommunicatorContext;
  }

  @Override
  public void initialize() throws Exception {
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void shutdown() throws Exception {
  }


  public abstract void registerRunningContainer(ContainerId containerId, String hostname, int port);

  public abstract void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason);

  public abstract void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                                  Map<String, LocalResource> additionalResources,
                                                  Credentials credentials,
                                                  boolean credentialsChanged, int priority);

  public abstract void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID, TaskAttemptEndReason endReason);

  public abstract InetSocketAddress getAddress();

  /**
   * Receive notifications on vertex state changes.
   * <p/>
   * State changes will be received based on the registration via {@link
   * org.apache.tez.runtime.api.InputInitializerContext#registerForVertexStateUpdates(String,
   * java.util.Set)}. Notifications will be received for all registered state changes, and not just
   * for the latest state update. They will be in order in which the state change occurred. </p>
   *
   * Extensive processing should not be performed via this method call. Instead this should just be
   * used as a notification mechanism.
   * <br>This method may be invoked concurrently with other invocations into the TaskCommunicator and
   * multi-threading/concurrency implications must be considered.
   * @param stateUpdate an event indicating the name of the vertex, and it's updated state.
   *                    Additional information may be available for specific events, Look at the
   *                    type hierarchy for {@link org.apache.tez.dag.api.event.VertexStateUpdate}
   * @throws Exception
   */
  public abstract void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception;

  /**
   * Indicates the current running dag is complete. The TaskCommunicatorContext can be used to
   * query information about the current dag during the duration of the dagComplete invocation.
   *
   * After this, the contents returned from querying the context may change at any point - due to
   * the next dag being submitted.
   */
  public abstract void dagComplete(String dagName);

  /**
   * Share meta-information such as host:port information where the Task Communicator may be listening.
   * Primarily for use by compatible launchers to learn this information.
   * @return
   */
  public abstract Object getMetaInfo();
}

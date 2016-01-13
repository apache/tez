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

package org.apache.tez.serviceplugins.api;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.ServicePluginLifecycle;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

// TODO TEZ-2003 (post) TEZ-2665. Move to the tez-api module
// TODO TEZ-2003 (post) TEZ-2664. Ideally, don't expose YARN containerId; instead expose a Tez specific construct.

/**
 * This class represents the API for a custom TaskCommunicator which can be run within the Tez AM.
 * This is used to communicate with running services, potentially launching tasks, and getting
 * updates from running tasks.
 * <p/>
 * The plugin is initialized with an instance of {@link TaskCommunicatorContext} - which provides
 * a mechanism to notify the system about allocation decisions and resources to the Tez framework.
 *
 * If setting up a heartbeat between the task and the AM, the framework is responsible for error checking
 * of this heartbeat mechanism, handling lost or duplicate responses.
 *
 */
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

  /**
   * Get the {@link TaskCommunicatorContext} associated with this instance of the scheduler, which
   * is
   * used to communicate with the rest of the system
   *
   * @return an instance of {@link TaskCommunicatorContext}
   */
  public TaskCommunicatorContext getContext() {
    return taskCommunicatorContext;
  }

  /**
   * An entry point for initialization.
   * Order of service setup. Constructor, initialize(), start() - when starting a service.
   *
   * @throws Exception
   */
  @Override
  public void initialize() throws Exception {
  }

  /**
   * An entry point for starting the service.
   * Order of service setup. Constructor, initialize(), start() - when starting a service.
   *
   * @throws Exception
   */
  @Override
  public void start() throws Exception {
  }

  /**
   * Stop the service. This could be invoked at any point, when the service is no longer required -
   * including in case of errors.
   *
   * @throws Exception
   */
  @Override
  public void shutdown() throws Exception {
  }


  /**
   * Register a new container.
   *
   * @param containerId the associated containerId
   * @param hostname    the hostname on which the container runs
   * @param port        the port for the service which is running the container
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void registerRunningContainer(ContainerId containerId, String hostname,
                                                int port) throws ServicePluginException;

  /**
   * Register the end of a container. This can be caused by preemption, the container completing
   * successfully, etc.
   *
   * @param containerId the associated containerId
   * @param endReason   the end reason for the container completing
   * @param diagnostics diagnostics associated with the container end
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason,
                                            @Nullable String diagnostics) throws
      ServicePluginException;

  /**
   * Register a task attempt to execute on a container
   *
   * @param containerId         the containerId on which this task needs to run
   * @param taskSpec            the task specifications for the task to be executed
   * @param additionalResources additional local resources which may be required to run this task
   *                            on
   *                            the container
   * @param credentials         the credentials required to run this task
   * @param credentialsChanged  whether the credentials are different from the original credentials
   *                            associated with this container
   * @param priority            the priority of the task being executed
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                                  Map<String, LocalResource> additionalResources,
                                                  Credentials credentials,
                                                  boolean credentialsChanged, int priority) throws
      ServicePluginException;

  /**
   * Register the completion of a task. This may be a result of preemption, the container dying,
   * the node dying, the task completing to success
   *
   * @param taskAttemptID the task attempt which has completed / needs to be completed
   * @param endReason     the endReason for the task attempt.
   * @param diagnostics   diagnostics associated with the task end
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID,
                                                    TaskAttemptEndReason endReason,
                                                    @Nullable String diagnostics) throws
      ServicePluginException;

  /**
   * Return the address, if any, that the service listens on
   *
   * @return the address
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract InetSocketAddress getAddress() throws ServicePluginException;

  /**
   * Receive notifications on vertex state changes.
   * <p/>
   * State changes will be received based on the registration via {@link
   * org.apache.tez.runtime.api.InputInitializerContext#registerForVertexStateUpdates(String,
   * java.util.Set)}. Notifications will be received for all registered state changes, and not just
   * for the latest state update. They will be in order in which the state change occurred. </p>
   * <p/>
   * Extensive processing should not be performed via this method call. Instead this should just be
   * used as a notification mechanism.
   * <br>This method may be invoked concurrently with other invocations into the TaskCommunicator
   * and
   * multi-threading/concurrency implications must be considered.
   *
   * @param stateUpdate an event indicating the name of the vertex, and it's updated state.
   *                    Additional information may be available for specific events, Look at the
   *                    type hierarchy for {@link org.apache.tez.dag.api.event.VertexStateUpdate}
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws ServicePluginException;

  /**
   * Indicates the current running dag is complete. The TaskCommunicatorContext can be used to
   * query information about the current dag during the duration of the dagComplete invocation.
   * <p/>
   * After this, the contents returned from querying the context may change at any point - due to
   * the next dag being submitted.
   *
   * @param dagIdentifier the unique numerical identifier for the DAG in the specified execution context.
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void dagComplete(int dagIdentifier) throws ServicePluginException;

  /**
   * Share meta-information such as host:port information where the Task Communicator may be
   * listening.
   * Primarily for use by compatible launchers to learn this information.
   *
   * @return meta info for the task communicator
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract Object getMetaInfo() throws ServicePluginException;
}

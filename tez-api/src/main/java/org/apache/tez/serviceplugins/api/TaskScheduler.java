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

package org.apache.tez.serviceplugins.api;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ServicePluginLifecycle;

/**
 * This class represents the API for a custom TaskScheduler which can be run within the Tez AM.
 * This can be used to source resources from different sources, as well as control the logic of
 * how these resources get allocated to the different tasks within a DAG which needs resources.
 * <p/>
 * The plugin is initialized with an instance of {@link TaskSchedulerContext} - which provides
 * a mechanism to notify the system about allocation decisions and resources to the Tez framework.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class TaskScheduler implements ServicePluginLifecycle {

  // TODO TEZ-2003 (post) TEZ-2668
  // - Should setRegister / unregister be part of APIs when not YARN specific ?
  // - Include vertex / task information in the request so that the scheduler can make decisions
  // around prioritizing tasks in the same vertex when others exist at the same priority.
  // There should be an interface around Object task - if it's meant to be used for equals / hashCode.

  private final TaskSchedulerContext taskSchedulerContext;

  public TaskScheduler(TaskSchedulerContext taskSchedulerContext) {
    this.taskSchedulerContext = taskSchedulerContext;
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
   * The first step of stopping the task scheduler service. This would typically be used to stop
   * allocating new resources. shutdown() will typically be used to unregister from external
   * services - especially YARN for instance, so that the app is not killed
   */
  public void initiateStop() {
  }

  /**
   * Get the {@link TaskSchedulerContext} associated with this instance of the scheduler, which is
   * used to communicate with the rest of the system
   *
   * @return an instance of {@link TaskSchedulerContext}
   */
  public final TaskSchedulerContext getContext() {
    return taskSchedulerContext;
  }

  /**
   * Get the currently available resources from this source
   *
   * @return the resources available at the time of invocation
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract Resource getAvailableResources() throws ServicePluginException;

  /**
   * Get the total available resources from this source
   *
   * @return the total available resources from the source
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract Resource getTotalResources() throws ServicePluginException;

  /**
   * Get the number of nodes available from the source
   *
   * @return the number of nodes
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract int getClusterNodeCount() throws ServicePluginException;

  /**
   * Indication to a source that a node has been blacklisted, and should not be used for subsequent
   * allocations.
   *
   * @param nodeId te nodeId to be blacklisted
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void blacklistNode(NodeId nodeId) throws ServicePluginException;

  /**
   * Indication to a source that a node has been un-blacklisted, and can be used from subsequent
   * allocations
   *
   * @param nodeId the nodeId to be unblacklisted
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void unblacklistNode(NodeId nodeId) throws ServicePluginException;

  /**
   * A request to the source to allocate resources for a requesting task, with location information
   * optionally specified
   *
   * @param task               the task for which resources are being accepted.
   * @param capability         the required resources to run this task
   * @param hosts              the preferred host locations for the task
   * @param racks              the preferred rack locations for the task
   * @param priority           the priority of the request for this allocation. A lower value
   *                           implies a higher priority
   * @param containerSignature the specifications for the container (environment, etc) which will
   *                           be
   *                           used for this task - if applicable
   * @param clientCookie       a cookie associated with this request. This should be returned back
   *                           via the {@link TaskSchedulerContext#taskAllocated(Object, Object,
   *                           Container)} method when a task is assigned to a resource
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void allocateTask(Object task, Resource capability,
                                    String[] hosts, String[] racks, Priority priority,
                                    Object containerSignature, Object clientCookie) throws ServicePluginException;

  /**
   * A request to the source to allocate resources for a requesting task, based on a previously used
   * container
   *
   * @param task               the task for which resources are being accepted.
   * @param capability         the required resources to run this task
   * @param containerId        a previous container which is used as an indication as to where this
   *                           task should be placed
   * @param priority           the priority of the request for this allocation. A lower value
   *                           implies a higher priority
   * @param containerSignature the specifications for the container (environment, etc) which will
   *                           be
   *                           used for this task - if applicable
   * @param clientCookie       a cookie associated with this request. This should be returned back
   *                           via the {@link TaskSchedulerContext#taskAllocated(Object, Object,
   *                           Container)} method when a task is assigned to a resource
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void allocateTask(Object task, Resource capability,
                                    ContainerId containerId, Priority priority,
                                    Object containerSignature,
                                    Object clientCookie) throws ServicePluginException;

  /**
   * A request to deallocate a task. This is typically a result of a task completing - with success
   * or failure. It could also be the result of a decision to not run the task, before it is
   * allocated or started.
   * <p/>
   * Plugin writers need to de-allocate containers via the context once it's no longer required, for
   * correct book-keeping
   *
   * @param task          the task being de-allocated.
   * @param taskSucceeded whether the task succeeded or not
   * @param endReason     the reason for the task failure
   * @param diagnostics   additional diagnostics information which may be relevant
   * @return true if the task was associated with a container, false if the task was not associated
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   * with a container
   */
  public abstract boolean deallocateTask(Object task, boolean taskSucceeded,
                                         TaskAttemptEndReason endReason,
                                         @Nullable String diagnostics) throws ServicePluginException;

  /**
   * A request to de-allocate a previously allocated container.
   *
   * @param containerId the containerId to de-allocate
   * @return the task which was previously associated with this container, null otherwise
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract Object deallocateContainer(ContainerId containerId) throws ServicePluginException;

  /**
   * Inform the scheduler that it should unregister. This is primarily valid for schedulers which
   * require registration (YARN a.t.m)
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void setShouldUnregister() throws ServicePluginException;

  /**
   * Checks with the scheduler whether it has unregistered.
   *
   * @return true if the scheduler has unregistered. False otherwise.
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract boolean hasUnregistered() throws ServicePluginException;

  /**
   * Indicates to the scheduler that the currently running dag has completed.
   * This can be used to reset dag specific statistics, potentially release resources and prepare
   * for a new DAG.
   * @throws ServicePluginException when the service runs into a fatal error which it cannot handle.
   *                               This will cause the app to shutdown.
   */
  public abstract void dagComplete() throws ServicePluginException;

}

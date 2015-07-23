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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.ServicePluginLifecycle;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class TaskScheduler implements ServicePluginLifecycle {

  private final TaskSchedulerContext taskSchedulerContext;

  public TaskScheduler(TaskSchedulerContext taskSchedulerContext) {
    this.taskSchedulerContext = taskSchedulerContext;
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

  public void initiateStop() {
  }

  public abstract Resource getAvailableResources();

  public abstract int getClusterNodeCount();

  public abstract void dagComplete();

  public abstract Resource getTotalResources();

  public abstract void blacklistNode(NodeId nodeId);

  public abstract void unblacklistNode(NodeId nodeId);

  public abstract void allocateTask(Object task, Resource capability,
                                    String[] hosts, String[] racks, Priority priority,
                                    Object containerSignature, Object clientCookie);

  /**
   * Allocate affinitized to a specific container
   */
  public abstract void allocateTask(Object task, Resource capability,
                                    ContainerId containerId, Priority priority, Object containerSignature,
                                    Object clientCookie);

  /** Plugin writers must ensure to de-allocate a container once it's done, so that it can be collected. */
  public abstract boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason);

  public abstract Object deallocateContainer(ContainerId containerId);

  public abstract void setShouldUnregister();

  public abstract boolean hasUnregistered();


  public final TaskSchedulerContext getContext() {
    return taskSchedulerContext;
  }
}

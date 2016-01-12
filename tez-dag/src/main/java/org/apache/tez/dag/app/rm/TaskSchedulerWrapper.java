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

package org.apache.tez.dag.app.rm;

import javax.annotation.Nullable;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;

public class TaskSchedulerWrapper {

  private final TaskScheduler real;

  public TaskSchedulerWrapper(TaskScheduler real) {
    this.real = real;
  }

  public Resource getAvailableResources() throws Exception {
    return real.getAvailableResources();
  }

  public Resource getTotalResources() throws Exception {
    return real.getTotalResources();
  }

  public int getClusterNodeCount() throws Exception {
    return real.getClusterNodeCount();
  }

  public void blacklistNode(NodeId nodeId) throws Exception {
    real.blacklistNode(nodeId);
  }

  public void unblacklistNode(NodeId nodeId) throws Exception {
    real.unblacklistNode(nodeId);
  }

  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                           Priority priority, Object containerSignature, Object clientCookie) throws
      Exception {
    real.allocateTask(task, capability, hosts, racks, priority, containerSignature, clientCookie);
  }

  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) throws
      Exception {
    real.allocateTask(task, capability, containerId, priority, containerSignature, clientCookie);
  }

  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason,
                                @Nullable String diagnostics) throws Exception {
    return real.deallocateTask(task, taskSucceeded, endReason, diagnostics);
  }

  public Object deallocateContainer(ContainerId containerId) throws Exception {
    return real.deallocateContainer(containerId);
  }

  public void setShouldUnregister() throws Exception {
    real.setShouldUnregister();
  }

  public boolean hasUnregistered() throws Exception {
    return real.hasUnregistered();
  }

  public void dagComplete() throws Exception {
    real.dagComplete();
  }

  public TaskScheduler getTaskScheduler() {
    return real;
  }
}

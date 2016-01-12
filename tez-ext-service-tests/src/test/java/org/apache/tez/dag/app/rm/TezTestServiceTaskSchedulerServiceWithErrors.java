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
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

public class TezTestServiceTaskSchedulerServiceWithErrors extends TaskScheduler {
  public TezTestServiceTaskSchedulerServiceWithErrors(
      TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
  }

  @Override
  public Resource getAvailableResources() {
    return Resource.newInstance(2048, 2);
  }

  @Override
  public Resource getTotalResources() {
    return Resource.newInstance(2048, 2);
  }

  @Override
  public int getClusterNodeCount() {
    return 1;
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks,
                           Priority priority, Object containerSignature, Object clientCookie) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId,
                           Priority priority, Object containerSignature, Object clientCookie) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason,
                                @Nullable String diagnostics) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    throw new RuntimeException("Simulated Error");
  }

  @Override
  public void setShouldUnregister() {
  }

  @Override
  public boolean hasUnregistered() {
    return false;
  }

  @Override
  public void dagComplete() {
  }
}

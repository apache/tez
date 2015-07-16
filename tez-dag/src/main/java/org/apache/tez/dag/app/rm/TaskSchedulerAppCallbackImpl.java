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

package org.apache.tez.dag.app.rm;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;

public class TaskSchedulerAppCallbackImpl implements TaskSchedulerService.TaskSchedulerAppCallback{

  private final TaskSchedulerEventHandler tseh;
  private final int schedulerId;

  public TaskSchedulerAppCallbackImpl(TaskSchedulerEventHandler tseh, int schedulerId) {
    this.tseh = tseh;
    this.schedulerId = schedulerId;
  }

  @Override
  public void taskAllocated(Object task, Object appCookie, Container container) {
    tseh.taskAllocated(schedulerId, task, appCookie, container);
  }

  @Override
  public void containerCompleted(Object taskLastAllocated, ContainerStatus containerStatus) {
    tseh.containerCompleted(schedulerId, taskLastAllocated, containerStatus);
  }

  @Override
  public void containerBeingReleased(ContainerId containerId) {
    tseh.containerBeingReleased(schedulerId, containerId);
  }

  @Override
  public void nodesUpdated(List<NodeReport> updatedNodes) {
    tseh.nodesUpdated(schedulerId, updatedNodes);
  }

  @Override
  public void appShutdownRequested() {
    tseh.appShutdownRequested(schedulerId);
  }

  @Override
  public void setApplicationRegistrationData(Resource maxContainerCapability,
                                             Map<ApplicationAccessType, String> appAcls,
                                             ByteBuffer clientAMSecretKey) {
    tseh.setApplicationRegistrationData(schedulerId, maxContainerCapability, appAcls, clientAMSecretKey);
  }

  @Override
  public void onError(Throwable t) {
    tseh.onError(schedulerId, t);
  }

  @Override
  public float getProgress() {
    return tseh.getProgress(schedulerId);
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    tseh.preemptContainer(schedulerId, containerId);
  }

  @Override
  public AppFinalStatus getFinalAppStatus() {
    return tseh.getFinalAppStatus();
  }
}

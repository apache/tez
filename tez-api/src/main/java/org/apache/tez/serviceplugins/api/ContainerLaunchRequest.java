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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * Contains specifications for a container which needs to be launched
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ContainerLaunchRequest extends ContainerLauncherOperationBase {

  private final ContainerLaunchContext clc;
  private final Container container;
  private final String schedulerName;
  private final String taskCommName;

  public ContainerLaunchRequest(NodeId nodeId,
                                ContainerId containerId,
                                Token containerToken,
                                ContainerLaunchContext clc,
                                Container container, String schedulerName, String taskCommName) {
    super(nodeId, containerId, containerToken);
    this.clc = clc;
    this.container = container;
    this.schedulerName = schedulerName;
    this.taskCommName = taskCommName;
  }


  // TODO Post TEZ-2003. TEZ-2625. ContainerLaunchContext needs to be built here instead of being passed in.
  // Basic specifications need to be provided here
  /**
   * The {@link ContainerLauncherContext} for the container being launched
   * @return the container launch context for the launch request
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return clc;
  }

  /**
   * Get the name of the task communicator which will be used to communicate
   * with the task that will run in this container.
   * @return the task communicator to be used for this request
   */
  public String getTaskCommunicatorName() {
    return taskCommName;
  }

  /**
   * Get the name of the scheduler which allocated this container.
   * @return the scheduler name which provided the container
   */
  public String getSchedulerName() {
    return schedulerName;
  }

  @Override
  public String toString() {
    return "ContainerLaunchRequest{" +
        "nodeId=" + getNodeId() +
        ", containerId=" + getContainerId() +
        ", schedulerName='" + schedulerName + '\'' +
        ", taskCommName='" + taskCommName + '\'' +
        '}';
  }
}

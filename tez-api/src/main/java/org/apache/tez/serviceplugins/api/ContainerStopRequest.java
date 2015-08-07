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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * Contains specifications for a container which needs to be stopped
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ContainerStopRequest extends ContainerLauncherOperationBase {

  private final String schedulerName;
  private final String taskCommName;

  public ContainerStopRequest(NodeId nodeId,
                              ContainerId containerId,
                              Token containerToken, String schedulerName, String taskCommName) {
    super(nodeId, containerId, containerToken);
    this.schedulerName = schedulerName;
    this.taskCommName = taskCommName;
  }

  @Override
  public String toString() {
    return "ContainerStopRequest{" +
        "nodeId=" + getNodeId() +
        ", containerId=" + getContainerId() +
        ", schedulerName='" + schedulerName + '\'' +
        ", taskCommName='" + taskCommName + '\'' +
        '}';
  }
}

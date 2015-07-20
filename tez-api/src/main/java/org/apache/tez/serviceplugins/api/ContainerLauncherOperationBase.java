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

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ContainerLauncherOperationBase {

  private final NodeId nodeId;
  private final ContainerId containerId;
  private final Token containerToken;

  public ContainerLauncherOperationBase(NodeId nodeId,
                                        ContainerId containerId,
                                        Token containerToken) {
    this.nodeId = nodeId;
    this.containerId = containerId;
    this.containerToken = containerToken;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Token getContainerToken() {
    return containerToken;
  }

  @Override
  public String toString() {
    return "ContainerLauncherOperationBase{" +
        "nodeId=" + nodeId +
        ", containerId=" + containerId +
        '}';
  }
}

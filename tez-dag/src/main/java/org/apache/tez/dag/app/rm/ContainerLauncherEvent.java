/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {

  private final ContainerId containerId;
  private final NodeId nodeId;
  private final Token containerToken;
  private final int launcherId;
  private final int schedulerId;
  private final int taskCommId;

  public ContainerLauncherEvent(ContainerId containerId, NodeId nodeId,
                                Token containerToken, ContainerLauncherEventType type,
                                int launcherId,
                                int schedulerId, int taskCommId) {
    super(type);
    this.containerId = containerId;
    this.nodeId = nodeId;
    this.containerToken = containerToken;
    this.launcherId = launcherId;
    this.schedulerId = schedulerId;
    this.taskCommId = taskCommId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }

  public Token getContainerToken() {
    return this.containerToken;
  }

  public int getLauncherId() {
    return launcherId;
  }

  public int getSchedulerId() {
    return schedulerId;
  }

  public int getTaskCommId() {
    return taskCommId;
  }

  public String toSrting() {
    return super.toString() + " for container " + containerId + ", nodeId: "
        + nodeId + ", launcherId: " + launcherId + ", schedulerId=" + schedulerId +
        ", taskCommId=" + taskCommId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((containerId == null) ? 0 : containerId.hashCode());
    result = prime * result
        + ((containerToken == null) ? 0 : containerToken.hashCode());
    result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerLauncherEvent other = (ContainerLauncherEvent) obj;
    if (containerId == null) {
      if (other.containerId != null)
        return false;
    } else if (!containerId.equals(other.containerId))
      return false;
    if (containerToken == null) {
      if (other.containerToken != null)
        return false;
    } else if (!containerToken.equals(other.containerToken))
      return false;
    if (nodeId == null) {
      if (other.nodeId != null)
        return false;
    } else if (!nodeId.equals(other.nodeId))
      return false;
    return true;
  }
}

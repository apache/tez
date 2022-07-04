/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public class ContainerLauncherLaunchRequestEvent extends ContainerLauncherEvent {

  private final ContainerLaunchContext clc;
  private final Container container;
  // The task communicator index for the specific container being launched.

  public ContainerLauncherLaunchRequestEvent(ContainerLaunchContext clc,
                                             Container container, int launcherId, int schedulerId,
                                             int taskCommId) {
    super(container.getId(), container.getNodeId(), container
            .getContainerToken(), ContainerLauncherEventType.CONTAINER_LAUNCH_REQUEST,
        launcherId, schedulerId, taskCommId);
    this.clc = clc;
    this.container = container;
  }

  public ContainerLaunchContext getContainerLaunchContext() {
    return this.clc;
  }

  public Container getContainer() {
    return container;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ContainerLauncherLaunchRequestEvent that = (ContainerLauncherLaunchRequestEvent) o;

    if (clc != null ? !clc.equals(that.clc) : that.clc != null) {
      return false;
    }
    if (container != null ? !container.equals(that.container) : that.container != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 7001 * result + (clc != null ? clc.hashCode() : 0);
    result = 7001 * result + (container != null ? container.hashCode() : 0);
    return result;
  }
}

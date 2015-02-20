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

package org.apache.tez.dag.app.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;

public class AMContainerEventLaunchRequest extends AMContainerEvent {

  private final TezVertexID vertexId;
  private final ContainerContext containerContext;
  private final int launcherId;
  private final int taskCommId;

  public AMContainerEventLaunchRequest(ContainerId containerId,
      TezVertexID vertexId, ContainerContext containerContext,
      int launcherId, int taskCommId) {
    super(containerId, AMContainerEventType.C_LAUNCH_REQUEST);
    this.vertexId = vertexId;
    this.containerContext = containerContext;
    this.launcherId = launcherId;
    this.taskCommId = taskCommId;
  }

  public TezDAGID getDAGId() {
    return this.vertexId.getDAGId();
  }

  public TezVertexID getVertexId() {
    return this.vertexId;
  }

  public ContainerContext getContainerContext() {
    return this.containerContext;
  }

  public int getLauncherId() {
    return launcherId;
  }

  public int getTaskCommId() {
    return taskCommId;
  }
}

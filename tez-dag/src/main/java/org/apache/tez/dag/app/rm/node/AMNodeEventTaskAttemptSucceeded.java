/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app.rm.node;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class AMNodeEventTaskAttemptSucceeded extends AMNodeEvent {

  // TODO These two parameters really aren't required in this event.
  private final ContainerId containerId;
  private final TezTaskAttemptID taskAttemptId;

  public AMNodeEventTaskAttemptSucceeded(NodeId nodeId, int sourceId,
      ContainerId containerId, TezTaskAttemptID taskAttemptId) {
    super(nodeId, sourceId, AMNodeEventType.N_TA_SUCCEEDED);
    this.containerId = containerId;
    this.taskAttemptId = taskAttemptId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public TezTaskAttemptID getTaskAttemptId() {
    return this.taskAttemptId;
  }

}

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

package org.apache.tez.dag.app.rm.node;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class AMNodeEvent extends AbstractEvent<AMNodeEventType> {

  private final NodeId nodeId;
  private final int schedulerId;
  private final ExtendedNodeId amNodeId;

  public AMNodeEvent(NodeId nodeId, int schedulerId, AMNodeEventType type) {
    super(type);
    this.nodeId = nodeId;
    this.schedulerId = schedulerId;
    this.amNodeId = null;
  }

  public AMNodeEvent(ExtendedNodeId amNodeId, int schedulerId, AMNodeEventType type) {
    super(type);
    this.nodeId = null;
    this.schedulerId = schedulerId;
    this.amNodeId = amNodeId;
  }

  public NodeId getNodeId() {
    return amNodeId == null ? this.nodeId : this.amNodeId;
  }

  public int getSchedulerId() {
    return schedulerId;
  }
}

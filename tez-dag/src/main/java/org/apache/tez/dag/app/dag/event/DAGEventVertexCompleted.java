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

package org.apache.tez.dag.app.dag.event;

import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.engine.records.TezVertexID;

public class DAGEventVertexCompleted extends DAGEvent {

  private TezVertexID vertexId;
  private VertexState vertexState;

  public DAGEventVertexCompleted(TezVertexID vertexId, VertexState vertexState) {
    super(vertexId.getDAGId(), DAGEventType.DAG_VERTEX_COMPLETED);
    this.vertexId = vertexId;
    this.vertexState = vertexState;
  }

  public TezVertexID getVertexId() {
    return vertexId;
  }

  public VertexState getVertexState() {
    return vertexState;
  }
}

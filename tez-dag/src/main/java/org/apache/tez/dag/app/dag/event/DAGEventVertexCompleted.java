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
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.records.TezVertexID;

public class DAGEventVertexCompleted extends DAGEvent {

  private final TezVertexID vertexId;
  private final VertexState vertexState;
  private final VertexTerminationCause terminationCause;

  public DAGEventVertexCompleted(TezVertexID vertexId, VertexState vertexState) {
    super(vertexId.getDAGID(), DAGEventType.DAG_VERTEX_COMPLETED);
    this.vertexId = vertexId;
    this.vertexState = vertexState;
    this.terminationCause = null;
  }

  public DAGEventVertexCompleted(TezVertexID vertexId, VertexState vertexState,
      VertexTerminationCause terminationCause) {
    super(vertexId.getDAGID(), DAGEventType.DAG_VERTEX_COMPLETED);
    this.vertexId = vertexId;
    this.vertexState = vertexState;
    this.terminationCause = terminationCause;
  }

  public TezVertexID getVertexId() {
    return vertexId;
  }

  public VertexState getVertexState() {
    return vertexState;
  }

  public VertexTerminationCause getVertexTerminationCause() {
    return this.terminationCause;
  }
}

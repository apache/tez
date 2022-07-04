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

package org.apache.tez.dag.app.dag.event;

import org.apache.tez.dag.app.dag.Vertex;

public class VertexShuffleDataDeletion extends VertexEvent {
  // child vertex
  private Vertex sourceVertex;
  // parent vertex
  private Vertex targetVertex;

  public VertexShuffleDataDeletion(Vertex sourceVertex, Vertex targetVertex) {
    super(targetVertex.getVertexId(), VertexEventType.V_DELETE_SHUFFLE_DATA);
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  public Vertex getSourceVertex() {
    return sourceVertex;
  }

  public Vertex getTargetVertex() {
    return targetVertex;
  }
}

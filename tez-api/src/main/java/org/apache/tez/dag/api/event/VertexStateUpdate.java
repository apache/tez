/*
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

package org.apache.tez.dag.api.event;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Updates that are sent to user code running within the AM, on Vertex state changes.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class VertexStateUpdate {

  private final String vertexName;
  private final VertexState vertexState;


  public VertexStateUpdate(String vertexName, VertexState vertexState) {
    this.vertexName = vertexName;
    this.vertexState = vertexState;
  }

  /**
   * Get the name of the vertex for which the state has changed
   * @return the name of the vertex
   */
  public String getVertexName() {
    return vertexName;
  }

  /**
   * Get the updated state
   * @return the updated state
   */
  public VertexState getVertexState() {
    return vertexState;
  }

  @Override
  public String toString() {
    return "VertexStateUpdate: vertexName=" + vertexName + ", State=" + vertexState;
  }
}

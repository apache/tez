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

/**
 * An event that is sent out when the parallelism of a vertex changes.
 */
public class VertexStateUpdateParallelismUpdated extends VertexStateUpdate {

  private final int parallelism;
  private final int previousParallelism;

  public VertexStateUpdateParallelismUpdated(String vertexName,
                                             int updatedParallelism, int previousParallelism) {
    super(vertexName, VertexState.PARALLELISM_UPDATED);
    this.parallelism = updatedParallelism;
    this.previousParallelism = previousParallelism;
  }

  /**
   * Returns the new parallelism for the vertex
   *
   * @return the new parallelism
   */
  public int getParallelism() {
    return parallelism;
  }

  /**
   * Returns the previous value of the parallelism
   *
   * @return the previous parallelism
   */
  public int getPreviousParallelism() {
    return previousParallelism;
  }
}

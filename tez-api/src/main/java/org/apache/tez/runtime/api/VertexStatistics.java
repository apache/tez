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

package org.apache.tez.runtime.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.Vertex;

/**
 * Provides various statistics about the physical execution of this
 * {@link Vertex}<br>
 * This only provides point in time values for the statistics and values are
 * refreshed based on when the implementations of the inputs/outputs/tasks etc.
 * update their reported statistics. The values may increase or decrease based
 * on task completions or failures.
 */
@Public
@Evolving
public interface VertexStatistics {

  /**
   * Get statistics about an {@link Edge} input or external input of this
   * {@link Vertex}. <br>
   * 
   * @param inputName
   *          Name of the input {@link Edge} or external input of this vertex
   * @return {@link InputStatistics} for the given input
   */
  public InputStatistics getInputStatistics(String inputName);

  /**
   * Get statistics about an {@link Edge} output or external output of this
   * {@link Vertex}. <br>
   * 
   * @param outputName
   *          Name of the output {@link Edge} or external output of this vertex
   * @return {@link OutputStatistics} for the given output
   */
  public OutputStatistics getOutputStatistics(String outputName);

}

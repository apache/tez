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
import org.apache.tez.dag.api.Vertex;

/**
 * Provides various statistics about physical execution activity happening on a
 * logical output in a {@link Vertex}. Outputs can be external outputs or
 * outputs to other vertices.
 */
@Public
@Evolving
public interface OutputStatistics {
  
  /**
   * Returns the data size associated with this logical output
   * <br>It is the size of the data written to this output by the vertex.
   * @return Data size in bytes
   */
 public long getDataSize();
 
 /**
   * Get the numbers of items processed. These could be key-value pairs, table
   * records etc.
   * 
   * @return Number of items processed
  */
 public long getItemsProcessed();

}

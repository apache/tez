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

/**
 * Context handle for the Output to initialize itself.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface OutputContext extends TaskContext {

  /**
   * Get the Vertex Name of the Destination that is the recipient of this
   * Output's data
   * @return Name of the Destination Vertex
   */
  public String getDestinationVertexName();
  
  /**
   * Get the index of the output in the set of all outputs for the task. The 
   * index will be consistent and valid only among the tasks of this vertex.
   * @return index
   */
  public int getOutputIndex();

}

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
 * Context handle for the Input to initialize itself.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface InputContext extends TaskContext {

  /**
   * Get the Vertex Name of the Source that generated data for this Input
   * @return Name of the Source Vertex
   */
  public String getSourceVertexName();
  
  /**
   * Get the index of the input in the set of all inputs for the task. The 
   * index will be consistent and valid only among the tasks of this vertex.
   * @return index
   */
  public int getInputIndex();
  
  /**
   * Inform the framework that the specific Input is ready for consumption.
   * 
   * This method can be invoked multiple times.
   */
  public void inputIsReady();
  
  /**
   * Get an {@link InputStatisticsReporter} for this {@link Input} that can
   * be used to report statistics like data size
   * @return {@link InputStatisticsReporter}
   */
  public InputStatisticsReporter getStatisticsReporter();
}

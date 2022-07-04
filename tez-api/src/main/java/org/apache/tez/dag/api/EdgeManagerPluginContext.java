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

package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Context information provided to {@link EdgeManagerPlugin}s
 * This interface is not supposed to be implemented by users
 *
 */
@Public
@Unstable
public interface EdgeManagerPluginContext {

  /**
   * Returns the payload specified by the user for the edge.
   * @return the {@link org.apache.tez.dag.api.UserPayload} specified by the user
   */
  public UserPayload getUserPayload();

  /**
   * Returns the source vertex name 
   * @return the source vertex name
   */
  public String getSourceVertexName();

  /**
   * Returns the destination vertex name
   * @return the destination vertex name
   */
  public String getDestinationVertexName();

  /**
   * Returns the number of tasks in the source vertex
   */
  public int getSourceVertexNumTasks();

  /**
   * Returns the number of tasks in the destination vertex
   */
  public int getDestinationVertexNumTasks();

  /**
   * @return the name of vertex group that source vertex belongs to, or null
   */
  String getVertexGroupName();
}

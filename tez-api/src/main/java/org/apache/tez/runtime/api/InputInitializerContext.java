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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.UserPayload;

/**
 * A context that provides information to the {@link InputInitializer}
 */
@Unstable
@Public
public interface InputInitializerContext {

  /**
   * Get the YARN application id given to the Tez Application Master
   * @return Application id
   */
  ApplicationId getApplicationId();
  
  /**
   * Get the name of the DAG
   * @return DAG name
   */
  String getDAGName();
  
  /**
   * Get the name of the input
   * @return Input name
   */
  String getInputName();

  /**
   * Get the user payload for the input
   * @return User payload
   */
  UserPayload getInputUserPayload();
  
  /**
   * Get the user payload for the initializer
   * @return User payload
   */
  UserPayload getUserPayload();
  
  /**
   * Get the number of tasks in this vertex. Maybe -1 if the vertex has not been
   * initialized with a pre-determined number of tasks.
   * @return number of tasks
   */
  int getNumTasks();
  
  /**
   * Get the resource allocated to a task of this vertex
   * @return Resource
   */
  Resource getVertexTaskResource();
  
  /**
   * Get the total resource allocated to this vertex. If the DAG is running in 
   * a busy cluster then it may have no resources available dedicated to it. The
   * DAG may divide its resources among member vertices.
   * @return Resource
   */
  Resource getTotalAvailableResource();
  
  /**
   * Get the number of nodes in the cluster
   * @return Number of nodes
   */
  int getNumClusterNodes();

  /**
   * @return DAG Attempt number
   */
  int getDAGAttemptNumber();

  /**
   * Get the number of tasks in the given vertex
   * @param vertexName
   * @return Total number of tasks in this vertex
   */
  public int getVertexNumTasks(String vertexName);

}

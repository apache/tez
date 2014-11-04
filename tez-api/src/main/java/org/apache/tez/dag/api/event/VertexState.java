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
 * Vertex state information.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum VertexState {
  /**
   * Indicates that the Vertex had entered the SUCCEEDED state. A vertex could
   * go back into RUNNING state after SUCCEEDING
   */
  SUCCEEDED,
  /**
   * Indicates that the Vertex had entered the RUNNING state. This state can be
   * reached after SUCCEEDED, if some tasks belonging to the vertex are
   * restarted due to errors
   */
  RUNNING,
  /**
   * Indicates that the Vertex has FAILED
   */
  FAILED,
  /**
   * Indicates that the Vertex has been KILLED
   */
  KILLED,
  /**
   * Indicates that the parallelism for the vertex had changed.
   */
  PARALLELISM_UPDATED,
  /**
   * Indicates that the vertex has been completely configured. Parallelism, edges, edge
   * properties, inputs/outputs have been set and will not be changed any
   * further. Listeners can depend on the vertex's configured state after
   * receiving this notification.
   */
  CONFIGURED
}

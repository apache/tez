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

package org.apache.tez.runtime.common.objectregistry;

/**
 * Defines the valid lifecycle and scope of Objects stored in ObjectRegistry.
 * Objects are guaranteed to not be valid outside of their defined life-cycle
 * period. Objects are not guaranteed to be retained through the defined period
 * as they may be evicted for various reasons.
 */
public enum ObjectLifeCycle {
  /** Objects are valid for the lifetime of the Tez JVM/Session
   */
  SESSION,
  /** Objects are valid for the lifetime of the DAG.
   */
  DAG,
  /** Objects are valid for the lifetime of the Vertex.
   */
  VERTEX,
}

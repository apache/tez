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

package org.apache.tez.dag.api.client.registry;

import org.apache.hadoop.service.AbstractService;
import org.apache.tez.client.registry.AMRecord;

/**
 * Base class for AMRegistry implementations.
 * The specific implementation class is configured by `tez.am.registry.class`.
 *
 * Implementations should handle the relevant service lifecycle operations:
 * `init`, `serviceStart`, `serviceStop`, etc.
 * - `init` and `serviceStart` are invoked during `DAGAppMaster.serviceInit`.
 * - `serviceStop` is invoked on `DAGAppMaster` shutdown.
 */
public abstract class AMRegistry extends AbstractService {

  /* Implementations should provide a public no-arg constructor. */
  protected AMRegistry(String name) {
    super(name);
  }

  /* Under typical usage, add() will be called once automatically with an AMRecord
     for the DAGClientServer that services an AM. */
  public abstract void add(AMRecord server) throws Exception;

  /* Under typical usage, implementations should remove any stale AMRecords upon serviceStop. */
  public abstract void remove(AMRecord server) throws Exception;

}
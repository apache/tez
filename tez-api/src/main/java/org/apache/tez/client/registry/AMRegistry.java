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

package org.apache.tez.client.registry;


import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;


/**
 * Base class for {@code AMRegistry} implementations.
 *
 * <p>The specific implementation is configured via the
 * {@code tez.am.registry.class} property.</p>
 *
 * <p>Implementations are expected to provide appropriate service lifecycle
 * behavior, including:
 * <ul>
 *   <li>{@code init}</li>
 *   <li>{@code serviceStart}</li>
 *   <li>{@code serviceStop}</li>
 * </ul>
 * </p>
 */
public abstract class AMRegistry extends AbstractService {
  /* Implementations should provide a public no-arg constructor */
  protected AMRegistry(String name) {
    super(name);
  }

  public abstract void add(AMRecord server) throws Exception;

  public abstract void remove(AMRecord server) throws Exception;

  public abstract ApplicationId generateNewId() throws Exception;

  public abstract AMRecord createAmRecord(ApplicationId appId, String hostName, String hostIp, int port,
                                          String computeName);
}

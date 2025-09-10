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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * <p>{@code init} and {@code serviceStart} are invoked during
 * {@code DAGAppMaster.serviceInit()}, while {@code serviceStop} is called
 * when {@code DAGAppMaster} shuts down.</p>
 */
public abstract class AMRegistry extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(AMRegistry.class);
  private List<AMRecord> amRecords = new ArrayList<>();

  @Override
  public void init(Configuration conf) {
    try {
      this.serviceInit(conf);
    } catch (Exception e) {
      LOG.error("Failed to init AMRegistry: name={}, type={}", getName(), getClass().getName());
      throw ServiceStateException.convert(e);
    }
  }

  @Override
  public void start() {
    try {
      this.serviceStart();
    } catch(Exception e) {
      LOG.error("Failed to start AMRegistry: name={}, type={}", getName(), getClass().getName());
      throw ServiceStateException.convert(e);
    }
  }

  /* Implementations should provide a public no-arg constructor */
  protected AMRegistry(String name) {
    super(name);
  }

  /* Under typical usage, add will be called once automatically with an AMRecord
     for the DAGClientServer servicing an AM
   */
  public void add(AMRecord server) throws Exception {
    amRecords.add(server);
  }

  public abstract void remove(AMRecord server) throws Exception;

  public ApplicationId generateNewId() throws Exception {
    return null;
  }

  public abstract AMRecord createAmRecord(ApplicationId appId, String hostName, String hostIp, int port, String computeName);

  @Override
  public void serviceStop() throws Exception {
    List<AMRecord> records = new ArrayList<>(amRecords);
    for(AMRecord record : records) {
      remove(record);
    }
  }
}

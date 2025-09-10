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
package org.apache.tez.frameworkplugins.zookeeper;


import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistry;
import org.apache.tez.frameworkplugins.AMExtensions;
import org.apache.tez.frameworkplugins.ServerFrameworkService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStandaloneServerFrameworkService implements ServerFrameworkService {
  private static final Logger LOG = LoggerFactory.getLogger(ZkStandaloneServerFrameworkService.class);
  private ZkAMRegistry amRegistry;

  @Override
  public synchronized AMRegistry getAMRegistry(Configuration conf) {
    if (amRegistry == null) {
      try {
        final String externalID = System.getenv(TezConstants.TEZ_AM_EXTERNAL_ID);
        amRegistry = new ZkAMRegistry(externalID);
        amRegistry.init(conf);
        amRegistry.start();
        LOG.info("Created Zookeeper based AM Registry with externalID: {}", externalID);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return amRegistry;
  }

  @Override
  public AMExtensions getAMExtensions() {
    return new ZkStandaloneAMExtensions(this);
  }
}

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

package org.apache.tez.dag.utils;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.client.registry.AMRegistry;

public final class AMRegistryUtils {

  private AMRegistryUtils() {}

  public static AMRecord recordForDAGClientServer(ApplicationId appId, String opaqueId,
      DAGClientServer dagClientServer) {
    InetSocketAddress address = dagClientServer.getBindAddress();
    return new AMRecord(appId, address.getHostName(), address.getPort(), opaqueId);
  }

  public static AMRegistry createAMRegistry(Configuration conf) throws Exception {
    String tezAMRegistryClass = conf.get(TezConfiguration.TEZ_AM_REGISTRY_CLASS);
    return tezAMRegistryClass == null ? null : ReflectionUtils.createClazzInstance(tezAMRegistryClass);
  }
}

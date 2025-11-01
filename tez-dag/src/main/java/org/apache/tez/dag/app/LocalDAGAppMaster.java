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
package org.apache.tez.dag.app;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;

/**
 * A DAGAppMaster implementation which is really local in a sense that it doesn't start an RPC
 * server for handling dag requests. It is typically used by LocalClient, which already has an
 * embedded DAGAppMaster, but by default, it calls RPC methods. With
 * tez.local.mode.without.network=true, LocalClient will call the DAGAppMaster's methods directly.
 */
public class LocalDAGAppMaster extends DAGAppMaster {

  public LocalDAGAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId,
      String nmHost, int nmPort, int nmHttpPort, Clock clock, long appSubmitTime, boolean isSession,
      String workingDirectory, String[] localDirs, String[] logDirs, String clientVersion,
      Credentials credentials, String jobUserName, AMPluginDescriptorProto pluginDescriptorProto) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime,
        isSession, workingDirectory, localDirs, logDirs, clientVersion, credentials, jobUserName,
        pluginDescriptorProto);
  }

  @Override
  protected void initClientRpcServer() {
    // nothing to do, in case of LocalDAGAppMaster clientRpcServer is not supposed to be used by clients
  }

  public int getRpcPort() {
    return 0;
  }
}

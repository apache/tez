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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.frameworkplugins.AMExtensions;
import org.apache.tez.frameworkplugins.ServerFrameworkService;

public class ZkStandaloneAMExtensions implements AMExtensions {

  private final ServerFrameworkService frameworkService;

  public ZkStandaloneAMExtensions(ServerFrameworkService frameworkService) {
    this.frameworkService = frameworkService;
  }

  @Override
  public ContainerId allocateContainerId(Configuration conf) {
    try {
      AMRegistry amRegistry = frameworkService.getAMRegistry(conf);
      if (amRegistry != null) {
        ApplicationId appId = amRegistry.generateNewId();
        // attemptId is set to 1 only then APP_LAUNCHED event gets triggered
        ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(appId, 1);
        return ContainerId.newContainerId(applicationAttemptId, 0);
      } else {
        throw new RuntimeException("AMRegistry is required for ZkStandaloneAmExtensions");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkTaskResources(Map<String, Vertex> vertices, ClusterInfo clusterInfo) {
    // no-op: Zookeeper-based framework current enforce task resources at the moment
  }

  @Override
  public DAGProtos.ConfigurationProto loadConfigurationProto() throws IOException {
    return TezUtilsInternal.loadConfProtoFromText();
  }

  @Override
  public Token<JobTokenIdentifier> getSessionToken(ApplicationAttemptId appAttemptID,
      JobTokenSecretManager jobTokenSecretManager, Credentials amCredentials) {
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(appAttemptID.getApplicationId().toString()));
    Token<JobTokenIdentifier> newSessionToken = new Token<>(identifier, jobTokenSecretManager);
    newSessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(newSessionToken, amCredentials);
    return newSessionToken;
  }

  @Override
  public DAGProtos.PlanLocalResourcesProto getAdditionalSessionResources(String workingDirectory) {
    return DAGProtos.PlanLocalResourcesProto.getDefaultInstance();
  }
}

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
package org.apache.tez.frameworkplugins;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Vertex;

/*
  Plugin points to provide alternate AM behavior that
  is either too small or too scattered to be usefully encapsulated as its own service
 */
public interface AmExtensions {

  //Override default Configuration loading at DAGAppMaster.main
  default Optional<DAGProtos.ConfigurationProto> loadConfigurationProto() { return Optional.empty(); }

  //Override default behavior to give ContainerId to AM
  default Optional<ContainerId> allocateContainerId(Configuration conf) { return Optional.empty(); }

  //Whether this framework requires addition of the default Yarn ServicePlugins
  default boolean isUsingYarnServicePlugin() {
    return true;
  }

  //Whether to check task resources against ClusterInfo
  default boolean checkTaskResources(Map<String, Vertex> vertices, AppContext appContext) { return true; }

  default Optional<Token<JobTokenIdentifier>> getSessionToken(
      ApplicationAttemptId appAttemptID,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials amCredentials
  ) { return Optional.empty(); }

  default Optional<DAGProtos.PlanLocalResourcesProto> getAdditionalSessionResources(String dir) {
    return Optional.empty();
  }
}

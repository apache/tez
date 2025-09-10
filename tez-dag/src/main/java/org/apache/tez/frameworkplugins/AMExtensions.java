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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.dag.Vertex;

/**
 * Extension points for customizing AM behavior.
 *
 * <p>These hooks allow injecting alternate or additional logic into the
 * Application Master without requiring a standalone service. They are
 * intended for behaviors that are too small or cross-cutting to justify
 * a dedicated service.</p>
 */
public interface AMExtensions {

  /**
   * Override the default configuration loading performed in
   * {@code DAGAppMaster.main(...)}.
   *
   * @return a {@link DAGProtos.ConfigurationProto} representing the final configuration
   * @throws IOException if configuration loading fails
   */
  DAGProtos.ConfigurationProto loadConfigurationProto() throws IOException;

  /**
   * Override the default logic used to assign a {@link ContainerId} to the AM.
   *
   * @param conf the Tez configuration
   * @return the allocated {@link ContainerId}
   */
  ContainerId allocateContainerId(Configuration conf);

  /**
   * Validate resource constraints for tasks before execution.
   *
   * @param vertices mapping of vertex names to their DAG vertices
   * @param clusterInfo cluster resource information
   * @throws TaskResourceException if resource requirements cannot be satisfied
   */
  void checkTaskResources(Map<String, Vertex> vertices, ClusterInfo clusterInfo) throws TaskResourceException;

  /**
   * Create or override the session token used for AM authentication.
   *
   * @param appAttemptID current application attempt ID
   * @param jobTokenSecretManager token secret manager
   * @param amCredentials AM credentials store
   * @return the session token
   */
  Token<JobTokenIdentifier> getSessionToken(
      ApplicationAttemptId appAttemptID,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials amCredentials
  );

  /**
   * Provide additional local resources required for the AM session.
   *
   * @param workingDirectory the AM working directory
   * @return protocol buffers describing local session resources
   * @throws IOException if resources cannot be discovered or packaged
   */
  DAGProtos.PlanLocalResourcesProto getAdditionalSessionResources(String workingDirectory) throws IOException;
}

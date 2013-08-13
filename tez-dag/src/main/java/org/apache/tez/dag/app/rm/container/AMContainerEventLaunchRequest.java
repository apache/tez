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

package org.apache.tez.dag.app.rm.container;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;

public class AMContainerEventLaunchRequest extends AMContainerEvent {

  private final TezVertexID vertexId;
  private final Token<JobTokenIdentifier> jobToken;
  private final Credentials credentials;
  private final boolean shouldProfile;
  private final Configuration conf;
  private final Map<String, LocalResource> localResources;
  private final Map<String, String> environment;
  private final String javaOpts;

  public AMContainerEventLaunchRequest(ContainerId containerId,
      TezVertexID vertexId,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, boolean shouldProfile, Configuration conf,
      Map<String, LocalResource> localResources,
      Map<String, String> environment, String javaOpts) {
    super(containerId, AMContainerEventType.C_LAUNCH_REQUEST);
    this.vertexId = vertexId;
    this.jobToken = jobToken;
    this.credentials = credentials;
    this.shouldProfile = shouldProfile;
    this.conf = conf;
    this.localResources = localResources;
    this.environment = environment;
    this.javaOpts = javaOpts;
  }

  public TezDAGID getDAGId() {
    return this.vertexId.getDAGId();
  }

  public TezVertexID getVertexId() {
    return this.vertexId;
  }

  public Token<JobTokenIdentifier> getJobToken() {
    return this.jobToken;
  }

  public Credentials getCredentials() {
    return this.credentials;
  }

  public boolean shouldProfile() {
    return this.shouldProfile;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public String getJavaOpts() {
	  return javaOpts;
  }
}

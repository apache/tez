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
package org.apache.tez.frameworkplugins.yarn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.frameworkplugins.AMExtensions;
import org.apache.tez.frameworkplugins.ServerFrameworkService;
import org.apache.tez.frameworkplugins.TaskResourceException;

/**
 * YARN-based server framework service implementation.
 * Provides default YARN framework server functionality with default implementations
 * for all AmExtensions methods.
 */
public class YarnServerFrameworkService implements ServerFrameworkService {

  private final YarnAMExtensions amExtensions = new YarnAMExtensions();

  @Override
  public AMRegistry getAMRegistry(Configuration conf) {
    // YARN mode doesn't require a custom AM registry
    return null;
  }

  @Override
  public AMExtensions getAMExtensions() {
    return amExtensions;
  }

  /**
   * Default YARN implementation of AmExtensions.
   * Provides sensible defaults for all methods.
   */
  public static class YarnAMExtensions implements AMExtensions {

    @Override
    public DAGProtos.ConfigurationProto loadConfigurationProto() throws IOException {
      return TezUtilsInternal
          .readUserSpecifiedTezConfiguration(System.getenv(ApplicationConstants.Environment.PWD.name()));
    }

    @Override
    public ContainerId allocateContainerId(Configuration conf) {
      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      return ConverterUtils.toContainerId(containerIdStr);
    }

    @Override
    public void checkTaskResources(Map<String, Vertex> vertices, ClusterInfo clusterInfo) throws TaskResourceException {
      Resource maxContainerCapability = clusterInfo.getMaxContainerCapability();
      for (Vertex v : vertices.values()) {
        // TODO TEZ-2003 (post) TEZ-2624 Ideally, this should be per source.
        if (v.getTaskResource().compareTo(maxContainerCapability) > 0) {
          String msg = "Vertex's TaskResource is beyond the cluster container capability," +
              "Vertex=" + v.getLogIdentifier() +", Requested TaskResource=" + v.getTaskResource()
              + ", Cluster MaxContainerCapability=" + maxContainerCapability;
          throw new TaskResourceException(msg);
        }
      }
    }

    @Override
    public Token<JobTokenIdentifier> getSessionToken(
        ApplicationAttemptId appAttemptID,
        JobTokenSecretManager jobTokenSecretManager,
        Credentials amCredentials) {
      return TokenCache.getSessionToken(amCredentials);
    }

    @Override
    public DAGProtos.PlanLocalResourcesProto getAdditionalSessionResources(String workingDirectory) throws IOException {
      FileInputStream sessionResourcesStream = null;
      try {
        sessionResourcesStream =
            new FileInputStream(new File(workingDirectory, TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
        return DAGProtos.PlanLocalResourcesProto.parseDelimitedFrom(sessionResourcesStream);
      } finally {
        if (sessionResourcesStream != null) {
          sessionResourcesStream.close();
        }
      }
    }
  }
}

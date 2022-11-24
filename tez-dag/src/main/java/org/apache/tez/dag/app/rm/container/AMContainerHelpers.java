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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.utils.TezRuntimeChildJVM;

import com.google.common.annotations.VisibleForTesting;

public final class AMContainerHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(AMContainerHelpers.class);

  private static final Object COMMON_CONTAINER_SPEC_LOCK = new Object();
  private static TezDAGID lastDAGID = null;
  private static final Map<TezDAGID, ContainerLaunchContext> COMMON_CONTAINER_SPECS =
          new HashMap<>();

  private AMContainerHelpers() {}

  public static void dagComplete(TezDAGID dagId) {
    synchronized (COMMON_CONTAINER_SPEC_LOCK) {
      COMMON_CONTAINER_SPECS.remove(dagId);
    }
  }

  /**
   * Create a {@link LocalResource} record with all the given parameters.
   */
  public static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat
        .getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return LocalResource.newInstance(resourceURL, type, visibility,
        resourceSize, resourceModificationTime);
  }

  /**
   * Create the common {@link ContainerLaunchContext} for all attempts.
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      Credentials credentials, String auxiliaryService) {

    // Application environment
    Map<String, String> environment = new HashMap<>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<>();

    // Tokens
    
    // Setup up task credentials buffer
    ByteBuffer containerCredentialsBuffer;
    try {
      Credentials containerCredentials = new Credentials();
      
      // All Credentials need to be set so that YARN can localize the resources
      // correctly, even though they may not be used by all tasks which will run
      // on this container.

      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding #" + credentials.numberOfTokens() + " tokens and #"
            + credentials.numberOfSecretKeys() + " secret keys for NM use for launching container in common CLC");
      }
      containerCredentials.addAll(credentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      containerCredentials.writeTokenStorageToStream(containerTokens_dob);
      containerCredentialsBuffer = ByteBuffer.wrap(containerTokens_dob.getData(), 0,
          containerTokens_dob.getLength());

      // Add shuffle token
      LOG.debug("Putting shuffle token in serviceData in common CLC");
      serviceData.put(auxiliaryService,
          TezCommonUtils.serializeServiceData(TokenCache.getSessionToken(containerCredentials)));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    return ContainerLaunchContext.newInstance(null, environment, null,
        serviceData, containerCredentialsBuffer, applicationACLs);
  }

  @VisibleForTesting
  public static ContainerLaunchContext createContainerLaunchContext(
      TezDAGID tezDAGID,
      Map<ApplicationAccessType, String> acls,
      ContainerId containerId,
      Map<String, LocalResource> localResources,
      Map<String, String> vertexEnv,
      String javaOpts,
      InetSocketAddress taskAttemptListenerAddress, Credentials credentials,
      AppContext appContext, Resource containerResource,
      Configuration conf, String auxiliaryService) {

    ContainerLaunchContext commonContainerSpec;
    synchronized (COMMON_CONTAINER_SPEC_LOCK) {
      if (!COMMON_CONTAINER_SPECS.containsKey(tezDAGID)) {
        commonContainerSpec =
            createCommonContainerLaunchContext(acls, credentials, auxiliaryService);
        COMMON_CONTAINER_SPECS.put(tezDAGID, commonContainerSpec);
      } else {
        commonContainerSpec = COMMON_CONTAINER_SPECS.get(tezDAGID);
      }

      // Ensure that we remove container specs for previous AMs to reduce
      // memory footprint
      if (lastDAGID == null) {
        lastDAGID = tezDAGID;
      } else if (!lastDAGID.equals(tezDAGID)) {
        COMMON_CONTAINER_SPECS.remove(lastDAGID);
        lastDAGID = tezDAGID;
      }
    }

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<>(env.size());
    myEnv.putAll(env);
    myEnv.putAll(vertexEnv);

    String modifiedJavaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(javaOpts,
        containerResource, conf.getDouble(TezConfiguration.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION,
            TezConfiguration.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION_DEFAULT));
    if (LOG.isDebugEnabled()) {
      if (!modifiedJavaOpts.equals(javaOpts)) {
        LOG.debug("Modified java opts for container"
          + ", containerId=" + containerId
          + ", originalJavaOpts=" + javaOpts
          + ", modifiedJavaOpts=" + modifiedJavaOpts);
      }
    }

    List<String> commands = TezRuntimeChildJVM.getVMCommand(
        taskAttemptListenerAddress, containerId.toString(),
        appContext.getApplicationID().toString(),
        appContext.getApplicationAttemptId().getAttemptId(), modifiedJavaOpts);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData()
        .entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container

    return ContainerLaunchContext.newInstance(localResources, myEnv, commands,
        myServiceData, commonContainerSpec.getTokens().duplicate(), acls);
  }
}

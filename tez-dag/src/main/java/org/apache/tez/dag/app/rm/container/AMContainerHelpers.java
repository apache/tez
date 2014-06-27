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
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
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
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.utils.TezRuntimeChildJVM;

import com.google.common.annotations.VisibleForTesting;

public class AMContainerHelpers {

  private static final Log LOG = LogFactory.getLog(AMContainerHelpers.class);

  private static Object commonContainerSpecLock = new Object();
  private static TezDAGID lastDAGID = null;
  private static Map<TezDAGID, ContainerLaunchContext> commonContainerSpecs =
      new HashMap<TezDAGID, ContainerLaunchContext>();


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
   *
   * @param applicationACLs
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      Credentials credentials) {

    // Application resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    
    // Setup up task credentials buffer
    ByteBuffer containerCredentialsBuffer = ByteBuffer.wrap(new byte[] {});
    try {
      Credentials containerCredentials = new Credentials();
      
      // All Credentials need to be set so that YARN can localize the resources
      // correctly, even though they may not be used by all tasks which will run
      // on this container.

      LOG.info("Adding #" + credentials.numberOfTokens() + " tokens and #"
          + credentials.numberOfSecretKeys() + " secret keys for NM use for launching container");
      containerCredentials.addAll(credentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      containerCredentials.writeTokenStorageToStream(containerTokens_dob);
      containerCredentialsBuffer = ByteBuffer.wrap(containerTokens_dob.getData(), 0,
          containerTokens_dob.getLength());

      // Add shuffle token
      LOG.info("Putting shuffle token in serviceData");
      serviceData.put(TezConfiguration.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
          serializeServiceData(TokenCache.getSessionToken(containerCredentials)));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(localResources, environment, null,
            serviceData, containerCredentialsBuffer, applicationACLs);
    return container;
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
      Configuration conf) {

    ContainerLaunchContext commonContainerSpec = null;
    synchronized (commonContainerSpecLock) {
      if (!commonContainerSpecs.containsKey(tezDAGID)) {
        commonContainerSpec =
            createCommonContainerLaunchContext(acls, credentials);
        commonContainerSpecs.put(tezDAGID, commonContainerSpec);
      } else {
        commonContainerSpec = commonContainerSpecs.get(tezDAGID);
      }

      // Ensure that we remove container specs for previous AMs to reduce
      // memory footprint
      if (lastDAGID == null) {
        lastDAGID = tezDAGID;
      } else if (!lastDAGID.equals(tezDAGID)) {
        commonContainerSpecs.remove(lastDAGID);
        lastDAGID = tezDAGID;
      }
    }

    // Fill in the fields needed per-container that are missing in the common
    // spec.
    Map<String, LocalResource> lResources =
        new TreeMap<String, LocalResource>();
    lResources.putAll(commonContainerSpec.getLocalResources());
    lResources.putAll(localResources);

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
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
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData()
        .entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(lResources, myEnv, commands,
            myServiceData, commonContainerSpec.getTokens().duplicate(), acls);

    return container;
  }
  
  /**
   * A helper function to serialize the JobTokenIdentifier to be sent to the
   * ShuffleHandler as ServiceData.
   * 
   * *NOTE* This is a copy of what is done by the MapReduce ShuffleHandler. Not using that directly
   * to avoid a dependency on mapreduce.
   * 
   * @param jobToken
   *          the job token to be used for authentication of shuffle data
   *          requests.
   * @return the serialized version of the jobToken.
   */
  private static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken)
      throws IOException {
    // TODO these bytes should be versioned
    DataOutputBuffer jobToken_dob = new DataOutputBuffer();
    jobToken.write(jobToken_dob);
    return ByteBuffer.wrap(jobToken_dob.getData(), 0, jobToken_dob.getLength());
  }

}

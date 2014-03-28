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

package org.apache.tez.client;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;

public class TezClient {
  private static final Log LOG = LogFactory.getLog(TezClient.class);

  private final TezConfiguration conf;
  private final YarnConfiguration yarnConf;
  private YarnClient yarnClient;
  Map<String, LocalResource> tezJarResources = null;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();

  /**
   * <p>
   * Create an instance of the TezClient which will be used to communicate with
   * a specific instance of YARN, or TezService when that exists.
   * </p>
   * <p>
   * Separate instances of TezClient should be created to communicate with
   * different instances of YARN
   * </p>
   *
   * @param conf
   *          the configuration which will be used to establish which YARN or
   *          Tez service instance this client is associated with.
   */
  public TezClient(TezConfiguration conf) {
    this.conf = conf;
    this.yarnConf = new YarnConfiguration(conf);
    yarnClient = new YarnClientImpl();
    yarnClient.init(yarnConf);
    yarnClient.start();
  }


  public DAGClient submitDAGApplication(DAG dag, AMConfiguration amConfig)
      throws TezException, IOException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, dag, amConfig);
  }

  @Private
  // To be used only by YarnRunner
  public DAGClient submitDAGApplication(ApplicationId appId,
      DAG dag, AMConfiguration amConfig)
          throws TezException, IOException {
    try {
      // Use the AMCredentials object in client mode, since this won't be re-used.
      // Ensures we don't fetch credentially unnecessarily if the user has already provided them.
      Credentials credentials = amConfig.getCredentials();
      if (credentials == null) {
        credentials = new Credentials();
      }
      TezClientUtils.processTezLocalCredentialsFile(credentials, conf);

      // Add session token for shuffle
      TezClientUtils.createSessionToken(appId.toString(),
          jobTokenSecretManager, credentials);

      // Add credentials for tez-local resources.
      Map<String, LocalResource> tezJarResources = getTezJarResources(credentials);
      ApplicationSubmissionContext appContext = TezClientUtils.createApplicationSubmissionContext(
          conf, appId, dag, dag.getName(), amConfig, tezJarResources, credentials);
      LOG.info("Submitting DAG to YARN"
          + ", applicationId=" + appId);
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return getDAGClient(appId);
  }

  /**
   * Create a new YARN application
   * @return <code>ApplicationId</code> for the new YARN application
   * @throws YarnException
   * @throws IOException
   */
  public ApplicationId createApplication() throws TezException, IOException {
    try {
      return yarnClient.createApplication().
          getNewApplicationResponse().getApplicationId();
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  private synchronized Map<String, LocalResource> getTezJarResources(Credentials credentials)
      throws IOException {
    if (tezJarResources == null) {
      tezJarResources = TezClientUtils.setupTezJarsLocalResources(conf, credentials);
    }
    return tezJarResources;
  }

  @Private
  public DAGClient getDAGClient(ApplicationId appId)
      throws IOException, TezException {
      return new DAGClientRPCImpl(appId, getDefaultTezDAGID(appId),
                                   conf);
  }

  // DO NOT CHANGE THIS. This code is replicated from TezDAGID.java
  private static final char SEPARATOR = '_';
  private static final String DAG = "dag";
  private static final ThreadLocal<NumberFormat> idFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(6);
      return fmt;
    }
  };

  String getDefaultTezDAGID(ApplicationId appId) {
     return (new StringBuilder(DAG)).append(SEPARATOR).
                   append(appId.getClusterTimestamp()).
                   append(SEPARATOR).
                   append(appId.getId()).
                   append(SEPARATOR).
                   append(idFormat.get().format(1)).toString();
  }

}

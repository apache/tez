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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

import com.google.protobuf.ServiceException;

public class TezClient {
  private static final Log LOG = LogFactory.getLog(TezClient.class);

  private final TezConfiguration conf;
  private YarnClient yarnClient;
  Map<String, LocalResource> tezJarResources = null;

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
    yarnClient = new YarnClientImpl();
    yarnClient.init(new YarnConfiguration(conf));
    yarnClient.start();
  }

  /**
   * Submit a Tez DAG to YARN as an application. The job will be submitted to
   * the yarn cluster which was specified when creating this
   * {@link TezClient} instance.
   *
   * @param dag
   *          <code>DAG</code> to be submitted
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws TezException
   */
  public DAGClient submitDAGApplication(DAG dag, Path appStagingDir,
      Credentials ts, String amQueueName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources,
      TezConfiguration amConf) throws IOException, TezException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, dag, appStagingDir, ts, amQueueName,
        dag.getName(), amArgs, amEnv, amLocalResources, amConf);
  }

  /**
   * Submit a Tez DAG to YARN as an application. The job will be submitted to
   * the yarn cluster which was specified when creating this
   * {@link TezClient} instance. The AM will wait for the <code>DAG</code> to
   * be submitted via RPC.
   *
   * @param amName
   *          Name of the application
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws TezException
   */
  public DAGClient submitDAGApplication(String amName, Path appStagingDir,
      Credentials ts, String amQueueName, List<String> amArgs,
      Map<String, String> amEnv, Map<String, LocalResource> amLocalResources,
      TezConfiguration amConf) throws IOException, TezException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, null, appStagingDir, ts, amQueueName,
        amName, amArgs, amEnv, amLocalResources, amConf);
  }

  /**
   * Submit a Tez DAG to YARN with known <code>ApplicationId</code>. This is a
   * private method and is only meant to be used within Tez for MR client
   * backward compatibility.
   *
   * @param appId
   *          - <code>ApplicationId</code> to be used
   * @param dag
   *          <code>DAG</code> to be submitted
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return <code>ApplicationId</code> of the submitted Tez application
   * @throws IOException
   * @throws TezException
   */
  @Private
  public DAGClient submitDAGApplication(ApplicationId appId, DAG dag,
      Path appStagingDir, Credentials ts, String amQueueName, String amName,
      List<String> amArgs, Map<String, String> amEnv,
      Map<String, LocalResource> amLocalResources, TezConfiguration amConf)
      throws IOException, TezException {
    try {
      ApplicationSubmissionContext appContext =
          TezClientUtils.createApplicationSubmissionContext(
          conf, appId, dag, appStagingDir, ts, amQueueName, amName, amArgs,
          amEnv, amLocalResources, amConf, getTezJarResources());
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

  private synchronized Map<String, LocalResource> getTezJarResources()
      throws IOException {
    if (tezJarResources == null) {
      tezJarResources = TezClientUtils.setupTezJarsLocalResources(conf);
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
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }

  String getDefaultTezDAGID(ApplicationId appId) {
     return (new StringBuilder(DAG)).append(SEPARATOR).
                   append(appId.getClusterTimestamp()).
                   append(SEPARATOR).
                   append(appId.getId()).
                   append(SEPARATOR).
                   append(idFormat.format(1)).toString();
  }

  /**
   * Create a Tez Session. This will launch a Tez AM on the cluster and
   * the session then can be used to submit dags to this Tez AM.
   * @param appId Application Id
   * @param sessionName
   *          Name of the Session
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return TezSession handle to submit subsequent jobs
   * @throws IOException
   * @throws TezException
   */
  public TezSession createSession(ApplicationId appId, String sessionName,
      Path appStagingDir, Credentials ts, String amQueueName,
      List<String> amArgs, Map<String, String> amEnv,
      Map<String, LocalResource> amLocalResources,
      TezConfiguration amConf) throws TezException, IOException {
    if (appId == null) {
      appId = createApplication();
    }
    TezSession tezSession = new TezSession(sessionName, appId);
    LOG.info("Creating a TezSession"
        + ", sessionName=" + sessionName
        + ", applicationId=" + appId);
    try {
      ApplicationSubmissionContext appContext =
          TezClientUtils.createApplicationSubmissionContext(
          conf, appId, null, appStagingDir, ts, amQueueName, sessionName,
          amArgs, amEnv, amLocalResources, amConf, getTezJarResources());
      tezSession.setTezConfigurationLocalResource(
          appContext.getAMContainerSpec().getLocalResources().get(
              TezConfiguration.TEZ_PB_BINARY_CONF_NAME));
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return tezSession;
  }

  /**
   * Create a Tez Session. This will launch a Tez AM on the cluster and
   * the session then can be used to submit dags to this Tez AM.
   * @param sessionName
   *          Name of the Session
   * @param appStagingDir
   *          FileSystem path in which resources will be copied
   * @param ts
   *          Application credentials
   * @param amQueueName
   *          Queue to which the application will be submitted
   * @param amArgs
   *          Command line Java arguments for the ApplicationMaster
   * @param amEnv
   *          Environment to be added to the ApplicationMaster
   * @param amLocalResources
   *          YARN local resource for the ApplicationMaster
   * @param conf
   *          Configuration for the Tez DAG AM. tez configuration keys from this
   *          config will be used when running the AM. Look at
   *          {@link TezConfiguration} for keys. This can be null if no DAG AM
   *          parameters need to be changed.
   * @return TezSession handle to submit subsequent jobs
   * @throws IOException
   * @throws TezException
   */
  public synchronized TezSession createSession(String sessionName,
      Path appStagingDir, Credentials ts, String amQueueName,
      List<String> amArgs, Map<String, String> amEnv, Map<String,
      LocalResource> amLocalResources, TezConfiguration amConf)
          throws TezException, IOException {
    return createSession(null, sessionName, appStagingDir, ts, amQueueName,
        amArgs, amEnv, amLocalResources, amConf);
  }

  private DAGClientAMProtocolBlockingPB getAMProxy(
      ApplicationId applicationId) throws TezException, IOException {
    return TezClientUtils.getAMProxy(yarnClient, conf, applicationId);
  }

  public synchronized DAGClient submitDAG(TezSession tezSession, DAG dag)
      throws IOException, TezException {
    String dagId = null;
    LOG.info("Submitting dag to TezSession"
        + ", sessionName=" + tezSession.getSessionName()
        + ", applicationId=" + tezSession.getApplicationId());
    // Add tez jars to vertices too
    for (Vertex v : dag.getVertices()) {
      v.getTaskLocalResources().putAll(getTezJarResources());
      if (null != tezSession.getTezConfigurationLocalResource()) {
        v.getTaskLocalResources().put(TezConfiguration.TEZ_PB_BINARY_CONF_NAME,
            tezSession.getTezConfigurationLocalResource());
      }
    }
    DAGPlan dagPlan = dag.createDag(null);
    SubmitDAGRequestProto requestProto =
        SubmitDAGRequestProto.newBuilder().setDAGPlan(dagPlan).build();

    DAGClientAMProtocolBlockingPB proxy;
    while (true) {
      proxy = getAMProxy(tezSession.getApplicationId());
      if (proxy != null) {
        break;
      }
      try {
        Thread.sleep(100l);
      } catch (InterruptedException e) {
        // Ignore
      }
    }

    try {
      dagId = proxy.submitDAG(null, requestProto).getDagId();
    } catch (ServiceException e) {
      throw new TezException(e);
    }
    LOG.info("Submitted dag to TezSession"
        + ", sessionName=" + tezSession.getSessionName()
        + ", applicationId=" + tezSession.getApplicationId()
        + ", dagId=" + dagId);

    return new DAGClientRPCImpl(tezSession.getApplicationId(), dagId, conf);
  }

  public synchronized void closeSession(TezSession tezSession)
      throws TezException, IOException {
    LOG.info("Closing down Tez Session"
        + ", sessionName=" + tezSession.getSessionName()
        + ", applicationId=" + tezSession.getApplicationId());
    DAGClientAMProtocolBlockingPB proxy = getAMProxy(
        tezSession.getApplicationId());
    if (proxy != null) {
      try {
        ShutdownSessionRequestProto request =
            ShutdownSessionRequestProto.newBuilder().build();
        proxy.shutdownSession(null, request);
        return;
      } catch (ServiceException e) {
        LOG.info("Failed to shutdown Tez Session via proxy", e);
      }
    }
    LOG.info("Could not connect to AM, killing session via YARN"
        + ", sessionName=" + tezSession.getSessionName()
        + ", applicationId=" + tezSession.getApplicationId());
    try {
      yarnClient.killApplication(tezSession.getApplicationId());
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

}

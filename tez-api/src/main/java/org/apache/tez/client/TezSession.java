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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DAGSubmissionTimedOut;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;

public class TezSession {

  private static final Log LOG = LogFactory.getLog(TezSession.class);

  private final String sessionName;
  private ApplicationId applicationId;
  private final TezSessionConfiguration sessionConfig;
  private YarnClient yarnClient;
  private boolean sessionStarted = false;
  private boolean sessionStopped = false;
  /** Tokens which will be required for all DAGs submitted to this session. */
  private Credentials sessionCredentials = new Credentials();
  private long clientTimeout;

  public TezSession(String sessionName,
      ApplicationId applicationId,
      TezSessionConfiguration sessionConfig) {
    this.sessionName = sessionName;
    this.sessionConfig = sessionConfig;
    this.applicationId = applicationId;
  }

  public TezSession(String sessionName,
      TezSessionConfiguration sessionConfig) {
    this(sessionName, null, sessionConfig);
  }

  /**
   * Start a Tez Session
   * @throws TezException
   * @throws IOException
   */
  public synchronized void start() throws TezException, IOException {
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(sessionConfig.getYarnConfiguration());
    yarnClient.start();

    Map<String, LocalResource> tezJarResources =
        TezClientUtils.setupTezJarsLocalResources(
          sessionConfig.getTezConfiguration(), sessionCredentials);

    clientTimeout = sessionConfig.getTezConfiguration().getInt(
        TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS,
        TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT);

    if (sessionConfig.getSessionResources() != null
      && !sessionConfig.getSessionResources().isEmpty()) {
      tezJarResources.putAll(sessionConfig.getSessionResources());
    }

    try {
      if (applicationId == null) {
        applicationId = yarnClient.createApplication().
            getNewApplicationResponse().getApplicationId();
      }

      ApplicationSubmissionContext appContext =
          TezClientUtils.createApplicationSubmissionContext(
              sessionConfig.getTezConfiguration(), applicationId,
              null, sessionName, sessionConfig.getAMConfiguration(),
              tezJarResources, sessionCredentials);
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      throw new TezException(e);
    }
    sessionStarted = true;
  }

  /**
   * Submit a DAG to a Tez Session. Blocks until either the DAG is submitted to
   * the session or configured timeout period expires. Cleans up session if the
   * submission timed out.
   * @param dag DAG to be submitted to Session
   * @return DAGClient to monitor the DAG
   * @throws TezException
   * @throws IOException
   * @throws SessionNotRunning if session is not alive
   * @throws DAGSubmissionTimedOut if submission timed out
   */
  public synchronized DAGClient submitDAG(DAG dag)
    throws TezException, IOException, InterruptedException {
    verifySessionStateForSubmission();

    String dagId;
    LOG.info("Submitting dag to TezSession"
      + ", sessionName=" + sessionName
      + ", applicationId=" + applicationId);

    // Obtain DAG specific credentials.
    TezClientUtils.setupDAGCredentials(dag, sessionCredentials, sessionConfig.getTezConfiguration());

    // setup env
    String classpath = TezClientUtils
        .getFrameworkClasspath(sessionConfig.getYarnConfiguration());
    for (Vertex v : dag.getVertices()) {
      Map<String, String> taskEnv = v.getTaskEnvironment();
      Apps.addToEnvironment(taskEnv,
          ApplicationConstants.Environment.CLASSPATH.name(),
          classpath);
    }
    
    DAGPlan dagPlan = dag.createDag(sessionConfig.getTezConfiguration());
    SubmitDAGRequestProto requestProto =
        SubmitDAGRequestProto.newBuilder().setDAGPlan(dagPlan).build();

    DAGClientAMProtocolBlockingPB proxy = waitForProxy();
    if (proxy == null) {
      try {
        LOG.warn("DAG submission to session timed out, stopping session");
        stop();
      } catch (Throwable t) {
        LOG.info("Got an exception when trying to stop session", t);
      }
      throw new DAGSubmissionTimedOut("Could not submit DAG to Tez Session"
          + ", timed out after " + clientTimeout + " seconds");
    }

    try {
      dagId = proxy.submitDAG(null, requestProto).getDagId();
    } catch (ServiceException e) {
      throw new TezException(e);
    }
    LOG.info("Submitted dag to TezSession"
        + ", sessionName=" + sessionName
        + ", applicationId=" + applicationId
        + ", dagId=" + dagId);
    return new DAGClientRPCImpl(applicationId, dagId,
        sessionConfig.getTezConfiguration());
  }

  /**
   * Shutdown a Tez Session.
   * @throws TezException
   * @throws IOException
   */
  public synchronized void stop() throws TezException, IOException {
    if (!sessionStarted) {
      LOG.info("Session not started. Ignoring stop command");
      return;
    }
    LOG.info("Shutting down Tez Session"
        + ", sessionName=" + sessionName
        + ", applicationId=" + applicationId);
    sessionStopped = true;
    try {
      DAGClientAMProtocolBlockingPB proxy = TezClientUtils.getSessionAMProxy(
          yarnClient, sessionConfig.getYarnConfiguration(), applicationId);
      if (proxy != null) {
        ShutdownSessionRequestProto request =
            ShutdownSessionRequestProto.newBuilder().build();
        proxy.shutdownSession(null, request);
        return;
      }
    } catch (TezException e) {
      LOG.info("Failed to shutdown Tez Session via proxy", e);
    } catch (ServiceException e) {
      LOG.info("Failed to shutdown Tez Session via proxy", e);
    }
    LOG.info("Could not connect to AM, killing session via YARN"
        + ", sessionName=" + sessionName
        + ", applicationId=" + applicationId);
    try {
      yarnClient.killApplication(applicationId);
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  public String getSessionName() {
    return sessionName;
  }

  @Private
  @VisibleForTesting
  public synchronized ApplicationId getApplicationId() {
    return applicationId;
  }

  public TezSessionStatus getSessionStatus() throws TezException, IOException {
    try {
      ApplicationReport appReport = yarnClient.getApplicationReport(
          applicationId);
      switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case ACCEPTED:
      case SUBMITTED:
        return TezSessionStatus.INITIALIZING;
      case FINISHED:
      case FAILED:
      case KILLED:
        return TezSessionStatus.SHUTDOWN;
      case RUNNING:
        try {
          DAGClientAMProtocolBlockingPB proxy = TezClientUtils.getSessionAMProxy(
              yarnClient, sessionConfig.getYarnConfiguration(), applicationId);
          if (proxy == null) {
            return TezSessionStatus.INITIALIZING;
          }
          GetAMStatusResponseProto response = proxy.getAMStatus(null,
              GetAMStatusRequestProto.newBuilder().build());
          return DagTypeConverters.convertTezSessionStatusFromProto(
              response.getStatus());
        } catch (TezException e) {
          LOG.info("Failed to retrieve AM Status via proxy", e);
        } catch (ServiceException e) {
          LOG.info("Failed to retrieve AM Status via proxy", e);
        }
      }
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return TezSessionStatus.INITIALIZING;
  }

  /**
   * Inform the Session to pre-warm containers for upcoming DAGs.
   * Can be invoked multiple times on the same session.
   * A subsequent call will release containers that are not compatible with the
   * new context.
   * This function can only be invoked if there is no DAG running on the Session
   * This function can be a no-op if the Session already holds the required
   * number of containers.
   * @param context Context for the pre-warm containers.
   */
  @Private
  @InterfaceStability.Unstable
  public void preWarm(PreWarmContext context)
      throws IOException, TezException, InterruptedException {
    verifySessionStateForSubmission();

    try {
      DAGClientAMProtocolBlockingPB proxy = waitForProxy();
      if (proxy == null) {
        throw new SessionNotRunning("Could not connect to Session within client"
            + " timeout interval, timeoutSecs=" + clientTimeout);
      }

      String classpath = TezClientUtils
        .getFrameworkClasspath(sessionConfig.getYarnConfiguration());
      Map<String, String> contextEnv = context.getEnvironment();
      Apps.addToEnvironment(contextEnv,
        ApplicationConstants.Environment.CLASSPATH.name(),
        classpath);

      DAGClientAMProtocolRPC.PreWarmRequestProto.Builder
        preWarmReqProtoBuilder =
          DAGClientAMProtocolRPC.PreWarmRequestProto.newBuilder();
      preWarmReqProtoBuilder.setPreWarmContext(
        DagTypeConverters.convertPreWarmContextToProto(context));
      proxy.preWarm(null, preWarmReqProtoBuilder.build());
      while (true) {
        try {
          Thread.sleep(1000);
          TezSessionStatus status = getSessionStatus();
          if (status.equals(TezSessionStatus.READY)) {
            break;
          } else if (status.equals(TezSessionStatus.SHUTDOWN)) {
            throw new SessionNotRunning("Could not connect to Session");
          }
        } catch (InterruptedException e) {
          return;
        }
      }
    } catch (ServiceException e) {
      throw new TezException(e);
    }
  }

  private DAGClientAMProtocolBlockingPB waitForProxy()
      throws IOException, TezException, InterruptedException {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + (clientTimeout * 1000);
    DAGClientAMProtocolBlockingPB proxy = null;
    while (true) {
      proxy = TezClientUtils.getSessionAMProxy(yarnClient,
          sessionConfig.getYarnConfiguration(), applicationId);
      if (proxy != null) {
        break;
      }
      Thread.sleep(100l);
      if (clientTimeout != -1 && System.currentTimeMillis() > endTime) {
        break;
      }
    }
    return proxy;
  }

  private void verifySessionStateForSubmission() throws SessionNotRunning {
    if (!sessionStarted) {
      throw new SessionNotRunning("Session not started");
    } else if (sessionStopped) {
      throw new SessionNotRunning("Session stopped");
    }
  }

}

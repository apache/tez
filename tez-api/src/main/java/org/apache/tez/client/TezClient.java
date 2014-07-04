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

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Map;

import javax.annotation.Nullable;

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
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
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
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ServiceException;

/**
 * TezClient is used to submit Tez DAGs for execution. DAG's are executed via a
 * Tez App Master. TezClient can run the App Master in session or non-session
 * mode. <br>
 * In non-session mode, each DAG is executed in a different App Master that
 * exits after the DAG execution completes. <br>
 * In session mode, the TezClient creates a single instance of the App Master
 * and all DAG's are submitted to the same App Master.<br>
 * Session mode may give better performance when a series of DAGs need to
 * executed because it enables resource re-use across those DAGs. Non-session
 * mode should be used when the user wants to submit a single DAG or wants to
 * disconnect from the cluster after submitting a set of unrelated DAGs. <br>
 * If API recommendations are followed, then the choice of running in session or
 * non-session mode is transparent to writing the application. By changing the
 * session mode configuration, the same application can be running in session or
 * non-session mode.
 */
public class TezClient {

  private static final Log LOG = LogFactory.getLog(TezClient.class);

  private final String clientName;
  private ApplicationId sessionAppId;
  private ApplicationId lastSubmittedAppId;
  private AMConfiguration amConfig;
  private YarnClient yarnClient;
  private boolean isSession;
  private boolean sessionStarted = false;
  private boolean sessionStopped = false;
  /** Tokens which will be required for all DAGs submitted to this session. */
  private Credentials sessionCredentials = new Credentials();
  private long clientTimeout;
  Map<String, LocalResource> cachedTezJarResources;
  private static final long SLEEP_FOR_READY = 500;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private Map<String, LocalResource> additionalLocalResources = Maps.newHashMap();

  /**
   * Create a new TezClient. Session or non-session execution mode will be
   * inferred from configuration. 
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf
   *          Configuration for the framework
   */
  public TezClient(String name, TezConfiguration tezConf) {
    this(name, tezConf, tezConf.getBoolean(
        TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT));    
  }
  
  /**
   * Create a new TezClient. Session or non-session execution mode will be
   * inferred from configuration. Set the initial resources and security
   * credentials for the App Master. If app master resources/credentials are
   * needed then this is the recommended constructor for session mode execution.
   * 
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf
   *          Configuration for the framework
   * @param localResources
   *          resources for the App Master
   * @param credentials
   *          Set security credentials to be used inside the app master, if
   *          needed. Tez App Master needs credentials to access the staging
   *          directory and for most HDFS cases these are automatically obtained
   *          by Tez client. If the staging directory is on a file system for
   *          which credentials cannot be obtained or for any credentials needed
   *          by user code running inside the App Master, credentials must be
   *          supplied by the user. These will be used by the App Master for the
   *          next DAG. <br>
   *          In session mode, credentials, if needed, must be set before
   *          calling start()
   */
  public TezClient(String name, TezConfiguration tezConf,
      @Nullable Map<String, LocalResource> localResources,
      @Nullable Credentials credentials) {
    this(name, tezConf, tezConf.getBoolean(
        TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT),
        localResources, credentials);
  }
  
  /**
   * Create a new TezClient with AM session mode set explicitly. This overrides
   * the setting from configuration.
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf Configuration for the framework
   * @param isSession The AM will run in session mode or not
   */
  public TezClient(String name, TezConfiguration tezConf, boolean isSession) {
    this(name, tezConf, isSession, null, null);
  }

  /**
   * Create a new TezClient with AM session mode set explicitly. This overrides
   * the setting from configuration.
   * Set the initial resources and security credentials for the App Master.
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf Configuration for the framework
   * @param isSession The AM will run in session mode or not
   * @param localResources resources for the App Master
   * @param credentials credentials for the App Master
   */
  public TezClient(String name, TezConfiguration tezConf, boolean isSession,
      @Nullable Map<String, LocalResource> localResources,
      @Nullable Credentials credentials) {
    this.clientName = name;
    this.isSession = isSession;
    this.amConfig = new AMConfiguration(tezConf, localResources, credentials);
  }
  
  /**
   * Add local resources for the DAG App Master. <br>
   * <p>
   * In non-session mode these will be added to the resources of the App Master
   * to be launched for the next DAG. Resources added via this method will
   * accumulate and be used for every new App Master until
   * clearAppMasterLocalResource() is invoked. <br>
   * <p>
   * In session mode, the recommended usage is to add all resources before
   * calling start() so that all needed resources are available to the app
   * master before it starts. When called after start(), these local resources
   * will be re-localized to the running session DAG App Master and will be
   * added to its classpath for execution of this DAG.
   * <p>
   * Caveats for invoking this method after start() in session mode: Resources
   * accumulate across DAG submissions and are never removed from the classpath.
   * Only LocalResourceType.FILE is supported. All resources will be treated as
   * private.
   * 
   * @param localResources
   */
  public synchronized void addAppMasterLocalResources(Map<String, LocalResource> localResources) {
    Preconditions.checkNotNull(localResources);
    if (isSession && sessionStarted) {
      additionalLocalResources.putAll(localResources);
    }
    amConfig.addLocalResources(localResources);
  }
  
  /**
   * If the next DAG App Master needs different local resources, then use this
   * method to clear the local resources and then add the new local resources
   * using addAppMasterLocalResources(). This method is a no-op in session mode,
   * after start() is called.
   */
  public synchronized void clearAppMasterLocalResource() {
    amConfig.clearLocalResources();
  }
  
  /**
   * Set security credentials to be used inside the app master, if needed. Tez App
   * Master needs credentials to access the staging directory and for most HDFS
   * cases these are automatically obtained by Tez client. If the staging
   * directory is on a file system for which credentials cannot be obtained or
   * for any credentials needed by user code running inside the App Master,
   * credentials must be supplied by the user. These will be used by the App
   * Master for the next DAG. <br>In session mode, credentials, if needed, must be
   * set before calling start()
   * 
   * @param credentials
   */
  public synchronized void setAppMasterCredentials(Credentials credentials) {
    Preconditions
        .checkState(!sessionStarted,
            "Credentials cannot be set after the session App Master has been started");
    amConfig.setCredentials(credentials);
  }
  
  /**
   * Start the client. This establishes a connection to the YARN cluster.
   * In session mode, this start the App Master thats runs all the DAGs in the
   * session.
   * @throws TezException
   * @throws IOException
   */
  public synchronized void start() throws TezException, IOException {
    amConfig.setYarnConfiguration(new YarnConfiguration(amConfig.getTezConfiguration()));

    yarnClient = createYarnClient();
    yarnClient.init(amConfig.getYarnConfiguration());
    yarnClient.start();    

    if (isSession) {
      LOG.info("Session mode. Starting session.");
      TezClientUtils.processTezLocalCredentialsFile(sessionCredentials,
          amConfig.getTezConfiguration());
  
      Map<String, LocalResource> tezJarResources = getTezJarResources(sessionCredentials);
  
      clientTimeout = amConfig.getTezConfiguration().getInt(
          TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS,
          TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT);
  
      try {
        if (sessionAppId == null) {
          sessionAppId = createApplication();
        }
  
        // Add session token for shuffle
        TezClientUtils.createSessionToken(sessionAppId.toString(),
            jobTokenSecretManager, sessionCredentials);
  
        ApplicationSubmissionContext appContext =
            TezClientUtils.createApplicationSubmissionContext(
                amConfig.getTezConfiguration(), sessionAppId,
                null, clientName, amConfig,
                tezJarResources, sessionCredentials);
  
        // Set Tez Sessions to not retry on AM crashes if recovery is disabled
        if (!amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.DAG_RECOVERY_ENABLED,
            TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT)) {
          appContext.setMaxAppAttempts(1);
        }  
        yarnClient.submitApplication(appContext);
        sessionStarted = true;
      } catch (YarnException e) {
        throw new TezException(e);
      }
    }
  }
  
  /**
   * Submit a DAG. <br>In non-session mode, it submits a new App Master to the
   * cluster.<br>In session mode, it submits the DAG to the session App Master. It
   * blocks until either the DAG is submitted to the session or configured
   * timeout period expires. Cleans up session if the submission timed out.
   * 
   * @param dag
   *          DAG to be submitted to Session
   * @return DAGClient to monitor the DAG
   * @throws TezException
   * @throws IOException
   * @throws DAGSubmissionTimedOut
   *           if submission timed out
   */  
  public synchronized DAGClient submitDAG(DAG dag) throws TezException, IOException,
      InterruptedException {
    if (isSession) {
      return submitDAGSession(dag);
    } else {
      return submitDAGApplication(dag);
    }
  }

  private synchronized DAGClient submitDAGSession(DAG dag)
    throws TezException, IOException, InterruptedException {
    Preconditions.checkState(isSession == true, 
        "submitDAG with additional resources applies to only session mode. " + 
        "In non-session mode please specify all resources in the initial configuration");
    
    verifySessionStateForSubmission();

    String dagId = null;
    LOG.info("Submitting dag to TezSession"
      + ", sessionName=" + clientName
      + ", applicationId=" + sessionAppId
      + ", dagName=" + dag.getName());
    
    if (!additionalLocalResources.isEmpty()) {
      for (LocalResource lr : additionalLocalResources.values()) {
        Preconditions.checkArgument(lr.getType() == LocalResourceType.FILE, "LocalResourceType: "
            + lr.getType() + " is not supported, only " + LocalResourceType.FILE + " is supported");
      }
    }

    // Obtain DAG specific credentials.
    TezClientUtils.setupDAGCredentials(dag, sessionCredentials, amConfig.getTezConfiguration());

    // TODO TEZ-1229 - fix jar resources
    // setup env
    for (Vertex v : dag.getVertices()) {
      Map<String, String> taskEnv = v.getTaskEnvironment();
      TezYARNUtils.setupDefaultEnv(taskEnv, amConfig.getTezConfiguration(),
          TezConfiguration.TEZ_TASK_LAUNCH_ENV, TezConfiguration.TEZ_TASK_LAUNCH_ENV_DEFAULT);
      TezClientUtils.setDefaultLaunchCmdOpts(v, amConfig.getTezConfiguration());
    }
    
    DAGPlan dagPlan = dag.createDag(amConfig.getTezConfiguration());
    SubmitDAGRequestProto.Builder requestBuilder = SubmitDAGRequestProto.newBuilder();
    requestBuilder.setDAGPlan(dagPlan).build();
    if (!additionalLocalResources.isEmpty()) {
      requestBuilder.setAdditionalAmResources(DagTypeConverters
          .convertFromLocalResources(additionalLocalResources));
    }
    
    additionalLocalResources.clear();

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
      SubmitDAGResponseProto response = proxy.submitDAG(null, requestBuilder.build());
      // the following check is only for testing since the final class
      // SubmitDAGResponseProto cannot be mocked
      if (response != null) {
        dagId = response.getDagId();
      }
    } catch (ServiceException e) {
      throw new TezException(e);
    }
    LOG.info("Submitted dag to TezSession"
        + ", sessionName=" + clientName
        + ", applicationId=" + sessionAppId
        + ", dagName=" + dag.getName());
    return new DAGClientRPCImpl(sessionAppId, dagId,
        amConfig.getTezConfiguration());
  }

  /**
   * Stop the client. This terminates the connection to the YARN cluster.
   * In session mode, this shuts down the session DAG App Master
   * @throws TezException
   * @throws IOException
   */
  public synchronized void stop() throws TezException, IOException {
    try {
      if (sessionStarted) {
        LOG.info("Shutting down Tez Session"
            + ", sessionName=" + clientName
            + ", applicationId=" + sessionAppId);
        sessionStopped = true;
        boolean sessionShutdownSuccessful = false;
        try {
          DAGClientAMProtocolBlockingPB proxy = getSessionAMProxy(sessionAppId);
          if (proxy != null) {
            ShutdownSessionRequestProto request =
                ShutdownSessionRequestProto.newBuilder().build();
            proxy.shutdownSession(null, request);
            sessionShutdownSuccessful = true;
          }
        } catch (TezException e) {
          LOG.info("Failed to shutdown Tez Session via proxy", e);
        } catch (ServiceException e) {
          LOG.info("Failed to shutdown Tez Session via proxy", e);
        }
        if (!sessionShutdownSuccessful) {
          LOG.info("Could not connect to AM, killing session via YARN"
              + ", sessionName=" + clientName
              + ", applicationId=" + sessionAppId);
          try {
            yarnClient.killApplication(sessionAppId);
          } catch (YarnException e) {
            throw new TezException(e);
          }
        }
      }
    } finally {
      if (yarnClient != null) {
        yarnClient.close();
      }
    }
  }

  /**
   * Get the name of the client
   * @return name
   */
  public String getClientName() {
    return clientName;
  }
  
  @Private
  @VisibleForTesting
  public synchronized ApplicationId getAppMasterApplicationId() {
    ApplicationId appId = null;
    if (isSession) {
      appId = sessionAppId;
    } else {
      appId = lastSubmittedAppId;
    }
    return appId;
  }

  /**
   * Get the status of the App Master executing the DAG
   * In non-session mode it returns the status of the last submitted DAG App Master 
   * In session mode, it returns the status of the App Master hosting the session
   * 
   * @return State of the session
   * @throws TezException
   * @throws IOException
   */
  public TezAppMasterStatus getAppMasterStatus() throws TezException, IOException {
    // Supporting per-DAG app master case since user may choose to run the same 
    // code in that mode and the code should continue to work. Its easy to provide 
    // the correct view for per-DAG app master too.
    ApplicationId appId = null;
    if (isSession) {
      appId = sessionAppId;
    } else {
      appId = lastSubmittedAppId;
    }
    Preconditions.checkState(appId != null, "Cannot get status without starting an application");
    try {
      ApplicationReport appReport = yarnClient.getApplicationReport(
          appId);
      switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case ACCEPTED:
      case SUBMITTED:
        return TezAppMasterStatus.INITIALIZING;
      case FINISHED:
      case FAILED:
      case KILLED:
        return TezAppMasterStatus.SHUTDOWN;
      case RUNNING:
        if (!isSession) {
          return TezAppMasterStatus.RUNNING;
        }
        try {
          DAGClientAMProtocolBlockingPB proxy = getSessionAMProxy(appId);
          if (proxy == null) {
            return TezAppMasterStatus.INITIALIZING;
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
    return TezAppMasterStatus.INITIALIZING;
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

      String classpath = TezYARNUtils
        .getFrameworkClasspath(amConfig.getYarnConfiguration());
      Map<String, String> contextEnv = context.getEnvironment();
      TezYARNUtils.addToEnvironment(contextEnv,
        ApplicationConstants.Environment.CLASSPATH.name(),
        classpath, File.pathSeparator);

      DAGClientAMProtocolRPC.PreWarmRequestProto.Builder
        preWarmReqProtoBuilder =
          DAGClientAMProtocolRPC.PreWarmRequestProto.newBuilder();
      preWarmReqProtoBuilder.setPreWarmContext(
        DagTypeConverters.convertPreWarmContextToProto(context));
      proxy.preWarm(null, preWarmReqProtoBuilder.build());
      while (true) {
        try {
          Thread.sleep(1000);
          TezAppMasterStatus status = getAppMasterStatus();
          if (status.equals(TezAppMasterStatus.READY)) {
            break;
          } else if (status.equals(TezAppMasterStatus.SHUTDOWN)) {
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
  
  /**
   * Wait till the DAG is ready to be submitted.
   * In non-session mode this is a no-op since the application can be immediately
   * submitted.
   * In session mode, this waits for the session host to be ready to accept a DAG
   * @throws IOException
   * @throws TezException
   */
  @InterfaceStability.Evolving
  public void waitTillReady() throws IOException, TezException {
    if (!isSession) {
      // nothing to wait for in non-session mode
      return;
    }
    verifySessionStateForSubmission();
    while (true) {
      TezAppMasterStatus status = getAppMasterStatus();
      if (status.equals(TezAppMasterStatus.SHUTDOWN)) {
        throw new SessionNotRunning("TezSession has already shutdown");
      }
      if (status.equals(TezAppMasterStatus.READY)) {
        return;
      }
      try {
        Thread.sleep(SLEEP_FOR_READY);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted", e);
        continue;
      }
    }
  }
  
  // for testing
  protected YarnClient createYarnClient() {
    return YarnClient.createYarnClient();
  }
  
  // for testing
  protected DAGClientAMProtocolBlockingPB getSessionAMProxy(ApplicationId appId) 
      throws TezException, IOException {
    return TezClientUtils.getSessionAMProxy(
        yarnClient, amConfig.getYarnConfiguration(), appId);
  }

  private DAGClientAMProtocolBlockingPB waitForProxy()
      throws IOException, TezException, InterruptedException {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + (clientTimeout * 1000);
    DAGClientAMProtocolBlockingPB proxy = null;
    while (true) {
      proxy = getSessionAMProxy(sessionAppId);
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
    Preconditions.checkState(isSession, "Invalid without session mode");
    if (!sessionStarted) {
      throw new SessionNotRunning("Session not started");
    } else if (sessionStopped) {
      throw new SessionNotRunning("Session stopped");
    }
  }
  
  private DAGClient submitDAGApplication(DAG dag)
      throws TezException, IOException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, dag);
  }

  @Private
  // To be used only by YarnRunner
  public DAGClient submitDAGApplication(ApplicationId appId, DAG dag)
          throws TezException, IOException {
    LOG.info("Submitting DAG application with id: " + appId);
    try {
      // Use the AMCredentials object in client mode, since this won't be re-used.
      // Ensures we don't fetch credentially unnecessarily if the user has already provided them.
      Credentials credentials = amConfig.getCredentials();
      if (credentials == null) {
        credentials = new Credentials();
      }
      TezClientUtils.processTezLocalCredentialsFile(credentials, 
          amConfig.getTezConfiguration());

      // Add session token for shuffle
      TezClientUtils.createSessionToken(appId.toString(),
          jobTokenSecretManager, credentials);

      // Add credentials for tez-local resources.
      Map<String, LocalResource> tezJarResources = getTezJarResources(credentials);
      ApplicationSubmissionContext appContext = TezClientUtils
          .createApplicationSubmissionContext(amConfig.getTezConfiguration(), 
              appId, dag, dag.getName(), amConfig, tezJarResources, credentials);
      LOG.info("Submitting DAG to YARN"
          + ", applicationId=" + appId
          + ", dagName=" + dag.getName());
      
      yarnClient.submitApplication(appContext);
      lastSubmittedAppId = appId;
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return getDAGClient(appId, amConfig.getTezConfiguration());
  }

  private ApplicationId createApplication() throws TezException, IOException {
    try {
      return yarnClient.createApplication().
          getNewApplicationResponse().getApplicationId();
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  private synchronized Map<String, LocalResource> getTezJarResources(Credentials credentials)
      throws IOException {
    if (cachedTezJarResources == null) {
      cachedTezJarResources = TezClientUtils.setupTezJarsLocalResources(
          amConfig.getTezConfiguration(), credentials);
    }
    return cachedTezJarResources;
  }

  @Private // Used only for MapReduce compatibility code
  public static DAGClient getDAGClient(ApplicationId appId, TezConfiguration tezConf)
      throws IOException, TezException {
      return new DAGClientRPCImpl(appId, getDefaultTezDAGID(appId), tezConf);
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

  private static String getDefaultTezDAGID(ApplicationId appId) {
     return (new StringBuilder(DAG)).append(SEPARATOR).
                   append(appId.getClusterTimestamp()).
                   append(SEPARATOR).
                   append(appId.getId()).
                   append(SEPARATOR).
                   append(idFormat.get().format(1)).toString();
  }

}

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
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DAGSubmissionTimedOut;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGResponseProto;
import org.apache.tez.dag.api.client.DAGClientImpl;
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
@Public
public class TezClient {

  private static final Logger LOG = LoggerFactory.getLogger(TezClient.class);
  
  @VisibleForTesting
  static final String NO_CLUSTER_DIAGNOSTICS_MSG = "No cluster diagnostics found.";

  private final String clientName;
  private ApplicationId sessionAppId;
  private ApplicationId lastSubmittedAppId;
  private AMConfiguration amConfig;
  private FrameworkClient frameworkClient;
  private String diagnostics;
  private boolean isSession;
  private boolean sessionStarted = false;
  private boolean sessionStopped = false;
  /** Tokens which will be required for all DAGs submitted to this session. */
  private Credentials sessionCredentials = new Credentials();
  private long clientTimeout;
  Map<String, LocalResource> cachedTezJarResources;
  boolean usingTezArchiveDeploy = false;
  private static final long SLEEP_FOR_READY = 500;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private Map<String, LocalResource> additionalLocalResources = Maps.newHashMap();
  private TezApiVersionInfo apiVersionInfo;
  private HistoryACLPolicyManager historyACLPolicyManager;

  private int preWarmDAGCounter = 0;

  private static final String atsHistoryLoggingServiceClassName =
      "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService";
  private static final String atsHistoryACLManagerClassName =
      "org.apache.tez.dag.history.ats.acls.ATSHistoryACLPolicyManager";

  private TezClient(String name, TezConfiguration tezConf) {
    this(name, tezConf, tezConf.getBoolean(
        TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT));    
  }

  @Private
  TezClient(String name, TezConfiguration tezConf,
            @Nullable Map<String, LocalResource> localResources,
            @Nullable Credentials credentials) {
    this(name, tezConf, tezConf.getBoolean(
        TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT),
        localResources, credentials);
  }

  private TezClient(String name, TezConfiguration tezConf, boolean isSession) {
    this(name, tezConf, isSession, null, null);
  }

  @Private
  protected TezClient(String name, TezConfiguration tezConf, boolean isSession,
            @Nullable Map<String, LocalResource> localResources,
            @Nullable Credentials credentials) {
    this.clientName = name;
    this.isSession = isSession;
    // Set in conf for local mode AM to figure out whether in session mode or not
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);
    this.amConfig = new AMConfiguration(tezConf, localResources, credentials);
    this.apiVersionInfo = new TezApiVersionInfo();

    LOG.info("Tez Client Version: " + apiVersionInfo.toString());
  }

  /**
   * Create a new TezClient. Session or non-session execution mode will be
   * inferred from configuration.
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf
   *          Configuration for the framework
   */
  public static TezClient create(String name, TezConfiguration tezConf) {
    return new TezClient(name, tezConf);
  }

  /**
   * Create a new TezClient. Session or non-session execution mode will be
   * inferred from configuration. Set the initial resources and security
   * credentials for the App Master. If app master resources/credentials are
   * needed then this is the recommended method for session mode execution.
   *
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf
   *          Configuration for the framework
   * @param localFiles
   *          local files for the App Master
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
  public static TezClient create(String name, TezConfiguration tezConf,
                                 @Nullable Map<String, LocalResource> localFiles,
                                 @Nullable Credentials credentials) {
    return new TezClient(name, tezConf, localFiles, credentials);
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
  public static TezClient create(String name, TezConfiguration tezConf, boolean isSession) {
    return new TezClient(name, tezConf, isSession);
  }

  /**
   * Create a new TezClient with AM session mode set explicitly. This overrides
   * the setting from configuration.
   * Set the initial files and security credentials for the App Master.
   * @param name
   *          Name of the client. Used for logging etc. This will also be used
   *          as app master name is session mode
   * @param tezConf Configuration for the framework
   * @param isSession The AM will run in session mode or not
   * @param localFiles local files for the App Master
   * @param credentials credentials for the App Master
   */
  public static TezClient create(String name, TezConfiguration tezConf, boolean isSession,
                                 @Nullable Map<String, LocalResource> localFiles,
                                 @Nullable Credentials credentials) {
    return new TezClient(name, tezConf, isSession, localFiles, credentials);
  }

  /**
   * Add local files for the DAG App Master. These may be files, archives, 
   * jars etc.<br>
   * <p>
   * In non-session mode these will be added to the files of the App Master
   * to be launched for the next DAG. Files added via this method will
   * accumulate and be used for every new App Master until
   * {@link #clearAppMasterLocalFiles()} is invoked. <br>
   * <p>
   * In session mode, the recommended usage is to add all files before
   * calling start() so that all needed files are available to the app
   * master before it starts. When called after start(), these local files
   * will be re-localized to the running session DAG App Master and will be
   * added to its classpath for execution of this DAG.
   * <p>
   * Caveats for invoking this method after start() in session mode: files
   * accumulate across DAG submissions and are never removed from the classpath.
   * Only LocalResourceType.FILE is supported. All files will be treated as
   * private.
   * 
   * @param localFiles
   */
  public synchronized void addAppMasterLocalFiles(Map<String, LocalResource> localFiles) {
    Preconditions.checkNotNull(localFiles);
    if (isSession && sessionStarted) {
      additionalLocalResources.putAll(localFiles);
    }
    amConfig.addAMLocalResources(localFiles);
  }
  
  /**
   * If the next DAG App Master needs different local files, then use this
   * method to clear the local files and then add the new local files
   * using {@link #addAppMasterLocalFiles(Map)}. This method is a no-op in session mode,
   * after start() is called.
   */
  public synchronized void clearAppMasterLocalFiles() {
    amConfig.clearAMLocalResources();
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

    frameworkClient = createFrameworkClient();
    frameworkClient.init(amConfig.getTezConfiguration(), amConfig.getYarnConfiguration());
    frameworkClient.start();

    if (this.amConfig.getTezConfiguration().get(
        TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "")
        .equals(atsHistoryLoggingServiceClassName)) {
      LOG.info("Using " + atsHistoryACLManagerClassName + " to manage Timeline ACLs");
      try {
        historyACLPolicyManager = ReflectionUtils.createClazzInstance(
            atsHistoryACLManagerClassName);
        historyACLPolicyManager.setConf(this.amConfig.getYarnConfiguration());
      } catch (TezUncheckedException e) {
        LOG.warn("Could not instantiate object for " + atsHistoryACLManagerClassName
            + ". ACLs cannot be enforced correctly for history data in Timeline", e);
        if (!amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS,
            TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS_DEFAULT)) {
          throw e;
        }
        historyACLPolicyManager = null;
      }
    }

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
                sessionAppId,
                null, clientName, amConfig,
                tezJarResources, sessionCredentials, usingTezArchiveDeploy, apiVersionInfo,
                historyACLPolicyManager);
  
        // Set Tez Sessions to not retry on AM crashes if recovery is disabled
        if (!amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.DAG_RECOVERY_ENABLED,
            TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT)) {
          appContext.setMaxAppAttempts(1);
        }  
        frameworkClient.submitApplication(appContext);
        ApplicationReport appReport = frameworkClient.getApplicationReport(sessionAppId);
        LOG.info("The url to track the Tez Session: " + appReport.getTrackingUrl());
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
  public synchronized DAGClient submitDAG(DAG dag) throws TezException, IOException {
    if (isSession) {
      return submitDAGSession(dag);
    } else {
      return submitDAGApplication(dag);
    }
  }

  private DAGClient submitDAGSession(DAG dag) throws TezException, IOException {
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

    Map<String, String> aclConfigs = null;
    if (historyACLPolicyManager != null) {
      aclConfigs = historyACLPolicyManager.setupSessionDAGACLs(
          amConfig.getTezConfiguration(), sessionAppId, dag.getName(), dag.getDagAccessControls());
    }

    Map<String, LocalResource> tezJarResources = getTezJarResources(sessionCredentials);
    DAGPlan dagPlan = TezClientUtils.prepareAndCreateDAGPlan(dag, amConfig, tezJarResources,
        usingTezArchiveDeploy, sessionCredentials, aclConfigs);

    SubmitDAGRequestProto.Builder requestBuilder = SubmitDAGRequestProto.newBuilder();
    requestBuilder.setDAGPlan(dagPlan).build();
    if (!additionalLocalResources.isEmpty()) {
      requestBuilder.setAdditionalAmResources(DagTypeConverters
          .convertFromLocalResources(additionalLocalResources));
    }
    
    additionalLocalResources.clear();

    DAGClientAMProtocolBlockingPB proxy = null;
    try {
      proxy = waitForProxy();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while trying to create a connection to the AM", e);
    }
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
    return new DAGClientImpl(sessionAppId, dagId,
        amConfig.getTezConfiguration(), frameworkClient);
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
            frameworkClient.killApplication(sessionAppId);
          } catch (ApplicationNotFoundException e) {
            LOG.info("Failed to kill nonexistent application " + sessionAppId, e);
          } catch (YarnException e) {
            throw new TezException(e);
          }
        }
      }
    } finally {
      if (frameworkClient != null) {
        frameworkClient.close();
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
    if (isSession) {
      return sessionAppId;
    } else {
      return lastSubmittedAppId;
    }
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
  public synchronized TezAppMasterStatus getAppMasterStatus() throws TezException, IOException {
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
      ApplicationReport appReport = frameworkClient.getApplicationReport(
          appId);
      switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case ACCEPTED:
      case SUBMITTED:
        return TezAppMasterStatus.INITIALIZING;
      case FAILED:
      case KILLED:
        diagnostics = appReport.getDiagnostics();
        LOG.info("App did not succeed. Diagnostics: "
            + (appReport.getDiagnostics() != null ? appReport.getDiagnostics()
                : NO_CLUSTER_DIAGNOSTICS_MSG));
        return TezAppMasterStatus.SHUTDOWN;
      case FINISHED:
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
    } catch (ApplicationNotFoundException e) {
      return TezAppMasterStatus.SHUTDOWN;
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return TezAppMasterStatus.INITIALIZING;
  }
  
  /**
   * API to help pre-allocate containers in session mode. In non-session mode
   * this is ignored. The pre-allocated containers may be re-used by subsequent 
   * job DAGs to improve performance. 
   * The preWarm vertex should be configured and setup exactly
   * like the other vertices in the job DAGs so that the pre-allocated containers 
   * may be re-used by the subsequent DAGs to improve performance.
   * The processor for the preWarmVertex may be used to pre-warm the containers
   * by pre-loading classes etc. It should be short-running so that pre-warming 
   * does not block real execution. Users can specify their custom processors or
   * use the PreWarmProcessor from the runtime library.
   * The parallelism of the preWarmVertex will determine the number of preWarmed
   * containers.
   * Pre-warming is best efforts and among other factors is limited by the free 
   * resources on the cluster.
   * @param preWarmVertex
   * @throws TezException
   * @throws IOException
   */
  @Unstable
  public synchronized void preWarm(PreWarmVertex preWarmVertex) throws TezException, IOException {
    if (!isSession) {
      // do nothing for non session mode. This is there to let the code 
      // work correctly in both modes
      return;
    }
    
    verifySessionStateForSubmission();
    
    DAG dag = org.apache.tez.dag.api.DAG.create(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX + "_"
        + preWarmDAGCounter++);
    dag.addVertex(preWarmVertex);

    try {
      waitTillReady();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for AM to become available", e);
    }
    submitDAG(dag);
  }

  
  /**
   * Wait till the DAG is ready to be submitted.
   * In non-session mode this is a no-op since the application can be immediately
   * submitted.
   * In session mode, this waits for the session host to be ready to accept a DAG
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException 
   */
  @Evolving
  public synchronized void waitTillReady() throws IOException, TezException, InterruptedException {
    if (!isSession) {
      // nothing to wait for in non-session mode
      return;
    }
    verifySessionStateForSubmission();
    while (true) {
      TezAppMasterStatus status = getAppMasterStatus();
      if (status.equals(TezAppMasterStatus.SHUTDOWN)) {
        throw new SessionNotRunning("TezSession has already shutdown. "
            + ((diagnostics != null) ? diagnostics : NO_CLUSTER_DIAGNOSTICS_MSG));
      }
      if (status.equals(TezAppMasterStatus.READY)) {
        return;
      }
      Thread.sleep(SLEEP_FOR_READY);
    }
  }
  
  @VisibleForTesting
  // for testing
  @Private
  protected FrameworkClient createFrameworkClient() {
    return FrameworkClient.createFrameworkClient(amConfig.getTezConfiguration());
  }

  @VisibleForTesting
  // for testing
  protected DAGClientAMProtocolBlockingPB getSessionAMProxy(ApplicationId appId) 
      throws TezException, IOException {
    return TezClientUtils.getSessionAMProxy(
        frameworkClient, amConfig.getYarnConfiguration(), appId);
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
      throw new SessionNotRunning("Session stopped by user");
    }
  }
  
  private DAGClient submitDAGApplication(DAG dag)
      throws TezException, IOException {
    ApplicationId appId = createApplication();
    return submitDAGApplication(appId, dag);
  }

  @Private // To be used only by YarnRunner
  DAGClient submitDAGApplication(ApplicationId appId, DAG dag)
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
          .createApplicationSubmissionContext( 
              appId, dag, dag.getName(), amConfig, tezJarResources, credentials,
              usingTezArchiveDeploy, apiVersionInfo, historyACLPolicyManager);
      LOG.info("Submitting DAG to YARN"
          + ", applicationId=" + appId
          + ", dagName=" + dag.getName());
      
      frameworkClient.submitApplication(appContext);
      ApplicationReport appReport = frameworkClient.getApplicationReport(appId);
      LOG.info("The url to track the Tez AM: " + appReport.getTrackingUrl());
      lastSubmittedAppId = appId;
    } catch (YarnException e) {
      throw new TezException(e);
    }
    return getDAGClient(appId, amConfig.getTezConfiguration(), frameworkClient);
  }

  private ApplicationId createApplication() throws TezException, IOException {
    try {
      return frameworkClient.createApplication().
          getNewApplicationResponse().getApplicationId();
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  private synchronized Map<String, LocalResource> getTezJarResources(Credentials credentials)
      throws IOException {
    if (cachedTezJarResources == null) {
      cachedTezJarResources = new HashMap<String, LocalResource>();
      usingTezArchiveDeploy = TezClientUtils.setupTezJarsLocalResources(
          amConfig.getTezConfiguration(), credentials, cachedTezJarResources);
    }
    return cachedTezJarResources;
  }

  @Private // Used only for MapReduce compatibility code
  static DAGClient getDAGClient(ApplicationId appId, TezConfiguration tezConf,
                                FrameworkClient frameworkClient)
      throws IOException, TezException {
    return new DAGClientImpl(appId, getDefaultTezDAGID(appId), tezConf, frameworkClient);
  }

  // DO NOT CHANGE THIS. This code is replicated from TezDAGID.java
  private static final char SEPARATOR = '_';
  public static final String DAG = "dag";
  static final ThreadLocal<NumberFormat> tezAppIdFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(4);
      return fmt;
    }
  };

  static final ThreadLocal<NumberFormat> tezDagIdFormat = new ThreadLocal<NumberFormat>() {
    @Override
    public NumberFormat initialValue() {
      NumberFormat fmt = NumberFormat.getInstance();
      fmt.setGroupingUsed(false);
      fmt.setMinimumIntegerDigits(1);
      return fmt;
    }
  };

  // Used only for MapReduce compatibility code
  private static String getDefaultTezDAGID(ApplicationId applicationId) {
     return (new StringBuilder(DAG)).append(SEPARATOR).
         append(applicationId.getClusterTimestamp()).
         append(SEPARATOR).
         append(tezAppIdFormat.get().format(applicationId.getId())).
         append(SEPARATOR).
         append(tezDagIdFormat.get().format(1)).toString();
  }
}

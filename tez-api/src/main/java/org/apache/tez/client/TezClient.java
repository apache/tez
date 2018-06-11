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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.JavaOptsChecker;
import org.apache.tez.common.RPCUtil;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.dag.api.TezConfigurationConstants;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.util.Time;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DAGSubmissionTimedOut;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.HistoryLogLevel;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotReady;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

  private static final String appIdStrPrefix = "application";
  private static final String APPLICATION_ID_PREFIX = appIdStrPrefix + '_';
  private static final long PREWARM_WAIT_MS = 500;
  
  @VisibleForTesting
  static final String NO_CLUSTER_DIAGNOSTICS_MSG = "No cluster diagnostics found.";

  @VisibleForTesting
  final String clientName;
  private ApplicationId sessionAppId;
  private ApplicationId lastSubmittedAppId;
  @VisibleForTesting
  final AMConfiguration amConfig;
  private FrameworkClient frameworkClient;
  private String diagnostics;
  @VisibleForTesting
  final boolean isSession;
  private final AtomicBoolean sessionStarted = new AtomicBoolean(false);
  private final AtomicBoolean sessionStopped = new AtomicBoolean(false);
  /** Tokens which will be required for all DAGs submitted to this session. */
  private Credentials sessionCredentials = new Credentials();
  private long clientTimeout;
  Map<String, LocalResource> cachedTezJarResources;
  boolean usingTezArchiveDeploy = false;
  private static final long SLEEP_FOR_READY = 500;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private final Map<String, LocalResource> additionalLocalResources = Maps.newHashMap();
  @VisibleForTesting
  final TezApiVersionInfo apiVersionInfo;
  @VisibleForTesting
  final ServicePluginsDescriptor servicePluginsDescriptor;
  private JavaOptsChecker javaOptsChecker = null;
  private DAGClient prewarmDagClient = null;
  private int preWarmDAGCounter = 0;

  /* max submitDAG request size through IPC; beyond this we transfer them in the same way we transfer local resource */
  private int maxSubmitDAGRequestSizeThroughIPC;
  /* this counter counts number of serialized DAGPlan and is used to give unique name to each serialized DAGPlan */
  private AtomicInteger serializedSubmitDAGPlanRequestCounter = new AtomicInteger(0);
  private FileSystem stagingFs = null;

  private ScheduledExecutorService amKeepAliveService;

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
    this(name, tezConf, isSession, localResources, credentials, null);
  }

  @Private
  protected TezClient(String name, TezConfiguration tezConf, boolean isSession,
            @Nullable Map<String, LocalResource> localResources,
            @Nullable Credentials credentials, ServicePluginsDescriptor servicePluginsDescriptor) {
    this.clientName = name;
    this.isSession = isSession;
    // Set in conf for local mode AM to figure out whether in session mode or not
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);
    try {
      InetAddress ip = InetAddress.getLocalHost();
      if (ip != null) {
        tezConf.set(TezConfigurationConstants.TEZ_SUBMIT_HOST, ip.getCanonicalHostName());
        tezConf.set(TezConfigurationConstants.TEZ_SUBMIT_HOST_ADDRESS, ip.getHostAddress());
      }
    } catch (UnknownHostException e) {
      LOG.warn("The host name of the client the tez application was submitted from was unable to be retrieved", e);
    }

    this.amConfig = new AMConfiguration(tezConf, localResources, credentials);
    this.apiVersionInfo = new TezApiVersionInfo();
    this.servicePluginsDescriptor = servicePluginsDescriptor;
    this.maxSubmitDAGRequestSizeThroughIPC = tezConf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT) -
        tezConf.getInt(TezConfiguration.TEZ_IPC_PAYLOAD_RESERVED_BYTES,
        TezConfiguration.TEZ_IPC_PAYLOAD_RESERVED_BYTES_DEFAULT);
    Limits.setConfiguration(tezConf);

    LOG.info("Tez Client Version: " + apiVersionInfo.toString());
  }


  /**
   * Create a new TezClientBuilder. This can be used to setup additional parameters
   * like session mode, local resources, credentials, servicePlugins, etc.
   * <p/>
   * If session mode is not specified in the builder, this will be inferred from
   * the provided TezConfiguration.
   *
   * @param name    Name of the client. Used for logging etc. This will also be used
   *                as app master name is session mode
   * @param tezConf Configuration for the framework
   * @return An instance of {@link org.apache.tez.client.TezClient.TezClientBuilder}
   * which can be used to construct the final TezClient.
   */
  public static TezClientBuilder newBuilder(String name, TezConfiguration tezConf) {
    return new TezClientBuilder(name, tezConf);
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
   * @param localFiles the files to be made available in the AM
   */
  public synchronized void addAppMasterLocalFiles(Map<String, LocalResource> localFiles) {
    Preconditions.checkNotNull(localFiles);
    if (isSession && sessionStarted.get()) {
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
   * @param credentials credentials
   */
  public synchronized void setAppMasterCredentials(Credentials credentials) {
    Preconditions
        .checkState(!sessionStarted.get(),
            "Credentials cannot be set after the session App Master has been started");
    amConfig.setCredentials(credentials);
  }

  /**
   * Sets the history log level for this session. It will be in effect for DAGs submitted after this
   * call.
   *
   * @param historyLogLevel The log level to be used.
   */
  public synchronized void setHistoryLogLevel(HistoryLogLevel historyLogLevel) {
    amConfig.getTezConfiguration().setEnum(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL,
        historyLogLevel);
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
    startFrameworkClient();
    setupJavaOptsChecker();

    if (isSession) {
      LOG.info("Session mode. Starting session.");
      TezClientUtils.processTezLocalCredentialsFile(sessionCredentials,
          amConfig.getTezConfiguration());
  
      clientTimeout = amConfig.getTezConfiguration().getInt(
          TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS,
          TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT);
  
      try {
        if (sessionAppId == null) {
          sessionAppId = createApplication();
        }
  
        ApplicationSubmissionContext appContext = setupApplicationContext();
        frameworkClient.submitApplication(appContext);
        ApplicationReport appReport = frameworkClient.getApplicationReport(sessionAppId);
        LOG.info("The url to track the Tez Session: " + appReport.getTrackingUrl());
        sessionStarted.set(true);
      } catch (YarnException e) {
        throw new TezException(e);
      }

      startClientHeartbeat();
      this.stagingFs = FileSystem.get(amConfig.getTezConfiguration());
    }
  }

  public synchronized TezClient getClient(String appIdStr) throws IOException, TezException {
    return getClient(appIdfromString(appIdStr));
  }

  /**
   * Alternative to start() that explicitly sets sessionAppId and doesn't start a new AM.
   * The caller of getClient is responsible for initializing the new TezClient with a
   * Configuration compatible with the existing AM. It is expected the caller has cached the
   * original Configuration (e.g. in Zookeeper).
   *
   * In contrast to "start", no resources are localized. It is the responsibility of the caller to
   * ensure that existing localized resources and staging dirs are still valid.
   *
   * @param appId
   * @return 'this' just as a convenience for fluent style chaining
   */
  public synchronized TezClient getClient(ApplicationId appId) throws TezException, IOException {
    sessionAppId = appId;
    amConfig.setYarnConfiguration(new YarnConfiguration(amConfig.getTezConfiguration()));
    startFrameworkClient();
    setupJavaOptsChecker();

    if (!isSession) {
      String msg = "Must be in session mode to bind TezClient to existing AM";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    LOG.info("Session mode. Reconnecting to session: " + sessionAppId.toString());

    clientTimeout = amConfig.getTezConfiguration().getInt(
            TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS,
            TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT);

    try {
      setupApplicationContext();
      ApplicationReport appReport = frameworkClient.getApplicationReport(sessionAppId);
      LOG.info("The url to track the Tez Session: " + appReport.getTrackingUrl());
      sessionStarted.set(true);
    } catch (YarnException e) {
      throw new TezException(e);
    }

    startClientHeartbeat();
    this.stagingFs = FileSystem.get(amConfig.getTezConfiguration());
    return this;
  }

  private void startFrameworkClient() {
    frameworkClient = createFrameworkClient();
    frameworkClient.init(amConfig.getTezConfiguration(), amConfig.getYarnConfiguration());
    frameworkClient.start();
  }

  private ApplicationSubmissionContext setupApplicationContext() throws IOException, YarnException {
    TezClientUtils.processTezLocalCredentialsFile(sessionCredentials,
            amConfig.getTezConfiguration());

    Map<String, LocalResource> tezJarResources = getTezJarResources(sessionCredentials);
    // Add session token for shuffle
    TezClientUtils.createSessionToken(sessionAppId.toString(),
            jobTokenSecretManager, sessionCredentials);

    ApplicationSubmissionContext appContext =
            TezClientUtils.createApplicationSubmissionContext(
                    sessionAppId,
                    null, clientName, amConfig,
                    tezJarResources, sessionCredentials, usingTezArchiveDeploy, apiVersionInfo,
                    servicePluginsDescriptor, javaOptsChecker);

    // Set Tez Sessions to not retry on AM crashes if recovery is disabled
    if (!amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.DAG_RECOVERY_ENABLED,
            TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT)) {
      appContext.setMaxAppAttempts(1);
    }
    return appContext;
  }

  private void setupJavaOptsChecker() {
    if (this.amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_ENABLED,
            TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_ENABLED_DEFAULT)) {
      String javaOptsCheckerClassName = this.amConfig.getTezConfiguration().get(
              TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_CLASS, "");
      if (!javaOptsCheckerClassName.isEmpty()) {
        try {
          javaOptsChecker = ReflectionUtils.createClazzInstance(javaOptsCheckerClassName);
        } catch (Exception e) {
          LOG.warn("Failed to initialize configured Java Opts Checker"
                  + " (" + TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_CLASS
                  + ") , checkerClass=" + javaOptsCheckerClassName
                  + ". Disabling checker.", e);
          javaOptsChecker = null;
        }
      } else {
        javaOptsChecker = new JavaOptsChecker();
      }

    }
  }

  private void startClientHeartbeat() {
    long amClientKeepAliveTimeoutIntervalMillis =
            TezCommonUtils.getAMClientHeartBeatTimeoutMillis(amConfig.getTezConfiguration());
    // Poll at minimum of 1 second interval
    long pollPeriod = TezCommonUtils.
            getAMClientHeartBeatPollIntervalMillis(amConfig.getTezConfiguration(),
                    amClientKeepAliveTimeoutIntervalMillis, 10);

    boolean isLocal = amConfig.getTezConfiguration().getBoolean(
            TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);
    if (!isLocal && amClientKeepAliveTimeoutIntervalMillis > 0) {
      amKeepAliveService = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                      .setDaemon(true).setNameFormat("AMKeepAliveThread #%d").build());
      amKeepAliveService.scheduleWithFixedDelay(new Runnable() {

        private DAGClientAMProtocolBlockingPB proxy;

        @Override
        public void run() {
          proxy = sendAMHeartbeat(proxy);
        }
      }, pollPeriod, pollPeriod, TimeUnit.MILLISECONDS);
    }
  }

  public DAGClientAMProtocolBlockingPB sendAMHeartbeat(DAGClientAMProtocolBlockingPB proxy) {
    if (sessionStopped.get()) {
      // Ignore sending heartbeat as session being stopped
      return null;
    }
    try {
      if (proxy == null) {
        try {
          proxy = waitForProxy();
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while trying to create a connection to the AM", e);
        } catch (SessionNotRunning e) {
          LOG.error("Cannot create a connection to the AM, stopping heartbeat to AM", e);
          cancelAMKeepAlive(false);
        }
      }
      if (proxy != null) {
        LOG.debug("Sending heartbeat to AM");
        proxy.getAMStatus(null, GetAMStatusRequestProto.newBuilder().build());
      }
      return proxy;
    } catch (Exception e) {
      LOG.info("Exception when sending heartbeat to AM for app {}: {}", sessionAppId,
          e.getMessage());
      LOG.debug("Error when sending heartbeat ping to AM. Resetting AM proxy for app: {}"
          + " due to exception :", sessionAppId, e);
      return null;
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
    DAGClient result = isSession ? submitDAGSession(dag) : submitDAGApplication(dag);
    if (result != null) {
      closePrewarmDagClient(); // Assume the current DAG replaced the prewarm one; no need to kill.
    }
    return result;
  }

  private void killAndClosePrewarmDagClient(long waitTimeMs) {
    if (prewarmDagClient == null) {
      return;
    }
    try {
      prewarmDagClient.tryKillDAG();
      if (waitTimeMs > 0) {
        LOG.info("Waiting for prewarm DAG to shut down");
        prewarmDagClient.waitForCompletion(waitTimeMs);
      }
    }
    catch (Exception ex) {
      LOG.warn("Failed to shut down the prewarm DAG " + prewarmDagClient, ex);
    }
    closePrewarmDagClient();
  }

  private void closePrewarmDagClient() {
    if (prewarmDagClient == null) {
      return;
    }
    try {
      prewarmDagClient.close();
    } catch (Exception e) {
      LOG.warn("Failed to close prewarm DagClient " + prewarmDagClient, e);
    }
    prewarmDagClient = null;
  }
  
  private DAGClient submitDAGSession(DAG dag) throws TezException, IOException {
    Preconditions.checkState(isSession == true, 
        "submitDAG with additional resources applies to only session mode. " + 
        "In non-session mode please specify all resources in the initial configuration");
    
    verifySessionStateForSubmission();

    String dagId = null;
    String callerContextStr = "";
    if (dag.getCallerContext() != null) {
      callerContextStr = ", callerContext=" + dag.getCallerContext().contextAsSimpleString();
    }
    LOG.info("Submitting dag to TezSession"
      + ", sessionName=" + clientName
      + ", applicationId=" + sessionAppId
      + ", dagName=" + dag.getName()
      + callerContextStr);
    
    if (!additionalLocalResources.isEmpty()) {
      for (LocalResource lr : additionalLocalResources.values()) {
        Preconditions.checkArgument(lr.getType() == LocalResourceType.FILE, "LocalResourceType: "
            + lr.getType() + " is not supported, only " + LocalResourceType.FILE + " is supported");
      }
    }

    Map<String, LocalResource> tezJarResources = getTezJarResources(sessionCredentials);
    DAGPlan dagPlan = TezClientUtils.prepareAndCreateDAGPlan(dag, amConfig, tezJarResources,
        usingTezArchiveDeploy, sessionCredentials, servicePluginsDescriptor, javaOptsChecker);

    SubmitDAGRequestProto.Builder requestBuilder = SubmitDAGRequestProto.newBuilder();
    requestBuilder.setDAGPlan(dagPlan);
    if (!additionalLocalResources.isEmpty()) {
      requestBuilder.setAdditionalAmResources(DagTypeConverters
          .convertFromLocalResources(additionalLocalResources));
    }
    
    additionalLocalResources.clear();

    // if request size exceeds maxSubmitDAGRequestSizeThroughIPC, we serialize them to HDFS
    SubmitDAGRequestProto request = requestBuilder.build();
    if (request.getSerializedSize() > maxSubmitDAGRequestSizeThroughIPC) {
      Path dagPlanPath = new Path(TezCommonUtils.getTezSystemStagingPath(amConfig.getTezConfiguration(),
          sessionAppId.toString()), TezConstants.TEZ_PB_PLAN_BINARY_NAME +
          serializedSubmitDAGPlanRequestCounter.incrementAndGet());

      try (FSDataOutputStream fsDataOutputStream = stagingFs.create(dagPlanPath, false)) {
        LOG.info("Send dag plan using YARN local resources since it's too large"
            + ", dag plan size=" + request.getSerializedSize()
            + ", max dag plan size through IPC=" + maxSubmitDAGRequestSizeThroughIPC
            + ", max IPC message size= " + amConfig.getTezConfiguration().getInt(
            CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT));
        request.writeTo(fsDataOutputStream);
        request = requestBuilder.clear().setSerializedRequestPath(stagingFs.resolvePath(dagPlanPath).toString()).build();
      }
    }

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
      SubmitDAGResponseProto response = proxy.submitDAG(null, request);
      // the following check is only for testing since the final class
      // SubmitDAGResponseProto cannot be mocked
      if (response != null) {
        dagId = response.getDagId();
      }
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
    }

    LOG.info("Submitted dag to TezSession"
        + ", sessionName=" + clientName
        + ", applicationId=" + sessionAppId
        + ", dagId=" + dagId
        + ", dagName=" + dag.getName());
    return new DAGClientImpl(sessionAppId, dagId,
        amConfig.getTezConfiguration(),
        amConfig.getYarnConfiguration(),
        frameworkClient);
  }

  @VisibleForTesting
  protected long getPrewarmWaitTimeMs() {
    return PREWARM_WAIT_MS;
  }

  /**
   * Stop the client. This terminates the connection to the YARN cluster.
   * In session mode, this shuts down the session DAG App Master
   * @throws TezException
   * @throws IOException
   */
  public synchronized void stop() throws TezException, IOException {
    killAndClosePrewarmDagClient(getPrewarmWaitTimeMs());
    try {
      if (amKeepAliveService != null) {
        amKeepAliveService.shutdownNow();
      }
      if (sessionStarted.get()) {
        LOG.info("Shutting down Tez Session"
            + ", sessionName=" + clientName
            + ", applicationId=" + sessionAppId);
        sessionStopped.set(true);
        boolean sessionShutdownSuccessful = false;
        try {
          DAGClientAMProtocolBlockingPB proxy = getAMProxy(sessionAppId);
          if (proxy != null) {
            ShutdownSessionRequestProto request =
                ShutdownSessionRequestProto.newBuilder().build();
            proxy.shutdownSession(null, request);
            sessionShutdownSuccessful = true;
            boolean asynchronousStop = amConfig.getTezConfiguration().getBoolean(
                TezConfiguration.TEZ_CLIENT_ASYNCHRONOUS_STOP,
                TezConfiguration.TEZ_CLIENT_ASYNCHRONOUS_STOP_DEFAULT);
            if (!asynchronousStop) {
              LOG.info("Waiting until application is in a final state");
              long currentTimeMillis = System.currentTimeMillis();
              long timeKillIssued = currentTimeMillis;
              long killTimeOut = amConfig.getTezConfiguration().getLong(
                  TezConfiguration.TEZ_CLIENT_HARD_KILL_TIMEOUT_MS,
                  TezConfiguration.TEZ_CLIENT_HARD_KILL_TIMEOUT_MS_DEFAULT);
              ApplicationReport appReport = frameworkClient
                  .getApplicationReport(sessionAppId);
              while ((currentTimeMillis < timeKillIssued + killTimeOut)
                  && !isJobInTerminalState(appReport.getYarnApplicationState())) {
                try {
                  Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                  /** interrupted, just break */
                  break;
                }
                currentTimeMillis = System.currentTimeMillis();
                appReport = frameworkClient.getApplicationReport(sessionAppId);
              }

              if (!isJobInTerminalState(appReport.getYarnApplicationState())) {
                frameworkClient.killApplication(sessionAppId);
              }
            }
          }
        } catch (TezException e) {
          LOG.info("Failed to shutdown Tez Session via proxy", e);
        } catch (ServiceException e) {
          LOG.info("Failed to shutdown Tez Session via proxy", e);
        } catch (ApplicationNotFoundException e) {
          LOG.info("Failed to kill nonexistent application " + sessionAppId, e);
        } catch (YarnException e) {
          throw new TezException(e);
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
  private boolean isJobInTerminalState(YarnApplicationState yarnApplicationState) {
    return (yarnApplicationState == YarnApplicationState.FINISHED
        || yarnApplicationState == YarnApplicationState.FAILED
        || yarnApplicationState == YarnApplicationState.KILLED);
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
        try {
          DAGClientAMProtocolBlockingPB proxy = getAMProxy(appId);
          if (proxy == null) {
            return TezAppMasterStatus.INITIALIZING;
          }
          GetAMStatusResponseProto response = proxy.getAMStatus(null,
              GetAMStatusRequestProto.newBuilder().build());
          return DagTypeConverters.convertTezAppMasterStatusFromProto(
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
    preWarm(preWarmVertex, 0, TimeUnit.MILLISECONDS);
  }

  /**
   * API to help pre-allocate containers in session mode. In non-session mode
   * this is ignored. The pre-allocated containers may be re-used by subsequent
   * job DAGs to improve performance.
   * The preWarm vertex should be configured and setup exactly
   * like the other vertices in the job DAGs so that the pre-allocated
   * containers may be re-used by the subsequent DAGs to improve performance.
   * The processor for the preWarmVertex may be used to pre-warm the containers
   * by pre-loading classes etc. It should be short-running so that pre-warming
   * does not block real execution. Users can specify their custom processors or
   * use the PreWarmProcessor from the runtime library.
   * The parallelism of the preWarmVertex will determine the number of preWarmed
   * containers.
   * Pre-warming is best efforts and among other factors is limited by the free
   * resources on the cluster. Based on the specified timeout value it returns
   * false if the status is not READY after the wait period.
   * @param preWarmVertex
   * @param timeout
   * @param unit
   * @throws TezException
   * @throws IOException
   */
  @Unstable
  public synchronized void preWarm(PreWarmVertex preWarmVertex,
      long timeout, TimeUnit unit)
      throws TezException, IOException {
    if (!isSession) {
      // do nothing for non session mode. This is there to let the code
      // work correctly in both modes
      LOG.warn("preWarm is not supported in non-session mode," +
          "please use session-mode of TezClient");
      return;
    }

    verifySessionStateForSubmission();
    
    DAG dag = org.apache.tez.dag.api.DAG.create(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX + "_"
        + preWarmDAGCounter++);
    dag.addVertex(preWarmVertex);

    boolean isReady;
    try {
      isReady = waitTillReady(timeout, unit);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for AM to become " +
          "available", e);
    }
    if(isReady) {
      prewarmDagClient = submitDAG(dag);
    } else {
      throw new SessionNotReady("Tez AM not ready, could not submit DAG");
    }
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
    waitTillReady(0, TimeUnit.MILLISECONDS);
  }

  /**
   * Wait till the DAG is ready to be submitted.
   * In non-session mode this is a no-op since the application can be
   * immediately submitted.
   * In session mode, this waits for the session host to be ready to accept
   * a DAG and returns false if not ready after a configured time wait period.
   * @param timeout
   * @param unit
   * @return true if READY or is not in session mode, false otherwise.
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException
   */
  @Evolving
  public synchronized boolean waitTillReady(long timeout, TimeUnit unit)
      throws IOException, TezException, InterruptedException {
    timeout = unit.toMillis(timeout);
    if (!isSession) {
      // nothing to wait for in non-session mode
      return true;
    }

    verifySessionStateForSubmission();
    long startTime = Time.monotonicNow();
    long timeLimit = startTime + timeout;
    while (true) {
      TezAppMasterStatus status = getAppMasterStatus();
      if (status.equals(TezAppMasterStatus.SHUTDOWN)) {
        throw new SessionNotRunning("TezSession has already shutdown. "
            + ((diagnostics != null) ? diagnostics : NO_CLUSTER_DIAGNOSTICS_MSG));
      }
      if (status.equals(TezAppMasterStatus.READY)) {
        return true;
      }
      if (timeout == 0) {
        Thread.sleep(SLEEP_FOR_READY);
        continue;
      }
      long now = Time.monotonicNow();
      if (timeLimit > now) {
        long sleepTime = Math.min(SLEEP_FOR_READY, timeLimit - now);
        Thread.sleep(sleepTime);
      } else {
        return false;
      }
    }
  }

  private void waitNonSessionTillReady() throws IOException, TezException {
    Preconditions.checkArgument(!isSession, "It is supposed to be only called in non-session mode");
    while (true) {
      TezAppMasterStatus status = getAppMasterStatus();
      // DAGClient will handle the AM SHUTDOWN case
      if (status.equals(TezAppMasterStatus.RUNNING)
          || status.equals(TezAppMasterStatus.SHUTDOWN)) {
        return;
      }
      try {
        Thread.sleep(SLEEP_FOR_READY);
      } catch (InterruptedException e) {
        throw new TezException("TezClient is interrupted");
      }
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
  protected DAGClientAMProtocolBlockingPB getAMProxy(ApplicationId appId)
      throws TezException, IOException {
    return TezClientUtils.getAMProxy(
        frameworkClient, amConfig.getYarnConfiguration(), appId);
  }

  private DAGClientAMProtocolBlockingPB waitForProxy()
      throws IOException, TezException, InterruptedException {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + (clientTimeout * 1000);
    DAGClientAMProtocolBlockingPB proxy = null;
    while (true) {
      proxy = getAMProxy(sessionAppId);
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
    if (!sessionStarted.get()) {
      throw new SessionNotRunning("Session not started");
    } else if (sessionStopped.get()) {
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
              usingTezArchiveDeploy, apiVersionInfo, servicePluginsDescriptor, javaOptsChecker);
      String callerContextStr = "";
      if (dag.getCallerContext() != null) {
        callerContextStr = ", callerContext=" + dag.getCallerContext().contextAsSimpleString();
      }
      LOG.info("Submitting DAG to YARN"
          + ", applicationId=" + appId
          + ", dagName=" + dag.getName()
          + callerContextStr);
      
      frameworkClient.submitApplication(appContext);
      ApplicationReport appReport = frameworkClient.getApplicationReport(appId);
      LOG.info("The url to track the Tez AM: " + appReport.getTrackingUrl());
      lastSubmittedAppId = appId;
    } catch (YarnException e) {
      throw new TezException(e);
    }
    // wait for dag in non-session mode to start running, so that we can start to getDAGStatus
    waitNonSessionTillReady();
    return getDAGClient(appId, amConfig.getTezConfiguration(), amConfig.getYarnConfiguration(),
        frameworkClient);
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

  @Private
  static DAGClient getDAGClient(ApplicationId appId, TezConfiguration tezConf, YarnConfiguration
      yarnConf, FrameworkClient frameworkClient)
      throws IOException, TezException {
    return new DAGClientImpl(appId, getDefaultTezDAGID(appId), tezConf,
        yarnConf, frameworkClient);
  }

  @Private // Used only for MapReduce compatibility code
  static DAGClient getDAGClient(ApplicationId appId, TezConfiguration tezConf,
                                FrameworkClient frameworkClient)
      throws IOException, TezException {
    return getDAGClient(appId, tezConf, new YarnConfiguration(tezConf), frameworkClient);
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

  @VisibleForTesting
  @Private
  public synchronized void cancelAMKeepAlive(boolean shutdownNow) {
    if (amKeepAliveService != null) {
      if (shutdownNow) {
        amKeepAliveService.shutdownNow();
      } else {
        amKeepAliveService.shutdown();
      }
    }
  }

  @VisibleForTesting
  protected synchronized ScheduledExecutorService getAMKeepAliveService() {
    return amKeepAliveService;
  }

  /**
   * A builder for setting up an instance of {@link org.apache.tez.client.TezClient}
   */
  @Public
  public static class TezClientBuilder {
    final String name;
    final TezConfiguration tezConf;
    boolean isSession;
    private Map<String, LocalResource> localResourceMap;
    private Credentials credentials;
    ServicePluginsDescriptor servicePluginsDescriptor;

    /**
     * Create an instance of a TezClientBuilder
     *
     * @param name
     *          Name of the client. Used for logging etc. This will also be used
     *          as app master name is session mode
     * @param tezConf
     *          Configuration for the framework
     */
    private TezClientBuilder(String name, TezConfiguration tezConf) {
      this.name = name;
      this.tezConf = tezConf;
      isSession = tezConf.getBoolean(
          TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT);
    }

    /**
     * Specify whether this client is a session or not
     * @param isSession whether the client is a session
     * @return the current builder
     */
    public TezClientBuilder setIsSession(boolean isSession) {
      this.isSession = isSession;
      return this;
    }

    /**
     * Set local resources to be used by the AppMaster
     *
     * @param localResources local files for the App Master
     * @return the files to be added to the AM
     */
    public TezClientBuilder setLocalResources(Map<String, LocalResource> localResources) {
      this.localResourceMap = localResources;
      return this;
    }

    /**
     * Setup security credentials
     *
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
     * @return the current builder
     */
    public TezClientBuilder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    /**
     * Specify the service plugins that will be running in the AM
     * @param servicePluginsDescriptor the service plugin descriptor with details about the plugins running in the AM
     * @return the current builder
     */
    public TezClientBuilder setServicePluginDescriptor(ServicePluginsDescriptor servicePluginsDescriptor) {
      this.servicePluginsDescriptor = servicePluginsDescriptor;
      return this;
    }

    /**
     * Build the actual instance of the {@link TezClient}
     * @return an instance of {@link TezClient}
     */
    public TezClient build() {
      return new TezClient(name, tezConf, isSession, localResourceMap, credentials,
          servicePluginsDescriptor);
    }
  }

  //Copied this helper method from 
  //org.apache.hadoop.yarn.api.records.ApplicationId in Hadoop 2.8+
  //to simplify implementation on 2.7.x
  @Public
  @Unstable
  public static ApplicationId appIdfromString(String appIdStr) {
    if (!appIdStr.startsWith(APPLICATION_ID_PREFIX)) {
      throw new IllegalArgumentException("Invalid ApplicationId prefix: "
              + appIdStr + ". The valid ApplicationId should start with prefix "
              + appIdStrPrefix);
    }
    try {
      int pos1 = APPLICATION_ID_PREFIX.length() - 1;
      int pos2 = appIdStr.indexOf('_', pos1 + 1);
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid ApplicationId: "
                + appIdStr);
      }
      long rmId = Long.parseLong(appIdStr.substring(pos1 + 1, pos2));
      int appId = Integer.parseInt(appIdStr.substring(pos2 + 1));
      ApplicationId applicationId = ApplicationId.newInstance(rmId, appId);
      return applicationId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid ApplicationId: "
              + appIdStr, n);
    }
  }
}

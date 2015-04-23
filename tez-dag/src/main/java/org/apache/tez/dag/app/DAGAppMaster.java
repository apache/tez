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

package org.apache.tez.dag.app;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDagCleanup;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.GcTimeUpdater;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezConverterUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourcesProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.RecoveryParser.RecoveredDAGData;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventSchedulingServiceError;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventRecoverEvent;
import org.apache.tez.dag.app.dag.event.DAGEventStartDag;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.SpeculatorEvent;
import org.apache.tez.dag.app.dag.event.SpeculatorEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.launcher.ContainerLauncher;
import org.apache.tez.dag.app.launcher.ContainerLauncherImpl;
import org.apache.tez.dag.app.launcher.LocalContainerLauncher;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.app.rm.node.AMNodeTracker;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.Graph;
import org.apache.tez.dag.utils.RelocalizationUtils;
import org.apache.tez.dag.utils.Simple2LevelVersionComparator;
import org.codehaus.jettison.json.JSONException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The Tez DAG Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event
 * results in a Finite State Transition in Job.
 *
 * Tez DAG AppMaster is the composition of loosely coupled services. The services
 * interact with each other via events. The components resembles the
 * Actors model. The component acts on received event and send out the
 * events to other components.
 * This keeps it highly concurrent with no or minimal synchronization needs.
 *
 * The events are dispatched by a central Dispatch mechanism. All components
 * register to the Dispatcher.
 *
 * The information is shared across different components using AppContext.
 */

@SuppressWarnings("rawtypes")
public class DAGAppMaster extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(DAGAppMaster.class);

  /**
   * Priority of the DAGAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static Pattern sanitizeLabelPattern = Pattern.compile("[:\\-\\W]+");

  private Clock clock;
  private final boolean isSession;
  private long appsStartTime;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  private final String workingDirectory;
  private final String[] localDirs;
  private final String[] logDirs;
  private ContainerSignatureMatcher containerSignatureMatcher;
  private AMContainerMap containers;
  private AMNodeTracker nodes;
  private AppContext context;
  private Configuration amConf;
  private AsyncDispatcher dispatcher;
  private ContainerLauncher containerLauncher;
  private ContainerHeartbeatHandler containerHeartbeatHandler;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private Token<JobTokenIdentifier> sessionToken;
  private DagEventDispatcher dagEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private TaskSchedulerEventHandler taskSchedulerEventHandler;
  private WebUIService webUIService;
  private HistoryEventHandler historyEventHandler;
  private final Map<String, LocalResource> amResources = new HashMap<String, LocalResource>();
  private final Map<String, LocalResource> cumulativeAdditionalResources = new HashMap<String, LocalResource>();
  private final int maxAppAttempts;
  private final List<String> diagnostics = new ArrayList<String>();

  private boolean isLocal = false; //Local mode flag

  @VisibleForTesting
  protected DAGAppMasterShutdownHandler shutdownHandler;
  private final AtomicBoolean shutdownHandlerRunning = new AtomicBoolean(false);

  private DAGAppMasterState state;

  DAGClientServer clientRpcServer;
  private DAGClientHandler clientHandler;

  private DAG currentDAG;
  private final Credentials amCredentials;
  private final UserGroupInformation appMasterUgi;

  private AtomicBoolean sessionStopped = new AtomicBoolean(false);
  private long sessionTimeoutInterval;
  private long lastDAGCompletionTime;
  private Timer dagSubmissionTimer;
  private boolean recoveryEnabled;
  private Path recoveryDataDir;
  private Path currentRecoveryDataDir;
  private Path tezSystemStagingDir;
  private FileSystem recoveryFS;
  
  private ExecutorService rawExecutor;
  private ListeningExecutorService execService;
  
  /**
   * set of already executed dag names.
   */
  Set<String> dagNames = new HashSet<String>();
  Set<String> dagIDs = new HashSet<String>();

  protected boolean isLastAMRetry = false;

  // DAG Counter
  private final AtomicInteger dagCounter = new AtomicInteger();

  // Session counters
  private final AtomicInteger submittedDAGs = new AtomicInteger();
  private final AtomicInteger successfulDAGs = new AtomicInteger();
  private final AtomicInteger failedDAGs = new AtomicInteger();
  private final AtomicInteger killedDAGs = new AtomicInteger();
  private ACLManager aclManager;

  // Version checks
  private TezDagVersionInfo dagVersionInfo;
  private String clientVersion;
  private boolean versionMismatch = false;
  private String versionMismatchDiagnostics;
  
  private ResourceCalculatorProcessTree cpuPlugin;
  private GcTimeUpdater gcPlugin;

  // must be LinkedHashMap to preserve order of service addition
  Map<Service, ServiceWithDependency> services =
      new LinkedHashMap<Service, ServiceWithDependency>();

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, boolean isSession, String workingDirectory,
      String [] localDirs, String[] logDirs, String clientVersion, int maxAppAttempts,
      Credentials credentials, String jobUserName) {
    super(DAGAppMaster.class.getName());
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.state = DAGAppMasterState.NEW;
    this.isSession = isSession;
    this.workingDirectory = workingDirectory;
    this.localDirs = localDirs;
    this.logDirs = logDirs;
    this.shutdownHandler = new DAGAppMasterShutdownHandler();
    this.dagVersionInfo = new TezDagVersionInfo();
    this.clientVersion = clientVersion;
    this.maxAppAttempts = maxAppAttempts;
    this.amCredentials = credentials;
    this.appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    this.appMasterUgi.addCredentials(amCredentials);

    LOG.info("Created DAGAppMaster for application " + applicationAttemptId
        + ", versionInfo=" + dagVersionInfo.toString());
  }
  
  private void initResourceCalculatorPlugins() {
    Class<? extends ResourceCalculatorProcessTree> clazz = amConf.getClass(
        TezConfiguration.TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS, null,
        ResourceCalculatorProcessTree.class);

    // this is set by YARN NM
    String pid = System.getenv().get("JVM_PID");
    // for the local debug test cases fallback to JVM hooks that are not portable
    if (pid == null || pid.length() == 0) {
      String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      pid = processName.split("@")[0];
    }
    cpuPlugin = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(pid, clazz, amConf);
    
    gcPlugin = new GcTimeUpdater(null);
  }
  
  private long getAMCPUTime() {
    if (cpuPlugin != null) {
      cpuPlugin.updateProcessTree();
      return cpuPlugin.getCumulativeCpuTime();
    }
    return 0;
  }

  private long getAMGCTime() {
    if (gcPlugin != null) {
      return gcPlugin.getCumulativaGcTime();
    }
    return 0;
  }

  @Override
  public synchronized void serviceInit(final Configuration conf) throws Exception {

    this.amConf = conf;
    initResourceCalculatorPlugins();
    this.isLocal = conf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
        TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);

    boolean disableVersionCheck = conf.getBoolean(
        TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK,
        TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK_DEFAULT);

    isLastAMRetry = appAttemptID.getAttemptId() >= maxAppAttempts;

    // Check client - AM version compatibility
    LOG.info("Comparing client version with AM version"
        + ", clientVersion=" + clientVersion
        + ", AMVersion=" + dagVersionInfo.getVersion());
    Simple2LevelVersionComparator versionComparator = new Simple2LevelVersionComparator();
    if (versionComparator.compare(clientVersion, dagVersionInfo.getVersion()) != 0) {
      versionMismatchDiagnostics = "Incompatible versions found"
          + ", clientVersion=" + clientVersion
          + ", AMVersion=" + dagVersionInfo.getVersion();
      addDiagnostic(versionMismatchDiagnostics);
      if (disableVersionCheck) {
        LOG.warn("Ignoring client-AM version mismatch as check disabled. "
            + versionMismatchDiagnostics);
      } else {
        LOG.error(versionMismatchDiagnostics);
        versionMismatch = true;
      }
    }

    if (isLocal) {
       conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
       conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
           TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS_DEFAULT);
    }
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, !isLocal);
    String strAppId = this.appAttemptID.getApplicationId().toString();
    this.tezSystemStagingDir = TezCommonUtils.getTezSystemStagingPath(conf, strAppId);

    dispatcher = createDispatcher();
    context = new RunningAppContext(conf);
    this.aclManager = new ACLManager(appMasterUgi.getShortUserName(), this.amConf);

    clientHandler = new DAGClientHandler(this);

    addIfService(dispatcher, false);

    clientRpcServer = new DAGClientServer(clientHandler, appAttemptID);
    addIfService(clientRpcServer, true);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    addIfService(taskHeartbeatHandler, true);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    addIfService(containerHeartbeatHandler, true);

    sessionToken =
        TokenCache.getSessionToken(amCredentials);
    if (sessionToken == null) {
      throw new RuntimeException("Could not find session token in AM Credentials");
    }

    // Prepare the TaskAttemptListener server for authentication of Containers
    // TaskAttemptListener gets the information via jobTokenSecretManager.
    LOG.info("Adding session token to jobTokenSecretManager for application");
    jobTokenSecretManager.addTokenForJob(
        appAttemptID.getApplicationId().toString(), sessionToken);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context,
        taskHeartbeatHandler, containerHeartbeatHandler);
    addIfService(taskAttemptListener, true);

    containerSignatureMatcher = createContainerSignatureMatcher();
    containers = new AMContainerMap(containerHeartbeatHandler,
        taskAttemptListener, containerSignatureMatcher, context);
    addIfService(containers, true);
    dispatcher.register(AMContainerEventType.class, containers);

    nodes = new AMNodeTracker(dispatcher.getEventHandler(), context);
    addIfService(nodes, true);
    dispatcher.register(AMNodeEventType.class, nodes);

    this.dagEventDispatcher = new DagEventDispatcher();
    this.vertexEventDispatcher = new VertexEventDispatcher();

    //register the event dispatchers
    dispatcher.register(DAGAppMasterEventType.class, new DAGAppMasterEventHandler());
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
    
    // register other delegating dispatchers
    dispatcher.registerAndCreateDispatcher(SpeculatorEventType.class, new SpeculatorEventHandler(), "Speculator");


    if (enableWebUIService()) {
      this.webUIService = new WebUIService(context);
      addIfService(webUIService, false);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Web UI Service is not enabled.");
      }
    }

    this.taskSchedulerEventHandler = new TaskSchedulerEventHandler(context,
        clientRpcServer, dispatcher.getEventHandler(), containerSignatureMatcher, webUIService);
    addIfService(taskSchedulerEventHandler, true);

    if (enableWebUIService()) {
      addIfServiceDependency(taskSchedulerEventHandler, webUIService);
    }

    if (isLastAMRetry) {
      LOG.info("AM will unregister as this is the last attempt"
          + ", currentAttempt=" + appAttemptID.getAttemptId()
          + ", maxAttempts=" + maxAppAttempts);
      this.taskSchedulerEventHandler.setShouldUnregisterFlag();
    }

    dispatcher.register(AMSchedulerEventType.class,
        taskSchedulerEventHandler);
    addIfServiceDependency(taskSchedulerEventHandler, clientRpcServer);

    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher, true);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    historyEventHandler = createHistoryEventHandler(context);
    addIfService(historyEventHandler, true);

    this.sessionTimeoutInterval = 1000 * amConf.getInt(
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT);

    recoveryDataDir = TezCommonUtils.getRecoveryPath(tezSystemStagingDir, conf);
    recoveryFS = recoveryDataDir.getFileSystem(conf);
    currentRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir,
        appAttemptID.getAttemptId());
    if (LOG.isDebugEnabled()) {
      LOG.info("Stage directory information for AppAttemptId :" + this.appAttemptID
          + " tezSystemStagingDir :" + tezSystemStagingDir + " recoveryDataDir :" + recoveryDataDir
          + " recoveryAttemptDir :" + currentRecoveryDataDir);
    }
    recoveryEnabled = conf.getBoolean(TezConfiguration.DAG_RECOVERY_ENABLED,
        TezConfiguration.DAG_RECOVERY_ENABLED_DEFAULT);

    if (!versionMismatch) {
      if (isSession) {
        FileInputStream sessionResourcesStream = null;
        try {
          sessionResourcesStream = new FileInputStream(
              new File(workingDirectory, TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
          PlanLocalResourcesProto amLocalResourceProto = PlanLocalResourcesProto
              .parseDelimitedFrom(sessionResourcesStream);
          amResources.putAll(DagTypeConverters.convertFromPlanLocalResources(amLocalResourceProto));
        } finally {
          if (sessionResourcesStream != null) {
            sessionResourcesStream.close();
          }
        }
      }
    }

    rawExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("App Shared Pool - " + "#%d").build());
    execService = MoreExecutors.listeningDecorator(rawExecutor);

    initServices(conf);
    super.serviceInit(conf);

    if (!versionMismatch) {
      if (this.appAttemptID.getAttemptId() == 1) {
        AppLaunchedEvent appLaunchedEvent = new AppLaunchedEvent(appAttemptID.getApplicationId(),
            startTime, appSubmitTime, appMasterUgi.getShortUserName(), this.amConf,
            dagVersionInfo);
        historyEventHandler.handle(
            new DAGHistoryEvent(appLaunchedEvent));
      }
      AMLaunchedEvent launchedEvent = new AMLaunchedEvent(appAttemptID,
          startTime, appSubmitTime, appMasterUgi.getShortUserName());
      historyEventHandler.handle(
          new DAGHistoryEvent(launchedEvent));

      this.state = DAGAppMasterState.INITED;
    } else {
      this.state = DAGAppMasterState.ERROR;
    }
  }

  @VisibleForTesting
  protected ContainerSignatureMatcher createContainerSignatureMatcher() {
    return new ContainerContextMatcher();
  }
  
  @VisibleForTesting
  protected AsyncDispatcher createDispatcher() {
    return new AsyncDispatcher("Central");
  }

  @VisibleForTesting
  protected HistoryEventHandler createHistoryEventHandler(AppContext appContext) {
    return new HistoryEventHandler(appContext);
  }

  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    if (!isLocal) {
      System.exit(0);
    }
  }
  
  @VisibleForTesting
  protected TaskSchedulerEventHandler getTaskSchedulerEventHandler() {
    return taskSchedulerEventHandler;
  }

  @VisibleForTesting
  protected synchronized void handle(DAGAppMasterEvent event) {
    switch (event.getType()) {
    case SCHEDULING_SERVICE_ERROR:
      // Scheduling error - probably an issue with the communication with the RM
      // In this scenario, the AM should shutdown. Expectation is that the RM
      // will restart a new AM attempt.
      // Should not kill the current running DAG to ensure that on restart, we
      // can recover it and continue.
      DAGAppMasterEventSchedulingServiceError schedulingServiceErrorEvent =
          (DAGAppMasterEventSchedulingServiceError) event;
      state = DAGAppMasterState.ERROR;
      LOG.info("Error in the TaskScheduler. Shutting down.",
          schedulingServiceErrorEvent.getThrowable());
      shutdownHandler.shutdown();
      break;
    case INTERNAL_ERROR:
      state = DAGAppMasterState.ERROR;
      if(currentDAG != null) {
        _updateLoggers(currentDAG, "_post");
        // notify dag to finish which will send the DAG_FINISHED event
        LOG.info("Internal Error. Notifying dags to finish.");
        sendEvent(new DAGEvent(currentDAG.getID(), DAGEventType.INTERNAL_ERROR));
      } else {
        LOG.info("Internal Error. Finishing directly as no dag is active.");
        this.taskSchedulerEventHandler.setShouldUnregisterFlag();
        shutdownHandler.shutdown();
      }
      break;
    case DAG_FINISHED:
      DAGAppMasterEventDAGFinished finishEvt =
          (DAGAppMasterEventDAGFinished) event;
      if (!isSession) {
        LOG.info("Not a session, AM will unregister as DAG has completed");
        this.taskSchedulerEventHandler.setShouldUnregisterFlag();
        _updateLoggers(currentDAG, "_post");
        setStateOnDAGCompletion();
        LOG.info("Shutting down on completion of dag:" +
              finishEvt.getDAGId().toString());
        shutdownHandler.shutdown();
      } else {
        LOG.info("DAG completed, dagId="
            + finishEvt.getDAGId().toString()
            + ", dagState=" + finishEvt.getDAGState());
        lastDAGCompletionTime = clock.getTime();
        _updateLoggers(currentDAG, "_post");
        if (this.historyEventHandler.hasRecoveryFailed()) {
          String recoveryErrorMsg = "Recovery had a fatal error, shutting down session after" +
              " DAG completion";
          LOG.warn(recoveryErrorMsg);
          addDiagnostic(recoveryErrorMsg);
          sessionStopped.set(true);
        }
        switch(finishEvt.getDAGState()) {
        case SUCCEEDED:
          if (!currentDAG.getName().startsWith(
              TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            successfulDAGs.incrementAndGet();
          }
          break;
        case FAILED:
          if (!currentDAG.getName().startsWith(
              TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            failedDAGs.incrementAndGet();
          }
          break;
        case KILLED:
          if (!currentDAG.getName().startsWith(
              TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            killedDAGs.incrementAndGet();
          }
          break;
        case ERROR:
          if (!currentDAG.getName().startsWith(
              TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX)) {
            failedDAGs.incrementAndGet();
          }
          // This is a pass-through. Kill the AM if DAG state is ERROR.
        default:
          LOG.error("Received a DAG Finished Event with state="
              + finishEvt.getDAGState()
              + ". Error. Shutting down.");
          state = DAGAppMasterState.ERROR;
          this.taskSchedulerEventHandler.setShouldUnregisterFlag();
          shutdownHandler.shutdown();
          break;
        }
        if (!state.equals(DAGAppMasterState.ERROR)) {
          if (!sessionStopped.get()) {
            LOG.info("Central Dispatcher queue size after DAG completion, before cleanup: " +
                dispatcher.getQueueSize());
            LOG.info("Waiting for next DAG to be submitted.");

            // Sending this via the event queue, in case there are pending events which need to be
            // processed. TaskKilled for example, or ContainerCompletions.
            // The DAG needs to be part of the event, since the dag can get reset when the next
            // dag is submitted. The next DAG, however, will not start executing till the cleanup
            // is complete, since execution start is on the same dispatcher.
            sendEvent(new DAGAppMasterEventDagCleanup(context.getCurrentDAG()));

            // Leaving the taskSchedulerEventHandler here for now. Doesn't generate new events.
            // However, eventually it needs to be moved out.
            this.taskSchedulerEventHandler.dagCompleted();
            state = DAGAppMasterState.IDLE;
          } else {
            LOG.info("Session shutting down now.");
            this.taskSchedulerEventHandler.setShouldUnregisterFlag();
            if (this.historyEventHandler.hasRecoveryFailed()) {
              state = DAGAppMasterState.FAILED;
            } else {
              state = DAGAppMasterState.SUCCEEDED;
            }
            shutdownHandler.shutdown();
          }
        }
      }
      //close all fs related caches
      try {
        FileSystem.closeAllForUGI(context.getCurrentDAG().getDagUGI());
      } catch (IOException e) {
        LOG.warn("Error occurred when trying to close FileSystem for userName " + context
            .getCurrentDAG().getDagUGI().getUserName(), e);
      }
      break;
    case AM_REBOOT:
      LOG.info("Received an AM_REBOOT signal");
      this.state = DAGAppMasterState.KILLED;
      shutdownHandler.shutdown(true);
      break;
    case DAG_CLEANUP:
      DAGAppMasterEventDagCleanup cleanupEvent = (DAGAppMasterEventDagCleanup) event;
      LOG.info("Cleaning up DAG: name=" + cleanupEvent.getDag().getName() + ", with id=" +
          cleanupEvent.getDag().getID());
      containerLauncher.dagComplete(cleanupEvent.getDag());
      taskAttemptListener.dagComplete(cleanupEvent.getDag());
      nodes.dagComplete(cleanupEvent.getDag());
      containers.dagComplete(cleanupEvent.getDag());
      TezTaskAttemptID.clearCache();
      TezTaskID.clearCache();
      TezVertexID.clearCache();
      TezDAGID.clearCache();
      LOG.info("Completed cleanup for DAG: name=" + cleanupEvent.getDag().getName() + ", with id=" +
          cleanupEvent.getDag().getID());
      break;
    case NEW_DAG_SUBMITTED:
      // Inform sub-components that a new DAG has been submitted.
      taskSchedulerEventHandler.dagSubmitted();
      containerLauncher.dagSubmitted();
      taskAttemptListener.dagSubmitted();
      break;
    default:
      throw new TezUncheckedException(
          "AppMaster: No handler for event type: " + event.getType());
    }
  }

  private void _updateLoggers(DAG dag, String appender) {
    try {
      TezUtilsInternal.updateLoggers(dag.getID().toString() + appender);
    } catch (FileNotFoundException e) {
      LOG.warn("Unable to update the logger. Continue with the old logger", e );
    }
  }

  public void setCurrentDAG(DAG currentDAG) {
    this.currentDAG = currentDAG;
    context.setDAG(currentDAG);
  }

  public ACLManager getACLManager() {
    return this.aclManager;
  }

  private class DAGAppMasterEventHandler implements
      EventHandler<DAGAppMasterEvent> {
    @Override
    public void handle(DAGAppMasterEvent event) {
      // don't handle events if DAGAppMaster is in the state of STOPPED,
      // otherwise there may be dead-lock happen.  TEZ-2204
      if (DAGAppMaster.this.getServiceState() == STATE.STOPPED) {
        LOG.info("ignore event when DAGAppMaster is in the state of STOPPED, eventType="
          + event.getType());
        return;
      }
      DAGAppMaster.this.handle(event);
    }
  }

  protected class DAGAppMasterShutdownHandler {
    private AtomicBoolean shutdownHandled = new AtomicBoolean(false);

    public void shutdown() {
      shutdown(false);
    }

    public void shutdown(boolean now) {
      LOG.info("DAGAppMasterShutdownHandler invoked");
      if(!shutdownHandled.compareAndSet(false, true)) {
        LOG.info("Ignoring multiple shutdown events");
        return;
      }

      synchronized (shutdownHandlerRunning) {
        shutdownHandlerRunning.set(true);
      }
      LOG.info("Handling DAGAppMaster shutdown");

      AMShutdownRunnable r = new AMShutdownRunnable(now);
      Thread t = new Thread(r, "AMShutdownThread");
      t.start();
    }

    private class AMShutdownRunnable implements Runnable {
      private final boolean immediateShutdown;

      public AMShutdownRunnable(boolean immediateShutdown) {
        this.immediateShutdown = immediateShutdown;
      }

      @Override
      public void run() {
        // TODO:currently just wait for some time so clients can know the
        // final states. Will be removed once RM come on. TEZ-160.
        if (!immediateShutdown) {
          try {
            LOG.info("Sleeping for 5 seconds before shutting down");
            Thread.sleep(TezConstants.TEZ_DAG_SLEEP_TIME_BEFORE_EXIT);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        try {
          // Stop all services
          // This will also send the final report to the ResourceManager
          LOG.info("Calling stop for all the services");
          stop();

          synchronized (shutdownHandlerRunning) {
            shutdownHandlerRunning.set(false);
            shutdownHandlerRunning.notify();
          }

          //Bring the process down by force.
          //Not needed after HADOOP-7140
          LOG.info("Exiting DAGAppMaster..GoodBye!");
          sysexit();

        } catch (Throwable t) {
          LOG.warn("Graceful stop failed ", t);
        }
      }
    }
  }

  protected DAG createDAG(DAGPlan dagPB) {
    return createDAG(dagPB, null);
  }

  /** Create and initialize (but don't start) a single dag. */
  DAGImpl createDAG(DAGPlan dagPB, TezDAGID dagId) {
    if (dagId == null) {
      dagId = TezDAGID.getInstance(appAttemptID.getApplicationId(),
          dagCounter.incrementAndGet());
    }

    Credentials dagCredentials = null;
    if (dagPB.hasCredentialsBinary()) {
      dagCredentials = DagTypeConverters.convertByteStringToCredentials(dagPB
          .getCredentialsBinary());
      TezCommonUtils.logCredentials(LOG, dagCredentials, "dag");
    } else {
      dagCredentials = new Credentials();
    }
    // TODO Does this move to the client in case of work-preserving recovery.
    TokenCache.setSessionToken(sessionToken, dagCredentials);

    // create single dag
    DAGImpl newDag =
        new DAGImpl(dagId, amConf, dagPB, dispatcher.getEventHandler(),
            taskAttemptListener, dagCredentials, clock,
            appMasterUgi.getShortUserName(),
            taskHeartbeatHandler, context);

    try {
      if (LOG.isDebugEnabled()) {
        LOG.info("JSON dump for submitted DAG, dagId=" + dagId.toString()
            + ", json="
            + DAGUtils.generateSimpleJSONPlan(dagPB).toString());
      }
    } catch (JSONException e) {
      LOG.warn("Failed to generate json for DAG", e);
    }

    generateDAGVizFile(dagId, dagPB, logDirs);
    writePBTextFile(newDag);
    return newDag;
  } // end createDag()

  String getShortClassName(String className) {
    int pos = className.lastIndexOf(".");
    if (pos != -1 && pos < className.length()-1) {
      return className.substring(pos+1);
    }
    return className;
  }


  private String sanitizeLabelForViz(String label) {
    Matcher m = sanitizeLabelPattern.matcher(label);
    return m.replaceAll("_");
  }

  private void generateDAGVizFile(TezDAGID dagId, DAGPlan dagPB, String[] logDirs) {
    Graph graph = new Graph(sanitizeLabelForViz(dagPB.getName()));

    for (VertexPlan v : dagPB.getVertexList()) {
      String nodeLabel = sanitizeLabelForViz(v.getName())
          + "[" + getShortClassName(v.getProcessorDescriptor().getClassName() + "]");
      Graph.Node n = graph.newNode(sanitizeLabelForViz(v.getName()), nodeLabel);
      for (DAGProtos.RootInputLeafOutputProto input : v.getInputsList()) {
        Graph.Node inputNode = graph.getNode(sanitizeLabelForViz(v.getName())
            + "_" + sanitizeLabelForViz(input.getName()));
        inputNode.setLabel(sanitizeLabelForViz(v.getName())
            + "[" + sanitizeLabelForViz(input.getName()) + "]");
        inputNode.setShape("box");
        inputNode.addEdge(n, "Input"
            + " [inputClass=" + getShortClassName(input.getIODescriptor().getClassName())
            + ", initializer=" + getShortClassName(input.getControllerDescriptor().getClassName()) + "]");
      }
      for (DAGProtos.RootInputLeafOutputProto output : v.getOutputsList()) {
        Graph.Node outputNode = graph.getNode(sanitizeLabelForViz(v.getName())
            + "_" + sanitizeLabelForViz(output.getName()));
        outputNode.setLabel(sanitizeLabelForViz(v.getName())
            + "[" + sanitizeLabelForViz(output.getName()) + "]");
        outputNode.setShape("box");
        n.addEdge(outputNode, "Output"
            + " [outputClass=" + getShortClassName(output.getIODescriptor().getClassName())
            + ", committer=" + getShortClassName(output.getControllerDescriptor().getClassName()) + "]");
      }
    }

    for (DAGProtos.EdgePlan e : dagPB.getEdgeList()) {

      Graph.Node n = graph.getNode(sanitizeLabelForViz(e.getInputVertexName()));
      n.addEdge(graph.getNode(sanitizeLabelForViz(e.getOutputVertexName())),
          "["
          + "input=" + getShortClassName(e.getEdgeSource().getClassName())
          + ", output=" + getShortClassName(e.getEdgeDestination().getClassName())
          + ", dataMovement=" + e.getDataMovementType().name().trim()
          + ", schedulingType=" + e.getSchedulingType().name().trim() + "]");
    }

    String outputFile = "";
    if (logDirs != null && logDirs.length != 0) {
      outputFile += logDirs[0];
      outputFile += File.separator;
    }
    outputFile += dagId.toString() + ".dot";

    try {
      LOG.info("Generating DAG graphviz file"
          + ", dagId=" + dagId.toString()
          + ", filePath=" + outputFile);
      graph.save(outputFile);
    } catch (Exception e) {
      LOG.warn("Error occurred when trying to save graph structure"
          + " for dag " + dagId.toString(), e);
    }
  }

  private void writePBTextFile(DAG dag) {
    if (dag.getConf().getBoolean(TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS,
        TezConfiguration.TEZ_GENERATE_DEBUG_ARTIFACTS_DEFAULT)) {

      String logFile = logDirs[new Random().nextInt(logDirs.length)] + File.separatorChar +
          dag.getID().toString()  + "-" + TezConstants.TEZ_PB_PLAN_TEXT_NAME;

      LOG.info("Writing DAG plan to: " + logFile);
      File outFile = new File(logFile);
      try {
        PrintWriter printWriter = new PrintWriter(outFile, "UTF-8");
        printWriter.println(TezUtilsInternal.convertDagPlanToString(dag.getJobPlan()));
        printWriter.close();
      } catch (IOException e) {
        LOG.warn("Failed to write TEZ_PLAN to " + outFile.toString(), e);
      }
    }
  }

  protected void addIfService(Object object, boolean addDispatcher) {
    if (object instanceof Service) {
      Service service = (Service) object;
      ServiceWithDependency sd = new ServiceWithDependency(service);
      services.put(service, sd);
      if(addDispatcher) {
        addIfServiceDependency(service, dispatcher);
      }
    }
  }

  protected void addIfServiceDependency(Object object, Object dependency) {
    if (object instanceof Service && dependency instanceof Service) {
      Service service = (Service) object;
      Service dependencyService = (Service) dependency;
      ServiceWithDependency sd = services.get(service);
      sd.dependencies.add(dependencyService);
      dependencyService.registerServiceListener(sd);
    }
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpTezDag(context, thh, chh,jobTokenSecretManager);
    return lis;
  }

  protected TaskHeartbeatHandler createTaskHeartbeatHandler(AppContext context,
      Configuration conf) {
    TaskHeartbeatHandler thh = new TaskHeartbeatHandler(context, conf.getInt(
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT));
    return thh;
  }

  protected ContainerHeartbeatHandler createContainerHeartbeatHandler(
      AppContext context, Configuration conf) {
    ContainerHeartbeatHandler chh = new ContainerHeartbeatHandler(context,
        conf.getInt(
            TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
            TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT));
    return chh;
  }

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) throws UnknownHostException {
    if(isLocal){
      return new LocalContainerLauncher(context, taskAttemptListener, workingDirectory);
    } else {
      return new ContainerLauncherImpl(context);
    }
  }

  public ApplicationId getAppID() {
    return appAttemptID.getApplicationId();
  }

  public ApplicationAttemptId getAttemptID() {
    return appAttemptID;
  }

  public int getStartCount() {
    return appAttemptID.getAttemptId();
  }

  public AppContext getContext() {
    return context;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public ContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public TaskAttemptListener getTaskAttemptListener() {
    return taskAttemptListener;
  }

  public ContainerId getAppContainerId() {
    return containerID;
  }

  public String getAppNMHost() {
    return nmHost;
  }

  public int getAppNMPort() {
    return nmPort;
  }

  public int getAppNMHttpPort() {
    return nmHttpPort;
  }

  public int getRpcPort() {
    return clientRpcServer.getBindAddress().getPort();
  }

  public DAGAppMasterState getState() {
    return state;
  }

  private void addDiagnostic(String diag) {
    synchronized (diagnostics) {
      diagnostics.add(diag);
    }
  }

  public List<String> getDiagnostics() {
    // always create new diagnostics to return
    // This is to avoid the case that this method is called multiple times and diagnostics is accumulated.
    List<String> diagResult = new ArrayList<String>();
    synchronized (diagnostics) {
      diagResult.addAll(this.diagnostics);
    }

    if (!isSession) {
      if(currentDAG != null) {
        diagResult.addAll(currentDAG.getDiagnostics());
      }
    } else {
      diagResult.add("Session stats:"
          + "submittedDAGs=" + submittedDAGs.get()
          + ", successfulDAGs=" + successfulDAGs.get()
          + ", failedDAGs=" + failedDAGs.get()
          + ", killedDAGs=" + killedDAGs.get());
    }
    return diagResult;
  }

  public float getProgress() {
    if (isSession && state.equals(DAGAppMasterState.IDLE)) {
      return 0.0f;
    }
    if(currentDAG != null) {
      DAGState state = currentDAG.getState();
      switch (state) {
        case NEW:
        case INITED:
          return 0.0f;
        case RUNNING:
          return currentDAG.getProgress();
        case SUCCEEDED:
        case TERMINATING:
        case ERROR:
        case FAILED:
        case KILLED:
          return 1.0f;
      }
    }
    return 0.0f;
  }

  private synchronized void setStateOnDAGCompletion() {
    DAGAppMasterState oldState = state;
    if(isSession) {
      return;
    }
    switch(currentDAG.getState()) {
    case SUCCEEDED:
      state = DAGAppMasterState.SUCCEEDED;
      break;
    case FAILED:
      state = DAGAppMasterState.FAILED;
      break;
    case KILLED:
      state = DAGAppMasterState.KILLED;
      break;
    case ERROR:
      state = DAGAppMasterState.ERROR;
      break;
    default:
      state = DAGAppMasterState.ERROR;
      break;
    }
    LOG.info("On DAG completion. Old state: "
        + oldState + " new state: " + state);
  }

  public synchronized void shutdownTezAM() {
    sessionStopped.set(true);
    this.taskSchedulerEventHandler.setShouldUnregisterFlag();
    if (currentDAG != null
        && !currentDAG.isComplete()) {
      //send a DAG_KILL message
      LOG.info("Sending a kill event to the current DAG"
          + ", dagId=" + currentDAG.getID());
      sendEvent(new DAGEvent(currentDAG.getID(), DAGEventType.DAG_KILL));
    } else {
      LOG.info("No current running DAG, shutting down the AM");
      if (isSession && !state.equals(DAGAppMasterState.ERROR)) {
        state = DAGAppMasterState.SUCCEEDED;
      }
      shutdownHandler.shutdown();
    }
  }

  public synchronized String submitDAGToAppMaster(DAGPlan dagPlan,
      Map<String, LocalResource> additionalResources) throws TezException {
    if (this.versionMismatch) {
      throw new TezException("Unable to accept DAG submissions as the ApplicationMaster is"
          + " incompatible with the client. " + versionMismatchDiagnostics);
    }
    if(currentDAG != null
        && !state.equals(DAGAppMasterState.IDLE)) {
      throw new TezException("App master already running a DAG");
    }
    if (state.equals(DAGAppMasterState.ERROR)
        || sessionStopped.get()) {
      throw new TezException("AM unable to accept new DAG submissions."
          + " In the process of shutting down");
    }

    // RPC server runs in the context of the job user as it was started in
    // the job user's UGI context
    LOG.info("Starting DAG submitted via RPC: " + dagPlan.getName());
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Invoked with additional local resources: " + additionalResources);
    }
    submittedDAGs.incrementAndGet();
    startDAG(dagPlan, additionalResources);
    return currentDAG.getID().toString();
  }

  @SuppressWarnings("unchecked")
  public void tryKillDAG(DAG dag){
    dispatcher.getEventHandler().handle(new DAGEvent(dag.getID(), DAGEventType.DAG_KILL));
  }
  
  private Map<String, LocalResource> getAdditionalLocalResourceDiff(
      DAG dag, Map<String, LocalResource> additionalResources) throws TezException {
    if (additionalResources == null) {
      return Collections.emptyMap();
    }
    // Check for existing resources.
    Iterator<Entry<String, LocalResource>> lrIter = additionalResources.entrySet().iterator();
    while (lrIter.hasNext()) {
      Entry<String, LocalResource> lrEntry = lrIter.next();
      LocalResource existing = amResources.get(lrEntry.getKey());
      if (existing != null) {
        if (!isSameFile(dag, lrEntry.getKey(), existing, lrEntry.getValue())) {
          throw new TezUncheckedException(
              "Cannot add different additional resources with the same name : "
                  + lrEntry.getKey() + ", Existing: [" + existing + "], New: ["
                  + lrEntry.getValue() + "]");
        } else {
          lrIter.remove();
        }
      }
    }
    return containerSignatureMatcher.getAdditionalResources(amResources, additionalResources);
  }

  private boolean isSameFile(DAG dag, final String fileName,
      final LocalResource oldLr, final LocalResource newLr) throws TezException {
    try {
      return oldLr.equals(newLr) || dag.getDagUGI().doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          Configuration conf = getConfig();
          byte[] oldSha = null;
          try {
            // The existing file must already be in usercache... let's try to find it.
            Path localFile = findLocalFileForResource(fileName);
            if (localFile != null) {
              oldSha = RelocalizationUtils.getLocalSha(localFile, conf);
            } else {
              LOG.warn("Couldn't find local file for " + oldLr);
            }
          } catch (Exception ex) {
            LOG.warn("Error getting SHA from local file for " + oldLr, ex);
          }
          if (oldSha == null) { // Well, no dice.
            oldSha = RelocalizationUtils.getResourceSha(getLocalResourceUri(oldLr), conf);
          }
          // Get the new SHA directly from Hadoop stream. If it matches, we already have the
          // file, and if it doesn't we are going to fail; no need to download either way.
          byte[] newSha = RelocalizationUtils.getResourceSha(getLocalResourceUri(newLr), conf);
          return Arrays.equals(oldSha, newSha);
        }
      });
    } catch (InterruptedException ex) {
      throw new TezException(ex);
    } catch (IOException ex) {
      throw new TezException(ex);
    }
  }

  private static Path findLocalFileForResource(String fileName) {
    URL localResource = ClassLoader.getSystemClassLoader().getResource(fileName);
    if (localResource == null) return null;
    return new Path(localResource.getPath());
  }

  private static URI getLocalResourceUri(LocalResource input) {
    try {
      return TezConverterUtils.getURIFromYarnURL(input.getResource());
    } catch (URISyntaxException e) {
      throw new TezUncheckedException("Failed while handling : " + input, e);
    }
  }

  private List<URL> processAdditionalResources(Map<String, LocalResource> lrDiff)
      throws TezException {
    if (lrDiff == null || lrDiff.isEmpty()) {
      return Collections.emptyList();
    } else {
      LOG.info("Localizing additional local resources for AM : " + lrDiff);
      List<URL> downloadedURLs;
      try {
        downloadedURLs = RelocalizationUtils.processAdditionalResources(
            Maps.transformValues(lrDiff, new Function<LocalResource, URI>() {

              @Override
              public URI apply(LocalResource input) {
                return getLocalResourceUri(input);
              }
            }), getConfig(), workingDirectory);
      } catch (IOException e) {
        throw new TezException(e);
      }
      LOG.info("Done downloading additional AM resources");
      return downloadedURLs;
    }
  }

  private class RunningAppContext implements AppContext {

    private DAG dag;
    private final Configuration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    private final EventHandler eventHandler;
    public RunningAppContext(Configuration config) {
      checkNotNull(config, "config is null");
      this.conf = config;
      this.eventHandler = dispatcher.getEventHandler();
    }

    @Override
    public DAGAppMaster getAppMaster() {
      return DAGAppMaster.this;
    }

    @Override
    public Configuration getAMConf() {
      return conf;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appAttemptID.getApplicationId();
    }

    @Override
    public String getApplicationName() {
      return appName;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public DAG getCurrentDAG() {
      return dag;
    }
    
    @Override
    public ListeningExecutorService getExecService() {
      return execService;
    }

    @Override
    public Set<String> getAllDAGIDs() {
      return dagIDs;
    }

    @Override
    public EventHandler getEventHandler() {
      return eventHandler;
    }

    @Override
    public String getUser() {
      return dag.getUserName();
    }

    @Override
    public Clock getClock() {
      return clock;
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return this.clusterInfo;
    }

    @Override
    public AMContainerMap getAllContainers() {
      return containers;
    }

    @Override
    public AMNodeTracker getNodeTracker() {
      return nodes;
    }

    @Override
    public TaskSchedulerEventHandler getTaskScheduler() {
      return taskSchedulerEventHandler;
    }

    @Override
    public boolean isSession() {
      return isSession;
    }

    @Override
    public DAGAppMasterState getAMState() {
      return state;
    }

    @Override
    public HistoryEventHandler getHistoryHandler() {
      return historyEventHandler;
    }

    @Override
    public Path getCurrentRecoveryDir() {
      return currentRecoveryDataDir;
    }

    @Override
    public boolean isRecoveryEnabled() {
      return recoveryEnabled;
    }

    @Override
    public ACLManager getAMACLManager() {
      return aclManager;
    }

    @Override
    public String[] getLogDirs() {
      return logDirs;
    }

    @Override
    public String[] getLocalDirs() {
      return localDirs;
    }

    @Override
    public String getAMUser() {
      return appMasterUgi.getShortUserName();
    }

    @Override
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      if (getServiceState() != STATE.STARTED) {
        throw new TezUncheckedException(
            "Cannot get ApplicationACLs before all services have started");
      }
      return taskSchedulerEventHandler.getApplicationAcls();
    }

    @Override
    public TezDAGID getCurrentDAGID() {
      try {
        rLock.lock();
        if(dag != null) {
          return dag.getID();
        }
        return null;
      } finally {
        rLock.unlock();
      }
    }

    @Override
    public void setDAG(DAG dag) {
      Preconditions.checkNotNull(dag, "dag is null");
      try {
        wLock.lock();
        this.dag = dag;
      } finally {
        wLock.unlock();
      }
    }

    @Override
    public long getCumulativeCPUTime() {
      return getAMCPUTime();
    }
    
    @Override
    public long getCumulativeGCTime() {
      return getAMGCTime();
    }
  }

  private static class ServiceWithDependency implements ServiceStateChangeListener {
    ServiceWithDependency(Service service) {
      this.service = service;
    }
    Service service;
    List<Service> dependencies = new ArrayList<Service>();
    AtomicInteger dependenciesStarted = new AtomicInteger(0);
    volatile boolean canStart = false;
    volatile boolean dependenciesFailed = false;

    @Override
    public void stateChanged(Service dependency) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Service dependency: " + dependency.getName() + " notify" +
                  " for service: " + service.getName());
      }
      if (dependency.isInState(Service.STATE.STARTED)) {
        if(dependenciesStarted.incrementAndGet() == dependencies.size()) {
          synchronized(this) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Service: " + service.getName() + " notified to start");
            }
            canStart = true;
            this.notifyAll();
          }
        }
      } else if (!service.isInState(Service.STATE.STARTED)
          && dependency.getFailureState() != null) {
        synchronized(this) {
          dependenciesFailed = true;
          if(LOG.isDebugEnabled()) {
            LOG.debug("Service: " + service.getName() + " will fail to start"
                + " as dependent service " + dependency.getName()
                + " failed to start");
          }
          this.notifyAll();
        }
      }
    }

    void start() throws InterruptedException {
      if(dependencies.size() > 0) {
        synchronized(this) {
          while(!canStart) {
            this.wait(1000*60*3L);
            if (dependenciesFailed) {
              throw new TezUncheckedException("Skipping service start for "
                  + service.getName()
                  + " as dependencies failed to start");
            }
          }
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Service: " + service.getName() + " trying to start");
      }
      for(Service dependency : dependencies) {
        if(!dependency.isInState(Service.STATE.STARTED)){
          LOG.info("Service: " + service.getName() + " not started because "
                   + " service: " + dependency.getName() +
                   " is in state: " + dependency.getServiceState());
          return;
        }
      }
      service.start();
    }
  }

  private static class ServiceThread extends Thread {
    final ServiceWithDependency serviceWithDependency;
    Throwable error = null;
    public ServiceThread(ServiceWithDependency serviceWithDependency) {
      this.serviceWithDependency = serviceWithDependency;
      this.setName("ServiceThread:" + serviceWithDependency.service.getName());
    }

    public void run() {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Starting thread " + serviceWithDependency.service.getName());
      }
      long start = System.currentTimeMillis();
      try {
        serviceWithDependency.start();
      } catch (Throwable t) {
        error = t;
      } finally {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Service: " + serviceWithDependency.service.getName() +
              " started in " + (System.currentTimeMillis() - start) + "ms");
        }
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Service thread completed for "
            + serviceWithDependency.service.getName());
      }
    }
  }

  void startServices(){
    try {
      Throwable firstError = null;
      List<ServiceThread> threads = new ArrayList<ServiceThread>();
      if(LOG.isDebugEnabled()) {
        LOG.debug("Begin parallel start");
      }
      for(ServiceWithDependency sd : services.values()) {
        // start the service. If this fails that service
        // will be stopped and an exception raised
        ServiceThread st = new ServiceThread(sd);
        threads.add(st);
      }

      for(ServiceThread st : threads) {
        st.start();
      }
      for(ServiceThread st : threads) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Waiting for service thread to join for " + st.getName());
        }
        st.join();
        if(st.error != null && firstError == null) {
          firstError = st.error;
        }
      }

      if(firstError != null) {
        throw ServiceStateException.convert(firstError);
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("End parallel start");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  void initServices(Configuration conf) {
    for (ServiceWithDependency sd : services.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initing service : " + sd.service);
      }
      sd.service.init(conf);
    }
  }

  void stopServices() {
    // stop in reverse order of start
    List<Service> serviceList = new ArrayList<Service>(services.size());
    for (ServiceWithDependency sd : services.values()) {
      serviceList.add(sd.service);
    }
    Exception firstException = null;
    for (int i = services.size() - 1; i >= 0; i--) {
      Service service = serviceList.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping service : " + service);
      }
      Exception ex = ServiceOperations.stopQuietly(service);
      if (ex != null && firstException == null) {
        LOG.warn("Failed to stop service, name=" + service.getName(), ex);
        firstException = ex;
      }
    }
    //after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

  private RecoveredDAGData recoverDAG() throws IOException, TezException {
    if (recoveryEnabled) {
      if (this.appAttemptID.getAttemptId() > 1) {
        LOG.info("Recovering data from previous attempts"
            + ", currentAttemptId=" + this.appAttemptID.getAttemptId());
        this.state = DAGAppMasterState.RECOVERING;
        RecoveryParser recoveryParser = new RecoveryParser(
            this, recoveryFS, recoveryDataDir, appAttemptID.getAttemptId());
        RecoveredDAGData recoveredDAGData = recoveryParser.parseRecoveryData();
        return recoveredDAGData;
      }
    }
    return null;
  }
  
  @Override
  public synchronized void serviceStart() throws Exception {

    //start all the components
    startServices();
    super.serviceStart();

    if (versionMismatch) {
      // Short-circuit and return as no DAG should not be run
      this.taskSchedulerEventHandler.setShouldUnregisterFlag();
      shutdownHandler.shutdown();
      return;
    }

    this.appsStartTime = clock.getTime();
    AMStartedEvent startEvent = new AMStartedEvent(appAttemptID,
        appsStartTime, appMasterUgi.getShortUserName());
    historyEventHandler.handle(
        new DAGHistoryEvent(startEvent));

    this.lastDAGCompletionTime = clock.getTime();

    RecoveredDAGData recoveredDAGData;
    try {
      recoveredDAGData = recoverDAG();
    } catch (IOException e) {
      LOG.error("Error occurred when trying to recover data from previous attempt."
          + " Shutting down AM", e);
      this.state = DAGAppMasterState.ERROR;
      this.taskSchedulerEventHandler.setShouldUnregisterFlag();
      shutdownHandler.shutdown();
      return;
    }

    if (!isSession) {
      LOG.info("In Non-Session mode.");
    } else {
      LOG.info("In Session mode. Waiting for DAG over RPC");
      this.state = DAGAppMasterState.IDLE;
    }

    if (recoveredDAGData != null) {
      List<URL> classpathUrls = null;
      if (recoveredDAGData.cumulativeAdditionalResources != null) {
        classpathUrls = processAdditionalResources(recoveredDAGData.cumulativeAdditionalResources);
        amResources.putAll(recoveredDAGData.cumulativeAdditionalResources);
        cumulativeAdditionalResources.putAll(recoveredDAGData.cumulativeAdditionalResources);
      }
      
      if (recoveredDAGData.isCompleted
          || recoveredDAGData.nonRecoverable) {
        LOG.info("Found previous DAG in completed or non-recoverable state"
            + ", dagId=" + recoveredDAGData.recoveredDagID
            + ", isCompleted=" + recoveredDAGData.isCompleted
            + ", isNonRecoverable=" + recoveredDAGData.nonRecoverable
            + ", state=" + (recoveredDAGData.dagState == null ? "null" :
                recoveredDAGData.dagState)
            + ", failureReason=" + recoveredDAGData.reason);
        _updateLoggers(recoveredDAGData.recoveredDAG, "");
        if (recoveredDAGData.nonRecoverable) {
          DAGEventRecoverEvent recoverDAGEvent =
              new DAGEventRecoverEvent(recoveredDAGData.recoveredDAG.getID(),
                  DAGState.FAILED, classpathUrls);
          dagEventDispatcher.handle(recoverDAGEvent);
          this.state = DAGAppMasterState.RUNNING;
        } else {
          DAGEventRecoverEvent recoverDAGEvent =
              new DAGEventRecoverEvent(recoveredDAGData.recoveredDAG.getID(),
                  recoveredDAGData.dagState, classpathUrls);
          dagEventDispatcher.handle(recoverDAGEvent);
          this.state = DAGAppMasterState.RUNNING;
        }
      } else {
        LOG.info("Found DAG to recover, dagId=" + recoveredDAGData.recoveredDAG.getID());
        _updateLoggers(recoveredDAGData.recoveredDAG, "");
        DAGEventRecoverEvent recoverDAGEvent = new DAGEventRecoverEvent(
            recoveredDAGData.recoveredDAG.getID(), classpathUrls);
        dagEventDispatcher.handle(recoverDAGEvent);
        this.state = DAGAppMasterState.RUNNING;
      }
    } else {
      if (!isSession) {
        // No dag recovered - in non-session, just restart the original DAG
        dagCounter.set(0);
        startDAG();
      }
    }

    if (isSession) {
      this.dagSubmissionTimer = new Timer(true);
      this.dagSubmissionTimer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          checkAndHandleSessionTimeout();
        }
      }, sessionTimeoutInterval, sessionTimeoutInterval / 10);
    }
  }


  @Override
  public synchronized void serviceStop() throws Exception {
    if (isSession) {
      sessionStopped.set(true);
    }
    if (this.dagSubmissionTimer != null) {
      this.dagSubmissionTimer.cancel();
    }
    stopServices();

    // Given pre-emption, we should delete tez scratch dir only if unregister is
    // successful
    boolean deleteTezScratchData = this.amConf.getBoolean(
        TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE,
        TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE_DEFAULT);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking whether tez scratch data dir should be deleted, deleteTezScratchData="
          + deleteTezScratchData);
    }
    if (deleteTezScratchData && this.taskSchedulerEventHandler != null
        && this.taskSchedulerEventHandler.hasUnregistered()) {
      // Delete tez scratch data dir
      if (this.tezSystemStagingDir != null) {
        try {
          this.appMasterUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              FileSystem fs = tezSystemStagingDir.getFileSystem(amConf);
              boolean deletedStagingDir = fs.delete(tezSystemStagingDir, true);
              if (!deletedStagingDir) {
                LOG.warn("Failed to delete tez scratch data dir, path="
                + tezSystemStagingDir);
              } else {
                LOG.info("Completed deletion of tez scratch data dir, path="
                  + tezSystemStagingDir);
              }
              return null;
            }
          });
        } catch (IOException e) {
          // Best effort to delete tez scratch data dir
          LOG.warn("Failed to delete tez scratch data dir", e);
        }
      }
    }

    if (execService != null) {
      execService.shutdownNow();
    }

    super.serviceStop();
  }

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(DAGEvent event) {
      DAG dag = context.getCurrentDAG();
      int eventDagIndex = event.getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      ((EventHandler<DAGEvent>)dag).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      DAG dag = context.getCurrentDAG();
      int eventDagIndex = 
          event.getTaskID().getVertexID().getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      
      Task task =
          dag.getVertex(event.getTaskID().getVertexID()).
              getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }
  
  private class SpeculatorEventHandler implements EventHandler<SpeculatorEvent> {
    @Override
    public void handle(SpeculatorEvent event) {
      DAG dag = context.getCurrentDAG();
      TezVertexID vertexId = event.getVertexId();
      Vertex v = dag.getVertex(vertexId);
      Preconditions.checkState(v != null,
          "Unknown vertex: " + vertexId + " for DAG: " + dag.getID());
      v.handleSpeculatorEvent(event);
    }
  }

  private class TaskAttemptEventDispatcher
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = context.getCurrentDAG();
      int eventDagIndex = 
          event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      Task task =
          dag.getVertex(event.getTaskAttemptID().getTaskID().getVertexID()).
              getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }

  private class VertexEventDispatcher
    implements EventHandler<VertexEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      DAG dag = context.getCurrentDAG();
      int eventDagIndex = 
          event.getVertexId().getDAGId().getId();
      if (dag == null || eventDagIndex != dag.getID().getId()) {
        return; // event not relevant any more
      }
      
      Vertex vertex =
          dag.getVertex(event.getVertexId());
      ((EventHandler<VertexEvent>) vertex).handle(event);
    }
  }

  private static void validateInputParam(String value, String param)
      throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  private synchronized void checkAndHandleSessionTimeout() {
    if (EnumSet.of(DAGAppMasterState.RUNNING,
        DAGAppMasterState.RECOVERING).contains(this.state)
        || sessionStopped.get()) {
      // DAG running or session already completed, cannot timeout session
      return;
    }
    long currentTime = clock.getTime();
    if (currentTime < (lastDAGCompletionTime + sessionTimeoutInterval)) {
      return;
    }
    LOG.info("Session timed out"
        + ", lastDAGCompletionTime=" + lastDAGCompletionTime + " ms"
        + ", sessionTimeoutInterval=" + sessionTimeoutInterval + " ms");
    shutdownTezAM();
  }

  public boolean isSession() {
    return isSession;
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr =
          System.getenv(Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(Environment.NM_HOST.name());
      String nodePortString = System.getenv(Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String clientVersion = System.getenv(TezConstants.TEZ_CLIENT_VERSION_ENV);
      if (clientVersion == null) {
        clientVersion = VersionInfo.UNKNOWN;
      }

      // TODO Should this be defaulting to 1. Was there a version of YARN where this was not setup ?
      int maxAppAttempts = 1;
      String maxAppAttemptsEnv = System.getenv(
          ApplicationConstants.MAX_APP_ATTEMPTS_ENV);
      if (maxAppAttemptsEnv != null) {
        maxAppAttempts = Integer.valueOf(maxAppAttemptsEnv);
      }

      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());

      // Command line options
      Options opts = new Options();
      opts.addOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION,
          false, "Run Tez Application Master in Session mode");

      CommandLine cliParser = new GnuParser().parse(opts, args);

      // TODO Does this really need to be a YarnConfiguration ?
      Configuration conf = new Configuration(new YarnConfiguration());
      TezUtilsInternal.addUserSpecifiedTezConfiguration(System.getenv(Environment.PWD.name()), conf);
      UserGroupInformation.setConfiguration(conf);
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

      DAGAppMaster appMaster =
          new DAGAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), new SystemClock(), appSubmitTime,
              cliParser.hasOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION),
              System.getenv(Environment.PWD.name()),
              TezCommonUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name())),
              TezCommonUtils.getTrimmedStrings(System.getenv(Environment.LOG_DIRS.name())),
              clientVersion, maxAppAttempts, credentials, jobUserName);
      ShutdownHookManager.get().addShutdownHook(
        new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

      initAndStartAppMaster(appMaster, conf);

    } catch (Throwable t) {
      LOG.error("Error starting DAGAppMaster", t);
      System.exit(1);
    }
  }

  // The shutdown hook that runs when a signal is received AND during normal
  // close of the JVM.
  static class DAGAppMasterShutdownHook implements Runnable {
    DAGAppMaster appMaster;
    DAGAppMasterShutdownHook(DAGAppMaster appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      LOG.info("DAGAppMasterShutdownHook invoked");
      if(appMaster.getServiceState() == STATE.STOPPED) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("DAGAppMaster already stopped. Ignoring signal");
        }
        synchronized (appMaster.shutdownHandlerRunning) {
          try {
            if (appMaster.shutdownHandlerRunning.get()) {
              LOG.info("The shutdown handler is still running, waiting for it to complete");
              appMaster.shutdownHandlerRunning.wait();
              LOG.info("The shutdown handler has completed");
            }
          } catch (InterruptedException e) {
            // Ignore
          }
        }
        return;
      }

      if(appMaster.getServiceState() == STATE.STARTED) {
        // Notify TaskScheduler that a SIGTERM has been received so that it
        // unregisters quickly with proper status
        LOG.info("DAGAppMaster received a signal. Signaling TaskScheduler");
        appMaster.taskSchedulerEventHandler.setSignalled(true);
      }

      if (EnumSet.of(DAGAppMasterState.NEW, DAGAppMasterState.INITED,
          DAGAppMasterState.IDLE).contains(appMaster.state)) {
            // DAG not in a final state. Must have receive a KILL signal
        appMaster.state = DAGAppMasterState.KILLED;
      } else if (appMaster.state == DAGAppMasterState.RUNNING) {
        appMaster.state = DAGAppMasterState.ERROR;
      }

      appMaster.stop();

    }
  }

  private void startDAG() throws IOException, TezException {
    FileInputStream dagPBBinaryStream = null;
    try {
      DAGPlan dagPlan = null;

      // Read the protobuf DAG
      dagPBBinaryStream = new FileInputStream(new File(workingDirectory,
          TezConstants.TEZ_PB_PLAN_BINARY_NAME));
      dagPlan = DAGPlan.parseFrom(dagPBBinaryStream);

      startDAG(dagPlan, null);

    } finally {
      if (dagPBBinaryStream != null) {
        dagPBBinaryStream.close();
      }
    }
  }

  private void startDAG(DAGPlan dagPlan, Map<String, LocalResource> additionalAMResources)
      throws TezException {
    long submitTime = this.clock.getTime();
    this.state = DAGAppMasterState.RUNNING;
    this.appName = dagPlan.getName();
    if (dagNames.contains(dagPlan.getName())) {
      throw new TezException("Duplicate dag name '" + dagPlan.getName() + "'");
    }
    dagNames.add(dagPlan.getName());

    // /////////////////// Create the job itself.
    DAG newDAG = createDAG(dagPlan);
    _updateLoggers(newDAG, "");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running a DAG with " + dagPlan.getVertexCount()
          + " vertices ");
      for (VertexPlan v : dagPlan.getVertexList()) {
        LOG.debug("DAG has vertex " + v.getName());
      }
    }
    Map<String, LocalResource> lrDiff = getAdditionalLocalResourceDiff(
        newDAG, additionalAMResources);
    if (lrDiff != null) {
      amResources.putAll(lrDiff);
      cumulativeAdditionalResources.putAll(lrDiff);
    }

    LOG.info("Running DAG: " + dagPlan.getName());
    // Job name is the same as the app name until we support multiple dags
    // for an app later
    DAGSubmittedEvent submittedEvent = new DAGSubmittedEvent(newDAG.getID(),
        submitTime, dagPlan, this.appAttemptID, cumulativeAdditionalResources,
        newDAG.getUserName(), newDAG.getConf());
    try {
      historyEventHandler.handleCriticalEvent(
          new DAGHistoryEvent(newDAG.getID(), submittedEvent));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    startDAGExecution(newDAG, lrDiff);
  }

  private void startDAGExecution(DAG dag, final Map<String, LocalResource> additionalAmResources)
      throws TezException {
    currentDAG = dag;
    // Try localizing the actual resources.
    List<URL> additionalUrlsForClasspath;
    try {
      additionalUrlsForClasspath = dag.getDagUGI().doAs(new PrivilegedExceptionAction<List<URL>>() {
        @Override
        public List<URL> run() throws Exception {
          return processAdditionalResources(additionalAmResources);
        }
      });
    } catch (IOException e) {
      throw new TezException(e);
    } catch (InterruptedException e) {
      throw new TezException(e);
    }

    dagIDs.add(currentDAG.getID().toString());
    // End of creating the job.
    ((RunningAppContext) context).setDAG(currentDAG);

    // Send out an event to inform components that a new DAG has been submitted.
    // Information about this DAG is available via the context.
    sendEvent(new DAGAppMasterEvent(DAGAppMasterEventType.NEW_DAG_SUBMITTED));
    // create a job event for job initialization
    DAGEvent initDagEvent = new DAGEvent(currentDAG.getID(), DAGEventType.DAG_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    dagEventDispatcher.handle(initDagEvent);

    // All components have started, start the job.
    /** create a job-start event to get this ball rolling */
    DAGEvent startDagEvent = new DAGEventStartDag(currentDAG.getID(), additionalUrlsForClasspath);
    /** send the job-start event. this triggers the job execution. */
    sendEvent(startDagEvent);
  }

  public static void initAndStartAppMaster(final DAGAppMaster appMaster,
      final Configuration conf) throws IOException,
      InterruptedException {

    // Do not automatically close FileSystem objects so that in case of
    // SIGTERM I have a chance to write out the job history. I'll be closing
    // the objects myself.
    conf.setBoolean("fs.automatic.close", false);
    Limits.setConfiguration(conf);

    // Now remove the AM->RM token so tasks don't have it
    Iterator<Token<?>> iter = appMaster.amCredentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }

    appMaster.appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        appMaster.init(conf);
        appMaster.start();
        return null;
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    dispatcher.getEventHandler().handle(event);
  }

  synchronized void setDAGCounter(int dagCounter) {
    this.dagCounter.set(dagCounter);
  }

  private boolean enableWebUIService() {
    return amConf.getBoolean(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE,
        TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE_DEFAULT);
  }
}

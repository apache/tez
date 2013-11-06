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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
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
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourcesProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.launcher.ContainerLauncher;
import org.apache.tez.dag.app.launcher.ContainerLauncherImpl;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.app.taskclean.TaskCleaner;
import org.apache.tez.dag.app.taskclean.TaskCleanerImpl;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.runtime.library.common.security.JobTokenIdentifier;
import org.apache.tez.runtime.library.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.TokenCache;

/**
 * The Map-Reduce Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event
 * results in a Finite State Transition in Job.
 *
 * MR AppMaster is the composition of loosely coupled services. The services
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

  private static final Log LOG = LogFactory.getLog(DAGAppMaster.class);

  /**
   * Priority of the DAGAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

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
  private AMContainerMap containers;
  private AMNodeMap nodes;
  private AppContext context;
  private Configuration amConf;
  private Dispatcher dispatcher;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  private ContainerHeartbeatHandler containerHeartbeatHandler;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private Token<JobTokenIdentifier> sessionToken;
  private DagEventDispatcher dagEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private TaskSchedulerEventHandler taskSchedulerEventHandler;
  private HistoryEventHandler historyEventHandler;
  private final Map<String, LocalResource> sessionResources =
    new HashMap<String, LocalResource>();

  private DAGAppMasterShutdownHandler shutdownHandler =
      new DAGAppMasterShutdownHandler();

  private DAGAppMasterState state;

  DAGClientServer clientRpcServer;
  private DAGClientHandler clientHandler;

  private DAG currentDAG;
  private Credentials tokens = new Credentials(); // Filled during init
  private UserGroupInformation currentUser; // Will be setup during init

  private AtomicBoolean sessionStopped = new AtomicBoolean(false);
  private long sessionTimeoutInterval;
  private long lastDAGCompletionTime;
  private Timer dagSubmissionTimer;

  // DAG Counter
  private final AtomicInteger dagCounter = new AtomicInteger();

  // Session counters
  private final AtomicInteger submittedDAGs = new AtomicInteger();
  private final AtomicInteger successfulDAGs = new AtomicInteger();
  private final AtomicInteger failedDAGs = new AtomicInteger();
  private final AtomicInteger killedDAGs = new AtomicInteger();

  // must be LinkedHashMap to preserve order of service addition
  Map<Service, ServiceWithDependency> services =
      new LinkedHashMap<Service, ServiceWithDependency>();

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime, boolean isSession) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime, isSession);
  }

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, boolean isSession) {
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
    // TODO Metrics
    //this.metrics = DAGAppMetrics.create();
    LOG.info("Created DAGAppMaster for application " + applicationAttemptId);
  }

  @Override
  public synchronized void serviceInit(final Configuration conf) throws Exception {

    this.amConf = conf;
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    context = new RunningAppContext(conf);

    clientHandler = new DAGClientHandler();

    dispatcher = createDispatcher();
    addIfService(dispatcher, false);

    clientRpcServer = new DAGClientServer(clientHandler);
    addIfService(clientRpcServer, true);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    addIfService(taskHeartbeatHandler, true);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    addIfService(containerHeartbeatHandler, true);

    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(UUID
        .randomUUID().toString()));
    sessionToken = new Token<JobTokenIdentifier>(identifier,
        jobTokenSecretManager);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setJobToken(sessionToken, tokens);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context,
        taskHeartbeatHandler, containerHeartbeatHandler);
    addIfService(taskAttemptListener, true);

    containers = new AMContainerMap(containerHeartbeatHandler,
        taskAttemptListener, context);
    addIfService(containers, true);
    dispatcher.register(AMContainerEventType.class, containers);

    nodes = new AMNodeMap(dispatcher.getEventHandler(), context);
    addIfService(nodes, true);
    dispatcher.register(AMNodeEventType.class, nodes);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    addIfService(taskCleaner, true);

    this.dagEventDispatcher = new DagEventDispatcher();
    this.vertexEventDispatcher = new VertexEventDispatcher();

    //register the event dispatchers
    dispatcher.register(DAGAppMasterEventType.class, new DAGAppMasterEventHandler());
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);

    taskSchedulerEventHandler = new TaskSchedulerEventHandler(context,
        clientRpcServer, dispatcher.getEventHandler());
    addIfService(taskSchedulerEventHandler, true);
    dispatcher.register(AMSchedulerEventType.class,
        taskSchedulerEventHandler);
    addIfServiceDependency(taskSchedulerEventHandler, clientRpcServer);

    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher, true);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    historyEventHandler = new HistoryEventHandler(context);
    addIfService(historyEventHandler, true);
    dispatcher.register(HistoryEventType.class, historyEventHandler);

    this.sessionTimeoutInterval = 1000 * amConf.getInt(
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT);

    if (isSession) {
      FileInputStream sessionResourcesStream = null;
      try {
        sessionResourcesStream = new FileInputStream(
          TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME);
        PlanLocalResourcesProto localResourcesProto =
          PlanLocalResourcesProto.parseFrom(sessionResourcesStream);
        sessionResources.putAll(DagTypeConverters.convertFromPlanLocalResources(
          localResourcesProto));
      } finally {
        if (sessionResourcesStream != null) {
          sessionResourcesStream.close();
        }
      }
    }

    initServices(conf);
    super.serviceInit(conf);

    this.state = DAGAppMasterState.INITED;

  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }

  private synchronized void handle(DAGAppMasterEvent event) {
    switch (event.getType()) {
    case INTERNAL_ERROR:
      state = DAGAppMasterState.ERROR;
      if(currentDAG != null) {
        // notify dag to finish which will send the DAG_FINISHED event
        LOG.info("Internal Error. Notifying dags to finish.");
        sendEvent(new DAGEvent(currentDAG.getID(), DAGEventType.INTERNAL_ERROR));
      } else {
        LOG.info("Internal Error. Finishing directly as no dag is active.");
        shutdownHandler.shutdown();
      }
      break;
    case DAG_FINISHED:
      DAGAppMasterEventDAGFinished finishEvt =
          (DAGAppMasterEventDAGFinished) event;
      if (!isSession) {
        setStateOnDAGCompletion();
        LOG.info("Shutting down on completion of dag:" +
              finishEvt.getDAGId().toString());
        shutdownHandler.shutdown();
      } else {
        LOG.info("DAG completed, dagId="
            + finishEvt.getDAGId().toString()
            + ", dagState=" + finishEvt.getDAGState());
        lastDAGCompletionTime = clock.getTime();
        switch(finishEvt.getDAGState()) {
        case SUCCEEDED:
          successfulDAGs.incrementAndGet();
          break;
        case ERROR:
        case FAILED:
          failedDAGs.incrementAndGet();
          break;
        case KILLED:
          killedDAGs.incrementAndGet();
          break;
        default:
          LOG.fatal("Received a DAG Finished Event with state="
              + finishEvt.getDAGState()
              + ". Error. Shutting down.");
          state = DAGAppMasterState.ERROR;
          shutdownHandler.shutdown();
          break;
        }
        if (!state.equals(DAGAppMasterState.ERROR)) {
          if (!sessionStopped.get()) {
            LOG.info("Waiting for next DAG to be submitted.");
            taskSchedulerEventHandler.dagCompleted();
            state = DAGAppMasterState.IDLE;
          } else {
            LOG.info("Session shutting down now.");
            state = DAGAppMasterState.SUCCEEDED;
            shutdownHandler.shutdown();
          }
        }
      }
      break;
    default:
      throw new TezUncheckedException(
          "AppMaster: No handler for event type: " + event.getType());
    }
  }

  private class DAGAppMasterEventHandler implements
      EventHandler<DAGAppMasterEvent> {
    @Override
    public void handle(DAGAppMasterEvent event) {
      DAGAppMaster.this.handle(event);
    }
  }

  private class DAGAppMasterShutdownHandler {
    private AtomicBoolean shutdownHandled = new AtomicBoolean(false);

    public void shutdown() {
      if(!shutdownHandled.compareAndSet(false, true)) {
        LOG.info("Ignoring multiple shutdown events");
        return;
      }

      LOG.info("Handling DAGAppMaster shutdown");

      AMShutdownRunnable r = new AMShutdownRunnable();
      Thread t = new Thread(r, "AMShutdownThread");
      t.start();
    }

    private class AMShutdownRunnable implements Runnable {
      @Override
      public void run() {
        // TODO:currently just wait for some time so clients can know the
        // final states. Will be removed once RM come on. TEZ-160.
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        try {
          // Stop all services
          // This will also send the final report to the ResourceManager
          LOG.info("Calling stop for all the services");
          stop();

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

  /** Create and initialize (but don't start) a single dag. */
  protected DAG createDAG(DAGPlan dagPB) {
    TezDAGID dagId = new TezDAGID(appAttemptID.getApplicationId(),
        dagCounter.incrementAndGet());

    // Prepare the TaskAttemptListener server for authentication of Containers
    // TaskAttemptListener gets the information via jobTokenSecretManager.
    String dagIdString = dagId.toString();
    jobTokenSecretManager.addTokenForJob(dagIdString, sessionToken);
    LOG.info("Adding job token for " + dagIdString
        + " to jobTokenSecretManager");

    Iterator<PlanKeyValuePair> iter =
        dagPB.getDagKeyValues().getConfKeyValuesList().iterator();
    Configuration dagConf = new Configuration(amConf);

    while (iter.hasNext()) {
      PlanKeyValuePair keyValPair = iter.next();
      dagConf.set(keyValPair.getKey(), keyValPair.getValue());
    }

    // create single dag
    DAG newDag =
        new DAGImpl(dagId, dagConf, dagPB, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, tokens, clock,
            currentUser.getShortUserName(),
            taskHeartbeatHandler, context);

    return newDag;
  } // end createDag()

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


  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleanerImpl(context);
  }

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) {
    return new ContainerLauncherImpl(context);
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

  public DAGAppMasterState getState() {
    return state;
  }

  public List<String> getDiagnostics() {
    if (!isSession) {
      if(currentDAG != null) {
        return currentDAG.getDiagnostics();
      }
    } else {
      return Collections.singletonList("Session stats:"
          + "submittedDAGs=" + submittedDAGs.get()
          + ", successfulDAGs=" + successfulDAGs.get()
          + ", failedDAGs=" + failedDAGs.get()
          + ", killedDAGs=" + killedDAGs.get());
    }
    return null;
  }

  public float getProgress() {
    if(currentDAG != null && currentDAG.getState() == DAGState.RUNNING) {
      return currentDAG.getProgress();
    }
    return 0;
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

  synchronized void shutdownTezAM() {
    sessionStopped.set(true);
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

  synchronized String submitDAGToAppMaster(DAGPlan dagPlan)
      throws TezException  {
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
    LOG.info("Starting DAG submitted via RPC");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing DAG plan to: "
          + TezConfiguration.TEZ_PB_PLAN_TEXT_NAME);

      File outFile = new File(TezConfiguration.TEZ_PB_PLAN_TEXT_NAME);
      try {
        PrintWriter printWriter = new PrintWriter(outFile);
        String dagPbString = dagPlan.toString();
        printWriter.println(dagPbString);
        printWriter.close();
      } catch (IOException e) {
        throw new TezException("Failed to write TEZ_PLAN to "
            + outFile.toString(), e);
      }
    }

    submittedDAGs.incrementAndGet();
    startDAG(dagPlan);
    return currentDAG.getID().toString();
  }

  public class DAGClientHandler {

    public List<String> getAllDAGs() throws TezException {
      return Collections.singletonList(currentDAG.getID().toString());
    }

    public DAGStatus getDAGStatus(String dagIdStr,
                                  Set<StatusGetOpts> statusOptions)
        throws TezException {
      return getDAG(dagIdStr).getDAGStatus(statusOptions);
    }

    public VertexStatus getVertexStatus(String dagIdStr, String vertexName,
        Set<StatusGetOpts> statusOptions)
        throws TezException{
      VertexStatus status = getDAG(dagIdStr)
          .getVertexStatus(vertexName, statusOptions);
      if(status == null) {
        throw new TezException("Unknown vertexName: " + vertexName);
      }

      return status;
    }

    DAG getDAG(String dagIdStr) throws TezException {
      TezDAGID dagId = TezDAGID.fromString(dagIdStr);
      if(dagId == null) {
        throw new TezException("Bad dagId: " + dagIdStr);
      }

      if(currentDAG == null) {
        throw new TezException("No running dag at present");
      }
      if(!dagId.equals(currentDAG.getID())) {
        LOG.warn("Current DAGID : "
            + (currentDAG.getID() == null ? "NULL" : currentDAG.getID())
            + ", Looking for string (not found): " + dagIdStr + ", dagIdObj: "
            + dagId);
        throw new TezException("Unknown dagId: " + dagIdStr);
      }

      return currentDAG;
    }

    public void tryKillDAG(String dagIdStr)
        throws TezException {
      DAG dag = getDAG(dagIdStr);
      LOG.info("Sending client kill to dag: " + dagIdStr);
      //send a DAG_KILL message
      sendEvent(new DAGEvent(dag.getID(), DAGEventType.DAG_KILL));
    }

    public synchronized String submitDAG(DAGPlan dagPlan) throws TezException {
      return submitDAGToAppMaster(dagPlan);
    }

    public synchronized void shutdownAM() {
      LOG.info("Received message to shutdown AM");
      shutdownTezAM();
    }

    public synchronized TezSessionStatus getSessionStatus() throws TezException {
      if (!isSession) {
        throw new TezException("Unsupported operation as AM not running in"
            + " session mode");
      }
      switch (state) {
      case NEW:
      case INITED:
        return TezSessionStatus.INITIALIZING;
      case IDLE:
        return TezSessionStatus.READY;
      case RUNNING:
        return TezSessionStatus.RUNNING;
      case ERROR:
      case FAILED:
      case SUCCEEDED:
      case KILLED:
        return TezSessionStatus.SHUTDOWN;
      }
      return TezSessionStatus.INITIALIZING;
    }
  }

  private class RunningAppContext implements AppContext {

    private DAG dag;
    private final Configuration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    public RunningAppContext(Configuration config) {
      this.conf = config;
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
      try {
        rLock.lock();
        return dag;
      } finally {
        rLock.unlock();
      }
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
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
    public AMNodeMap getAllNodes() {
      return nodes;
    }

    @Override
    public TaskSchedulerEventHandler getTaskScheduler() {
      return taskSchedulerEventHandler;
    }

    @Override
    public Map<String, LocalResource> getSessionResources() {
      return sessionResources;
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
      try {
        wLock.lock();
        this.dag = dag;
      } finally {
        wLock.unlock();
      }
    }

  }

  private class ServiceWithDependency implements ServiceStateChangeListener {
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

  private class ServiceThread extends Thread {
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
      Exception ex = ServiceOperations.stopQuietly(LOG, service);
      if (ex != null && firstException == null) {
        firstException = ex;
      }
    }
    //after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void serviceStart() throws Exception {

    //start all the components
    startServices();
    super.serviceStart();

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("DAGAppMaster");

    this.appsStartTime = clock.getTime();
    AMStartedEvent startEvent = new AMStartedEvent(appAttemptID,
        startTime, appsStartTime, appSubmitTime);
    dispatcher.getEventHandler().handle(
        new DAGHistoryEvent(startEvent));

    this.lastDAGCompletionTime = clock.getTime();

    if (!isSession) {
      startDAG();
    } else {
      boolean preWarmContainersEnabled = amConf.getBoolean(
        TezConfiguration.TEZ_SESSION_PRE_WARM_ENABLED,
        TezConfiguration.TEZ_SESSION_PRE_WARM_ENABLED_DEFAULT);

      boolean ranPreWarmContainersDAG = false;
      if (preWarmContainersEnabled) {
        ranPreWarmContainersDAG = runPreWarmContainersDAG();
      }

      if (!ranPreWarmContainersDAG) {
        LOG.info("In Session mode. Waiting for DAG over RPC");
        this.state = DAGAppMasterState.IDLE;

        this.dagSubmissionTimer = new Timer(true);
        this.dagSubmissionTimer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            checkAndHandleSessionTimeout();
          }
        }, sessionTimeoutInterval, sessionTimeoutInterval / 10);
      }
    }
  }

  private boolean runPreWarmContainersDAG() throws Exception {

    InputStream dagPBBinaryStream = null;
    try {
      DAGPlan preWarmDAGPlan = null;
      String preWarmDAGPlanPathStr =
        amConf.get(TezConfiguration.TEZ_PRE_WARM_PB_PLAN_BINARY_PATH);
      if (preWarmDAGPlanPathStr == null
            || preWarmDAGPlanPathStr.isEmpty()) {
        LOG.info("No path to pre-warm DAG plan specified");
        return false;
      }

      LOG.info("Trying to run pre-warm DAG plan from specified path: "
          + preWarmDAGPlanPathStr);

      FileSystem fs = FileSystem.get(amConf);
      Path preWarmDAGPlanPath = new Path(preWarmDAGPlanPathStr);
      if (!fs.exists(preWarmDAGPlanPath)) {
        LOG.info("Could not find pre-warm DAG plan file, path="
          + preWarmDAGPlanPathStr);
        return false;
      }

      // Read the protobuf-based pre-warm DAG plan
      dagPBBinaryStream = fs.open(preWarmDAGPlanPath);
      preWarmDAGPlan = DAGPlan.parseFrom(dagPBBinaryStream);
      startDAG(preWarmDAGPlan);

    } finally {
      if (dagPBBinaryStream != null) {
        dagPBBinaryStream.close();
      }
    }
    return true;
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
    super.serviceStop();
  }

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(DAGEvent event) {
      ((EventHandler<DAGEvent>)context.getCurrentDAG()).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task =
          context.getCurrentDAG().getVertex(event.getTaskID().getVertexID()).
              getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = context.getCurrentDAG();
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
      org.apache.tez.dag.app.dag.Vertex vertex =
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
    if (this.state.equals(DAGAppMasterState.RUNNING)
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

      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      final Configuration conf = new Configuration(new YarnConfiguration());
      TezUtils.addUserSpecifiedTezConfiguration(conf);

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());

      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);

      // Command line options
      Options opts = new Options();
      opts.addOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION,
          false, "Run Tez Application Master in Session mode");

      CommandLine cliParser = new GnuParser().parse(opts, args);

      DAGAppMaster appMaster =
          new DAGAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime,
              cliParser.hasOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION));
      ShutdownHookManager.get().addShutdownHook(
        new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

      initAndStartAppMaster(appMaster, conf,
          jobUserName);

    } catch (Throwable t) {
      LOG.fatal("Error starting DAGAppMaster", t);
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
      if(appMaster.getServiceState() == STATE.STOPPED) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("DAGAppMaster already stopped. Ignoring signal");
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
          DAGAppMasterState.IDLE, DAGAppMasterState.RUNNING)
          .contains(appMaster.state)) {
            // DAG not in a final state. Must have receive a KILL signal
        appMaster.state = DAGAppMasterState.KILLED;
      }
      appMaster.stop();
    }
  }

  private void startDAG() throws IOException {
    FileInputStream dagPBBinaryStream = null;
    try {
      DAGPlan dagPlan = null;

      // Read the protobuf DAG
      dagPBBinaryStream = new FileInputStream(
          TezConfiguration.TEZ_PB_PLAN_BINARY_NAME);
      dagPlan = DAGPlan.parseFrom(dagPBBinaryStream);

      startDAG(dagPlan);

    } finally {
      if (dagPBBinaryStream != null) {
        dagPBBinaryStream.close();
      }
    }
  }

  private void startDAG(DAGPlan dagPlan) {
    this.state = DAGAppMasterState.RUNNING;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running a DAG with " + dagPlan.getVertexCount()
          + " vertices ");
      for (VertexPlan v : dagPlan.getVertexList()) {
        LOG.debug("DAG has vertex " + v.getName());
      }
    }

    // Job name is the same as the app name until we support multiple dags
    // for an app later
    appName = dagPlan.getName();

    // /////////////////// Create the job itself.
    DAG newDAG = createDAG(dagPlan);
    startDAG(newDAG);
  }

  private void startDAG(DAG dag) {
    currentDAG = dag;

    // End of creating the job.
    ((RunningAppContext) context).setDAG(currentDAG);

    // create a job event for job initialization
    DAGEvent initDagEvent = new DAGEvent(currentDAG.getID(), DAGEventType.DAG_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    dagEventDispatcher.handle(initDagEvent);

    // All components have started, start the job.
    /** create a job-start event to get this ball rolling */
    DAGEvent startDagEvent = new DAGEvent(currentDAG.getID(), DAGEventType.DAG_START);
    /** send the job-start event. this triggers the job execution. */
    sendEvent(startDagEvent);
  }

  // TODO XXX Does this really need to be a YarnConfiguration ?
  protected static void initAndStartAppMaster(final DAGAppMaster appMaster,
      final Configuration conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    appMaster.currentUser = UserGroupInformation.getCurrentUser();
        Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();

    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.addCredentials(credentials);

    // Now remove the AM->RM token so tasks don't have it
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }

    appMaster.tokens = credentials;

    appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
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
}

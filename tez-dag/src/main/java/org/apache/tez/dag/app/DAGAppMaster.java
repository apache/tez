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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGFinishEvent;
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
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.app.taskclean.TaskCleaner;
import org.apache.tez.dag.app.taskclean.TaskCleanerImpl;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.engine.common.security.JobTokenSecretManager;

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
  private final DAGPlan dagPlan;
  private long dagsStartTime;
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
  // TODO Metrics
  //protected final DAGAppMetrics metrics;
  // TODO Recovery
  //private Map<TezTaskID, TaskInfo> completedTasksFromPreviousRun;
  private AppContext context;
  private TezConfiguration conf;
  private Dispatcher dispatcher;
  // TODO Recovery
  //private Recovery recoveryServ;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  //private Speculator speculator;
  private ContainerHeartbeatHandler containerHeartbeatHandler;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  // TODODAGAM Define DAGID
  private TezDAGID dagId;
  //  private boolean newApiCommitter;
  private DagEventDispatcher dagEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  //private SpeculatorEventDispatcher speculatorEventDispatcher;
  private TaskSchedulerEventHandler taskSchedulerEventHandler;
  private HistoryEventHandler historyEventHandler;

  private DAGAppMasterState state;

  DAGClientServer clientRpcServer;
  private DAGClientHandler clientHandler;

  private DAG dag;
  private Credentials fsTokens = new Credentials(); // Filled during init
  private UserGroupInformation currentUser; // Will be setup during init

  // must be LinkedHashMap to preserve order of service addition
  Map<Service, ServiceWithDependency> services = 
      new LinkedHashMap<Service, ServiceWithDependency>();


  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime, DAGPlan dagPB) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime, dagPB);
  }

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, DAGPlan dagPB) {
    super(DAGAppMaster.class.getName());
    this.dagPlan = dagPB;
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.state = DAGAppMasterState.NEW;
    // TODO Metrics
    //this.metrics = DAGAppMetrics.create();
    LOG.info("Created DAGAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void serviceInit(final Configuration tezConf) throws Exception {

    this.state = DAGAppMasterState.INITED;

    assert tezConf instanceof TezConfiguration;

    this.conf = (TezConfiguration) tezConf;
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    downloadTokensAndSetupUGI(conf);

    context = new RunningAppContext(conf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = dagPlan.getName();

    dagId = new TezDAGID(appAttemptID.getApplicationId(), 1);

    clientHandler = new DAGClientHandler();

    dispatcher = createDispatcher();
    addIfService(dispatcher, false);

    clientRpcServer = new DAGClientServer(clientHandler);
    addIfService(clientRpcServer, true);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    addIfService(taskHeartbeatHandler, true);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    addIfService(containerHeartbeatHandler, true);

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

    //    TODO XXX: Rename to NMComm
    //    corresponding service to launch allocated containers via NodeManager
    //    containerLauncher = createNMCommunicator(context);
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher, true);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    historyEventHandler = new HistoryEventHandler(context);
    addIfService(historyEventHandler, true);
    dispatcher.register(HistoryEventType.class, historyEventHandler);

    initServices(conf);
    super.serviceInit(conf);
  } // end of init()

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }

  protected class JobFinishEventHandlerCR
    implements EventHandler<DAGFinishEvent> {
    // Considering TaskAttempts are marked as completed before a container exit,
    // it's very likely that a Container may not have "completed" by the time a
    // job completes. This would imply that TaskAtetmpts may not be at a FINAL
    // internal state (state machine state), and cleanup would not have happened.

    // Since the shutdown handler has been called in the same thread which
    // is handling all other async events, creating a separate thread for shutdown.
    //
    // For now, checking to see if all containers have COMPLETED, with a 5
    // second timeout before the exit.
    public void handle(DAGFinishEvent event) {
      setStateOnDAGCompletion();
      LOG.info("Handling JobFinished Event");
      AMShutdownRunnable r = new AMShutdownRunnable();
      Thread t = new Thread(r, "AMShutdownThread");
      t.start();
    }

    // TODO Job End Notification
    /*
    protected void maybeSendJobEndNotification() {
      if (getConfig().get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL) != null) {
        try {
          LOG.info("Job end notification started for jobID : "
              + job.getID());
          JobEndNotifier notifier = new JobEndNotifier();
          notifier.setConf(getConfig());
          notifier.notify(job.getReport());
        } catch (InterruptedException ie) {
          LOG.warn("Job end notification interrupted for jobID : "
              + job.getReport().getDAGId(), ie);
        }
      }
    }
    */

    protected void stopAllServices() {
      try {
        // Stop all services
        // This will also send the final report to the ResourceManager
        LOG.info("Calling stop for all the services");
        stop();

      } catch (Throwable t) {
        LOG.warn("Graceful stop failed ", t);
      }
    }

    protected void exit() {
      LOG.info("Exiting MR AppMaster..GoodBye!");
      sysexit();
    }

    private void stopAM() {
      stopAllServices();
      exit();
    }

    protected boolean allContainersComplete() {
      for (AMContainer amContainer : context.getAllContainers().values()) {
        if (amContainer.getState() != AMContainerState.COMPLETED) {
          return false;
        }
      }
      return true;
    }

    protected boolean allTaskAttemptsComplete() {
      // TODO XXX: Implement.
      // TaskAttempts will transition to their final state machine state only
      // after a container is complete and sends out a TA_TERMINATED event.
      return true;
    }

    private class AMShutdownRunnable implements Runnable {
      @Override
      public void run() {
        // TODO Job End Notification
        //maybeSendJobEndNotification();
        // TODO XXX Add a timeout.
        LOG.info("Waiting for all containers and TaskAttempts to complete");
        if (!dag.isUber()) {
          while (!allContainersComplete() || !allTaskAttemptsComplete()) {
            try {
              synchronized (this) {
                wait(100l);
              }
            } catch (InterruptedException e) {
              LOG.info("AM Shutdown Thread interrupted. Exiting");
              break;
            }
          }
          LOG.info("All Containers and TaskAttempts Complete. Stopping services");
        } else {
          LOG.info("Uberized job. Not waiting for all containers to finish");
        }
        stopAM();
        LOG.info("AM Shutdown Thread Completing");
      }
    }
  }

  private void handle(DAGAppMasterEvent event) {
    switch (event.getType()) {
    case INTERNAL_ERROR:
      state = DAGAppMasterState.ERROR;
      sendEvent(new DAGEvent(dag.getID(), DAGEventType.INTERNAL_ERROR));
      break;
    default:
      LOG.warn("No handler for event type: " + event.getType());
    }
  }

  private class DAGAppMasterEventHandler implements
      EventHandler<DAGAppMasterEvent> {
    @Override
    public void handle(DAGAppMasterEvent event) {
      DAGAppMaster.this.handle(event);
    }
  }

  private class DAGFinishEventHandler implements EventHandler<DAGFinishEvent> {
    @Override
    public void handle(DAGFinishEvent event) {
      // job has finished
      // this is the only job, so shut down the Appmaster
      // note in a workflow scenario, this may lead to creation of a new
      // job (FIXME?)
      // TODO Job End Notification
      /*
      // Send job-end notification
      if (getConfig().get(MRJobConfig.MR_JOB_END_NOTIFICATION_URL) != null) {
        try {
          LOG.info("Job end notification started for jobID : "
              + job.getReport().getDAGId());
          JobEndNotifier notifier = new JobEndNotifier();
          notifier.setConf(getConfig());
          notifier.notify(job.getReport());
        } catch (InterruptedException ie) {
          LOG.warn("Job end notification interrupted for jobID : "
              + job.getReport().getDAGId(), ie);
        }
      }
    */
      // TODO:currently just wait for some time so clients can know the
      // final states. Will be removed once RM come on.

      setStateOnDAGCompletion();

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

      } catch (Throwable t) {
        LOG.warn("Graceful stop failed ", t);
      }

      //Bring the process down by force.
      //Not needed after HADOOP-7140
      LOG.info("Exiting MR AppMaster..GoodBye!");
      sysexit();
    }
  }

  /**
   * create an event handler that handles the job finish event.
   * @return the dag finish event handler.
   */
  protected EventHandler<DAGFinishEvent> createDAGFinishEventHandler() {
    return new DAGFinishEventHandler();
  }

  /**
   * Create the recovery service.
   * @return an instance of the recovery service.
   */
  // TODO Recovery
  /*
  protected Recovery createRecoveryService(AppContext appContext) {
    return new RecoveryService(appContext, getCommitter());
  }
  */

  /**
   * Create the RMContainerRequestor.
   *
   * @param clientService
   *          the MR Client Service.
   * @param appContext
   *          the application context.
   * @return an instance of the RMContainerRequestor.
   */
//  protected ContainerRequestor createContainerRequestor(
//      ClientService clientService, AppContext appContext) {
//    return new ContainerRequestorRouter(clientService, appContext);
//  }

  /** Create and initialize (but don't start) a single dag. */
  protected DAG createDAG(DAGPlan dagPB) {

    // create single job
    DAG newDag =
        new DAGImpl(dagId, conf, dagPB, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, fsTokens, clock,
            currentUser.getShortUserName(),
            taskHeartbeatHandler, context);
    ((RunningAppContext) context).setDAG(newDag);

    dispatcher.register(DAGFinishEvent.Type.class,
        createDAGFinishEventHandler());
    return newDag;
  } // end createDag()


  /**
   * Obtain the tokens needed by the job and put them in the UGI
   * @param conf
   */
  protected void downloadTokensAndSetupUGI(TezConfiguration conf) {
    // TODO remove - TEZ-71
    try {
      this.currentUser = UserGroupInformation.getCurrentUser();

      if (UserGroupInformation.isSecurityEnabled()) {
        // Read the file-system tokens from the localized tokens-file.
        Path jobSubmitDir =
            FileContext.getLocalFSFileContext().makeQualified(
                new Path(new File(TezConfiguration.JOB_SUBMIT_DIR)
                    .getAbsolutePath()));
        Path jobTokenFile =
            new Path(jobSubmitDir, TezConfiguration.APPLICATION_TOKENS_FILE);
        fsTokens.addAll(Credentials.readTokenStorageFile(jobTokenFile, conf));
        LOG.info("jobSubmitDir=" + jobSubmitDir + " jobTokenFile="
            + jobTokenFile);

        for (Token<? extends TokenIdentifier> tk : fsTokens.getAllTokens()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Token of kind " + tk.getKind()
                + "in current ugi in the AppMaster for service "
                + tk.getService());
          }
          currentUser.addToken(tk); // For use by AppMaster itself.
        }
      }
    } catch (IOException e) {
      throw new TezUncheckedException(e);
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
      TezConfiguration conf) {
    TaskHeartbeatHandler thh = new TaskHeartbeatHandler(context, conf.getInt(
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT));
    return thh;
  }

  protected ContainerHeartbeatHandler createContainerHeartbeatHandler(AppContext context,
      TezConfiguration conf) {
    ContainerHeartbeatHandler chh = new ContainerHeartbeatHandler(context, conf.getInt(
        TezConfiguration.TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT,
        TezConfiguration.TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT_DEFAULT));
    return chh;
  }


  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleanerImpl(context);
  }

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) {
    return new ContainerLauncherImpl(context);
  }

  //TODO:should have an interface for MRClientService
  /*
  protected ClientService createClientService(AppContext context) {
    return new MRClientService(context);
  }
  */

  public ApplicationId getAppID() {
    return appAttemptID.getApplicationId();
  }

  public ApplicationAttemptId getAttemptID() {
    return appAttemptID;
  }

  public TezDAGID getDAGId() {
    return dagId;
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
    return dag.getDiagnostics();
  }

  public float getProgress() {
    return dag.getProgress();
  }

  void setStateOnDAGCompletion() {
    DAGAppMasterState oldState = state;
    if(state == DAGAppMasterState.RUNNING) {
      switch(dag.getState()) {
      case SUCCEEDED:
        state = DAGAppMasterState.SUCCEEDED;
        break;
      case FAILED:
        state = DAGAppMasterState.FAILED;
        break;
      case KILLED:
        state = DAGAppMasterState.KILLED;
      case ERROR:
        state = DAGAppMasterState.ERROR;
      default:
        state = DAGAppMasterState.ERROR;
      }
    }
    LOG.info("On DAG completion. Old state: " + oldState + " new state: " + state);
  }

  public class DAGClientHandler {

    public List<String> getAllDAGs() throws TezException {
      return Collections.singletonList(dag.getID().toString());
    }

    public DAGStatus getDAGStatus(String dagIdStr)
                                      throws TezException {
      return getDAG(dagIdStr).getDAGStatus();
    }

    public VertexStatus getVertexStatus(String dagIdStr, String vertexName)
        throws TezException{
      VertexStatus status = getDAG(dagIdStr).getVertexStatus(vertexName);
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
      if(!dagId.equals(dag.getID())) {
        throw new TezException("Unknown dagId: " + dagIdStr);
      }
      return dag;
    }
    
    public void tryKillDAG(String dagIdStr)
        throws TezException {
      DAG dag = getDAG(dagIdStr);
      dag.tryKill();
    }
  }

  private class RunningAppContext implements AppContext {

    private DAG dag;
    private final TezConfiguration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    public RunningAppContext(TezConfiguration config) {
      this.conf = config;
    }
    
    @Override
    public DAGAppMaster getAppMaster() {
      return DAGAppMaster.this;
    }
    
    @Override
    public TezConfiguration getConf() {
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
    public DAG getDAG() {
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
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      if (getServiceState() != STATE.STARTED) {
        throw new TezUncheckedException(
            "Cannot get ApplicationACLs before all services have started");
      }
      return taskSchedulerEventHandler.getApplicationAcls();
    }
    
    @Override
    public TezDAGID getDAGID() {
      try {
        rLock.lock();
        return dag.getID();
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

    @Override
    public void stateChanged(Service dependency) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Service dependency: " + dependency.getName() + " notify" + 
                  " for service: " + service.getName());
      }
      if(dependency.isInState(Service.STATE.STARTED)) {
        if(dependenciesStarted.incrementAndGet() == dependencies.size()) {
          synchronized(this) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Service: " + service.getName() + " notified to start");
            }
            canStart = true;
            this.notifyAll();
          }
        }
      }
    }
    
    void start() throws InterruptedException {
      if(dependencies.size() > 0) {
        synchronized(this) {
          while(!canStart) {
            this.wait(1000*60*3L);
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
  
  void initServices(TezConfiguration conf) {
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
  public void serviceStart() throws Exception {

    this.state = DAGAppMasterState.RUNNING;

    // /////////////////// Create the job itself.
    dag = createDAG(dagPlan);

    // End of creating the job.

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("DAGAppMaster");

    // create a job event for job intialization
    DAGEvent initDagEvent = new DAGEvent(dag.getID(), DAGEventType.DAG_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    dagEventDispatcher.handle(initDagEvent);

    //start all the components
    startServices();
    super.serviceStart();

    this.dagsStartTime = clock.getTime();
    AMStartedEvent startEvent = new AMStartedEvent(appAttemptID,
        startTime, dagsStartTime, appSubmitTime);
    dispatcher.getEventHandler().handle(
        new DAGHistoryEvent(this.dagId, startEvent));

    // All components have started, start the job.
    startDags();
  }
  
  @Override
  public void serviceStop() throws Exception {
    stopServices();
    super.serviceStop();
  }

  /**
   * This can be overridden to instantiate multiple jobs and create a
   * workflow.
   *
   * TODO:  Rework the design to actually support this.  Currently much of the
   * job stuff has been moved to init() above to support uberization (MR-1220).
   * In a typical workflow, one presumably would want to uberize only a subset
   * of the jobs (the "small" ones), which is awkward with the current design.
   */
  @SuppressWarnings("unchecked")
  protected void startDags() {
    /** create a job-start event to get this ball rolling */
    DAGEvent startDagEvent = new DAGEvent(dag.getID(), DAGEventType.DAG_START);
    /** send the job-start event. this triggers the job execution. */
    dispatcher.getEventHandler().handle(startDagEvent);
  }

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(DAGEvent event) {
      ((EventHandler<DAGEvent>)context.getDAG()).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task =
          context.getDAG().getVertex(event.getTaskID().getVertexID()).
              getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = context.getDAG();
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
      DAG dag = context.getDAG();
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

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      // TODO: Deprecated keys?
      //DeprecatedKeys.init();
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

      TezConfiguration conf = new TezConfiguration(new YarnConfiguration());

      DAGPlan dagPlan = null;

      // Read the protobuf DAG
      DAGPlan.Builder dagPlanBuilder = DAGPlan.newBuilder();
      FileInputStream dagPBBinaryStream = null;
      try {
        dagPBBinaryStream = new FileInputStream(
            TezConfiguration.TEZ_AM_PLAN_PB_BINARY);
        dagPlanBuilder.mergeFrom(dagPBBinaryStream);
      } finally {
        if (dagPBBinaryStream != null) {
          dagPBBinaryStream.close();
        }
      }

      dagPlan = dagPlanBuilder.build();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Running a DAG with "
            + dagPlan.getVertexCount() + " vertices ");
        for (VertexPlan v : dagPlan.getVertexList()) {
          LOG.debug("DAG has vertex " + v.getName());
        }
      }

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());

      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);

      Map<String, String> config = DagTypeConverters.createSettingsMapFromDAGPlan(dagPlan.getJobSettingList());
      for(Entry<String, String> entry : config.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }

      DAGAppMaster appMaster =
          new DAGAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime, dagPlan);
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
      LOG.info("DAGAppMaster received a signal. Signaling TaskScheduler and "
        + "JobHistoryEventHandler.");
      // Notify TaskScheduler that a SIGTERM has been received so
      // that it doesnt take too long in shutting down

      // Signal the task scheduler.
      if(appMaster.getServiceState() == STATE.STARTED) {
        appMaster.taskSchedulerEventHandler.setSignalled(true);
      }
      if (EnumSet.of(DAGAppMasterState.NEW, DAGAppMasterState.INITED,
          DAGAppMasterState.RUNNING).contains(appMaster.state)) {
        // DAG not in a final state. Must have receive a KILL signal
        appMaster.state = DAGAppMasterState.KILLED;
      }

      appMaster.stop();
    }
  }

  // TODO XXX Does this really need to be a YarnConfiguration ?
  protected static void initAndStartAppMaster(final DAGAppMaster appMaster,
      final TezConfiguration conf, String jobUserName) throws IOException,
      InterruptedException {
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.addCredentials(credentials);
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

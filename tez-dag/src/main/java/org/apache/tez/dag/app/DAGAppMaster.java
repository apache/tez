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
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.DAGConfiguration;
import org.apache.tez.dag.api.DAGLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.client.ClientService;
import org.apache.tez.dag.app.client.impl.TezClientService;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
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
import org.apache.tez.dag.app.local.LocalContainerRequestor;
import org.apache.tez.dag.app.rm.AMSchedulerEvent;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.ContainerAllocator;
import org.apache.tez.dag.app.rm.ContainerRequestor;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.RMCommunicator;
import org.apache.tez.dag.app.rm.RMCommunicatorEvent;
import org.apache.tez.dag.app.rm.RMContainerRequestor;
import org.apache.tez.dag.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.app.taskclean.TaskCleaner;
import org.apache.tez.dag.app.taskclean.TaskCleanerImpl;
import org.apache.tez.engine.common.security.JobTokenSecretManager;
import org.apache.tez.engine.records.TezDAGID;

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
public class DAGAppMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(DAGAppMaster.class);

  /**
   * Priority of the MRAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private Clock clock;
  private final DAGConfiguration dagPlan;
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
  //protected final MRAppMetrics metrics;
  // TODO Recovery
  //private Map<TezTaskID, TaskInfo> completedTasksFromPreviousRun;
  private AppContext context;
  private TezConfiguration conf; 
  private Dispatcher dispatcher;
  private ClientService clientService;
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
  private AbstractService stagingDirCleanerService;
  private boolean inRecovery = false;
  //private SpeculatorEventDispatcher speculatorEventDispatcher;
  private TaskSchedulerEventHandler taskSchedulerEventHandler;

  private DAGLocationHint dagLocationHint;


  private DAG dag;
  private Credentials fsTokens = new Credentials(); // Filled during init
  private UserGroupInformation currentUser; // Will be setup during init

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime, DAGConfiguration dagPlan) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime, dagPlan);
  }

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, DAGConfiguration dagPlan) {
    super(DAGAppMaster.class.getName());
    this.dagPlan = dagPlan;
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    // TODO Metrics
    //this.metrics = MRAppMetrics.create();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void init(final Configuration tezConf) {

    assert tezConf instanceof TezConfiguration;
    
    this.conf = (TezConfiguration) tezConf;
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    downloadTokensAndSetupUGI(conf);
    setupDAGLocationHint(dagPlan);

    context = new RunningAppContext(conf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = dagPlan.get(TezConfiguration.JOB_NAME, "<missing app name>");

    dagId = new TezDAGID(appAttemptID.getApplicationId(), 1);

    // TODO Committer.
    //    committer = createOutputCommitter(conf);

    // TODO Recovery
    /*
    boolean recoveryEnabled = conf.getBoolean(
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    boolean recoverySupportedByCommitter = committer.isRecoverySupported();
    if (recoveryEnabled && recoverySupportedByCommitter
        && appAttemptID.getAttemptId() > 1) {
      LOG.info("Recovery is enabled. "
          + "Will try to recover from previous life on best effort basis.");
      recoveryServ = createRecoveryService(context);
      addIfService(recoveryServ);
      dispatcher = recoveryServ.getDispatcher();
      clock = recoveryServ.getClock();
      inRecovery = true;
    } else {
      LOG.info("Not starting RecoveryService: recoveryEnabled: "
          + recoveryEnabled + " recoverySupportedByCommitter: "
          + recoverySupportedByCommitter + " ApplicationAttemptID: "
          + appAttemptID.getAttemptId());
      dispatcher = createDispatcher();
      addIfService(dispatcher);
    }
    */

    dispatcher = createDispatcher();
    addIfService(dispatcher);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    addIfService(taskHeartbeatHandler);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    addIfService(containerHeartbeatHandler);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context,
        taskHeartbeatHandler, containerHeartbeatHandler);
    addIfService(taskAttemptListener);

    containers = new AMContainerMap(containerHeartbeatHandler,
        taskAttemptListener, context);
    addIfService(containers);
    dispatcher.register(AMContainerEventType.class, containers);

    nodes = new AMNodeMap(dispatcher.getEventHandler(), context);
    addIfService(nodes);
    dispatcher.register(AMNodeEventType.class, nodes);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    addIfService(taskCleaner);

    // TODO TEZ-9
    //service to handle requests from JobClient
    clientService = new TezClientService();
    addIfService(clientService);

    // TODO JobHistory
    /*
    //service to log job history events
    jobHistoryEventHandler = createJobHistoryHandler(context);
    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        jobHistoryEventHandler);
     */

    this.dagEventDispatcher = new DagEventDispatcher();
    this.vertexEventDispatcher = new VertexEventDispatcher();

    //register the event dispatchers
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);

    // TODO TEZ-14
    // speculator = createSpeculator(conf, context);
    // addIfService(speculator);
    //speculatorEventDispatcher = new SpeculatorEventDispatcher(conf);
    //dispatcher.register(Speculator.EventType.class,
    //    speculatorEventDispatcher);

    //    TODO XXX: Rename to NMComm
    //    corresponding service to launch allocated containers via NodeManager
    //    containerLauncher = createNMCommunicator(context);
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    taskSchedulerEventHandler = new TaskSchedulerEventHandler(context,
        clientService);
    addIfService(taskSchedulerEventHandler);
    dispatcher.register(AMSchedulerEventType.class,
        taskSchedulerEventHandler);

    // Add the staging directory cleaner before the history server but after
    // the container allocator so the staging directory is cleaned after
    // the history has been flushed but before unregistering with the RM.
    this.stagingDirCleanerService = createStagingDirCleaningService();
    addService(stagingDirCleanerService);


    // Add the JobHistoryEventHandler last so that it is properly stopped first.
    // This will guarantee that all history-events are flushed before AM goes
    // ahead with shutdown.
    // Note: Even though JobHistoryEventHandler is started last, if any
    // component creates a JobHistoryEvent in the meanwhile, it will be just be
    // queued inside the JobHistoryEventHandler
    // TODO JobHistory
    //addIfService(this.jobHistoryEventHandler);

    super.init(conf);
  } // end of init()

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

//  private OutputCommitter createOutputCommitter(Configuration conf) {
//    OutputCommitter committer = null;
//
//    LOG.info("OutputCommitter set in config "
//        + conf.get("mapred.output.committer.class"));
//
//    if (newApiCommitter) {
//      TezTaskID taskID =
//          TezBuilderUtils.newTaskId(dagId, -1, 0);
//      TezTaskAttemptID attemptID =
//          TezBuilderUtils.newTaskAttemptId(taskID, 0);
//      TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf,
//          TezMRTypeConverter.fromTez(attemptID));
//      OutputFormat outputFormat;
//      try {
//        outputFormat = ReflectionUtils.newInstance(taskContext
//            .getOutputFormatClass(), conf);
//        committer = outputFormat.getOutputCommitter(taskContext);
//      } catch (Exception e) {
//        throw new YarnException(e);
//      }
//    } else {
//      committer = ReflectionUtils.newInstance(conf.getClass(
//          "mapred.output.committer.class", FileOutputCommitter.class,
//          org.apache.hadoop.mapred.OutputCommitter.class), conf);
//    }
//    LOG.info("OutputCommitter is " + committer.getClass().getName());
//    return committer;
//  }

//  protected boolean keepJobFiles(JobConf conf) {
//    return (conf.getKeepTaskFilesPattern() != null || conf
//        .getKeepFailedTaskFiles());
//  }

  /**
   * Create the default file System for this job.
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  /**
   * clean up staging directories for the job.
   * @throws IOException
   */
  // TODO DAG Cleanup staging directory as a user task, or a post dag plugin.
//  public void cleanupStagingDir() throws IOException {
//    /* make sure we clean the staging files */
//    String jobTempDir = null;
//    FileSystem fs = getFileSystem(getConfig());
//    try {
//      if (!keepJobFiles(new JobConf(getConfig()))) {
//        jobTempDir = getConfig().get(MRJobConfig.MAPREDUCE_JOB_DIR);
//        if (jobTempDir == null) {
//          LOG.warn("Job Staging directory is null");
//          return;
//        }
//        Path jobTempDirPath = new Path(jobTempDir);
//        LOG.info("Deleting staging directory " + FileSystem.getDefaultUri(getConfig()) +
//            " " + jobTempDir);
//        fs.delete(jobTempDirPath, true);
//      }
//    } catch(IOException io) {
//      LOG.error("Failed to cleanup staging dir " + jobTempDir, io);
//    }
//  }

  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }
  protected class JobFinishEventHandlerCR implements EventHandler<DAGFinishEvent> {
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
  protected ContainerRequestor createContainerRequestor(
      ClientService clientService, AppContext appContext) {
    return new ContainerRequestorRouter(clientService, appContext);
  }

  /**
   * Create the AM Scheduler.
   *
   * @param requestor
   *          The Container Requestor.
   * @param appContext
   *          the application context.
   * @return an instance of the AMScheduler.
   */
  protected ContainerAllocator createAMScheduler(ContainerRequestor requestor,
      AppContext appContext) {
    return new AMSchedulerRouter(requestor, appContext);
  }

  /** Create and initialize (but don't start) a single dag. */
  protected DAG createDAG(DAGConfiguration dagPlan) {

    // create single job
    DAG newDag =
        new DAGImpl(dagId, appAttemptID, conf, dagPlan, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, fsTokens, clock,
            // TODO Recovery
            //completedTasksFromPreviousRun,
            // TODO Metrics
            //metrics,
            //committer, newApiCommitter,
            currentUser.getUserName(), appSubmitTime,
            //amInfos,
            taskHeartbeatHandler, context, dagLocationHint);
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
      throw new YarnException(e);
    }
  }

  protected void setupDAGLocationHint(DAGConfiguration conf) {
    try {
      String dagLocationHintFile =
          conf.get(DAGConfiguration.DAG_LOCATION_HINT_RESOURCE_FILE,
              DAGConfiguration.DEFAULT_DAG_LOCATION_HINT_RESOURCE_FILE);
      File f = new File(dagLocationHintFile);
      if (f.exists()) {
        this.dagLocationHint = DAGLocationHint.initDAGDagLocationHint(
            dagLocationHintFile);
      } else {
        this.dagLocationHint = new DAGLocationHint();
      }
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  // TODO JobHistory
  /*
  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(
      AppContext context) {
    return new JobHistoryEventHandler2(context, getStartCount());
  }
  */

  protected AbstractService createStagingDirCleaningService() {
    return new StagingDirCleaningService();
  }

//  protected Speculator createSpeculator(Configuration conf, AppContext context) {
//    Class<? extends Speculator> speculatorClass;
//
//    try {
//      speculatorClass
//          // "yarn.mapreduce.job.speculator.class"
//          = conf.getClass(DAGConfiguration.DAG_AM_SPECULATOR_CLASS,
//                          DefaultSpeculator.class,
//                          Speculator.class);
//      Constructor<? extends Speculator> speculatorConstructor
//          = speculatorClass.getConstructor
//               (Configuration.class, AppContext.class);
//      Speculator result = speculatorConstructor.newInstance(conf, context);
//
//      return result;
//    } catch (InstantiationException ex) {
//      LOG.error("Can't make a speculator -- check "
//          + DAGConfiguration.DAG_AM_SPECULATOR_CLASS, ex);
//      throw new YarnException(ex);
//    } catch (IllegalAccessException ex) {
//      LOG.error("Can't make a speculator -- check "
//          + DAGConfiguration.DAG_AM_SPECULATOR_CLASS, ex);
//      throw new YarnException(ex);
//    } catch (InvocationTargetException ex) {
//      LOG.error("Can't make a speculator -- check "
//          + DAGConfiguration.DAG_AM_SPECULATOR_CLASS, ex);
//      throw new YarnException(ex);
//    } catch (NoSuchMethodException ex) {
//      LOG.error("Can't make a speculator -- check "
//          + DAGConfiguration.DAG_AM_SPECULATOR_CLASS, ex);
//      throw new YarnException(ex);
//    }
//  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpTezDag(context, thh, chh,jobTokenSecretManager);
    return lis;
  }

  protected TaskHeartbeatHandler createTaskHeartbeatHandler(AppContext context,
      TezConfiguration conf) {
    TaskHeartbeatHandler thh = new TaskHeartbeatHandler(context, conf.getInt(
        TezConfiguration.DAG_AM_TASK_LISTENER_THREAD_COUNT,
        TezConfiguration.DAG_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT));
    return thh;
  }

  protected ContainerHeartbeatHandler createContainerHeartbeatHandler(AppContext context,
      TezConfiguration conf) {
    ContainerHeartbeatHandler chh = new ContainerHeartbeatHandler(context, conf.getInt(
        TezConfiguration.DAG_AM_CONTAINER_LISTENER_THREAD_COUNT,
        TezConfiguration.DAG_AM_CONTAINER_LISTENER_THREAD_COUNT_DEFAULT));
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

  // TODO Recovery
  /*
  public Map<TezTaskID, TaskInfo> getCompletedTaskFromPreviousRun() {
    return completedTasksFromPreviousRun;
  }
  */

  public ContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public TaskAttemptListener getTaskAttemptListener() {
    return taskAttemptListener;
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerRequestorRouter extends AbstractService
      implements ContainerRequestor {
    private final ClientService clientService;
    private final AppContext context;
    private ContainerRequestor real;

    public ContainerRequestorRouter(ClientService clientService,
        AppContext appContext) {
      super(ContainerRequestorRouter.class.getName());
      this.clientService = clientService;
      this.context = appContext;
    }

    @Override
    public void start() {
      if (dag.isUber()) {
        real = new LocalContainerRequestor(clientService,
            context);
      } else {
        real = new RMContainerRequestor(clientService, context);
      }
      ((Service)this.real).init(getConfig());
      ((Service)this.real).start();
      super.start();
    }

    @Override
    public void stop() {
      if (real != null) {
        ((Service) real).stop();
      }
      super.stop();
    }

    @Override
    public void handle(RMCommunicatorEvent event) {
      real.handle(event);
    }

    @Override
    public Resource getAvailableResources() {
      return real.getAvailableResources();
    }

    @Override
    public void addContainerReq(ContainerRequest req) {
      real.addContainerReq(req);
    }

    @Override
    public void decContainerReq(ContainerRequest req) {
      real.decContainerReq(req);
    }

    public void setSignalled(boolean isSignalled) {
      ((RMCommunicator) real).setSignalled(isSignalled);
    }

    @Override
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      return ((RMCommunicator)real).getApplicationAcls();
    }
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class AMSchedulerRouter extends AbstractService
      implements ContainerAllocator {
    private final ContainerRequestor requestor;
    private final AppContext context;
    private ContainerAllocator containerAllocator;

    AMSchedulerRouter(ContainerRequestor requestor,
        AppContext context) {
      super(AMSchedulerRouter.class.getName());
      this.requestor = requestor;
      this.context = context;
    }

    @Override
    public synchronized void start() {
      // TODO LocalContainerAllocator
      /*
      if (job.isUber()) {
        this.containerAllocator = new LocalContainerAllocator(this.context,
            jobId, nmHost, nmPort, nmHttpPort, containerID,
            (MRxTaskUmbilicalProtocol) taskAttemptListener, taskAttemptListener,
            (RMCommunicator) this.requestor);
      } else {
        this.containerAllocator = new RMContainerAllocator(this.requestor,
            this.context);
      }
      */
      // TODO Fix ContainerAllocator?
      this.containerAllocator = null;
      //new RMContainerAllocator(this.requestor,this.context);

      ((Service)this.containerAllocator).init(getConfig());
      ((Service)this.containerAllocator).start();
      super.start();
    }

    @Override
    public synchronized void stop() {
      if (containerAllocator != null) {
        ((Service) this.containerAllocator).stop();
        super.stop();
      }
    }

    @Override
    public void handle(AMSchedulerEvent event) {
      this.containerAllocator.handle(event);
    }
  }

  public TaskHeartbeatHandler getTaskHeartbeatHandler() {
    return taskHeartbeatHandler;
  }

  private final class StagingDirCleaningService extends AbstractService {
    StagingDirCleaningService() {
      super(StagingDirCleaningService.class.getName());
    }

    @Override
    public synchronized void stop() {
//      try {
//        cleanupStagingDir();
//      } catch (IOException io) {
//        LOG.error("Failed to cleanup staging dir: ", io);
//      }
      super.stop();
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
      return this.conf.get(TezConfiguration.USER_NAME);
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
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      if (getServiceState() != STATE.STARTED) {
        throw new YarnException(
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

  @SuppressWarnings("unchecked")
  @Override
  public void start() {

    // TODO Recovery
    // Pull completedTasks etc from recovery
    /*
    if (inRecovery) {
      completedTasksFromPreviousRun = recoveryServ.getCompletedTasks();
      amInfos = recoveryServ.getAMInfos();
    }
    */
    
    // /////////////////// Create the job itself.
    dag = createDAG(dagPlan);

    // End of creating the job.

    // TODO: JobHistory
    // Send out an MR AM inited event for this AM and all previous AMs.
    /*
    for (AMInfo info : amInfos) {
      dispatcher.getEventHandler().handle(
          new JobHistoryEvent(job.getID(), new AMStartedEvent(info
              .getAppAttemptId(), info.getStartTime(), info.getContainerId(),
              info.getNodeManagerHost(), info.getNodeManagerPort(), info
                  .getNodeManagerHttpPort())));
    }
    */

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("MRAppMaster");

    // create a job event for job intialization
    DAGEvent initDagEvent = new DAGEvent(dag.getID(), DAGEventType.DAG_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    dagEventDispatcher.handle(initDagEvent);


    // JobImpl's InitTransition is done (call above is synchronous), so the
    // "uber-decision" (MR-1220) has been made.  Query job and switch to
    // ubermode if appropriate (by registering different container-allocator
    // and container-launcher services/event-handlers).

//    if (dag.isUber()) {
//      speculatorEventDispatcher.disableSpeculation();
//      LOG.info("MRAppMaster uberizing job " + dag.getID()
//               + " in local container (\"uber-AM\") on node "
//               + nmHost + ":" + nmPort + ".");
//    } else {
//      // send init to speculator only for non-uber jobs.
//      // This won't yet start as dispatcher isn't started yet.
//      dispatcher.getEventHandler().handle(
//          new SpeculatorEvent(dag.getID(), clock.getTime()));
//      LOG.info("MRAppMaster launching normal, non-uberized, multi-container "
//               + "job " + dag.getID() + ".");
//    }

    //start all the components
    super.start();

    // All components have started, start the job.
    startDags();
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

//  private class SpeculatorEventDispatcher implements
//      EventHandler<SpeculatorEvent> {
//    private final Configuration conf;
//    private volatile boolean disabled;
//
//    public SpeculatorEventDispatcher(Configuration config) {
//      this.conf = config;
//    }
//
//    @Override
//    public void handle(SpeculatorEvent event) {
//      if (disabled) {
//        return;
//      }
//
//      // FIX handle speculation events properly
//      // if vertex has speculation enabled then handle event else drop it
//      // speculator.handle(event);
//    }
//
//    public void disableSpeculation() {
//      disabled = true;
//    }
//
//  }

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

      Options opts = getCliOptions();
      CommandLine cliParser = new GnuParser().parse(opts, args);

      // Default to running mr if nothing specified.
      // TODO change this once the client is ready.
      String type;
      DAGConfiguration dagPlan = null;
      TezConfiguration conf = new TezConfiguration(new YarnConfiguration());
      if (cliParser.hasOption(OPT_PREDEFINED)) {
        LOG.info("Running with PreDefined configuration");
        type = cliParser.getOptionValue(OPT_PREDEFINED, "mr");
        LOG.info("Running job type: " + type);

        if (type.equals("mr")) {
          dagPlan = (DAGConfiguration)MRRExampleHelper.createDAGConfigurationForMR();
        } else if (type.equals("mrr")) {
          dagPlan = (DAGConfiguration)MRRExampleHelper.createDAGConfigurationForMRR();
        }
      } else {
        dagPlan = new DAGConfiguration();
        dagPlan.addResource(TezConfiguration.DAG_AM_PLAN_CONFIG_XML);
      }

      LOG.info("XXXX Running a DAG with "
          + dagPlan.getVertices().length + " vertices ");
      for (String v : dagPlan.getVertices()) {
        LOG.info("XXXX DAG has vertex " + v);
      }

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());

      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);

      conf.set(TezConfiguration.USER_NAME, jobUserName);
      
      Map<String, String> config = dagPlan.getConfig();
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
      LOG.fatal("Error starting MRAppMaster", t);
      System.exit(1);
    }
  }

  private static String OPT_PREDEFINED = "predefined";

  private static Options getCliOptions() {
    Options opts = new Options();
    opts.addOption(OPT_PREDEFINED, true,
        "Whether to run the predefined MR/MRR jobs");
    return opts;
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
      // Notify the JHEH and TaskScheduler that a SIGTERM has been received so
      // that they don't take too long in shutting down

      // Signal the task scheduler.
      appMaster.taskSchedulerEventHandler.setSignalled(true);

      // TODO: JobHistory
      /*
      if(appMaster.jobHistoryEventHandler != null) {
        ((JobHistoryEventHandler2) appMaster.jobHistoryEventHandler)
            .setSignalled(true);
      }
      */
      appMaster.stop();
    }
  }

  // TODO XXX Does this really need to be a YarnConfiguration ?
  protected static void initAndStartAppMaster(final DAGAppMaster appMaster,
      final TezConfiguration conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        appMaster.init(conf);
        appMaster.start();
        return null;
      }
    });
  }
}

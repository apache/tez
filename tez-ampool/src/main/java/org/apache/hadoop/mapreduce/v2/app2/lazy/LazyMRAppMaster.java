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

package org.apache.hadoop.mapreduce.v2.app2.lazy;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app2.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.NotRunningJob;
import org.apache.hadoop.mapreduce.v2.app2.rm.AMSchedulerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app2.rm.NMCommunicatorEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMCommunicatorEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.container.AMContainerMap;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeEventType;
import org.apache.hadoop.mapreduce.v2.app2.rm.node.AMNodeMap;
import org.apache.hadoop.mapreduce.v2.app2.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app2.taskclean.TaskCleaner;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.ClusterInfo;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.ampool.AMPoolConfiguration;
import org.apache.tez.ampool.rest.ApplicationPollResponse;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.codehaus.jackson.map.ObjectMapper;

public class LazyMRAppMaster extends MRAppMaster {

  private static final Log LOG = LogFactory.getLog(LazyMRAppMaster.class);

  private volatile boolean stopped;
  private volatile boolean jobInitialized;
  private volatile boolean jobStarted;

  protected final String jobPollingUrl;
  private int jobPollingIntervalSeconds;
  private NotRunningJob notRunningJob;

  private List<Service> services;

  public LazyMRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime, String pollingUrl) {
    super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        appSubmitTime);
    this.jobPollingUrl = pollingUrl;
    this.stopped = false;
    this.jobInitialized = false;
    this.jobStarted = false;
    this.services = new ArrayList<Service>();
  }

  private void initAfterJobSubmission(Configuration jobConf) {

    downloadTokensAndSetupUGI(jobConf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = jobConf.get(MRJobConfig.JOB_NAME, "<missing app name>");


    newApiCommitter = false;
    jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(),
        appAttemptID.getApplicationId().getId());
    int numReduceTasks = jobConf.getInt(MRJobConfig.NUM_REDUCES, 0);
    if ((numReduceTasks > 0 &&
        jobConf.getBoolean("mapred.reducer.new-api", false)) ||
          (numReduceTasks == 0 &&
           jobConf.getBoolean("mapred.mapper.new-api", false)))  {
      newApiCommitter = true;
      LOG.info("Using mapred newApiCommitter.");
    }

    committer = createOutputCommitter(jobConf);
    boolean recoveryEnabled = jobConf.getBoolean(
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, true);
    boolean recoverySupportedByCommitter = committer.isRecoverySupported();

    // TODO fix - recovery will currently never happen with lazy start
    // need to follow a different startup route for non-first attempts
    // for recovery to work with the recovery dispatcher
    LOG.info("Not starting RecoveryService: recoveryEnabled: "
        + recoveryEnabled + " recoverySupportedByCommitter: "
        + recoverySupportedByCommitter + " ApplicationAttemptID: "
        + appAttemptID.getAttemptId());


    if (jobConf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
        || jobConf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
      //optional service to speculate on task attempts' progress
      speculator = createSpeculator(jobConf, context);
      addService(speculator);
    }

    speculatorEventDispatcher = new SpeculatorEventDispatcher(jobConf);
    dispatcher.register(Speculator.EventType.class,
        speculatorEventDispatcher);

    //service to log job history events
    jobHistoryEventHandler = createJobHistoryHandler(context);
    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        jobHistoryEventHandler);

    // Add the JobHistoryEventHandler last so that it is properly stopped first.
    // This will guarantee that all history-events are flushed before AM goes
    // ahead with shutdown.
    // Note: Even though JobHistoryEventHandler is started last, if any
    // component creates a JobHistoryEvent in the meanwhile, it will be just be
    // queued inside the JobHistoryEventHandler
    addService(this.jobHistoryEventHandler);

    // init newly added services
    initServices();
  }

  @Override
  protected ContainerRequestor createContainerRequestor(
      ClientService clientService, AppContext context) {
    return new LazyRMContainerRequestor(clientService, context);
  }

  @Override
  protected ContainerAllocator createAMScheduler(ContainerRequestor requestor,
      AppContext appContext) {
    return new LazyRMContainerAllocator(requestor, context);
  }


  @Override
  public void init(Configuration conf) {
    this.jobPollingIntervalSeconds =
        conf.getInt(LazyAMConfig.POLLING_INTERVAL_SECONDS,
            LazyAMConfig.DEFAULT_POLLING_INTERVAL_SECONDS);

    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        appAttemptID.getAttemptId());
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    this.conf = conf;
    super.startTime = -1;
    super.appSubmitTime = -1;

    context = new LazyStartAppContext(this, conf);
    notRunningJob = new NotRunningJob(super.appAttemptID,
        context, conf);

    // add services that can be started without a job

    dispatcher = createDispatcher();
    addService(dispatcher);

    // service to handle requests from JobClient, start webapp handler
    clientService = new LazyMRClientService(context);
    addService(clientService);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    addService(taskHeartbeatHandler);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    addService(containerHeartbeatHandler);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context,
        taskHeartbeatHandler, containerHeartbeatHandler);
    addService(taskAttemptListener);

    containers = new AMContainerMap(containerHeartbeatHandler,
        taskAttemptListener, context);
    addService(containers);
    dispatcher.register(AMContainerEventType.class, containers);

    nodes = new AMNodeMap(dispatcher.getEventHandler(), context);
    addService(nodes);
    dispatcher.register(AMNodeEventType.class, nodes);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    addService(taskCleaner);

    this.jobEventDispatcher = new JobEventDispatcher();

    //register the event dispatchers
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);

    //    TODO XXX: Rename to NMComm
    //    corresponding service to launch allocated containers via NodeManager
    //    containerLauncher = createNMCommunicator(context);
    containerLauncher = createContainerLauncher(context);
    addService(containerLauncher);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    // service to allocate containers from RM (if non-uber) or to fake it (uber)
    containerRequestor = createContainerRequestor(clientService, context);
    addService(containerRequestor);
    dispatcher.register(RMCommunicatorEventType.class, containerRequestor);

    amScheduler = createAMScheduler(containerRequestor, context);
    addService(amScheduler);
    dispatcher.register(AMSchedulerEventType.class, amScheduler);

    // Add the staging directory cleaner before the history server but after
    // the container allocator so the staging directory is cleaned after
    // the history has been flushed but before unregistering with the RM.
    this.stagingDirCleanerService = createStagingDirCleaningService();
    addService(stagingDirCleanerService);

    initServices();
  }

  private void addService(Object o) {
    if ( o instanceof Service) {
      services.add((Service) o);
    }
  }

  private void initServices() {
    for (Service s : services) {
      if (s.getServiceState().equals(STATE.NOTINITED)) {
        s.init(conf);
      }
    }
  }

  private void startServices() {
    for (Service s : services) {
      if (s.getServiceState().equals(STATE.INITED)) {
        s.start();
      }
    }
  }

  private void stopServices() {
    // stop in reverse order
    for (int i = services.size() - 1; i >= 0; --i) {
      Service s = services.get(i);
      if (s.getServiceState().equals(STATE.STARTED)) {
        s.stop();
      }
    }
  }

  @Override
  public void start() {
    startServices();
    if (!jobInitialized) {
      Thread poller = new Thread(
          new JobPollerRunnable(jobPollingUrl, jobPollingIntervalSeconds));
      poller.setName("JobPollerThread");
      poller.run();
    } else {
      LOG.info("Job submitted to LazyMRAM. Starting job");
      super.startAM(conf);
      this.jobStarted = true;
      super.startJobs();
    }
  }

  public void lazyStart(String jobConfFileLocation,
      String jobJarLocation,
      long jobSubmissionTime) {
    super.startTime = super.clock.getTime();
    super.appSubmitTime = jobSubmissionTime;
    LOG.info("Launching job using"
        + ", jobConfLocation=" + jobConfFileLocation
        + ", jobJarLocation=" + jobJarLocation
        + ", jobSubmissionTime=" + jobSubmissionTime);

    FileSystem fs;
    Path confPath = new Path(jobConfFileLocation);

    Configuration mergeConf = new Configuration(conf);
    try {
      fs = FileSystem.get(conf);
      FileStatus fStatus = fs.getFileStatus(confPath);
      if (!fStatus.isFile()
          || fStatus.getLen() <= 0) {
        LOG.error("Invalid conf file provided"
            + ", jobConfLocation=" + jobConfFileLocation);
        System.exit(-1);
      } else {
        LOG.info("Job Conf File Status"
          + fStatus.toString());
        mergeConf.addResource(fs.open(confPath));
      }
    } catch (IOException e) {
      LOG.error("Invalid conf file provided"
          + ", jobConfLocation=" + jobConfFileLocation, e);
      System.exit(-1);
    }
    if (jobJarLocation != null
        && !jobJarLocation.isEmpty()) {
      mergeConf.set(MRJobConfig.JOB_JAR,
          jobJarLocation);
    }

    // Explicitly disable uber jobs as AM cannot be relocalized
    mergeConf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);

    // for now disable recovery
    // TODO fix
    mergeConf.setBoolean(MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE, false);

    this.conf = new JobConf(mergeConf);
    ((LazyStartAppContext)context).setConfiguration(conf);

    // Init and start main AM
    LOG.info("Initializing main MR AM");
    initAfterJobSubmission(conf);
    jobInitialized = true;
    LOG.info("Starting main MR AM");
    start();
    LOG.info("Started main MR AM");
  }

  @Override
  public void stop() {
    stopServices();
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      DeprecatedKeys.init();
      String containerIdStr =
          System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
      String nodeHostString = System.getenv(ApplicationConstants.NM_HOST_ENV);
      String nodePortString = System.getenv(ApplicationConstants.NM_PORT_ENV);
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.NM_HTTP_PORT_ENV);
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
      String pollingUrl =
          System.getenv(AMPoolConfiguration.LAZY_AM_POLLING_URL_ENV);

      MRAppMaster.validateInputParam(containerIdStr,
          ApplicationConstants.AM_CONTAINER_ID_ENV);
      MRAppMaster.validateInputParam(nodeHostString, ApplicationConstants.NM_HOST_ENV);
      MRAppMaster.validateInputParam(nodePortString, ApplicationConstants.NM_PORT_ENV);
      MRAppMaster.validateInputParam(nodeHttpPortString,
          ApplicationConstants.NM_HTTP_PORT_ENV);
      MRAppMaster.validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);
      MRAppMaster.validateInputParam(pollingUrl,
          AMPoolConfiguration.LAZY_AM_POLLING_URL_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);
      pollingUrl += applicationAttemptId.toString();
      LOG.info("Polling url for lazy init of job is "
          + pollingUrl);

      LazyMRAppMaster appMaster =
          new LazyMRAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime,
              pollingUrl);
      ShutdownHookManager.get().addShutdownHook(
        new LazyMRAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration(new LazyAMConfig());
      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());
      conf.set(MRJobConfig.USER_NAME, jobUserName);
      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);
      initAndStartAppMaster(appMaster, conf, jobUserName);
    } catch (Throwable t) {
      LOG.fatal("Error starting MRAppMaster", t);
      System.exit(1);
    }
  }

  protected static void initAndStartAppMaster(final LazyMRAppMaster appMaster,
      final YarnConfiguration conf, String jobUserName) throws IOException,
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

  private class JobPollerRunnable implements Runnable {

    private final Log LOG = LogFactory.getLog(JobPollerRunnable.class);
    private final HttpClient httpClient;
    private final GetMethod getMethod;
    private final int pollingIntervalSeconds;

    public JobPollerRunnable(String pollingUrl,
        int pollingIntervalSeconds) {
      this.httpClient = new HttpClient();
      this.getMethod = new GetMethod(pollingUrl);
      this.getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
          new DefaultHttpMethodRetryHandler(1, false));
      this.pollingIntervalSeconds = pollingIntervalSeconds;
    }

    @Override
    public void run() {
      while (!stopped) {
        boolean exceptionThrown = false;
        try {
          LOG.debug("Polling for job"
              + ", pollingUrl=" + jobPollingUrl);
          int responseCode = httpClient.executeMethod(getMethod);
          if (responseCode == HttpStatus.SC_OK) {
            String responseBody = getMethod.getResponseBodyAsString();
            ObjectMapper mapper = new ObjectMapper();
            ApplicationPollResponse response =
                mapper.readValue(responseBody,
                    ApplicationPollResponse.class);
            LOG.info("Received poll response"
                + ", pollingUrl=" + jobPollingUrl
                + ", pollResonse=" + response);
            lazyStart(response.getConfigurationFileLocation(),
                response.getApplicationJarLocation(),
                response.getApplicationSubmissionTime());
            break;
          }
        } catch (HttpException e) {
          LOG.error(e);
          exceptionThrown = true;
        } catch (IOException e) {
          LOG.error(e);
          exceptionThrown = true;
        }
        if (exceptionThrown) {
          LOG.info("Could not connect to polling url. Shutting down");
          System.exit(-1);
        }
        try {
          Thread.sleep(pollingIntervalSeconds*1000);
        } catch (InterruptedException e) {
        }
      }
      getMethod.releaseConnection();
    }
  }


  // The shutdown hook that runs when a signal is received AND during normal
  // close of the JVM.
  static class LazyMRAppMasterShutdownHook implements Runnable {
    LazyMRAppMaster appMaster;
    LazyMRAppMasterShutdownHook(LazyMRAppMaster appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      // TODO
      appMaster.stop();
    }
  }

  public class LazyStartAppContext extends RunningAppContext {

    public LazyStartAppContext(LazyMRAppMaster appMaster,
        Configuration config) {
      super(config);
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
      if (jobInitialized) {
        return super.getApplicationName();
      }
      return "Job not initialized";
    }

    public long getSubmitTime() {
      return appSubmitTime;
    }

    @Override
    public long getStartTime() {
      return super.getStartTime();
    }

    @Override
    public Job getJob(JobId jobID) {
      if (!jobStarted && super.getAllJobs().isEmpty()) {
        LOG.info("Getting polled when job is not initialized yet."
            + " Sending back dummy not running job");
        return notRunningJob;
      }
      return super.getJob(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return super.getAllJobs();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return super.getEventHandler();
    }

    @Override
    public CharSequence getUser() {
      if (jobInitialized) {
        return conf.get(MRJobConfig.USER_NAME);
      }
      return "";
    }

    @Override
    public Clock getClock() {
      return super.getClock();
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return super.getClusterInfo();
    }

    @Override
    public AMContainerMap getAllContainers() {
      return super.getAllContainers();
    }

    @Override
    public AMNodeMap getAllNodes() {
      return super.getAllNodes();
    }

    @Override
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      if (!jobStarted) {
        throw new YarnException(
            "Cannot get ApplicationACLs before all services have started");
      }
      return containerRequestor.getApplicationACLs();
    }

  }


}

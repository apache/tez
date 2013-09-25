///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.hadoop.mapred;
//
//import java.io.IOException;
//import java.io.OutputStream;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.classification.InterfaceStability;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.ipc.ProtocolSignature;
//import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
//import org.apache.hadoop.mapreduce.ClusterMetrics;
//import org.apache.hadoop.mapreduce.OutputFormat;
//import org.apache.hadoop.mapreduce.QueueInfo;
//import org.apache.hadoop.mapreduce.TaskCompletionEvent;
//import org.apache.hadoop.mapreduce.TaskTrackerInfo;
//import org.apache.hadoop.mapreduce.TaskType;
//import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
//import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
//import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
//import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
//import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
//import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
//import org.apache.hadoop.mapreduce.v2.LogParams;
//import org.apache.hadoop.security.Credentials;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.authorize.AccessControlList;
//import org.apache.hadoop.security.token.Token;
//import org.apache.hadoop.util.ReflectionUtils;
//import org.apache.tez.common.Constants;
//import org.apache.tez.common.ContainerContext;
//import org.apache.tez.common.ContainerTask;
//import org.apache.tez.common.InputSpec;
//import org.apache.tez.common.OutputSpec;
//import org.apache.tez.common.TezEngineTaskContext;
//import org.apache.tez.common.TezJobConfig;
//import org.apache.tez.common.TezTaskUmbilicalProtocol;
//import org.apache.tez.common.counters.TezCounters;
//import org.apache.tez.common.records.ProceedToCompletionResponse;
//import org.apache.tez.dag.api.ProcessorDescriptor;
//import org.apache.tez.dag.records.TezTaskAttemptID;
//import org.apache.tez.engine.api.Task;
//import org.apache.tez.engine.api.impl.TezHeartbeatRequest;
//import org.apache.tez.engine.api.impl.TezHeartbeatResponse;
//import org.apache.tez.engine.common.task.local.output.TezLocalTaskOutputFiles;
//import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
//import org.apache.tez.engine.lib.input.LocalMergedInput;
//import org.apache.tez.engine.lib.output.LocalOnFileSorterOutput;
//import org.apache.tez.engine.records.OutputContext;
//import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;
//import org.apache.tez.mapreduce.hadoop.IDConverter;
//import org.apache.tez.mapreduce.hadoop.mapred.MRCounters;
//import org.apache.tez.mapreduce.input.MRInput;
//import org.apache.tez.mapreduce.output.MROutput;
//import org.apache.tez.mapreduce.processor.map.MapProcessor;
//import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
//
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//
///** Implements MapReduce locally, in-process, for debugging. */
//@InterfaceAudience.Private
//@InterfaceStability.Unstable
//public class LocalJobRunnerTez implements ClientProtocol {
//  public static final Log LOG =
//    LogFactory.getLog(LocalJobRunnerTez.class);
//
//  /** The maximum number of map tasks to run in parallel in LocalJobRunner */
//  public static final String LOCAL_MAX_MAPS =
//    "mapreduce.local.map.tasks.maximum";
//
//  private FileSystem fs;
//  private HashMap<JobID, Job> jobs = new HashMap<JobID, Job>();
//  private JobConf conf;
//  private AtomicInteger map_tasks = new AtomicInteger(0);
//  private int reduce_tasks = 0;
//  final Random rand = new Random();
//
//  private LocalJobRunnerMetricsTez myMetrics = null;
//
//  private static final String jobDir =  "localRunner/";
//
//  private static final TezCounters EMPTY_COUNTERS = new TezCounters();
//
//  public long getProtocolVersion(String protocol, long clientVersion) {
//    return ClientProtocol.versionID;
//  }
//
//  @Override
//  public ProtocolSignature getProtocolSignature(String protocol,
//      long clientVersion, int clientMethodsHash) throws IOException {
//    return ProtocolSignature.getProtocolSignature(
//        this, protocol, clientVersion, clientMethodsHash);
//  }
//
//  private class Job extends Thread implements TezTaskUmbilicalProtocol {
//    // The job directory on the system: JobClient places job configurations here.
//    // This is analogous to JobTracker's system directory.
//    private Path systemJobDir;
//    private Path systemJobFile;
//
//    // The job directory for the task.  Analagous to a task's job directory.
//    private Path localJobDir;
//    private Path localJobFile;
//
//    private JobID id;
//    private JobConf job;
//
//    private int numMapTasks;
//    private float [] partialMapProgress;
//    private TezCounters [] mapCounters;
//    private TezCounters reduceCounters;
//
//    private JobStatus status;
//    private List<TaskAttemptID> mapIds = Collections.synchronizedList(
//        new ArrayList<TaskAttemptID>());
//
//    private JobProfile profile;
//    private FileSystem localFs;
//    boolean killed = false;
//
//    private LocalDistributedCacheManager localDistributedCacheManager;
//
//    public long getProtocolVersion(String protocol, long clientVersion) {
//      return TaskUmbilicalProtocol.versionID;
//    }
//
//    @Override
//    public ProtocolSignature getProtocolSignature(String protocol,
//        long clientVersion, int clientMethodsHash) throws IOException {
//      return ProtocolSignature.getProtocolSignature(
//          this, protocol, clientVersion, clientMethodsHash);
//    }
//
//    public Job(JobID jobid, String jobSubmitDir) throws IOException {
//      this.systemJobDir = new Path(jobSubmitDir);
//      this.systemJobFile = new Path(systemJobDir, "job.xml");
//      this.id = jobid;
//      JobConf conf = new JobConf(systemJobFile);
//      this.localFs = FileSystem.getLocal(conf);
//      this.localJobDir = localFs.makeQualified(conf.getLocalPath(jobDir));
//      this.localJobFile = new Path(this.localJobDir, id + ".xml");
//
//      // Manage the distributed cache.  If there are files to be copied,
//      // this will trigger localFile to be re-written again.
//      localDistributedCacheManager = new LocalDistributedCacheManager();
//      localDistributedCacheManager.setup(conf);
//
//      // Write out configuration file.  Instead of copying it from
//      // systemJobFile, we re-write it, since setup(), above, may have
//      // updated it.
//      OutputStream out = localFs.create(localJobFile);
//      try {
//        conf.writeXml(out);
//      } finally {
//        out.close();
//      }
//      this.job = new JobConf(localJobFile);
//
//      // Job (the current object) is a Thread, so we wrap its class loader.
//      if (localDistributedCacheManager.hasLocalClasspaths()) {
//        setContextClassLoader(localDistributedCacheManager.makeClassLoader(
//                getContextClassLoader()));
//      }
//
//      profile = new JobProfile(job.getUser(), id, systemJobFile.toString(),
//                               "http://localhost:8080/", job.getJobName());
//      status = new JobStatus(id, 0.0f, 0.0f, JobStatus.RUNNING,
//          profile.getUser(), profile.getJobName(), profile.getJobFile(),
//          profile.getURL().toString());
//
//      jobs.put(id, this);
//
//      this.start();
//    }
//
//    /**
//     * A Runnable instance that handles a map task to be run by an executor.
//     */
//    protected class MapTaskRunnable implements Runnable {
//      private final int taskId;
//      private final TaskSplitMetaInfo info;
//      private final JobID jobId;
//      private final JobConf localConf;
//
//      // This is a reference to a shared object passed in by the
//      // external context; this delivers state to the reducers regarding
//      // where to fetch mapper outputs.
//      private final Map<TaskAttemptID, TezTaskOutput> mapOutputFiles;
//
//      public volatile Throwable storedException;
//
//      public MapTaskRunnable(TaskSplitMetaInfo info, int taskId, JobID jobId,
//          Map<TaskAttemptID, TezTaskOutput> mapOutputFiles) {
//        this.info = info;
//        this.taskId = taskId;
//        this.mapOutputFiles = mapOutputFiles;
//        this.jobId = jobId;
//        this.localConf = new JobConf(job);
//      }
//
//      public void run() {
//        try {
//          TaskAttemptID mapId = new TaskAttemptID(new TaskID(
//              jobId, TaskType.MAP, taskId), 0);
//          LOG.info("Starting task: " + mapId);
//          final String user =
//              UserGroupInformation.getCurrentUser().getShortUserName();
//          setupChildMapredLocalDirs(mapId, user, localConf);
//          localConf.setUser(user);
//
//          TezTaskAttemptID tezMapId =
//              IDConverter.fromMRTaskAttemptId(mapId);
//          mapIds.add(mapId);
//          // FIXME invalid task context
//          ProcessorDescriptor mapProcessorDesc = new ProcessorDescriptor(
//                      MapProcessor.class.getName());
//          TezEngineTaskContext taskContext =
//              new TezEngineTaskContext(
//                  tezMapId, user, localConf.getJobName(), "TODO_vertexName",
//                  mapProcessorDesc,
//                  Collections.singletonList(new InputSpec("srcVertex", 0,
//                      MRInput.class.getName())),
//                  Collections.singletonList(new OutputSpec("tgtVertex", 0,
//                      LocalOnFileSorterOutput.class.getName())));
//
//          TezTaskOutput mapOutput = new TezLocalTaskOutputFiles(localConf, "TODO_uniqueId");
//          mapOutputFiles.put(mapId, mapOutput);
//
//          try {
//            map_tasks.getAndIncrement();
//            myMetrics.launchMap(mapId);
//            Task t = RuntimeUtils.createRuntimeTask(taskContext);
//            t.initialize(localConf, null, Job.this);
//            t.run();
//            myMetrics.completeMap(mapId);
//          } finally {
//            map_tasks.getAndDecrement();
//          }
//
//          LOG.info("Finishing task: " + mapId);
//        } catch (Throwable e) {
//          this.storedException = e;
//        }
//      }
//    }
//
//    /**
//     * Create Runnables to encapsulate map tasks for use by the executor
//     * service.
//     * @param taskInfo Info about the map task splits
//     * @param jobId the job id
//     * @param mapOutputFiles a mapping from task attempts to output files
//     * @return a List of Runnables, one per map task.
//     */
//    protected List<MapTaskRunnable> getMapTaskRunnables(
//        TaskSplitMetaInfo [] taskInfo, JobID jobId,
//        Map<TaskAttemptID, TezTaskOutput> mapOutputFiles) {
//
//      int numTasks = 0;
//      ArrayList<MapTaskRunnable> list = new ArrayList<MapTaskRunnable>();
//      for (TaskSplitMetaInfo task : taskInfo) {
//        list.add(new MapTaskRunnable(task, numTasks++, jobId,
//            mapOutputFiles));
//      }
//
//      return list;
//    }
//
//    /**
//     * Initialize the counters that will hold partial-progress from
//     * the various task attempts.
//     * @param numMaps the number of map tasks in this job.
//     */
//    private synchronized void initCounters(int numMaps) {
//      // Initialize state trackers for all map tasks.
//      this.partialMapProgress = new float[numMaps];
//      this.mapCounters = new TezCounters[numMaps];
//      for (int i = 0; i < numMaps; i++) {
//        this.mapCounters[i] = EMPTY_COUNTERS;
//      }
//
//      this.reduceCounters = EMPTY_COUNTERS;
//    }
//
//    /**
//     * Creates the executor service used to run map tasks.
//     *
//     * @param numMapTasks the total number of map tasks to be run
//     * @return an ExecutorService instance that handles map tasks
//     */
//    protected ExecutorService createMapExecutor(int numMapTasks) {
//
//      // Determine the size of the thread pool to use
//      int maxMapThreads = job.getInt(LOCAL_MAX_MAPS, 1);
//      if (maxMapThreads < 1) {
//        throw new IllegalArgumentException(
//            "Configured " + LOCAL_MAX_MAPS + " must be >= 1");
//      }
//      this.numMapTasks = numMapTasks;
//      maxMapThreads = Math.min(maxMapThreads, this.numMapTasks);
//      maxMapThreads = Math.max(maxMapThreads, 1); // In case of no tasks.
//
//      initCounters(this.numMapTasks);
//
//      LOG.debug("Starting thread pool executor.");
//      LOG.debug("Max local threads: " + maxMapThreads);
//      LOG.debug("Map tasks to process: " + this.numMapTasks);
//
//      // Create a new executor service to drain the work queue.
//      ThreadFactory tf = new ThreadFactoryBuilder()
//        .setNameFormat("LocalJobRunner Map Task Executor #%d")
//        .build();
//      ExecutorService executor = Executors.newFixedThreadPool(maxMapThreads, tf);
//
//      return executor;
//    }
//
//    private org.apache.hadoop.mapreduce.OutputCommitter
//    createOutputCommitter(boolean newApiCommitter, JobID jobId, Configuration conf) throws Exception {
//      org.apache.hadoop.mapreduce.OutputCommitter committer = null;
//
//      LOG.info("OutputCommitter set in config "
//          + conf.get("mapred.output.committer.class"));
//
//      if (newApiCommitter) {
//        org.apache.hadoop.mapreduce.TaskID taskId =
//            new org.apache.hadoop.mapreduce.TaskID(jobId, TaskType.MAP, 0);
//        org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID =
//            new org.apache.hadoop.mapreduce.TaskAttemptID(taskId, 0);
//        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
//            new TaskAttemptContextImpl(conf, taskAttemptID);
//        @SuppressWarnings("rawtypes")
//        OutputFormat outputFormat =
//          ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), conf);
//        committer = outputFormat.getOutputCommitter(taskContext);
//      } else {
//        committer = ReflectionUtils.newInstance(conf.getClass(
//            "mapred.output.committer.class", FileOutputCommitter.class,
//            org.apache.hadoop.mapred.OutputCommitter.class), conf);
//      }
//      LOG.info("OutputCommitter is " + committer.getClass().getName());
//      return committer;
//    }
//
//    @Override
//    public void run() {
//      JobID jobId = profile.getJobID();
//      JobContext jContext = new JobContextImpl(job, jobId);
//
//      org.apache.hadoop.mapreduce.OutputCommitter outputCommitter = null;
//      try {
//        outputCommitter = createOutputCommitter(conf.getUseNewMapper(), jobId, conf);
//      } catch (Exception e) {
//        LOG.info("Failed to createOutputCommitter", e);
//        return;
//      }
//
//      try {
//        TaskSplitMetaInfo[] taskSplitMetaInfos =
//          SplitMetaInfoReader.readSplitMetaInfo(jobId, localFs, conf, systemJobDir);
//
//        int numReduceTasks = job.getNumReduceTasks();
//        if (numReduceTasks > 1 || numReduceTasks < 0) {
//          // we only allow 0 or 1 reducer in local mode
//          numReduceTasks = 1;
//          job.setNumReduceTasks(1);
//        }
//        outputCommitter.setupJob(jContext);
//        status.setSetupProgress(1.0f);
//
//        Map<TaskAttemptID, TezTaskOutput> mapOutputFiles =
//            Collections.synchronizedMap(new HashMap<TaskAttemptID, TezTaskOutput>());
//
//        List<MapTaskRunnable> taskRunnables = getMapTaskRunnables(taskSplitMetaInfos,
//            jobId, mapOutputFiles);
//        ExecutorService mapService = createMapExecutor(taskRunnables.size());
//
//        // Start populating the executor with work units.
//        // They may begin running immediately (in other threads).
//        for (Runnable r : taskRunnables) {
//          mapService.submit(r);
//        }
//
//        try {
//          mapService.shutdown(); // Instructs queue to drain.
//
//          // Wait for tasks to finish; do not use a time-based timeout.
//          // (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179024)
//          LOG.info("Waiting for map tasks");
//          mapService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//        } catch (InterruptedException ie) {
//          // Cancel all threads.
//          mapService.shutdownNow();
//          throw ie;
//        }
//
//        LOG.info("Map task executor complete.");
//
//        // After waiting for the map tasks to complete, if any of these
//        // have thrown an exception, rethrow it now in the main thread context.
//        for (MapTaskRunnable r : taskRunnables) {
//          if (r.storedException != null) {
//            throw new Exception(r.storedException);
//          }
//        }
//
//        TaskAttemptID reduceId = new TaskAttemptID(new TaskID(
//            jobId, TaskType.REDUCE, 0), 0);
//        LOG.info("Starting task: " + reduceId);
//        try {
//          if (numReduceTasks > 0) {
//            String user =
//                UserGroupInformation.getCurrentUser().getShortUserName();
//            JobConf localConf = new JobConf(job);
//            localConf.setUser(user);
//            localConf.set("mapreduce.jobtracker.address", "local");
//            setupChildMapredLocalDirs(reduceId, user, localConf);
//            // FIXME invalid task context
//            ProcessorDescriptor reduceProcessorDesc = new ProcessorDescriptor(
//                ReduceProcessor.class.getName());
//            TezEngineTaskContext taskContext = new TezEngineTaskContext(
//                IDConverter.fromMRTaskAttemptId(reduceId), user,
//                localConf.getJobName(), "TODO_vertexName",
//                reduceProcessorDesc,
//                Collections.singletonList(new InputSpec("TODO_srcVertexName",
//                    mapIds.size(), LocalMergedInput.class.getName())),
//                Collections.singletonList(new OutputSpec("TODO_targetVertex",
//                    0, MROutput.class.getName())));
//
//            // move map output to reduce input
//            for (int i = 0; i < mapIds.size(); i++) {
//              if (!this.isInterrupted()) {
//                TaskAttemptID mapId = mapIds.get(i);
//                if (LOG.isDebugEnabled()) {
//                  // TODO NEWTEZ Fix this logging.
////                  LOG.debug("XXX mapId: " + i +
////                      " LOCAL_DIR = " +
////                      mapOutputFiles.get(mapId).getConf().get(
////                          TezJobConfig.LOCAL_DIRS));
//                }
//                Path mapOut = mapOutputFiles.get(mapId).getOutputFile();
//                TezTaskOutput localOutputFile = new TezLocalTaskOutputFiles(localConf, "TODO_uniqueId");
//                Path reduceIn =
//                  localOutputFile.getInputFileForWrite(
//                      mapId.getTaskID().getId(), localFs.getFileStatus(mapOut).getLen());
//                if (!localFs.mkdirs(reduceIn.getParent())) {
//                  throw new IOException("Mkdirs failed to create "
//                      + reduceIn.getParent().toString());
//                }
//                if (!localFs.rename(mapOut, reduceIn))
//                  throw new IOException("Couldn't rename " + mapOut);
//              } else {
//                throw new InterruptedException();
//              }
//            }
//            if (!this.isInterrupted()) {
//              reduce_tasks += 1;
//              myMetrics.launchReduce(reduceId);
//              Task t = RuntimeUtils.createRuntimeTask(taskContext);
//              t.initialize(localConf, null, Job.this);
//              t.run();
//              myMetrics.completeReduce(reduceId);
//              reduce_tasks -= 1;
//            } else {
//              throw new InterruptedException();
//            }
//          }
//        } finally {
//          for (TezTaskOutput output : mapOutputFiles.values()) {
//            output.removeAll();
//          }
//        }
//        // delete the temporary directory in output directory
//        // FIXME
//        //outputCommitter.commitJob(jContext);
//        status.setCleanupProgress(1.0f);
//
//        if (killed) {
//          this.status.setRunState(JobStatus.KILLED);
//        } else {
//          this.status.setRunState(JobStatus.SUCCEEDED);
//        }
//
//        JobEndNotifier.localRunnerNotification(job, status);
//
//      } catch (Throwable t) {
//        try {
//          outputCommitter.abortJob(jContext,
//            org.apache.hadoop.mapreduce.JobStatus.State.FAILED);
//        } catch (IOException ioe) {
//          LOG.info("Error cleaning up job:" + id);
//        }
//        status.setCleanupProgress(1.0f);
//        if (killed) {
//          this.status.setRunState(JobStatus.KILLED);
//        } else {
//          this.status.setRunState(JobStatus.FAILED);
//        }
//        LOG.warn(id, t);
//
//        JobEndNotifier.localRunnerNotification(job, status);
//
//      } finally {
//        try {
//          fs.delete(systemJobFile.getParent(), true);  // delete submit dir
//          localFs.delete(localJobFile, true);              // delete local copy
//          // Cleanup distributed cache
//          localDistributedCacheManager.close();
//        } catch (IOException e) {
//          LOG.warn("Error cleaning up "+id+": "+e);
//        }
//      }
//    }
//
//    // TaskUmbilicalProtocol methods
//    @Override
//    public ContainerTask getTask(ContainerContext containerContext)
//        throws IOException {
//      return null;
//    }
//
//    /** Return the current values of the counters for this job,
//     * including tasks that are in progress.
//     */
//    public synchronized TezCounters getCurrentCounters() {
//      if (null == mapCounters) {
//        // Counters not yet initialized for job.
//        return EMPTY_COUNTERS;
//      }
//
//      TezCounters current = EMPTY_COUNTERS;
//      for (TezCounters c : mapCounters) {
//        current.incrAllCounters(c);
//      }
//      current.incrAllCounters(reduceCounters);
//      return current;
//    }
//
//    @Override
//    public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
//      return true;
//    }
//
//    @Override
//    public TezTaskDependencyCompletionEventsUpdate
//    getDependentTasksCompletionEvents(
//        int fromEventIdx, int maxEventsToFetch,
//        TezTaskAttemptID reduce) {
//      throw new UnsupportedOperationException(
//          "getDependentTasksCompletionEvents not supported in LocalJobRunner");
//    }
//
//    @Override
//    public void outputReady(TezTaskAttemptID taskAttemptId,
//        OutputContext outputContext) throws IOException {
//      // Ignore for now.
//    }
//
//    @Override
//    public ProceedToCompletionResponse proceedToCompletion(
//        TezTaskAttemptID taskAttemptId) throws IOException {
//      // TODO TEZAM5 Really depends on the module - inmem shuffle or not.
//      return new ProceedToCompletionResponse(true, true);
//    }
//
//    @Override
//    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) {
//      // TODO Auto-generated method stub
//      // TODO TODONEWTEZ
//      return null;
//    }
//
//  }
//
//  public LocalJobRunnerTez(Configuration conf) throws IOException {
//    this(new JobConf(conf));
//  }
//
//  @Deprecated
//  public LocalJobRunnerTez(JobConf conf) throws IOException {
//    this.fs = FileSystem.getLocal(conf);
//    this.conf = conf;
//    myMetrics = new LocalJobRunnerMetricsTez(new JobConf(conf));
//  }
//
//  // JobSubmissionProtocol methods
//
//  private static int jobid = 0;
//  public synchronized org.apache.hadoop.mapreduce.JobID getNewJobID() {
//    return new org.apache.hadoop.mapreduce.JobID("local", ++jobid);
//  }
//
//  public org.apache.hadoop.mapreduce.JobStatus submitJob(
//      org.apache.hadoop.mapreduce.JobID jobid, String jobSubmitDir,
//      Credentials credentials) throws IOException {
//    Job job = new Job(JobID.downgrade(jobid), jobSubmitDir);
//    job.job.setCredentials(credentials);
//    return job.status;
//
//  }
//
//  public void killJob(org.apache.hadoop.mapreduce.JobID id) {
//    jobs.get(JobID.downgrade(id)).killed = true;
//    jobs.get(JobID.downgrade(id)).interrupt();
//  }
//
//  public void setJobPriority(org.apache.hadoop.mapreduce.JobID id,
//      String jp) throws IOException {
//    throw new UnsupportedOperationException("Changing job priority " +
//                      "in LocalJobRunner is not supported.");
//  }
//
//  /** Throws {@link UnsupportedOperationException} */
//  public boolean killTask(org.apache.hadoop.mapreduce.TaskAttemptID taskId,
//      boolean shouldFail) throws IOException {
//    throw new UnsupportedOperationException("Killing tasks in " +
//    "LocalJobRunner is not supported");
//  }
//
//  public org.apache.hadoop.mapreduce.TaskReport[] getTaskReports(
//      org.apache.hadoop.mapreduce.JobID id, TaskType type) {
//    return new org.apache.hadoop.mapreduce.TaskReport[0];
//  }
//
//  public org.apache.hadoop.mapreduce.JobStatus getJobStatus(
//      org.apache.hadoop.mapreduce.JobID id) {
//    Job job = jobs.get(JobID.downgrade(id));
//    if(job != null)
//      return job.status;
//    else
//      return null;
//  }
//
//  public org.apache.hadoop.mapreduce.Counters getJobCounters(
//      org.apache.hadoop.mapreduce.JobID id) {
//    Job job = jobs.get(JobID.downgrade(id));
//
//    return new org.apache.hadoop.mapreduce.Counters(
//        new MRCounters(job.getCurrentCounters()));
//  }
//
//  public String getFilesystemName() throws IOException {
//    return fs.getUri().toString();
//  }
//
//  public ClusterMetrics getClusterMetrics() {
//    int numMapTasks = map_tasks.get();
//    return new ClusterMetrics(numMapTasks, reduce_tasks, numMapTasks,
//        reduce_tasks, 0, 0, 1, 1, jobs.size(), 1, 0, 0);
//  }
//
//  public JobTrackerStatus getJobTrackerStatus() {
//    return JobTrackerStatus.RUNNING;
//  }
//
//  public long getTaskTrackerExpiryInterval()
//      throws IOException, InterruptedException {
//    return 0;
//  }
//
//  /**
//   * Get all active trackers in cluster.
//   * @return array of TaskTrackerInfo
//   */
//  public TaskTrackerInfo[] getActiveTrackers()
//      throws IOException, InterruptedException {
//    return null;
//  }
//
//  /**
//   * Get all blacklisted trackers in cluster.
//   * @return array of TaskTrackerInfo
//   */
//  public TaskTrackerInfo[] getBlacklistedTrackers()
//      throws IOException, InterruptedException {
//    return null;
//  }
//
//  public TaskCompletionEvent[] getTaskCompletionEvents(
//      org.apache.hadoop.mapreduce.JobID jobid
//      , int fromEventId, int maxEvents) throws IOException {
//    return TaskCompletionEvent.EMPTY_ARRAY;
//  }
//
//  public org.apache.hadoop.mapreduce.JobStatus[] getAllJobs() {return null;}
//
//
//  /**
//   * Returns the diagnostic information for a particular task in the given job.
//   * To be implemented
//   */
//  public String[] getTaskDiagnostics(
//      org.apache.hadoop.mapreduce.TaskAttemptID taskid) throws IOException{
//	  return new String [0];
//  }
//
//  /**
//   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
//   */
//  public String getSystemDir() {
//    Path sysDir = new Path(
//      conf.get(JTConfig.JT_SYSTEM_DIR, "/tmp/hadoop/mapred/system"));
//    return fs.makeQualified(sysDir).toString();
//  }
//
//  /**
//   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getQueueAdmins(String)
//   */
//  public AccessControlList getQueueAdmins(String queueName) throws IOException {
//	  return new AccessControlList(" ");// no queue admins for local job runner
//  }
//
//  /**
//   * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getStagingAreaDir()
//   */
//  public String getStagingAreaDir() throws IOException {
//    Path stagingRootDir = new Path(conf.get(JTConfig.JT_STAGING_AREA_ROOT,
//        "/tmp/hadoop/mapred/staging"));
//    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
//    String user;
//    if (ugi != null) {
//      user = ugi.getShortUserName() + rand.nextInt();
//    } else {
//      user = "dummy" + rand.nextInt();
//    }
//    return fs.makeQualified(new Path(stagingRootDir, user+"/.staging")).toString();
//  }
//
//  public String getJobHistoryDir() {
//    return null;
//  }
//
//  @Override
//  public QueueInfo[] getChildQueues(String queueName) throws IOException {
//    return null;
//  }
//
//  @Override
//  public QueueInfo[] getRootQueues() throws IOException {
//    return null;
//  }
//
//  @Override
//  public QueueInfo[] getQueues() throws IOException {
//    return null;
//  }
//
//
//  @Override
//  public QueueInfo getQueue(String queue) throws IOException {
//    return null;
//  }
//
//  @Override
//  public org.apache.hadoop.mapreduce.QueueAclsInfo[]
//      getQueueAclsForCurrentUser() throws IOException{
//    return null;
//  }
//
//  /**
//   * Set the max number of map tasks to run concurrently in the LocalJobRunner.
//   * @param job the job to configure
//   * @param maxMaps the maximum number of map tasks to allow.
//   */
//  public static void setLocalMaxRunningMaps(
//      org.apache.hadoop.mapreduce.JobContext job,
//      int maxMaps) {
//    job.getConfiguration().setInt(LOCAL_MAX_MAPS, maxMaps);
//  }
//
//  /**
//   * @return the max number of map tasks to run concurrently in the
//   * LocalJobRunner.
//   */
//  public static int getLocalMaxRunningMaps(
//      org.apache.hadoop.mapreduce.JobContext job) {
//    return job.getConfiguration().getInt(LOCAL_MAX_MAPS, 1);
//  }
//
//  @Override
//  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
//                                       ) throws IOException,
//                                                InterruptedException {
//  }
//
//  @Override
//  public Token<DelegationTokenIdentifier>
//     getDelegationToken(Text renewer) throws IOException, InterruptedException {
//    return null;
//  }
//
//  @Override
//  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
//                                      ) throws IOException,InterruptedException{
//    return 0;
//  }
//
//  @Override
//  public LogParams getLogFileParams(org.apache.hadoop.mapreduce.JobID jobID,
//      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID)
//      throws IOException, InterruptedException {
//    throw new UnsupportedOperationException("Not supported");
//  }
//
//  static void setupChildMapredLocalDirs(
//      TaskAttemptID taskAttemptID, String user, JobConf conf) {
//    String[] localDirs =
//        conf.getTrimmedStrings(
//            TezJobConfig.LOCAL_DIRS, TezJobConfig.DEFAULT_LOCAL_DIRS);
//    String jobId = taskAttemptID.getJobID().toString();
//    String taskId = taskAttemptID.getTaskID().toString();
//    boolean isCleanup = false;
//    StringBuffer childMapredLocalDir =
//        new StringBuffer(localDirs[0] + Path.SEPARATOR
//            + getLocalTaskDir(user, jobId, taskId, isCleanup));
//    for (int i = 1; i < localDirs.length; i++) {
//      childMapredLocalDir.append("," + localDirs[i] + Path.SEPARATOR
//          + getLocalTaskDir(user, jobId, taskId, isCleanup));
//    }
//    LOG.info(TezJobConfig.LOCAL_DIRS + " for child : " + taskAttemptID +
//        " is " + childMapredLocalDir);
//    conf.set(TezJobConfig.LOCAL_DIRS, childMapredLocalDir.toString());
//    conf.setClass(Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
//        TezLocalTaskOutputFiles.class, TezTaskOutput.class);
//  }
//
//  static final String TASK_CLEANUP_SUFFIX = ".cleanup";
//  static final String SUBDIR = jobDir;
//  static final String JOBCACHE = "jobcache";
//
//  static String getLocalTaskDir(String user, String jobid, String taskid,
//      boolean isCleanupAttempt) {
//    String taskDir = SUBDIR + Path.SEPARATOR + user + Path.SEPARATOR + JOBCACHE
//      + Path.SEPARATOR + jobid + Path.SEPARATOR + taskid;
//    if (isCleanupAttempt) {
//      taskDir = taskDir + TASK_CLEANUP_SUFFIX;
//    }
//    return taskDir;
//  }
//
//
//}

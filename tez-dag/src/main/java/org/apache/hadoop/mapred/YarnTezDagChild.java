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

package org.apache.hadoop.mapred;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.InputSpec;
import org.apache.tez.common.OutputSpec;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.runtime.RuntimeUtils;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.MRTask;

/**
 * The main() for TEZ MapReduce task processes.
 */
public class YarnTezDagChild {

  private static final Log LOG = LogFactory.getLog(YarnTezDagChild.class);

  public static void main(String[] args) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    LOG.debug("Child starting");

    DeprecatedKeys.init();
    
    final JobConf defaultConf = new JobConf();
    // HACK Eventually load the DagConf for security etc setup.
//    defaultConf.addResource(MRJobConfig.JOB_CONF_FILE);
    UserGroupInformation.setConfiguration(defaultConf);

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address =
        NetUtils.createSocketAddrForHost(host, port);

    final ContainerId containerId = ConverterUtils.toContainerId(args[2]);
    final ApplicationId appID =
        containerId.getApplicationAttemptId().getApplicationId();

    // FIXME fix initialize metrics in child runner
    DefaultMetricsSystem.initialize("VertexTask");

    // Security framework already loaded the tokens into current ugi
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    LOG.info("Executing with tokens:");
    for (Token<?> token: credentials.getAllTokens()) {
      LOG.info(token);
    }

    // Create TaskUmbilicalProtocol as actual task owner.
    UserGroupInformation taskOwner =
      UserGroupInformation.createRemoteUser(appID.toString());
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    SecurityUtil.setTokenService(jt, address);
    taskOwner.addToken(jt);
    final TezTaskUmbilicalProtocol umbilical =
      taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
      @Override
      public TezTaskUmbilicalProtocol run() throws Exception {
        return (TezTaskUmbilicalProtocol)RPC.getProxy(TezTaskUmbilicalProtocol.class,
            TezTaskUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    // report non-pid to application master
    String pid = System.getenv().get("JVM_PID");
    if (LOG.isDebugEnabled()) {
      LOG.debug("PID, containerId: " + pid + ", " + containerId);
    }
    TezEngineTaskContext taskContext = null;
    ContainerTask containerTask = null;
    UserGroupInformation childUGI = null;
    TezTaskAttemptID taskAttemptId = null;
    MRTask task = null;
    ContainerContext containerContext = new ContainerContext(containerId, pid);
    
    try {
      while (true) {
        // poll for new task
        for (int idle = 0; null == containerTask; ++idle) {
          long sleepTimeMilliSecs = Math.min(idle * 500, 1500);
          LOG.info("Sleeping for " + sleepTimeMilliSecs
              + "ms before retrying again. Got null now.");
          MILLISECONDS.sleep(sleepTimeMilliSecs);
          containerTask = umbilical.getTask(containerContext);
        }
        LOG.info("TaskInfo: shouldDie: "
            + containerTask.shouldDie()
            + (containerTask.shouldDie() == true ? "" : ", taskAttemptId: "
                + containerTask.getTezEngineTaskContext().getTaskAttemptId()));

        if (containerTask.shouldDie()) {
          return;
        }
        taskContext = (TezEngineTaskContext) containerTask
            .getTezEngineTaskContext();
        LOG.info("XXXX: New container task context:"
                + taskContext.toString());

        taskAttemptId = taskContext.getTaskAttemptId();

        final Task t = createAndConfigureTezTask(taskContext, umbilical,
            credentials, jt,
            containerId.getApplicationAttemptId().getAttemptId());
        task = (MRTask) t.getProcessor();
        final JobConf job = task.getConf();

        // Initiate Java VM metrics
        JvmMetrics.initSingleton(containerId.toString(), job.getSessionId());
        childUGI = UserGroupInformation.createRemoteUser(System
            .getenv(ApplicationConstants.Environment.USER.toString()));
        // Add tokens to new user so that it may execute its task correctly.
        childUGI.addCredentials(credentials);

        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            runTezTask(t, umbilical, job); // run the task
            return null;
          }
        });
        FileSystem.closeAllForUGI(childUGI);
        containerTask = null;
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskAttemptId, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      try {
        if (task != null) {
          // do cleanup for the task
          if (childUGI == null) { // no need to job into doAs block
            task.taskCleanup(umbilical);
          } else {
            final MRTask taskFinal = task;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                taskFinal.taskCleanup(umbilical);
                return null;
              }
            });
          }
        }
      } catch (Exception e) {
        LOG.info("Exception cleaning up: " + StringUtils.stringifyException(e));
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskAttemptId != null) {
        umbilical.fatalError(taskAttemptId, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
    	        + StringUtils.stringifyException(throwable));
      if (taskAttemptId != null) {
        Throwable tCause = throwable.getCause();
        String cause = tCause == null
                                 ? throwable.getMessage()
                                 : StringUtils.stringifyException(tCause);
        umbilical.fatalError(taskAttemptId, cause);
      }
    } finally {
      RPC.stopProxy(umbilical);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  /**
   * Configure mapred-local dirs. This config is used by the task for finding
   * out an output directory.
   * @throws IOException 
   */
  private static void configureLocalDirs(MRTask task, JobConf job) throws IOException {
    String[] localSysDirs = StringUtils.getTrimmedStrings(
        System.getenv(Environment.LOCAL_DIRS.name()));
    job.setStrings(TezJobConfig.LOCAL_DIR, localSysDirs);
    LOG.info(TezJobConfig.LOCAL_DIR + " for child: " +
        job.get(TezJobConfig.LOCAL_DIR));
    LocalDirAllocator lDirAlloc = new LocalDirAllocator(TezJobConfig.LOCAL_DIR);
    Path workDir = null;
    // First, try to find the JOB_LOCAL_DIR on this host.
    try {
      workDir = lDirAlloc.getLocalPathToRead("work", job);
    } catch (DiskErrorException e) {
      // DiskErrorException means dir not found. If not found, it will
      // be created below.
    }
    if (workDir == null) {
      // JOB_LOCAL_DIR doesn't exist on this host -- Create it.
      workDir = lDirAlloc.getLocalPathForWrite("work", job);
      FileSystem lfs = FileSystem.getLocal(job).getRaw();
      boolean madeDir = false;
      try {
        madeDir = lfs.mkdirs(workDir);
      } catch (FileAlreadyExistsException e) {
        // Since all tasks will be running in their own JVM, the race condition
        // exists where multiple tasks could be trying to create this directory
        // at the same time. If this task loses the race, it's okay because
        // the directory already exists.
        madeDir = true;
        workDir = lDirAlloc.getLocalPathToRead("work", job);
      }
      if (!madeDir) {
          throw new IOException("Mkdirs failed to create "
              + workDir.toString());
      }
    }
    // TODO TEZ This likely needs fixing to make sure things work when there are multiple local-dirs etc.
    job.set(MRJobConfig.JOB_LOCAL_DIR,workDir.toString());
  }

  private static JobConf configureTask(MRTask task, Credentials credentials,
      Token<JobTokenIdentifier> jt, int appAttemptId)
          throws IOException, InterruptedException {
    JobConf job = task.getConf();
    
    // Set it in conf, so as to be able to be used the the OutputCommitter.
    job.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptId);

    // set tcp nodelay
    job.setBoolean("ipc.client.tcpnodelay", true);
    job.setClass(MRConfig.TASK_LOCAL_OUTPUT_CLASS,
        YarnOutputFiles.class, MapOutputFile.class);
    // set the jobTokenFile into task
    SecretKey sk = JobTokenSecretManager.createSecretKey(jt.getPassword());

    task.setJobTokenSecret(sk);
//    task.setJobTokenSecret(
//        JobTokenSecretManager.createSecretKey(jt.getPassword()));

    // setup the child's MRConfig.LOCAL_DIR.
    configureLocalDirs(task, job);

    // setup the child's attempt directories
    // Do the task-type specific localization
    task.localizeConfiguration(job);

    // Set up the DistributedCache related configs
    setupDistributedCacheConfig(job);

    // Overwrite the localized task jobconf which is linked to in the current
    // work-dir.
    Path localTaskFile = new Path(MRJobConfig.JOB_CONF_FILE);
    writeLocalJobFile(localTaskFile, job);
    task.setConf(job);
    return job;
  }

  /**
   * Set up the DistributedCache related configs to make
   * {@link DistributedCache#getLocalCacheFiles(Configuration)}
   * and
   * {@link DistributedCache#getLocalCacheArchives(Configuration)}
   * working.
   * @param job
   * @throws IOException
   */
  private static void setupDistributedCacheConfig(final JobConf job)
      throws IOException {

    String localWorkDir = System.getenv("PWD");
    //        ^ ^ all symlinks are created in the current work-dir

    // Update the configuration object with localized archives.
    URI[] cacheArchives = DistributedCache.getCacheArchives(job);
    if (cacheArchives != null) {
      List<String> localArchives = new ArrayList<String>();
      for (int i = 0; i < cacheArchives.length; ++i) {
        URI u = cacheArchives[i];
        Path p = new Path(u);
        Path name =
            new Path((null == u.getFragment()) ? p.getName()
                : u.getFragment());
        String linkName = name.toUri().getPath();
        localArchives.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localArchives.isEmpty()) {
        job.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
            .arrayToString(localArchives.toArray(new String[localArchives
                .size()])));
      }
    }

    // Update the configuration object with localized files.
    URI[] cacheFiles = DistributedCache.getCacheFiles(job);
    if (cacheFiles != null) {
      List<String> localFiles = new ArrayList<String>();
      for (int i = 0; i < cacheFiles.length; ++i) {
        URI u = cacheFiles[i];
        Path p = new Path(u);
        Path name =
            new Path((null == u.getFragment()) ? p.getName()
                : u.getFragment());
        String linkName = name.toUri().getPath();
        localFiles.add(new Path(localWorkDir, linkName).toUri().getPath());
      }
      if (!localFiles.isEmpty()) {
        job.set(MRJobConfig.CACHE_LOCALFILES,
            StringUtils.arrayToString(localFiles
                .toArray(new String[localFiles.size()])));
      }
    }
  }

  private static final FsPermission urw_gr =
    FsPermission.createImmutable((short) 0640);

  /**
   * Write the task specific job-configuration file.
   * @throws IOException
   */
  private static void writeLocalJobFile(Path jobFile, JobConf conf)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(jobFile);
    OutputStream out = null;
    try {
      out = FileSystem.create(localFs, jobFile, urw_gr);
      conf.writeXml(out);
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

  private static Task createAndConfigureTezTask(
      TezEngineTaskContext taskContext,
      TezTaskUmbilicalProtocol master,
      Credentials credentials, Token<JobTokenIdentifier> jt,
      int appAttemptId)
      throws IOException, InterruptedException {

    Configuration jConf = new JobConf(MRJobConfig.JOB_CONF_FILE);
    Configuration conf = MultiStageMRConfigUtil.getConfForVertex(jConf,
        taskContext.getVertexName());
    
    // TOOD Post MRR
    // A single file per vertex will likely be a better solution. Does not
    // require translation - client can take care of this. Will work independent
    // of whether the configuration is for intermediate tasks or not. Has the
    // overhead of localizing multiple files per job - i.e. the client would
    // need to write these files to hdfs, add them as local resources per
    // vertex. A solution like this may be more practical once it's possible to
    // submit configuration parameters to the AM and effectively tasks via RPC.

    // TODO Avoid all this extra config manipulation.
    // FIXME we need I/O/p level configs to be used in init below
    final JobConf job = new JobConf(conf);
    job.setCredentials(credentials);

    // FIXME need Input/Output vertices else we have this hack
    if (taskContext.getInputSpecList().isEmpty()) {
      taskContext.getInputSpecList().add(
          new InputSpec("null", 0, SimpleInput.class.getName()));
    }
    if (taskContext.getOutputSpecList().isEmpty()) {
      taskContext.getOutputSpecList().add(
          new OutputSpec("null", 0, SimpleOutput.class.getName()));
    }
    Task t = RuntimeUtils.createRuntimeTask(taskContext);
    
    // FIXME wrapper should initialize all of processor, inputs and outputs
    // Currently, processor is inited via task init
    // and processor then inits inputs and outputs
    t.initialize(job, master);
    
    MRTask task = (MRTask)t.getProcessor();
    configureTask(task, credentials, jt, appAttemptId);
    
    return t;
  }
  
  private static void runTezTask(
      Task t, TezTaskUmbilicalProtocol master, JobConf job) 
  throws IOException, InterruptedException {
    // use job-specified working directory
    FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
    
    // Run!
    t.run();
    t.close();
  }
}

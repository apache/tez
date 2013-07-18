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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
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
import org.apache.tez.common.counters.Limits;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.common.security.TokenCache;
import org.apache.tez.engine.runtime.RuntimeUtils;
import org.apache.tez.engine.task.RuntimeTask;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;


/**
 * The main() for TEZ Task processes.
 */
public class YarnTezDagChild {

  private static final Log LOG = LogFactory.getLog(YarnTezDagChild.class);

  public static void main(String[] args) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Child starting");
    }

    final Configuration defaultConf = new Configuration();
    // Security settings will be loaded based on core-site and core-default. Don't
    // depend on the jobConf for this.
    UserGroupInformation.setConfiguration(defaultConf);
    Limits.setConfiguration(defaultConf);

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

    if (LOG.isDebugEnabled()) {
      LOG.info("Executing with tokens:");
      for (Token<?> token: credentials.getAllTokens()) {
        LOG.info(token);
      }
    }

    // Create TaskUmbilicalProtocol as actual task owner.
    UserGroupInformation taskOwner =
      UserGroupInformation.createRemoteUser(appID.toString());

    Token<JobTokenIdentifier> jobToken = TokenCache.getJobToken(credentials);
    SecurityUtil.setTokenService(jobToken, address);
    taskOwner.addToken(jobToken);
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
        if (LOG.isDebugEnabled()) {
          LOG.debug("New container task context:"
              + taskContext.toString());
        }
        taskAttemptId = taskContext.getTaskAttemptId();

        final Task t = createAndConfigureTezTask(taskContext, umbilical,
            credentials, jobToken,
            containerId.getApplicationAttemptId().getAttemptId());

        final Configuration conf = ((RuntimeTask)t).getConfiguration();

        // TODO Initiate Java VM metrics
        // JvmMetrics.initSingleton(containerId.toString(), job.getSessionId());
        childUGI = UserGroupInformation.createRemoteUser(System
            .getenv(ApplicationConstants.Environment.USER.toString()));
        // Add tokens to new user so that it may execute its task correctly.
        childUGI.addCredentials(credentials);

        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            runTezTask(t, umbilical, conf); // run the task
            return null;
          }
        });
        FileSystem.closeAllForUGI(childUGI);
        containerTask = null;
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(taskAttemptId, e.getMessage());
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
  /**
   * Configure tez-local-dirs, tez-localized-file-dir, etc. Also create these
   * dirs.
   */

  private static void configureLocalDirs(Configuration conf) throws IOException {
    String[] localSysDirs = StringUtils.getTrimmedStrings(
        System.getenv(Environment.LOCAL_DIRS.name()));
    conf.setStrings(TezJobConfig.LOCAL_DIRS, localSysDirs);
    conf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR,
        System.getenv(Environment.PWD.name()));

    LOG.info(TezJobConfig.LOCAL_DIRS + " for child: " +
        conf.get(TezJobConfig.LOCAL_DIRS));
    LOG.info(TezJobConfig.TASK_LOCAL_RESOURCE_DIR + " for child: "
        + conf.get(TezJobConfig.TASK_LOCAL_RESOURCE_DIR));

    LocalDirAllocator lDirAlloc = new LocalDirAllocator(TezJobConfig.LOCAL_DIRS);
    Path workDir = null;
    // First, try to find the JOB_LOCAL_DIR on this host.
    try {
      workDir = lDirAlloc.getLocalPathToRead("work", conf);
    } catch (DiskErrorException e) {
      // DiskErrorException means dir not found. If not found, it will
      // be created below.
    }
    if (workDir == null) {
      // JOB_LOCAL_DIR doesn't exist on this host -- Create it.
      workDir = lDirAlloc.getLocalPathForWrite("work", conf);
      FileSystem lfs = FileSystem.getLocal(conf).getRaw();
      boolean madeDir = false;
      try {
        madeDir = lfs.mkdirs(workDir);
      } catch (FileAlreadyExistsException e) {
        // Since all tasks will be running in their own JVM, the race condition
        // exists where multiple tasks could be trying to create this directory
        // at the same time. If this task loses the race, it's okay because
        // the directory already exists.
        madeDir = true;
        workDir = lDirAlloc.getLocalPathToRead("work", conf);
      }
      if (!madeDir) {
          throw new IOException("Mkdirs failed to create "
              + workDir.toString());
      }
    }
    conf.set(TezJobConfig.JOB_LOCAL_DIR, workDir.toString());
  }

  private static Task createAndConfigureTezTask(
      TezEngineTaskContext taskContext, TezTaskUmbilicalProtocol master,
      Credentials cxredentials, Token<JobTokenIdentifier> jobToken,
      int appAttemptId) throws IOException, InterruptedException {

    Configuration conf = new Configuration(false);
    // set tcp nodelay
    conf.setBoolean("ipc.client.tcpnodelay", true);
    conf.setInt(TezJobConfig.APPLICATION_ATTEMPT_ID, appAttemptId);

    configureLocalDirs(conf);


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
    
    t.initialize(conf, taskContext.getProcessorUserPayload(), master);

    // FIXME wrapper should initialize all of processor, inputs and outputs
    // Currently, processor is inited via task init
    // and processor then inits inputs and outputs
    return t;
  }

  private static void runTezTask(
      Task t, TezTaskUmbilicalProtocol master, Configuration conf)
  throws IOException, InterruptedException {
    // use job-specified working directory
    FileSystem.get(conf).setWorkingDirectory(getWorkingDirectory(conf));

    // Run!
    t.run();
    t.close();
  }

  private static Path getWorkingDirectory(Configuration conf) {
    String name = conf.get(JobContext.WORKING_DIR);
    if (name != null) {
      return new Path(name);
    } else {
      try {
        Path dir = FileSystem.get(conf).getWorkingDirectory();
        conf.set(JobContext.WORKING_DIR, dir.toString());
        return dir;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

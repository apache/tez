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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.objectregistry.ObjectLifeCycle;
import org.apache.tez.engine.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.engine.common.objectregistry.ObjectRegistryModule;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.common.security.TokenCache;
import org.apache.tez.engine.newapi.impl.InputSpec;
import org.apache.tez.engine.newapi.impl.OutputSpec;
import org.apache.tez.engine.newapi.impl.TaskSpec;
import org.apache.tez.engine.newapi.impl.TezEvent;
import org.apache.tez.engine.newapi.impl.TezHeartbeatRequest;
import org.apache.tez.engine.newapi.impl.TezHeartbeatResponse;
import org.apache.tez.engine.newapi.impl.TezUmbilical;
import org.apache.tez.engine.newruntime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;

import com.google.inject.Guice;
import com.google.inject.Injector;


/**
 * The main() for TEZ Task processes.
 */
public class YarnTezDagChild {

  private static final Logger LOG = Logger.getLogger(YarnTezDagChild.class);

  private static AtomicBoolean stopped = new AtomicBoolean(false);

  private static String containerIdStr;
  private static int eventCounter = 0;
  private static int maxEventsToGet = 0;
  private static LinkedList<TezEvent> eventsToSend =
      new LinkedList<TezEvent>();
  private static ConcurrentLinkedQueue<TezEvent> eventsToBeProcessed =
      new ConcurrentLinkedQueue<TezEvent>();
  private static AtomicLong requestCounter = new AtomicLong(0);
  private static TezTaskAttemptID currentTaskAttemptID;
  private static long amPollInterval;
  private static TezTaskUmbilicalProtocol umbilical;
  private static Object eventLock = new Object();
  private static ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();
  private static LogicalIOProcessorRuntimeTask currentTask = null;

  private static Thread startHeartbeatThread() {
    Thread heartbeatThread = new Thread(new Runnable() {
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(amPollInterval);
            try {
              heartbeat();
            } catch (TezException e) {
              LOG.error("Error communicating with AM: " + e.getMessage() , e);
              // TODO TODONEWTEZ
            } catch (InvalidToken e) {
              LOG.error("Error in authenticating with AM: ", e);
              // TODO TODONEWTEZ
            } catch (Exception e) {
              LOG.error("Error in heartbeating with AM. ", e);
              // TODO TODONEWTEZ
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Heartbeat thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    heartbeatThread.setName("Tez Container Heartbeat Thread ["
        + containerIdStr + "]");
    heartbeatThread.start();
    return heartbeatThread;
  }

  private static Thread startRouterThread() {
    Thread eventRouterThread = new Thread(new Runnable() {
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          while (true) {
            try {
              taskLock.readLock().lock();
              if (currentTask != null) {
                break;
              }
            } finally {
              taskLock.readLock().unlock();
            }
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              if (!stopped.get()) {
                LOG.warn("Event Router thread interrupted. Returning.");
              }
              return;
            }
          }
          try {
            TezEvent e = eventsToBeProcessed.poll();
            if (e == null) {
              eventsToBeProcessed.wait();
              continue;
            }
            // TODO TODONEWTEZ
            if (!e.getDestinationInfo().getTaskAttemptID().equals(
                currentTaskAttemptID)) {
              // error? or block?
              continue;
            }
            try {
              taskLock.readLock().lock();
              if (currentTask != null) {
                currentTask.handleEvent(e);
              }
            } finally {
              taskLock.readLock().unlock();
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Event Router thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    eventRouterThread.setName("Tez Container Event Router Thread ["
        + containerIdStr + "]");
    eventRouterThread.start();
    return eventRouterThread;
  }

  private static void heartbeat() throws TezException, IOException {
    try {
      taskLock.readLock().lock();
      if (currentTask == null) {
        return;
      }
    } finally {
      taskLock.readLock().unlock();
    }
    synchronized (eventLock) {
      List<TezEvent> events = new ArrayList<TezEvent>();
      events.addAll(eventsToSend);
      long reqId = requestCounter.incrementAndGet();
      TezHeartbeatRequest request = new TezHeartbeatRequest(reqId, events,
          currentTaskAttemptID, eventCounter, maxEventsToGet);
      TezHeartbeatResponse response = umbilical.heartbeat(request);
      if (response.getLastRequestId() != reqId) {
        // TODO TODONEWTEZ
        throw new TezException("AM and Task out of sync");
      }
      eventsToSend.clear();
      eventCounter += response.getEvents().size();
      eventsToBeProcessed.addAll(response.getEvents());
      eventsToBeProcessed.notifyAll();
    }
  }

  public static void main(String[] args) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Child starting");
    }

    final Configuration defaultConf = new Configuration();
    TezUtils.addUserSpecifiedTezConfiguration(defaultConf);
    // Security settings will be loaded based on core-site and core-default.
    // Don't depend on the jobConf for this.
    UserGroupInformation.setConfiguration(defaultConf);
    Limits.setConfiguration(defaultConf);

    assert args.length == 5;
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address =
        NetUtils.createSocketAddrForHost(host, port);
    final String containerIdentifier = args[2];
    final String tokenIdentifier = args[3];
    final int attemptNumber = Integer.parseInt(args[4]);
    if (LOG.isDebugEnabled()) {
      LOG.info("Info from cmd line: AM-host: " + host + " AM-port: " + port
          + " containerIdentifier: " + containerIdentifier + " attemptNumber: "
          + attemptNumber + " tokenIdentifier: " + tokenIdentifier);
    }
    // FIXME fix initialize metrics in child runner
    DefaultMetricsSystem.initialize("VertexTask");

    ObjectRegistryImpl objectRegistry = new ObjectRegistryImpl();
    @SuppressWarnings("unused")
    Injector injector = Guice.createInjector(
        new ObjectRegistryModule(objectRegistry));

    // Security framework already loaded the tokens into current ugi
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();

    if (LOG.isDebugEnabled()) {
      LOG.info("Executing with tokens:");
      for (Token<?> token: credentials.getAllTokens()) {
        LOG.info(token);
      }
    }

    amPollInterval = defaultConf.getLong(
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    maxEventsToGet = defaultConf.getInt(
        TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
        TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT);

    // Create TaskUmbilicalProtocol as actual task owner.
    UserGroupInformation taskOwner =
      UserGroupInformation.createRemoteUser(tokenIdentifier);

    Token<JobTokenIdentifier> jobToken = TokenCache.getJobToken(credentials);
    SecurityUtil.setTokenService(jobToken, address);
    taskOwner.addToken(jobToken);
    umbilical =
      taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
      @Override
      public TezTaskUmbilicalProtocol run() throws Exception {
        return (TezTaskUmbilicalProtocol)RPC.getProxy(TezTaskUmbilicalProtocol.class,
            TezTaskUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    Thread heartbeatThread = startHeartbeatThread();
    Thread eventRouterThread = startRouterThread();

    TezUmbilical tezUmbilical = new TezUmbilical() {
      @Override
      public void addEvents(Collection<TezEvent> events) {
        synchronized (eventLock) {
          eventsToSend.addAll(events);
        }
      }
    };

    // report non-pid to application master
    String pid = System.getenv().get("JVM_PID");
    if (LOG.isDebugEnabled()) {
      LOG.debug("PID, containerId: " + pid + ", " + containerIdentifier);
    }
    TaskSpec taskSpec = null;
    ContainerTask containerTask = null;
    UserGroupInformation childUGI = null;
    ContainerContext containerContext = new ContainerContext(
        containerIdentifier, pid);
    int getTaskMaxSleepTime = defaultConf.getInt(
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT);
    int taskCount = 0;
    TezVertexID currentVertexId = null;
    try {
      while (true) {
        // poll for new task
        if (taskCount > 0) {
          updateLoggers(null);
        }
        for (int idle = 0; null == containerTask; ++idle) {
          long sleepTimeMilliSecs = Math.min(idle * 10, getTaskMaxSleepTime);
          LOG.info("Sleeping for " + sleepTimeMilliSecs
              + "ms before retrying again. Got null now.");
          MILLISECONDS.sleep(sleepTimeMilliSecs);
          containerTask = umbilical.getTask(containerContext);
        }
        LOG.info("TaskInfo: shouldDie: "
            + containerTask.shouldDie()
            + (containerTask.shouldDie() == true ?
                "" : ", currentTaskAttemptId: "
                  + containerTask.getTaskSpec().getTaskAttemptID()));

        if (containerTask.shouldDie()) {
          return;
        }
        taskCount++;
        taskSpec = containerTask.getTaskSpec();
        if (LOG.isDebugEnabled()) {
          LOG.debug("New container task context:"
              + taskSpec.toString());
        }

        try {
          taskLock.writeLock().lock();
          currentTaskAttemptID = taskSpec.getTaskAttemptID();
          TezVertexID newVertexId =
              currentTaskAttemptID.getTaskID().getVertexID();

          if (currentVertexId != null) {
            if (!currentVertexId.equals(newVertexId)) {
              objectRegistry.clearCache(ObjectLifeCycle.VERTEX);
            }
            if (!currentVertexId.getDAGId().equals(newVertexId.getDAGId())) {
              objectRegistry.clearCache(ObjectLifeCycle.DAG);
            }
          }
          currentVertexId = newVertexId;
          updateLoggers(currentTaskAttemptID);
          currentTask = createLogicalTask(
              taskSpec, defaultConf, tezUmbilical);
        } finally {
          taskLock.writeLock().unlock();
        }


        // TODO Initiate Java VM metrics
        // JvmMetrics.initSingleton(containerId.toString(), job.getSessionId());
        childUGI = UserGroupInformation.createRemoteUser(System
            .getenv(ApplicationConstants.Environment.USER.toString()));
        // Add tokens to new user so that it may execute its task correctly.
        childUGI.addCredentials(credentials);

        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            currentTask.initialize();
            currentTask.run();
            currentTask.close();
            try {
              taskLock.writeLock().lock();
              currentTask = null;
            } finally {
              taskLock.writeLock().unlock();
            }
            return null;
          }
        });
        FileSystem.closeAllForUGI(childUGI);
        containerTask = null;
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      umbilical.fsError(currentTaskAttemptID, e.getMessage());
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
    	        + StringUtils.stringifyException(throwable));
      if (currentTaskAttemptID != null) {
        String cause = StringUtils.stringifyException(throwable);
        umbilical.fatalError(currentTaskAttemptID, cause);
      }
    } finally {
      stopped.set(true);
      eventRouterThread.join();
      heartbeatThread.join();
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

  private static LogicalIOProcessorRuntimeTask createLogicalTask(
      TaskSpec taskSpec, Configuration conf,
      TezUmbilical tezUmbilical) throws IOException {

    // FIXME TODONEWTEZ
    // conf.setBoolean("ipc.client.tcpnodelay", true);
    // conf.setInt(TezJobConfig.APPLICATION_ATTEMPT_ID, appAttemptId);
    configureLocalDirs(conf);
    FileSystem.get(conf).setWorkingDirectory(getWorkingDirectory(conf));

    // FIXME need Input/Output vertices else we have this hack
    if (taskSpec.getInputs().isEmpty()) {
      InputDescriptor simpleInputDesc =
          new InputDescriptor(SimpleInput.class.getName());
      simpleInputDesc.setUserPayload(
          taskSpec.getProcessorDescriptor().getUserPayload());
      taskSpec.getInputs().add(
          new InputSpec("null", simpleInputDesc, 0));
    }
    if (taskSpec.getOutputs().isEmpty()) {
      OutputDescriptor simpleOutputDesc =
          new OutputDescriptor(SimpleOutput.class.getName());
      simpleOutputDesc.setUserPayload(
          taskSpec.getProcessorDescriptor().getUserPayload());
      taskSpec.getOutputs().add(
          new OutputSpec("null", simpleOutputDesc, 0));
    }
    return new LogicalIOProcessorRuntimeTask(taskSpec, conf,
        tezUmbilical);
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

  private static void updateLoggers(TezTaskAttemptID tezTaskAttemptID)
      throws FileNotFoundException {
    String containerLogDir = null;

    LOG.info("Redirecting log files based on TaskAttemptId: " + tezTaskAttemptID);

    Appender appender = Logger.getRootLogger().getAppender(
        TezConfiguration.TEZ_CONTAINER_LOGGER_NAME);
    if (appender != null) {
      if (appender instanceof TezContainerLogAppender) {
        TezContainerLogAppender claAppender = (TezContainerLogAppender) appender;
        containerLogDir = claAppender.getContainerLogDir();
        claAppender.setLogFileName(constructLogFileName(
            TezConfiguration.TEZ_CONTAINER_LOG_FILE_NAME, tezTaskAttemptID));
        claAppender.activateOptions();
      } else {
        LOG.warn("Appender is a " + appender.getClass()
            + "; require an instance of "
            + TezContainerLogAppender.class.getName()
            + " to reconfigure the logger output");
      }
    } else {
      LOG.warn("Not configured with appender named: "
          + TezConfiguration.TEZ_CONTAINER_LOGGER_NAME
          + ". Cannot reconfigure logger output");
    }

    if (containerLogDir != null) {
      System.setOut(new PrintStream(new File(containerLogDir,
          constructLogFileName(TezConfiguration.TEZ_CONTAINER_OUT_FILE_NAME,
              tezTaskAttemptID))));
      System.setErr(new PrintStream(new File(containerLogDir,
          constructLogFileName(TezConfiguration.TEZ_CONTAINER_ERR_FILE_NAME,
              tezTaskAttemptID))));
    }
  }

  private static String constructLogFileName(String base,
      TezTaskAttemptID tezTaskAttemptID) {
    if (tezTaskAttemptID == null) {
      return base;
    } else {
      return base + "_" + tezTaskAttemptID.toString();
    }
  }
}

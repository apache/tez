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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
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
import org.apache.tez.engine.newapi.events.TaskAttemptCompletedEvent;
import org.apache.tez.engine.newapi.events.TaskAttemptFailedEvent;
import org.apache.tez.engine.newapi.events.TaskStatusUpdateEvent;
import org.apache.tez.engine.newapi.impl.EventMetaData;
import org.apache.tez.engine.newapi.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.engine.newapi.impl.InputSpec;
import org.apache.tez.engine.newapi.impl.OutputSpec;
import org.apache.tez.engine.newapi.impl.TaskSpec;
import org.apache.tez.engine.newapi.impl.TezEvent;
import org.apache.tez.engine.newapi.impl.TezHeartbeatRequest;
import org.apache.tez.engine.newapi.impl.TezHeartbeatResponse;
import org.apache.tez.engine.newapi.impl.TezUmbilical;
import org.apache.tez.engine.newruntime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.mapreduce.newinput.SimpleInputLegacy;
import org.apache.tez.mapreduce.newoutput.SimpleOutput;

import com.google.inject.Guice;
import com.google.inject.Injector;


/**
 * The main() for TEZ Task processes.
 */
public class YarnTezDagChild {

  private static final Logger LOG = Logger.getLogger(YarnTezDagChild.class);

  private static AtomicBoolean stopped = new AtomicBoolean(false);

  private static String containerIdStr;
  private static int maxEventsToGet = 0;
  private static LinkedBlockingQueue<TezEvent> eventsToSend =
      new LinkedBlockingQueue<TezEvent>();
  private static AtomicLong requestCounter = new AtomicLong(0);
  private static TezTaskAttemptID currentTaskAttemptID;
  private static long amPollInterval;
  private static TezTaskUmbilicalProtocol umbilical;
  private static ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();
  private static LogicalIOProcessorRuntimeTask currentTask = null;
  private static AtomicBoolean heartbeatError = new AtomicBoolean(false);
  private static Throwable heartbeatErrorException = null;

  private static Thread startHeartbeatThread() {
    Thread heartbeatThread = new Thread(new Runnable() {
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()
            && !heartbeatError.get()) {
          try {
            Thread.sleep(amPollInterval);
            try {
              if(!heartbeat()) {
                return;
              }
            } catch (InvalidToken e) {
              LOG.error("Heartbeat error in authenticating with AM: ", e);
              heartbeatErrorException = e;
              heartbeatError.set(true);
              return;
            } catch (Throwable e) {
              LOG.error("Heartbeat error in communicating with AM. ", e);
              heartbeatErrorException = e;
              heartbeatError.set(true);
              return;
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

  private static synchronized boolean heartbeat() throws TezException, IOException {
    return heartbeat(null);
  }

  private static synchronized boolean heartbeat(
      Collection<TezEvent> outOfBandEvents)
      throws TezException, IOException {
    TezEvent updateEvent = null;
    int eventCounter = 0;
    int eventsRange = 0;
    TezTaskAttemptID taskAttemptID = null;
    try {
      taskLock.readLock().lock();
      if (currentTask != null) {
        taskAttemptID = currentTaskAttemptID;
        eventCounter = currentTask.getEventCounter();
        eventsRange = maxEventsToGet;
        if (!currentTask.isTaskDone() && !currentTask.hadFatalError()) {
          updateEvent = new TezEvent(new TaskStatusUpdateEvent(
              currentTask.getCounters(), currentTask.getProgress()),
                new EventMetaData(EventProducerConsumerType.SYSTEM,
                    currentTask.getVertexName(), "", taskAttemptID));
        }
      }
    } finally {
      taskLock.readLock().unlock();
    }
    List<TezEvent> events = new ArrayList<TezEvent>();
    if (updateEvent != null) {
      events.add(updateEvent);
    }
    eventsToSend.drainTo(events);
    if (outOfBandEvents != null && !outOfBandEvents.isEmpty()) {
      events.addAll(outOfBandEvents);
    }
    long reqId = requestCounter.incrementAndGet();
    TezHeartbeatRequest request = new TezHeartbeatRequest(reqId, events,
        containerIdStr, taskAttemptID, eventCounter, eventsRange);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat to AM"
          + ", request=" + request.toString());
    }
    TezHeartbeatResponse response = umbilical.heartbeat(request);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received heartbeat response from AM"
          + ", response=" + response);
    }
    if(response.shouldDie()) {
      LOG.info("Received should die response from AM");
      return false;
    }
    if (response.getLastRequestId() != reqId) {
      throw new TezException("AM and Task out of sync"
          + ", responseReqId=" + response.getLastRequestId()
          + ", expectedReqId=" + reqId);
    }
    try {
      taskLock.readLock().lock();
      if (taskAttemptID == null
          || !taskAttemptID.equals(currentTaskAttemptID)) {
        if (response.getEvents() != null
            && !response.getEvents().isEmpty()) {
          LOG.warn("No current assigned task, ignoring all events in"
              + " heartbeat response, eventCount="
              + response.getEvents().size());
        }
        return true;
      }
      if (currentTask != null && response.getEvents() != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Routing events from heartbeat response to task"
              + ", currentTaskAttemptId=" + currentTaskAttemptID
              + ", eventCount=" + response.getEvents().size());
        }
        currentTask.handleEvents(response.getEvents());
      }
    } finally {
      taskLock.readLock().unlock();
    }
    return true;
  }

  public static void main(String[] args) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(
        new YarnUncaughtExceptionHandler());
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
    YarnTezDagChild.containerIdStr = containerIdentifier;

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

    TezUmbilical tezUmbilical = new TezUmbilical() {
      @Override
      public void addEvents(Collection<TezEvent> events) {
        eventsToSend.addAll(events);
      }

      @Override
      public void signalFatalError(TezTaskAttemptID taskAttemptID,
          String diagnostics,
          EventMetaData sourceInfo) {
        TezEvent taskAttemptFailedEvent =
            new TezEvent(new TaskAttemptFailedEvent(diagnostics),
                sourceInfo);
        try {
          heartbeat(Collections.singletonList(taskAttemptFailedEvent));
        } catch (Throwable t) {
          LOG.fatal("Failed to communicate task attempt failure to AM via"
              + " umbilical", t);
          heartbeatError.set(true);
          heartbeatErrorException = t;
        }
      }

      @Override
      public boolean canCommit(TezTaskAttemptID taskAttemptID)
          throws IOException {
        return umbilical.canCommit(taskAttemptID);
      }

      @Override
      public void commitPending(TezTaskAttemptID taskAttemptID)
          throws IOException, InterruptedException {
        umbilical.commitPending(taskAttemptID);
      }
    };

    // report non-pid to application master
    String pid = System.getenv().get("JVM_PID");
    if (LOG.isDebugEnabled()) {
      LOG.debug("PID, containerId: " + pid + ", " + containerIdentifier);
    }
    ContainerTask containerTask = null;
    UserGroupInformation childUGI = null;
    ContainerContext containerContext = new ContainerContext(
        containerIdentifier, pid);
    int getTaskMaxSleepTime = defaultConf.getInt(
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT);
    int taskCount = 0;
    TezVertexID lastVertexId = null;
    EventMetaData currentSourceInfo = null;
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
        final TaskSpec taskSpec = containerTask.getTaskSpec();
        if (LOG.isDebugEnabled()) {
          LOG.debug("New container task context:"
              + taskSpec.toString());
        }

        try {
          taskLock.writeLock().lock();
          currentTaskAttemptID = taskSpec.getTaskAttemptID();
          TezVertexID newVertexId =
              currentTaskAttemptID.getTaskID().getVertexID();

          if (lastVertexId != null) {
            if (!lastVertexId.equals(newVertexId)) {
              objectRegistry.clearCache(ObjectLifeCycle.VERTEX);
            }
            if (!lastVertexId.getDAGId().equals(newVertexId.getDAGId())) {
              objectRegistry.clearCache(ObjectLifeCycle.DAG);
            }
          }
          lastVertexId = newVertexId;
          updateLoggers(currentTaskAttemptID);
          currentTask = createLogicalTask(attemptNumber, taskSpec,
              defaultConf, tezUmbilical, jobToken);
        } finally {
          taskLock.writeLock().unlock();
        }

        final EventMetaData sourceInfo = new EventMetaData(
            EventProducerConsumerType.SYSTEM,
            taskSpec.getVertexName(), "", currentTaskAttemptID);
        currentSourceInfo = sourceInfo;

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
            if (!currentTask.hadFatalError()) {
              currentTask.run();
              currentTask.close();
            }
            // TODONEWTEZ check if task had a fatal error before
            // sending completed event
            if (!currentTask.hadFatalError()) {
              TezEvent statusUpdateEvent =
                  new TezEvent(new TaskStatusUpdateEvent(
                      currentTask.getCounters(), currentTask.getProgress()),
                      new EventMetaData(EventProducerConsumerType.SYSTEM,
                          currentTask.getVertexName(), "",
                          currentTask.getTaskAttemptID()));
              TezEvent taskCompletedEvent =
                  new TezEvent(new TaskAttemptCompletedEvent(), sourceInfo);
              heartbeat(Arrays.asList(statusUpdateEvent, taskCompletedEvent));
            }
            try {
              taskLock.writeLock().lock();
              if (currentTask != null) {
                currentTask.cleanup();
              }
              currentTask = null;
              currentTaskAttemptID = null;
            } finally {
              taskLock.writeLock().unlock();
            }
            return null;
          }
        });
        FileSystem.closeAllForUGI(childUGI);
        containerTask = null;
        if (heartbeatError.get()) {
          LOG.fatal("Breaking out of task loop, heartbeat error occurred",
              heartbeatErrorException);
          break;
        }
      }
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      // TODO NEWTEZ this should be a container failed event?
      TezEvent taskAttemptFailedEvent =
          new TezEvent(new TaskAttemptFailedEvent(
              StringUtils.stringifyException(e)),
              currentSourceInfo);
      heartbeat(Collections.singletonList(taskAttemptFailedEvent));
    } catch (Throwable throwable) {
      String cause = StringUtils.stringifyException(throwable);
      LOG.fatal("Error running child : " + cause);
      if (currentTaskAttemptID != null && !currentTask.hadFatalError()) {
        TezEvent taskAttemptFailedEvent =
            new TezEvent(new TaskAttemptFailedEvent(cause),
                currentSourceInfo);
        heartbeat(Collections.singletonList(taskAttemptFailedEvent));
      }
    } finally {
      stopped.set(true);
      heartbeatThread.interrupt();
      RPC.stopProxy(umbilical);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  private static LogicalIOProcessorRuntimeTask createLogicalTask(int attemptNum,
      TaskSpec taskSpec, Configuration conf, TezUmbilical tezUmbilical,
      Token<JobTokenIdentifier> jobToken) throws IOException {

    // FIXME TODONEWTEZ
    conf.setBoolean("ipc.client.tcpnodelay", true);
    FileSystem.get(conf).setWorkingDirectory(getWorkingDirectory(conf));

    // FIXME need Input/Output vertices else we have this hack
    if (taskSpec.getInputs().isEmpty()) {
      InputDescriptor simpleInputDesc =
          new InputDescriptor(SimpleInputLegacy.class.getName());
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
    String [] localDirs = StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));
    conf.setStrings(TezJobConfig.LOCAL_DIRS, localDirs);
    LOG.info("LocalDirs for child: " + localDirs);
    return new LogicalIOProcessorRuntimeTask(taskSpec, attemptNum, conf,
        tezUmbilical, jobToken);
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

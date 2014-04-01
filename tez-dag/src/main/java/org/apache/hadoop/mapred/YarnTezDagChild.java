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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezLocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.RelocalizationUtils;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.common.objectregistry.ObjectLifeCycle;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryModule;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
  private static long amPollInterval;
  private static TezTaskUmbilicalProtocol umbilical;
  private static ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();
  private static LogicalIOProcessorRuntimeTask currentTask = null;
  private static TezTaskAttemptID currentTaskAttemptID;
  private static AtomicBoolean heartbeatError = new AtomicBoolean(false);
  private static Throwable heartbeatErrorException = null;
  // Implies that the task is done - and the AM is being informed.
  private static AtomicBoolean currentTaskComplete = new AtomicBoolean(true);
  /**
   * Used to maintain information about which Inputs have been started by the
   * framework for the specific DAG. Makes an assumption that multiple DAGs do
   * not execute concurrently, and must be reset each time the running DAG
   * changes.
   */
  private static Multimap<String, String> startedInputsMap = HashMultimap.create();
  
  private static final int LOG_COUNTER_START_INTERVAL = 5000; // 5 seconds.
  private static final float LOG_COUNTER_BACKOFF = 1.3f;
  private static int taskNonOobHeartbeatCounter = 0;
  private static int nextHeartbeatNumToLog = 0;

  private static Thread startHeartbeatThread() {
    Thread heartbeatThread = new Thread(new Runnable() {
      public void run() {
        while (!(stopped.get() || heartbeatError.get())) {
          try {
            try {
              if(!heartbeat()) {
                // AM asked us to die
                break;
              }
            } catch (InvalidToken e) {
              // FIXME NEWTEZ maybe send a container failed event to AM?
              // Irrecoverable error unless heartbeat sync can be re-established
              LOG.error("Heartbeat error in authenticating with AM: ", e);
              heartbeatErrorException = e;
              heartbeatError.set(true);
              break;
            } catch (Throwable e) {
              // FIXME NEWTEZ maybe send a container failed event to AM?
              // Irrecoverable error unless heartbeat sync can be re-established
              LOG.error("Heartbeat error in communicating with AM. ", e);
              if (e instanceof Error) {
                LOG.error("Exception of type Error. Exiting now", e);
                ExitUtil.terminate(-1, e);
              }
              heartbeatErrorException = e;
              heartbeatError.set(true);
              break;
            }
            Thread.sleep(amPollInterval);
          } catch (InterruptedException e) {
            // we were interrupted so that we will stop.
            LOG.info("Heartbeat thread interrupted. " +
            " stopped: " + stopped.get() +  " error: " + heartbeatError.get());
            continue; 
          }
        }
        
        if (currentTaskComplete.get() || stopped.get()) {
          // Don't exit. The Tez framework has control, let the container finish after cleanup etc.
          // Makes an assumption that a heartbeat shouldDie will be reported as a getTask should die.
          LOG.info("Current task marked as complete. Stopping heartbeat thread and allowing normal container shutdown");
          return;
        } else {
          // Assuming the task is still running, and we've been asked to die or an error occurred.
          // Stop the process.
          if (heartbeatErrorException != null) {
            ExitUtil.terminate(-1, heartbeatErrorException);
          } else {
            ExitUtil.terminate(-1, "Exiting Tez Child Process");
          }
        }
      }
    });
    heartbeatThread.setName("Tez Container Heartbeat Thread ["
        + containerIdStr + "]");
    heartbeatThread.setDaemon(true);
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
    List<TezEvent> events = new ArrayList<TezEvent>();
    try {
      taskLock.readLock().lock();
      if (currentTask != null) {
        eventsToSend.drainTo(events);
        taskAttemptID = currentTaskAttemptID;
        eventCounter = currentTask.getEventCounter();
        eventsRange = maxEventsToGet;
        if (!currentTask.isTaskDone() && !currentTask.hadFatalError()) {
          updateEvent = new TezEvent(new TaskStatusUpdateEvent(
              currentTask.getCounters(), currentTask.getProgress()),
                new EventMetaData(EventProducerConsumerType.SYSTEM,
                    currentTask.getVertexName(), "", taskAttemptID));
          events.add(updateEvent);
        } else if (outOfBandEvents == null && events.isEmpty()) {
          LOG.info("Setting TaskAttemptID to null as the task has already"
            + " completed. Caused by race-condition between the normal"
            + " heartbeat and out-of-band heartbeats");
          taskAttemptID = null;
        } else {
          if (outOfBandEvents != null && !outOfBandEvents.isEmpty()) {
            events.addAll(outOfBandEvents);
          }
        }
      }
    } finally {
      taskLock.readLock().unlock();
    }

    if (LOG.isDebugEnabled()) {
      taskNonOobHeartbeatCounter++;
      if (taskNonOobHeartbeatCounter == nextHeartbeatNumToLog) {
        taskLock.readLock().lock();
        try {
          if (currentTask != null) {
            LOG.debug("Counters: " + currentTask.getCounters().toShortString());
            taskNonOobHeartbeatCounter = 0;
            nextHeartbeatNumToLog = (int) (nextHeartbeatNumToLog * (LOG_COUNTER_BACKOFF));
          }
        } finally {
          taskLock.readLock().unlock();
        }
      }
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
    LOG.info("YarnTezDagChild starting");

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
      LOG.debug("Executing with tokens:");
      for (Token<?> token : credentials.getAllTokens()) {
        LOG.debug(token);
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

    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
    SecurityUtil.setTokenService(jobToken, address);
    taskOwner.addToken(jobToken);
    // Will jobToken change across DAGs ?
    Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(jobToken));

    umbilical =
      taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
      @Override
      public TezTaskUmbilicalProtocol run() throws Exception {
        return (TezTaskUmbilicalProtocol)RPC.getProxy(TezTaskUmbilicalProtocol.class,
            TezTaskUmbilicalProtocol.versionID, address, defaultConf);
      }
    });

    final Thread heartbeatThread = startHeartbeatThread();

    TezUmbilical tezUmbilical = new TezUmbilical() {
      @Override
      public void addEvents(Collection<TezEvent> events) {
        eventsToSend.addAll(events);
      }

      @Override
      public void signalFatalError(TezTaskAttemptID taskAttemptID,
          String diagnostics,
          EventMetaData sourceInfo) {
        currentTask.setFrameworkCounters();
        TezEvent statusUpdateEvent =
            new TezEvent(new TaskStatusUpdateEvent(
                currentTask.getCounters(), currentTask.getProgress()),
                new EventMetaData(EventProducerConsumerType.SYSTEM,
                    currentTask.getVertexName(), "",
                    currentTask.getTaskAttemptID()));
        TezEvent taskAttemptFailedEvent =
            new TezEvent(new TaskAttemptFailedEvent(diagnostics),
                sourceInfo);
        try {
          // Not setting taskComplete - since the main loop responsible for cleanup doesn't have
          // control yet. Getting control depends on whether the I/P/O returns correctly after
          // reporting an error.
          heartbeat(Lists.newArrayList(statusUpdateEvent, taskAttemptFailedEvent));
        } catch (Throwable t) {
          LOG.fatal("Failed to communicate task attempt failure to AM via"
              + " umbilical", t);
          if (t instanceof Error) {
            LOG.error("Exception of type Error. Exiting now", t);
            ExitUtil.terminate(-1, t);
          }
          // FIXME NEWTEZ maybe send a container failed event to AM?
          // Irrecoverable error unless heartbeat sync can be re-established
          heartbeatErrorException = t;
          heartbeatError.set(true);
          heartbeatThread.interrupt();
        }
      }

      @Override
      public boolean canCommit(TezTaskAttemptID taskAttemptID)
          throws IOException {
        return umbilical.canCommit(taskAttemptID);
      }
    };

    // report non-pid to application master
    String pid = System.getenv().get("JVM_PID");
    
    LOG.info("PID, containerIdentifier: " + pid + ", " + containerIdentifier);
    
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
      String loggerAddend = "";
      while (true) {
        // poll for new task
        if (taskCount > 0) {
          TezUtils.updateLoggers(loggerAddend);
        }
        boolean isNewGetTask = true;
        long getTaskPollStartTime = System.currentTimeMillis();
        long nextGetTaskPrintTime = getTaskPollStartTime + 2000l;
        for (int idle = 0; null == containerTask; ++idle) {
          if (!isNewGetTask) { // Don't sleep on the first iteration.
            long sleepTimeMilliSecs = Math.min(idle * 10, getTaskMaxSleepTime);
            if (sleepTimeMilliSecs + System.currentTimeMillis() > nextGetTaskPrintTime) {
              LOG.info("Sleeping for "
                  + sleepTimeMilliSecs
                  + "ms before retrying getTask again. Got null now. "
                  + "Next getTask sleep message after 2s");
              nextGetTaskPrintTime = System.currentTimeMillis() + sleepTimeMilliSecs + 2000l;
            }
            MILLISECONDS.sleep(sleepTimeMilliSecs);
          } else {
            LOG.info("Attempting to fetch new task");
          }
          isNewGetTask = false;
          containerTask = umbilical.getTask(containerContext);
        }
        LOG.info("Got TaskUpdate: "
            + (System.currentTimeMillis() - getTaskPollStartTime)
            + " ms after starting to poll."
            + " TaskInfo: shouldDie: " + containerTask.shouldDie()
            + (containerTask.shouldDie() == true ? "" : ", currentTaskAttemptId: "
                + containerTask.getTaskSpec().getTaskAttemptID()));
        if (containerTask.shouldDie()) {
          return;
        }
        taskCount++;

        // Reset FileSystem statistics
        FileSystem.clearStatistics();

        // Re-use the UGI only if the Credentials have not changed.
        if (containerTask.haveCredentialsChanged()) {
          LOG.info("Refreshing UGI since Credentials have changed");
          Credentials taskCreds = containerTask.getCredentials();
          if (taskCreds != null) {
            LOG.info("Credentials : #Tokens=" + taskCreds.numberOfTokens() + ", #SecretKeys="
                + taskCreds.numberOfSecretKeys());
            childUGI = UserGroupInformation.createRemoteUser(System
                .getenv(ApplicationConstants.Environment.USER.toString()));
            childUGI.addCredentials(containerTask.getCredentials());
          } else {
            LOG.info("Not loading any credentials, since no credentials provided");
          }
        }

        Map<String, TezLocalResource> additionalResources = containerTask.getAdditionalResources();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Additional Resources added to container: " + additionalResources);
        }

        LOG.info("Localizing additional local resources for Task : " + additionalResources);
        List<URL> downloadedUrls = RelocalizationUtils.processAdditionalResources(
            Maps.transformValues(additionalResources, new Function<TezLocalResource, URI>() {
              @Override
              public URI apply(TezLocalResource input) {
                return input.getUri();
              }
            }), defaultConf);
        RelocalizationUtils.addUrlsToClassPath(downloadedUrls);

        LOG.info("Done localizing additional resources");
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
          currentTaskComplete.set(false);

          if (lastVertexId != null) {
            if (!lastVertexId.equals(newVertexId)) {
              objectRegistry.clearCache(ObjectLifeCycle.VERTEX);
            }
            if (!lastVertexId.getDAGId().equals(newVertexId.getDAGId())) {
              objectRegistry.clearCache(ObjectLifeCycle.DAG);
              startedInputsMap = HashMultimap.create();
            }
          }
          lastVertexId = newVertexId;
          TezUtils.updateLoggers(currentTaskAttemptID.toString());
          loggerAddend = currentTaskAttemptID.toString() + "_post";
          
          currentTask = createLogicalTask(attemptNumber, taskSpec,
              defaultConf, tezUmbilical, serviceConsumerMetadata);
          
          taskNonOobHeartbeatCounter = 0;
          nextHeartbeatNumToLog = (Math.max(1,
              (int) (LOG_COUNTER_START_INTERVAL / (amPollInterval == 0 ? 0.000001f
                  : (float) amPollInterval))));
        } finally {
          taskLock.writeLock().unlock();
        }

        final EventMetaData sourceInfo = new EventMetaData(
            EventProducerConsumerType.SYSTEM,
            taskSpec.getVertexName(), "", currentTaskAttemptID);
        currentSourceInfo = sourceInfo;

        // TODO Initiate Java VM metrics
        // JvmMetrics.initSingleton(containerId.toString(), job.getSessionId());

        childUGI.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            try {
              setFileSystemWorkingDir(defaultConf);
              LOG.info("Initializing task"
                  + ", taskAttemptId=" + currentTaskAttemptID);
              currentTask.initialize();
              if (!currentTask.hadFatalError()) {
                LOG.info("Running task"
                    + ", taskAttemptId=" + currentTaskAttemptID);
                currentTask.run();
                LOG.info("Closing task"
                    + ", taskAttemptId=" + currentTaskAttemptID);
                currentTask.close();
              }
              LOG.info("Task completed"
                  + ", taskAttemptId=" + currentTaskAttemptID
                  + ", fatalErrorOccurred=" + currentTask.hadFatalError());
              // Mark taskComplete - irrespective of failure, framework has control from this point.
              currentTaskComplete.set(true);
              // TODONEWTEZ Should the container continue to run if the running task reported a fatal error ?
              if (!currentTask.hadFatalError()) {
                // Set counters in case of a successful task.
                currentTask.setFrameworkCounters();
                TezEvent statusUpdateEvent =
                    new TezEvent(new TaskStatusUpdateEvent(
                        currentTask.getCounters(), currentTask.getProgress()),
                        new EventMetaData(EventProducerConsumerType.SYSTEM,
                            currentTask.getVertexName(), "",
                            currentTask.getTaskAttemptID()));
                TezEvent taskCompletedEvent =
                    new TezEvent(new TaskAttemptCompletedEvent(), sourceInfo);
                heartbeat(Arrays.asList(statusUpdateEvent, taskCompletedEvent));
              } // Should the fatalError be reported ?
            } finally {
              currentTask.cleanup();
            }
            try {
              taskLock.writeLock().lock();
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
      // Heartbeats controlled manually after this.
      stopped.set(true);
      heartbeatThread.interrupt();
      LOG.fatal("FSError from child", e);
      // TODO NEWTEZ this should be a container failed event?
      try {
        taskLock.readLock().lock();
        if (currentTask != null && !currentTask.hadFatalError()) {
          // TODO Is this of any use if the heartbeat thread is being interrupted first ?
          // Prevent dup failure events
          currentTask.setFrameworkCounters();
          TezEvent statusUpdateEvent =
              new TezEvent(new TaskStatusUpdateEvent(
                  currentTask.getCounters(), currentTask.getProgress()),
                  new EventMetaData(EventProducerConsumerType.SYSTEM,
                      currentTask.getVertexName(), "",
                      currentTask.getTaskAttemptID()));
          currentTask.setFatalError(e, "FS Error in Child JVM");
          TezEvent taskAttemptFailedEvent =
              new TezEvent(new TaskAttemptFailedEvent(
                  StringUtils.stringifyException(e)),
                  currentSourceInfo);
          currentTaskComplete.set(true);
          heartbeat(Lists.newArrayList(statusUpdateEvent, taskAttemptFailedEvent));
        }
      } finally {
        taskLock.readLock().unlock();
      }
    } catch (Throwable throwable) {
      // Heartbeats controlled manually after this.
      if (throwable instanceof Error) {
        LOG.error("Exception of type Error. Exiting now", throwable);
        ExitUtil.terminate(-1, throwable);
      }
      stopped.set(true);
      heartbeatThread.interrupt();
      String cause = StringUtils.stringifyException(throwable);
      LOG.fatal("Error running child : " + cause);
      taskLock.readLock().lock();
      try {
        if (currentTask != null && !currentTask.hadFatalError()) {
          // TODO Is this of any use if the heartbeat thread is being interrupted first ?
          // Prevent dup failure events
          currentTask.setFatalError(throwable, "Error in Child JVM");
          currentTask.setFrameworkCounters();
          TezEvent statusUpdateEvent =
              new TezEvent(new TaskStatusUpdateEvent(
                  currentTask.getCounters(), currentTask.getProgress()),
                  new EventMetaData(EventProducerConsumerType.SYSTEM,
                      currentTask.getVertexName(), "",
                      currentTask.getTaskAttemptID()));
          TezEvent taskAttemptFailedEvent =
            new TezEvent(new TaskAttemptFailedEvent(cause),
              currentSourceInfo);
          currentTaskComplete.set(true);
          heartbeat(Lists.newArrayList(statusUpdateEvent, taskAttemptFailedEvent));
        }
      } finally {
        taskLock.readLock().unlock();
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
      Map<String, ByteBuffer> serviceConsumerMetadata) throws IOException {

    // FIXME TODONEWTEZ
    conf.setBoolean("ipc.client.tcpnodelay", true);

    String [] localDirs = StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS.name()));
    conf.setStrings(TezJobConfig.LOCAL_DIRS, localDirs);
    LOG.info("LocalDirs for child: " + Arrays.toString(localDirs));

    return new LogicalIOProcessorRuntimeTask(taskSpec, attemptNum, conf,
        tezUmbilical, serviceConsumerMetadata, startedInputsMap);
  }
  
  // TODONEWTEZ Is this really required ?
  private static void setFileSystemWorkingDir(Configuration conf) throws IOException {
    FileSystem.get(conf).setWorkingDirectory(getWorkingDirectory(conf));
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

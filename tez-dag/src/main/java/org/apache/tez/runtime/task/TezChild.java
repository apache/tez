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

package org.apache.tez.runtime.task;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezLocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.RelocalizationUtils;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.common.objectregistry.ObjectLifeCycle;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryModule;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TezChild {

  private static final Logger LOG = Logger.getLogger(TezChild.class);

  private final Configuration defaultConf;
  private final String containerIdString;
  private final int appAttemptNumber;
  private final InetSocketAddress address;
  private final String[] localDirs;

  private final AtomicLong heartbeatCounter = new AtomicLong(0);

  private final int getTaskMaxSleepTime;
  private final int amHeartbeatInterval;
  private final long sendCounterInterval;
  private final int maxEventsToGet;

  private final ListeningExecutorService executor;
  private final ObjectRegistryImpl objectRegistry;
  private final Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();

  private Multimap<String, String> startedInputsMap = HashMultimap.create();

  private TaskReporter taskReporter;
  private TezTaskUmbilicalProtocol umbilical;
  private int taskCount = 0;
  private TezVertexID lastVertexID;

  public TezChild(Configuration conf, String host, int port, String containerIdentifier,
      String tokenIdentifier, int appAttemptNumber, String[] localDirs,
      ObjectRegistryImpl objectRegistry) throws IOException, InterruptedException {
    this.defaultConf = conf;
    this.containerIdString = containerIdentifier;
    this.appAttemptNumber = appAttemptNumber;
    this.localDirs = localDirs;

    getTaskMaxSleepTime = defaultConf.getInt(
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX,
        TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT);

    amHeartbeatInterval = defaultConf.getInt(TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT);

    sendCounterInterval = defaultConf.getLong(
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS,
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT);

    maxEventsToGet = defaultConf.getInt(TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
        TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT);

    address = NetUtils.createSocketAddrForHost(host, port);

    ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("TezChild").build());
    this.executor = MoreExecutors.listeningDecorator(executor);

    this.objectRegistry = objectRegistry;

    // Security framework already loaded the tokens into current ugi
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing with tokens:");
      for (Token<?> token : credentials.getAllTokens()) {
        LOG.debug(token);
      }
    }

    UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(tokenIdentifier);
    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
    SecurityUtil.setTokenService(jobToken, address);
    taskOwner.addToken(jobToken);

    serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(jobToken));

    umbilical = taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
      @Override
      public TezTaskUmbilicalProtocol run() throws Exception {
        return (TezTaskUmbilicalProtocol) RPC.getProxy(TezTaskUmbilicalProtocol.class,
            TezTaskUmbilicalProtocol.versionID, address, defaultConf);
      }
    });
  }
  
  void run() throws IOException, InterruptedException, TezException {

    ContainerContext containerContext = new ContainerContext(containerIdString);
    ContainerReporter containerReporter = new ContainerReporter(umbilical, containerContext,
        getTaskMaxSleepTime);

    taskReporter = new TaskReporter(umbilical, amHeartbeatInterval,
        sendCounterInterval, maxEventsToGet, heartbeatCounter, containerIdString);

    UserGroupInformation childUGI = null;

    while (!executor.isTerminated()) {
      if (taskCount > 0) {
        TezUtils.updateLoggers("");
      }
      ListenableFuture<ContainerTask> getTaskFuture = executor.submit(containerReporter);
      ContainerTask containerTask = null;
      try {
        containerTask = getTaskFuture.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        handleError(cause);
        return;
      } catch (InterruptedException e) {
        LOG.info("Interrupted while waiting for task to complete:"
            + containerTask.getTaskSpec().getTaskAttemptID());
        handleError(e);
        return;
      }
      if (containerTask.shouldDie()) {
        LOG.info("ContainerTask returned shouldDie=true, Exiting");
        shutdown();
        return;
      } else {
        String loggerAddend = containerTask.getTaskSpec().getTaskAttemptID().toString();
        taskCount++;
        TezUtils.updateLoggers(loggerAddend);
        FileSystem.clearStatistics();

        childUGI = handleNewTaskCredentials(containerTask, childUGI);
        handleNewTaskLocalResources(containerTask);
        cleanupOnTaskChanged(containerTask);

        // Execute the Actual Task
        TezTaskRunner taskRunner = new TezTaskRunner(new TezConfiguration(defaultConf), childUGI,
            localDirs, containerTask.getTaskSpec(), umbilical, appAttemptNumber,
            serviceConsumerMetadata, startedInputsMap, taskReporter, executor);
        boolean shouldDie = false;
        try {
          shouldDie = !taskRunner.run();
          if (shouldDie) {
            LOG.info("Got a shouldDie notification via hearbeats. Shutting down");
            shutdown();
            return;
          }
        } catch (IOException e) {
          handleError(e);
          return;
        } catch (TezException e) {
          handleError(e);
          return;
        } finally {
          FileSystem.closeAllForUGI(childUGI);
        }
      }
    }
  }

  /**
   * Setup
   * 
   * @param containerTask
   *          the new task specification. Must be a valid task
   * @param childUGI
   *          the old UGI instance being used
   * @return
   */
  UserGroupInformation handleNewTaskCredentials(ContainerTask containerTask,
      UserGroupInformation childUGI) {
    // Re-use the UGI only if the Credentials have not changed.
    Preconditions.checkState(containerTask.shouldDie() != true);
    Preconditions.checkState(containerTask.getTaskSpec() != null);
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
    return childUGI;
  }

  /**
   * Handles any additional resources to be localized for the new task
   * 
   * @param containerTask
   * @throws IOException
   * @throws TezException
   */
  private void handleNewTaskLocalResources(ContainerTask containerTask) throws IOException,
      TezException {
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
      LOG.debug("New container task context:" + taskSpec.toString());
    }
  }

  /**
   * Cleans entries from the object registry, and resets the startedInputsMap if required
   * 
   * @param containerTask
   *          the new task specification. Must be a valid task
   */
  private void cleanupOnTaskChanged(ContainerTask containerTask) {
    Preconditions.checkState(containerTask.shouldDie() != true);
    Preconditions.checkState(containerTask.getTaskSpec() != null);
    TezVertexID newVertexID = containerTask.getTaskSpec().getTaskAttemptID().getTaskID()
        .getVertexID();
    if (lastVertexID != null) {
      if (!lastVertexID.equals(newVertexID)) {
        objectRegistry.clearCache(ObjectLifeCycle.VERTEX);
      }
      if (!lastVertexID.getDAGId().equals(newVertexID.getDAGId())) {
        objectRegistry.clearCache(ObjectLifeCycle.DAG);
        startedInputsMap = HashMultimap.create();
      }
    }
    lastVertexID = newVertexID;
  }

  private void shutdown() {
    executor.shutdownNow();
    if (taskReporter != null) {
      taskReporter.shutdown();
    }
    RPC.stopProxy(umbilical);
    DefaultMetricsSystem.shutdown();
    LogManager.shutdown();
  }

  public static void main(String[] args) throws IOException, InterruptedException, TezException {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    LOG.info("TezChild starting");

    final Configuration defaultConf = new Configuration();
    // Pull in configuration specified for the session.
    TezUtils.addUserSpecifiedTezConfiguration(defaultConf);
    UserGroupInformation.setConfiguration(defaultConf);
    Limits.setConfiguration(defaultConf);

    assert args.length == 5;
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final String containerIdentifier = args[2];
    final String tokenIdentifier = args[3];
    final int attemptNumber = Integer.parseInt(args[4]);
    final String pid = System.getenv().get("JVM_PID");
    final String[] localDirs = StringUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS
        .name()));
    LOG.info("PID, containerIdentifier:  " + pid + ", " + containerIdentifier);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Info from cmd line: AM-host: " + host + " AM-port: " + port
          + " containerIdentifier: " + containerIdentifier + " appAttemptNumber: " + attemptNumber
          + " tokenIdentifier: " + tokenIdentifier);
    }

    // Should this be part of main - Metrics and ObjectRegistry. TezTask setup should be independent
    // of this class. Leaving it here, till there's some entity representing a running JVM.
    DefaultMetricsSystem.initialize("TezTask");

    ObjectRegistryImpl objectRegistry = new ObjectRegistryImpl();
    @SuppressWarnings("unused")
    Injector injector = Guice.createInjector(new ObjectRegistryModule(objectRegistry));

    TezChild tezChild = new TezChild(defaultConf, host, port, containerIdentifier, tokenIdentifier,
        attemptNumber, localDirs, objectRegistry);

    tezChild.run();
  }

  private void handleError(Throwable t) {
    shutdown();
  }

}

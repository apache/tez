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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezLocalResource;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.RelocalizationUtils;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezClassLoader;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TezChild {

  private static final Logger LOG = LoggerFactory.getLogger(TezChild.class);

  private final Configuration defaultConf;
  private final String containerIdString;
  private final int appAttemptNumber;
  private final String[] localDirs;

  private final AtomicLong heartbeatCounter = new AtomicLong(0);

  private final int getTaskMaxSleepTime;
  private final int amHeartbeatInterval;
  private final long sendCounterInterval;
  private final int maxEventsToGet;
  private final String workingDir;

  private final ListeningExecutorService executor;
  private final ObjectRegistryImpl objectRegistry;
  private final String pid;
  private final ExecutionContext executionContext;
  private final Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
  private final Map<String, String> serviceProviderEnvMap;
  private final Credentials credentials;
  private final long memAvailable;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final String user;
  private final boolean updateSysCounters;

  private Multimap<String, String> startedInputsMap = HashMultimap.create();
  private final boolean ownUmbilical;

  private final TezTaskUmbilicalProtocol umbilical;
  private TaskReporterInterface taskReporter;
  private int taskCount = 0;
  private TezVertexID lastVertexID;
  private final HadoopShim hadoopShim;
  private final TezExecutors sharedExecutor;

  public TezChild(Configuration conf, String host, int port, String containerIdentifier,
      String tokenIdentifier, int appAttemptNumber, String workingDir, String[] localDirs,
      Map<String, String> serviceProviderEnvMap,
      ObjectRegistryImpl objectRegistry, String pid,
      ExecutionContext executionContext,
      Credentials credentials, long memAvailable, String user, TezTaskUmbilicalProtocol umbilical,
      boolean updateSysCounters, HadoopShim hadoopShim) throws IOException, InterruptedException {
    this.defaultConf = conf;
    this.containerIdString = containerIdentifier;
    this.appAttemptNumber = appAttemptNumber;
    this.localDirs = localDirs;
    this.serviceProviderEnvMap = serviceProviderEnvMap;
    this.workingDir = workingDir;
    this.pid = pid;
    this.executionContext = executionContext;
    this.credentials = credentials;
    this.memAvailable = memAvailable;
    this.user = user;
    this.updateSysCounters = updateSysCounters;
    this.hadoopShim = hadoopShim;
    this.sharedExecutor = new TezSharedExecutor(defaultConf);

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

    ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("TezChild").build());
    this.executor = MoreExecutors.listeningDecorator(executor);

    this.objectRegistry = objectRegistry;


    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing with tokens:");
      for (Token<?> token : credentials.getAllTokens()) {
        LOG.debug("",token);
      }
    }

    UserGroupInformation taskOwner = UserGroupInformation.createRemoteUser(tokenIdentifier);
    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

    String auxiliaryService = defaultConf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    serviceConsumerMetadata.put(auxiliaryService,
        TezCommonUtils.convertJobTokenToBytes(jobToken));

    if (umbilical == null) {
      final InetSocketAddress address = NetUtils.createSocketAddrForHost(host, port);
      SecurityUtil.setTokenService(jobToken, address);
      taskOwner.addToken(jobToken);
      this.umbilical = taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
        @Override
        public TezTaskUmbilicalProtocol run() throws Exception {
          return RPC.getProxy(TezTaskUmbilicalProtocol.class,
              TezTaskUmbilicalProtocol.versionID, address, defaultConf);
        }
      });
      ownUmbilical = true;
    } else {
      this.umbilical = umbilical;
      ownUmbilical = false;
    }
    TezCommonUtils.logCredentials(LOG, credentials, "tezChildInit");
  }
  
  public ContainerExecutionResult run() throws IOException, InterruptedException, TezException {

    ContainerContext containerContext = new ContainerContext(containerIdString);
    ContainerReporter containerReporter = new ContainerReporter(umbilical, containerContext,
        getTaskMaxSleepTime);

    taskReporter = new TaskReporter(umbilical, amHeartbeatInterval,
        sendCounterInterval, maxEventsToGet, heartbeatCounter, containerIdString);

    UserGroupInformation childUGI = null;

    while (!executor.isTerminated() && !isShutdown.get()) {
      if (taskCount > 0) {
        TezUtilsInternal.updateLoggers("");
      }
      ListenableFuture<ContainerTask> getTaskFuture = executor.submit(containerReporter);
      boolean error = false;
      ContainerTask containerTask = null;
      try {
        containerTask = getTaskFuture.get();
      } catch (ExecutionException e) {
        error = true;
        Throwable cause = e.getCause();
        LOG.error("Error fetching new work for container {}", containerIdString,
            cause);
        return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
            cause, "Execution Exception while fetching new work: " + e.getMessage());
      } catch (InterruptedException e) {
        error = true;
        LOG.info("Interrupted while waiting for new work for container {}", containerIdString);
        return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.INTERRUPTED, e,
            "Interrupted while waiting for new work");
      } finally {
        if (error) {
          shutdown();
        }
      }
      TezCommonUtils.logCredentials(LOG, containerTask.getCredentials(), "containerTask");
      if (containerTask.shouldDie()) {
        LOG.info("ContainerTask returned shouldDie=true for container {}, Exiting", containerIdString);
        shutdown();
        return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
            "Asked to die by the AM");
      } else {
        String loggerAddend = containerTask.getTaskSpec().getTaskAttemptID().toString();
        taskCount++;
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
        System.err.println(timeStamp + " Starting to run new task attempt: " +
            containerTask.getTaskSpec().getTaskAttemptID().toString());
        System.out.println(timeStamp + " Starting to run new task attempt: " +
            containerTask.getTaskSpec().getTaskAttemptID().toString());
        TezUtilsInternal.setHadoopCallerContext(hadoopShim,
            containerTask.getTaskSpec().getTaskAttemptID());
        TezUtilsInternal.updateLoggers(loggerAddend);
        FileSystem.clearStatistics();

        childUGI = handleNewTaskCredentials(containerTask, childUGI);
        TezCommonUtils.logCredentials(LOG, childUGI.getCredentials(), "taskChildUGI");
        handleNewTaskLocalResources(containerTask, childUGI);
        cleanupOnTaskChanged(containerTask);

        // Execute the Actual Task
        TezTaskRunner2 taskRunner = new TezTaskRunner2(defaultConf, childUGI,
            localDirs, containerTask.getTaskSpec(), appAttemptNumber,
            serviceConsumerMetadata, serviceProviderEnvMap, startedInputsMap, taskReporter,
            executor, objectRegistry, pid, executionContext, memAvailable, updateSysCounters,
            hadoopShim, sharedExecutor);
        boolean shouldDie;
        try {
          TaskRunner2Result result = taskRunner.run();
          LOG.info("TaskRunner2Result: {}", result);
          shouldDie = result.isContainerShutdownRequested();
          if (shouldDie) {
            LOG.info("Got a shouldDie notification via heartbeats for container {}. Shutting down", containerIdString);
            shutdown();
            return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
                "Asked to die by the AM");
          }
          if (result.getError() != null) {
            Throwable e = result.getError();
            handleError(result.getError());
            return new ContainerExecutionResult(
                ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
                e, "TaskExecutionFailure: " + e.getMessage());
          }
        } finally {
          FileSystem.closeAllForUGI(childUGI);
        }
      }
    }
    return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
        null);
  }

  /**
   * Setup
   * 
   * @param containerTask
   *          the new task specification. Must be a valid task
   * @param childUGI
   *          the old UGI instance being used
   * @return childUGI
   */
  UserGroupInformation handleNewTaskCredentials(ContainerTask containerTask,
      UserGroupInformation childUGI) {
    // Re-use the UGI only if the Credentials have not changed.
    Preconditions.checkState(!containerTask.shouldDie());
    Preconditions.checkState(containerTask.getTaskSpec() != null);
    if (containerTask.haveCredentialsChanged()) {
      Credentials taskCreds = containerTask.getCredentials();
      if (taskCreds != null) {
        LOG.info("Refreshing UGI since Credentials have changed. Credentials : #Tokens=" +
            taskCreds.numberOfTokens() + ", #SecretKeys="
            + taskCreds.numberOfSecretKeys());
        childUGI = UserGroupInformation.createRemoteUser(user);
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
  private void handleNewTaskLocalResources(ContainerTask containerTask,
      UserGroupInformation ugi) throws IOException, TezException {

    final Map<String, TezLocalResource> additionalResources = containerTask.getAdditionalResources();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Additional Resources added to container: " + additionalResources);
    }

    if (additionalResources != null && !additionalResources.isEmpty()) {
      LOG.info("Localizing additional local resources for Task : " + additionalResources);

      try {
        List<URL> downloadedUrls = ugi.doAs(new PrivilegedExceptionAction<List<URL>>() {
          @Override
          public List<URL> run() throws Exception {
            return RelocalizationUtils.processAdditionalResources(
                Maps.transformValues(additionalResources, new Function<TezLocalResource, URI>() {
                  @Override
                  public URI apply(TezLocalResource input) {
                    return input.getUri();
                  }
                }), defaultConf, workingDir);
          }
        });
        RelocalizationUtils.addUrlsToClassPath(downloadedUrls);
      } catch (InterruptedException e) {
        throw new TezException(e);
      }
      LOG.info("Done localizing additional resources");
    }
  }

  /**
   * Cleans entries from the object registry, and resets the startedInputsMap if required
   * 
   * @param containerTask
   *          the new task specification. Must be a valid task
   */
  private void cleanupOnTaskChanged(ContainerTask containerTask) {
    Preconditions.checkState(!containerTask.shouldDie());
    Preconditions.checkState(containerTask.getTaskSpec() != null);
    TezVertexID newVertexID = containerTask.getTaskSpec().getTaskAttemptID().getTaskID()
        .getVertexID();
    if (lastVertexID != null) {
      if (!lastVertexID.equals(newVertexID)) {
        objectRegistry.clearCache(ObjectRegistryImpl.ObjectLifeCycle.VERTEX);
      }
      if (!lastVertexID.getDAGId().equals(newVertexID.getDAGId())) {
        objectRegistry.clearCache(ObjectRegistryImpl.ObjectLifeCycle.DAG);
        startedInputsMap = HashMultimap.create();
      }
    }
    lastVertexID = newVertexID;
  }

  public void shutdown() {
    LOG.info("Shutdown invoked for container {}", containerIdString);
    if (!isShutdown.getAndSet(true)) {
      LOG.info("Shutting down container {}", containerIdString);
      // It's possible that there's pending tasks on the executor. Those should be cancelled.
      List<Runnable> pendingRunnables = executor.shutdownNow();
      for (Runnable r : pendingRunnables) {
        LOG.info("Cancelling pending runnables during TezChild shutdown for containerId={}", containerIdString);
        ((FutureTask)r).cancel(false);
      }
      if (taskReporter != null) {
        taskReporter.shutdown();
      }
      if (ownUmbilical) {
        RPC.stopProxy(umbilical);
      }
    }
  }

  public static class ContainerExecutionResult {
    public static enum ExitStatus {
      SUCCESS(0),
      EXECUTION_FAILURE(1),
      INTERRUPTED(2),
      ASKED_TO_DIE(3);

      private final int exitCode;

      ExitStatus(int code) {
        this.exitCode = code;
      }

      public int getExitCode() {
        return this.exitCode;
      }
    }

    private final ExitStatus exitStatus;
    private final Throwable throwable;
    private final String errorMessage;

    public ContainerExecutionResult(ExitStatus exitStatus, @Nullable Throwable throwable,
                             @Nullable String errorMessage) {
      this.exitStatus = exitStatus;
      this.throwable = throwable;
      this.errorMessage = errorMessage;
    }

    public ExitStatus getExitStatus() {
      return this.exitStatus;
    }

    public Throwable getThrowable() {
      return this.throwable;
    }

    public String getErrorMessage() {
      return this.errorMessage;
    }

    @Override
    public String toString() {
      return "ContainerExecutionResult{" +
          "exitStatus=" + exitStatus +
          ", throwable=" + throwable +
          ", errorMessage='" + errorMessage + '\'' +
          '}';
    }
  }

  public static TezChild newTezChild(Configuration conf, String host, int port, String containerIdentifier,
      String tokenIdentifier, int attemptNumber, String[] localDirs, String workingDirectory,
      Map<String, String> serviceProviderEnvMap, @Nullable String pid,
      ExecutionContext executionContext, Credentials credentials, long memAvailable, String user,
      TezTaskUmbilicalProtocol tezUmbilical, boolean updateSysCounters, HadoopShim hadoopShim)
      throws IOException, InterruptedException, TezException {

    // Pull in configuration specified for the session.
    // TODO TEZ-1233. This needs to be moved over the wire rather than localizing the file
    // for each and every task, and reading it back from disk. Also needs to be per vertex.
    Limits.setConfiguration(conf);

    TezUtilsInternal.setSecurityUtilConfigration(LOG, conf);

    // singleton of ObjectRegistry for this JVM
    ObjectRegistryImpl objectRegistry = new ObjectRegistryImpl();

    return new TezChild(conf, host, port, containerIdentifier, tokenIdentifier,
        attemptNumber, workingDirectory, localDirs, serviceProviderEnvMap, objectRegistry, pid,
        executionContext, credentials, memAvailable, user, tezUmbilical, updateSysCounters,
        hadoopShim);
  }

  public static void main(String[] args) throws IOException, InterruptedException, TezException {
    TezClassLoader.setupTezClassLoader();
    final Configuration defaultConf = new Configuration();

    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    final String pid = System.getenv().get("JVM_PID");


    assert args.length == 5;
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final String containerIdentifier = args[2];
    final String tokenIdentifier = args[3];
    final int attemptNumber = Integer.parseInt(args[4]);
    final String[] localDirs = TezCommonUtils.getTrimmedStrings(System.getenv(Environment.LOCAL_DIRS
        .name()));
    LOG.info("TezChild starting with PID=" + pid + ", containerIdentifier=" + containerIdentifier);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Info from cmd line: AM-host: " + host + " AM-port: " + port
          + " containerIdentifier: " + containerIdentifier + " appAttemptNumber: " + attemptNumber
          + " tokenIdentifier: " + tokenIdentifier);
    }

    // Security framework already loaded the tokens into current ugi
    DAGProtos.ConfigurationProto confProto =
        TezUtilsInternal.readUserSpecifiedTezConfiguration(System.getenv(Environment.PWD.name()));
    TezUtilsInternal.addUserSpecifiedTezConfiguration(defaultConf, confProto.getConfKeyValuesList());
    UserGroupInformation.setConfiguration(defaultConf);
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

    HadoopShim hadoopShim = new HadoopShimsLoader(defaultConf).getHadoopShim();

    // log the system properties
    if (LOG.isInfoEnabled()) {
      String systemPropsToLog = TezCommonUtils.getSystemPropertiesToLog(defaultConf);
      if (systemPropsToLog != null) {
        LOG.info(systemPropsToLog);
      }
    }

    TezChild tezChild = newTezChild(defaultConf, host, port, containerIdentifier,
        tokenIdentifier, attemptNumber, localDirs, System.getenv(Environment.PWD.name()),
        System.getenv(), pid, new ExecutionContextImpl(System.getenv(Environment.NM_HOST.name())),
        credentials, Runtime.getRuntime().maxMemory(), System
            .getenv(ApplicationConstants.Environment.USER.toString()), null, true, hadoopShim);
    tezChild.run();
  }

  private void handleError(Throwable t) {
    shutdown();
  }

}

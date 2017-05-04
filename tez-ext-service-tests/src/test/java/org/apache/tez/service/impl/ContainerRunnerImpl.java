/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.service.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.task.TaskReporter;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.apache.tez.runtime.task.TezTaskRunner2;
import org.apache.tez.service.ContainerRunner;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.task.TezChild;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult;
import org.apache.tez.shufflehandler.ShuffleHandler;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.SubmitWorkRequestProto;
import org.apache.tez.util.ProtoConverters;
import org.apache.tez.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerRunnerImpl extends AbstractService implements ContainerRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerRunnerImpl.class);

  public static final String DAG_NAME_INSTRUMENTED_FAILURES = "InstrumentedFailures";

  private final ListeningExecutorService executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final String[] localDirsBase;
  private final Map<String, String> localEnv = new HashMap<String, String>();
  private volatile FileSystem localFs;
  private final long memoryPerExecutor;

  private final TezExecutors sharedExecutor;
  // TODO Support for removing queued containers, interrupting / killing specific containers - when preemption is supported




  public ContainerRunnerImpl(int numExecutors, String[] localDirsBase,
                             AtomicReference<InetSocketAddress> localAddress,
                             long totalMemoryAvailableBytes,
                             TezExecutors sharedExecutor) {
    super("ContainerRunnerImpl");
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.localDirsBase = localDirsBase;
    this.localAddress = localAddress;

    ExecutorService raw = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setNameFormat("ContainerExecutor %d").build());
    this.executorService = MoreExecutors.listeningDecorator(raw);


    // 80% of memory considered for accounted buffers. Rest for objects.
    // TODO Tune this based on the available size.
    this.memoryPerExecutor = (long)(totalMemoryAvailableBytes * 0.8 / (float) numExecutors);

    LOG.info("ContainerRunnerImpl config: " +
        "memoryPerExecutorDerived=" + memoryPerExecutor +
        ", numExecutors=" + numExecutors
    );
    this.sharedExecutor = sharedExecutor;
  }

  @Override
  public void serviceInit(Configuration conf) {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
  }

  @Override
  public void serviceStart() {
  }

  public void setShufflePort(String auxiliaryService, int shufflePort) {
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        auxiliaryService,
        ByteBuffer.allocate(4).putInt(shufflePort), localEnv);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  // TODO Move this into a utilities class
  private static String createAppSpecificLocalDir(String baseDir, String applicationIdString,
                                                  String user) {
    return baseDir + File.separator + "usercache" + File.separator + user + File.separator +
        "appcache" + File.separator + applicationIdString;
  }

  /**
   * Submit a container which is ready for running.
   * The regular pull mechanism will be used to fetch work from the AM
   * @param request
   * @throws TezException
   */
  @Override
  public void queueContainer(RunContainerRequestProto request) throws TezException {
    LOG.info("Queuing container for execution: " + request);

    Map<String, String> env = new HashMap<String, String>();
    env.putAll(localEnv);
    env.put(ApplicationConstants.Environment.USER.name(), request.getUser());

    String[] localDirs = new String[localDirsBase.length];

    // Setup up local dirs to be application specific, and create them.
    for (int i = 0; i < localDirsBase.length; i++) {
      localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], request.getApplicationIdString(),
          request.getUser());
      try {
        localFs.mkdirs(new Path(localDirs[i]));
      } catch (IOException e) {
        throw new TezException(e);
      }
    }
    LOG.info("Dirs for {} are {}", request.getContainerIdString(), Arrays.toString(localDirs));


    // Setup workingDir. This is otherwise setup as Environment.PWD
    // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)
    String workingDir = localDirs[0];

    Credentials credentials = new Credentials();
    DataInputBuffer dib = new DataInputBuffer();
    byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
    dib.reset(tokenBytes, tokenBytes.length);
    try {
      credentials.readTokenStorageStream(dib);
    } catch (IOException e) {
      throw new TezException(e);
    }

    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

    // TODO Unregistering does not happen at the moment, since there's no signals on when an app completes.
    LOG.info("Registering request with the ShuffleHandler for containerId {}", request.getContainerIdString());
    ShuffleHandler.get().registerApplication(request.getApplicationIdString(), jobToken, request.getUser());


    ContainerRunnerCallable callable = new ContainerRunnerCallable(request, new Configuration(getConfig()),
        new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
        workingDir, credentials, memoryPerExecutor);
    ListenableFuture<ContainerExecutionResult> future = executorService
        .submit(callable);
    Futures.addCallback(future, new ContainerRunnerCallback(request, callable));
  }

  /**
   * Submit an entire work unit - containerId + TaskSpec.
   * This is intended for a task push from the AM
   *
   * @param request
   * @throws org.apache.tez.dag.api.TezException
   */
  @Override
  public void submitWork(SubmitWorkRequestProto request) throws TezException {
    LOG.info("Queuing work for execution: " + request);

    checkAndThrowExceptionForTests(request);

    Map<String, String> env = new HashMap<String, String>();
    env.putAll(localEnv);
    env.put(ApplicationConstants.Environment.USER.name(), request.getUser());

    String[] localDirs = new String[localDirsBase.length];

    // Setup up local dirs to be application specific, and create them.
    for (int i = 0; i < localDirsBase.length; i++) {
      localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], request.getApplicationIdString(),
          request.getUser());
      try {
        localFs.mkdirs(new Path(localDirs[i]));
      } catch (IOException e) {
        throw new TezException(e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dirs are: " + Arrays.toString(localDirs));
    }

    // Setup workingDir. This is otherwise setup as Environment.PWD
    // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)
    String workingDir = localDirs[0];

    Credentials credentials = new Credentials();
    DataInputBuffer dib = new DataInputBuffer();
    byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
    dib.reset(tokenBytes, tokenBytes.length);
    try {
      credentials.readTokenStorageStream(dib);
    } catch (IOException e) {
      throw new TezException(e);
    }

    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

    // TODO Unregistering does not happen at the moment, since there's no signals on when an app completes.
    LOG.info("Registering request with the ShuffleHandler for containerId {}", request.getContainerIdString());
    ShuffleHandler.get().registerApplication(request.getApplicationIdString(), jobToken, request.getUser());
    TaskRunnerCallable callable = new TaskRunnerCallable(request, new Configuration(getConfig()),
        new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
        workingDir, credentials, memoryPerExecutor, sharedExecutor);
    ListenableFuture<ContainerExecutionResult> future = executorService.submit(callable);
    Futures.addCallback(future, new TaskRunnerCallback(request, callable));
  }


  static class ContainerRunnerCallable implements Callable<ContainerExecutionResult> {

    private final RunContainerRequestProto request;
    private final Configuration conf;
    private final String workingDir;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;
    private final Credentials credentials;
    private final long memoryAvailable;
    private volatile TezChild tezChild;


    ContainerRunnerCallable(RunContainerRequestProto request, Configuration conf,
                            ExecutionContext executionContext, Map<String, String> envMap,
                            String[] localDirs, String workingDir, Credentials credentials,
                            long memoryAvailable) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.workingDir = workingDir;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
      this.credentials = credentials;
      this.memoryAvailable = memoryAvailable;

    }

    @Override
    public ContainerExecutionResult call() throws Exception {
      StopWatch sw = new StopWatch().start();
      tezChild =
          new TezChild(conf, request.getAmHost(), request.getAmPort(),
              request.getContainerIdString(),
              request.getTokenIdentifier(), request.getAppAttemptNumber(), workingDir, localDirs,
              envMap, objectRegistry, pid,
              executionContext, credentials, memoryAvailable, request.getUser(), null, false,
              new DefaultHadoopShim());
      ContainerExecutionResult result = tezChild.run();
      LOG.info("ExecutionTime for Container: " + request.getContainerIdString() + "=" +
          sw.stop().now(TimeUnit.MILLISECONDS));
      return result;
    }

    public TezChild getTezChild() {
      return this.tezChild;
    }
  }


  final class ContainerRunnerCallback implements FutureCallback<ContainerExecutionResult> {

    private final RunContainerRequestProto request;
    private final ContainerRunnerCallable containerRunnerCallable;

    ContainerRunnerCallback(RunContainerRequestProto request,
                            ContainerRunnerCallable containerRunnerCallable) {
      this.request = request;
      this.containerRunnerCallable = containerRunnerCallable;
    }

    // TODO Proper error handling
    @Override
    public void onSuccess(ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString());
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString(), result.getThrowable());
          break;
        case INTERRUPTED:
          LOG.info(
              "Interrupted while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString(), result.getThrowable());
          break;
        case ASKED_TO_DIE:
          LOG.info(
              "Asked to die while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString());
          break;
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error(
          "TezChild execution failed for : " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString(), t);
      TezChild tezChild = containerRunnerCallable.getTezChild();
      if (tezChild != null) {
        tezChild.shutdown();
      }
    }
  }

  static class TaskRunnerCallable implements Callable<ContainerExecutionResult> {

    private final SubmitWorkRequestProto request;
    private final Configuration conf;
    private final String workingDir;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;
    private final Credentials credentials;
    private final long memoryAvailable;
    private final ListeningExecutorService executor;
    private volatile TezTaskRunner2 taskRunner;
    private volatile TaskReporter taskReporter;
    private TezTaskUmbilicalProtocol umbilical;
    private final TezExecutors sharedExecutor;


    TaskRunnerCallable(SubmitWorkRequestProto request, Configuration conf,
                             ExecutionContext executionContext, Map<String, String> envMap,
                             String[] localDirs, String workingDir, Credentials credentials,
                             long memoryAvailable, TezExecutors sharedExecutor) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.workingDir = workingDir;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
      this.credentials = credentials;
      this.memoryAvailable = memoryAvailable;
      // TODO This executor seems unnecessary. Here and TezChild
      ExecutorService executorReal = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("TezTaskRunner_" + request.getTaskSpec().getTaskAttemptIdString()).build());
      executor = MoreExecutors.listeningDecorator(executorReal);
      this.sharedExecutor = sharedExecutor;
    }

    @Override
    public ContainerExecutionResult call() throws Exception {

      // TODO Consolidate this code with TezChild.
      StopWatch sw = new StopWatch().start();
      UserGroupInformation taskUgi = UserGroupInformation.createRemoteUser(request.getUser());
      taskUgi.addCredentials(credentials);

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
      Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
      String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      serviceConsumerMetadata.put(auxiliaryService,
          TezCommonUtils.convertJobTokenToBytes(jobToken));
      Multimap<String, String> startedInputsMap = HashMultimap.create();

      UserGroupInformation taskOwner =
          UserGroupInformation.createRemoteUser(request.getTokenIdentifier());
      final InetSocketAddress address =
          NetUtils.createSocketAddrForHost(request.getAmHost(), request.getAmPort());
      SecurityUtil.setTokenService(jobToken, address);
      taskOwner.addToken(jobToken);
      umbilical = taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
        @Override
        public TezTaskUmbilicalProtocol run() throws Exception {
          return RPC.getProxy(TezTaskUmbilicalProtocol.class,
              TezTaskUmbilicalProtocol.versionID, address, conf);
        }
      });
      // TODO Stop reading this on each request.
      taskReporter = new TaskReporter(
          umbilical,
          conf.getInt(TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
              TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT),
          conf.getLong(
              TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS,
              TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT),
          conf.getInt(TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
              TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT),
          new AtomicLong(0),
          request.getContainerIdString());

      taskRunner = new TezTaskRunner2(conf, taskUgi, localDirs,
          ProtoConverters.getTaskSpecfromProto(request.getTaskSpec()),
          request.getAppAttemptNumber(),
          serviceConsumerMetadata, envMap, startedInputsMap, taskReporter, executor, objectRegistry,
          pid,
          executionContext, memoryAvailable, false, new DefaultHadoopShim(), sharedExecutor);

      boolean shouldDie;
      try {
        TaskRunner2Result result = taskRunner.run();
        LOG.info("TaskRunner2Result: {}", result);
        shouldDie = result.isContainerShutdownRequested();
        if (shouldDie) {
          LOG.info("Got a shouldDie notification via heartbeats. Shutting down");
          return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
              "Asked to die by the AM");
        }
        if (result.getError() != null) {
          Throwable e = result.getError();
          return new ContainerExecutionResult(
              ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
              e, "TaskExecutionFailure: " + e.getMessage());
        }
      } finally {
        FileSystem.closeAllForUGI(taskUgi);
      }
      LOG.info("ExecutionTime for Container: " + request.getContainerIdString() + "=" +
          sw.stop().now(TimeUnit.MILLISECONDS));
      return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
          null);
    }

    public void shutdown() {
      executor.shutdownNow();
      if (taskReporter != null) {
        taskReporter.shutdown();
      }
      if (umbilical != null) {
        RPC.stopProxy(umbilical);
      }
    }
  }


  final class TaskRunnerCallback implements FutureCallback<ContainerExecutionResult> {

    private final SubmitWorkRequestProto request;
    private final TaskRunnerCallable taskRunnerCallable;

    TaskRunnerCallback(SubmitWorkRequestProto request,
                            TaskRunnerCallable containerRunnerCallable) {
      this.request = request;
      this.taskRunnerCallable = containerRunnerCallable;
    }

    // TODO Proper error handling
    @Override
    public void onSuccess(ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString());
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString(), result.getThrowable());
          break;
        case INTERRUPTED:
          LOG.info(
              "Interrupted while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString(), result.getThrowable());
          break;
        case ASKED_TO_DIE:
          LOG.info(
              "Asked to die while running: " + request.getApplicationIdString() + ", containerId=" +
                  request.getContainerIdString());
          break;
      }
      taskRunnerCallable.shutdown();
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error(
          "TezTaskRunner execution failed for : " + request.getApplicationIdString() + ", containerId=" +
              request.getContainerIdString(), t);
      taskRunnerCallable.shutdown();
    }
  }


  private void checkAndThrowExceptionForTests(SubmitWorkRequestProto request) throws TezException {
    if (!request.getTaskSpec().getDagName().equals(DAG_NAME_INSTRUMENTED_FAILURES)) {
      return;
    }

    TaskSpec taskSpec = ProtoConverters.getTaskSpecfromProto(request.getTaskSpec());
    if (taskSpec.getTaskAttemptID().getTaskID().getId() == 0 &&
        taskSpec.getTaskAttemptID().getId() == 0) {
      LOG.info("Simulating Rejected work");
      throw new RejectedExecutionException(
          "Simulating Rejected work for taskAttemptId=" + taskSpec.getTaskAttemptID());
    } else if (taskSpec.getTaskAttemptID().getTaskID().getId() == 1 &&
        taskSpec.getTaskAttemptID().getId() == 0) {
      LOG.info("Simulating Task Setup Failure during launch");
      throw new TezException("Simulating Task Setup Failure during launch for taskAttemptId=" +
          taskSpec.getTaskAttemptID());
    }
  }
}

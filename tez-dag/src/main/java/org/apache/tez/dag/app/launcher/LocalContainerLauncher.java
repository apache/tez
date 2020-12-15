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

package org.apache.tez.dag.app.launcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.tez.common.DagContainerLauncher;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TezTaskCommunicatorImpl;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.task.TezChild;


/**
 * Runs the container task locally in a thread.
 * Since all (sub)tasks share the same local directory, they must be executed
 * sequentially in order to avoid creating/deleting the same files/dirs.
 */
public class LocalContainerLauncher extends DagContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(LocalContainerLauncher.class);

  private final AppContext context;
  private final AtomicBoolean serviceStopped = new AtomicBoolean(false);
  private final String workingDirectory;
  private final TaskCommunicatorManagerInterface tal;
  private final Map<String, String> localEnv;
  private final ExecutionContext executionContext;
  private final int numExecutors;
  private final boolean isLocalMode;
  int shufflePort = TezRuntimeUtils.INVALID_PORT;
  private DeletionTracker deletionTracker;

  private final ConcurrentHashMap<ContainerId, ListenableFuture<?>>
      runningContainers =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<ContainerId, TezLocalCacheManager>
          cacheManagers = new ConcurrentHashMap<>();

  private final ExecutorService callbackExecutor = Executors.newFixedThreadPool(1,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CallbackExecutor").build());

  private BlockingQueue<ContainerOp> eventQueue = new LinkedBlockingQueue<>();
  private Thread eventHandlingThread;


  private ListeningExecutorService taskExecutorService;


  public LocalContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                AppContext context,
                                TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                String workingDirectory,
                                boolean isLocalMode) throws UnknownHostException, TezException {
    // TODO Post TEZ-2003. Most of this information is dynamic and only available after the AM
    // starts up. It's not possible to set these up via a static payload.
    // Will need some kind of mechanism to dynamically crate payloads / bind to parameters
    // after the AM starts up.
    super(containerLauncherContext);
    this.context = context;
    this.tal = taskCommunicatorManagerInterface;
    this.workingDirectory = workingDirectory;
    this.isLocalMode = isLocalMode;

    // Check if the hostname is set in the environment before overriding it.
    String host = isLocalMode ? InetAddress.getLocalHost().getHostName() :
        System.getenv(Environment.NM_HOST.name());
    executionContext = new ExecutionContextImpl(host);

    Configuration conf;
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(
          "Failed to parse user payload for " + LocalContainerLauncher.class.getSimpleName(), e);
    }
    if (isLocalMode) {
      String auxiliaryService = conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
          TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
      localEnv = Maps.newHashMap();
      shufflePort = 0;
      AuxiliaryServiceHelper.setServiceDataIntoEnv(
          auxiliaryService, ByteBuffer.allocate(4).putInt(shufflePort), localEnv);
    } else {
      localEnv = System.getenv();
    }
    numExecutors = conf.getInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS,
        TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT);
    Preconditions.checkState(numExecutors >=1, "Must have at least 1 executor");
    ExecutorService rawExecutor = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LocalTaskExecutionThread #%d")
            .build());
    this.taskExecutorService = MoreExecutors.listeningDecorator(rawExecutor);
    boolean cleanupDagDataOnComplete = ShuffleUtils.isTezShuffleHandler(conf)
        && conf.getBoolean(TezConfiguration.TEZ_AM_DAG_CLEANUP_ON_COMPLETION,
        TezConfiguration.TEZ_AM_DAG_CLEANUP_ON_COMPLETION_DEFAULT);
    if (cleanupDagDataOnComplete) {
      String deletionTrackerClassName = conf.get(TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS,
          TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS_DEFAULT);
      deletionTracker = ReflectionUtils.createClazzInstance(
          deletionTrackerClassName, new Class[]{Configuration.class}, new Object[]{conf});
    }
  }

  @Override
  public void start() throws Exception {
    eventHandlingThread =
        new Thread(new TezSubTaskRunner(), "LocalContainerLauncher-SubTaskRunner");
    eventHandlingThread.start();
  }

  @Override
  public void shutdown() throws Exception {
    if (!serviceStopped.compareAndSet(false, true)) {
      LOG.info("Service Already stopped. Ignoring additional stop");
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      eventHandlingThread.join(2000l);
    }
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
    callbackExecutor.shutdownNow();
    if (deletionTracker != null) {
      deletionTracker.shutdown();
    }
  }



  // Thread to monitor the queue of incoming NMCommunicator events
  private class TezSubTaskRunner implements Runnable {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted() && !serviceStopped.get()) {
        ContainerOp event;
        try {
          event = eventQueue.take();
          switch (event.getOpType()) {
            case LAUNCH_REQUEST:
              launch(event.getLaunchRequest());
              break;
            case STOP_REQUEST:
              stop(event.getStopRequest());
              break;
          }
        } catch (InterruptedException e) {
          if (!serviceStopped.get()) {
            LOG.error("TezSubTaskRunner interrupted ", e);
          }
          return;
        } catch (Throwable e) {
          LOG.error("TezSubTaskRunner failed due to exception", e);
          throw e;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId, String message) {
    getContext().containerLaunchFailed(containerId, message);
  }

  private void handleLaunchFailed(Throwable t, ContainerId containerId) {
    String message;

    // clean up distributed cache files
    cleanupCacheFiles(containerId);

    if (t instanceof RejectedExecutionException) {
      message = "Failed to queue container launch for container Id: " + containerId;
    } else {
      message = "Failed to launch container for container Id: " + containerId;
    }
    LOG.error(message, t);
    sendContainerLaunchFailedMsg(containerId, message);
  }

  //launch tasks
  private void launch(ContainerLaunchRequest event) {

    String tokenIdentifier = context.getApplicationID().toString();
    try {
      TezChild tezChild;

      try {
        int taskCommId = context.getTaskCommunicatorIdentifier(event.getTaskCommunicatorName());

        Configuration conf = context.getAMConf();
        if (isLocalMode) {
          TezLocalCacheManager cacheManager = new TezLocalCacheManager(
              event.getContainerLaunchContext().getLocalResources(),
              conf
          );
          cacheManagers.put(event.getContainerId(), cacheManager);
          cacheManager.localize();
        }

        tezChild =
            createTezChild(conf, event.getContainerId(), tokenIdentifier,
                context.getApplicationAttemptId().getAttemptId(), context.getLocalDirs(),
                ((TezTaskCommunicatorImpl)tal.getTaskCommunicator(taskCommId).getTaskCommunicator()).getUmbilical(),
                TezCommonUtils.parseCredentialsBytes(event.getContainerLaunchContext().getTokens().array()));
      } catch (InterruptedException e) {
        handleLaunchFailed(e, event.getContainerId());
        return;
      } catch (TezException e) {
        handleLaunchFailed(e, event.getContainerId());
        return;
      } catch (IOException e) {
        handleLaunchFailed(e, event.getContainerId());
        return;
      }
      ListenableFuture<TezChild.ContainerExecutionResult> runningTaskFuture =
          taskExecutorService.submit(createSubTask(tezChild, event.getContainerId()));
      RunningTaskCallback callback = new RunningTaskCallback(event.getContainerId());
      runningContainers.put(event.getContainerId(), runningTaskFuture);
      Futures.addCallback(runningTaskFuture, callback, callbackExecutor);
      if (deletionTracker != null) {
        deletionTracker.addNodeShufflePort(event.getNodeId(), shufflePort);
      }
    } catch (RejectedExecutionException e) {
      handleLaunchFailed(e, event.getContainerId());
    }
  }

  private void stop(ContainerStopRequest event) {
    // A stop_request will come in when a task completes and reports back or a preemption decision
    // is made.
    ListenableFuture future =
        runningContainers.get(event.getContainerId());
    if (future == null) {
      LOG.info("Ignoring stop request for containerId: " + event.getContainerId());
    } else {
      LOG.info("Stopping containerId: {}, isDone: {}", event.getContainerId(),
          future.isDone());
      future.cancel(false);
      LOG.debug("Stopped containerId: {}, isCancelled: {}", event.getContainerId(),
          future.isCancelled());
    }
    // Send this event to maintain regular control flow. This isn't of much use though.
    getContext().containerStopRequested(event.getContainerId());
  }

  private class RunningTaskCallback
      implements FutureCallback<TezChild.ContainerExecutionResult> {

    private final ContainerId containerId;

    RunningTaskCallback(ContainerId containerId) {
      this.containerId = containerId;
    }

    @Override
    public void onSuccess(TezChild.ContainerExecutionResult result) {
      runningContainers.remove(containerId);
      LOG.info("ContainerExecutionResult for: " + containerId + " = " + result);
      if (result.getExitStatus() == TezChild.ContainerExecutionResult.ExitStatus.SUCCESS ||
          result.getExitStatus() ==
              TezChild.ContainerExecutionResult.ExitStatus.ASKED_TO_DIE) {
        LOG.info("Container: " + containerId + " completed successfully");
        getContext()
            .containerCompleted(containerId, result.getExitStatus().getExitCode(), null,
                TaskAttemptEndReason.CONTAINER_EXITED);
      } else {
        LOG.info("Container: " + containerId + " completed but with errors");
        getContext().containerCompleted(
            containerId, result.getExitStatus().getExitCode(),
            result.getErrorMessage() == null ?
                (result.getThrowable() == null ? null : result.getThrowable().getMessage()) :
                result.getErrorMessage(), TaskAttemptEndReason.APPLICATION_ERROR);
      }

      // clean up distributed cache files
      cleanupCacheFiles(containerId);
    }

    @Override
    public void onFailure(Throwable t) {
      runningContainers.remove(containerId);
      // Ignore CancellationException since that is triggered by the LocalContainerLauncher itself
      // TezChild would have exited by this time. There's no need to invoke shutdown again.
      if (!(t instanceof CancellationException)) {
        LOG.info("Container: " + containerId + ": Execution Failed: ", t);
        // Inform of failure with exit code 1.
        getContext().containerCompleted(containerId,
            TezChild.ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE.getExitCode(),
            t.getMessage(), TaskAttemptEndReason.APPLICATION_ERROR);
      } else {
        LOG.info("Ignoring CancellationException - triggered by LocalContainerLauncher");
        getContext().containerCompleted(containerId,
            TezChild.ContainerExecutionResult.ExitStatus.SUCCESS.getExitCode(),
            "CancellationException", TaskAttemptEndReason.COMMUNICATION_ERROR.CONTAINER_EXITED);
      }

      // clean up distributed cache files
      cleanupCacheFiles(containerId);
    }
  }

  private void cleanupCacheFiles(ContainerId container) {
    if (isLocalMode) {
      TezLocalCacheManager manager = cacheManagers.remove(container);
      try {
        if (manager != null) {
          manager.cleanup();
        }
      } catch (IOException e) {
        LOG.info("Unable to clean up local cache files: ", e);
      }
    }
  }


  //create a SubTask
  private synchronized Callable<TezChild.ContainerExecutionResult> createSubTask(
      final TezChild tezChild, final ContainerId containerId) {

    return new Callable<TezChild.ContainerExecutionResult>() {
      @Override
      public TezChild.ContainerExecutionResult call() throws InterruptedException, TezException,
          IOException {
        // Reset the interrupt status. Ideally the thread should not be in an interrupted state.
        // TezTaskRunner needs to be fixed to ensure this.
        Thread.interrupted();
        // Inform about the launch request now that the container has been allocated a thread to execute in.
        getContext().containerLaunched(containerId);
        return tezChild.run();
      }
    };
  }

  private TezChild createTezChild(Configuration defaultConf, ContainerId containerId,
                                  String tokenIdentifier, int attemptNumber, String[] localDirs,
                                  TezTaskUmbilicalProtocol tezTaskUmbilicalProtocol,
                                  Credentials credentials) throws
      InterruptedException, TezException, IOException {
    Map<String, String> containerEnv = new HashMap<String, String>();
    containerEnv.putAll(localEnv);
    // Use the user from env if it's available.
    String user = isLocalMode ? System.getenv(Environment.USER.name()) : context.getUser();
    containerEnv.put(Environment.USER.name(), user);

    long memAvailable;
    synchronized (this) { // needed to fix findbugs Inconsistent synchronization warning
      memAvailable = Runtime.getRuntime().maxMemory() / numExecutors;
    }
    TezChild tezChild =
        TezChild.newTezChild(defaultConf, null, 0, containerId.toString(), tokenIdentifier,
            attemptNumber, localDirs, workingDirectory, containerEnv, "", executionContext, credentials,
            memAvailable, context.getUser(), tezTaskUmbilicalProtocol, false,
            context.getHadoopShim());
    return tezChild;
  }


  @Override
  public void launchContainer(ContainerLaunchRequest launchRequest) {
    try {
      eventQueue.put(new ContainerOp(ContainerOp.OPType.LAUNCH_REQUEST, launchRequest));
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void stopContainer(ContainerStopRequest stopRequest) {
    try {
      eventQueue.put(new ContainerOp(ContainerOp.OPType.STOP_REQUEST, stopRequest));
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void dagComplete(TezDAGID dag, JobTokenSecretManager jobTokenSecretManager) {
    if (deletionTracker != null) {
      deletionTracker.dagComplete(dag, jobTokenSecretManager);
    }
  }

}

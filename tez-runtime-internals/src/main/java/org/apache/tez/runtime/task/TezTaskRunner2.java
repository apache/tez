/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.task.TaskRunner2Callable.TaskRunner2CallableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTaskRunner2 {

  // Behaviour changes as compared to TezTaskRunner
  // - Exception not thrown. Instead returned in the result.
  // - The actual exception is part of the result, instead of requiring a getCause().

  private static final Logger LOG = LoggerFactory.getLogger(TezTaskRunner2.class);

  @VisibleForTesting
  final LogicalIOProcessorRuntimeTask task;
  private final UserGroupInformation ugi;

  private final TaskReporterInterface taskReporter;
  private final ExecutorService executor;
  private final UmbilicalAndErrorHandler umbilicalAndErrorHandler;

  // TODO It may be easier to model this as a state machine.

  // Indicates whether a kill has been requested.
  private final AtomicBoolean killTaskRequested = new AtomicBoolean(false);

  // Indicates whether a stop container has been requested.
  private final AtomicBoolean stopContainerRequested = new AtomicBoolean(false);

  // Indicates whether the task is complete.
  private final AtomicBoolean taskComplete = new AtomicBoolean(false);

  // Separate flag from firstException, since an error can be reported without an exception.
  private final AtomicBoolean errorSeen = new AtomicBoolean(false);

  private volatile EndReason firstEndReason = null;

  // The first exception which caused the task to fail. This could come in from the
  // TaskRunnerCallable, a failure to heartbeat, or a signalFatalError on the context.
  private volatile Throwable firstException;
  private volatile EventMetaData exceptionSourceInfo;
  private volatile TaskFailureType firstTaskFailureType;
  private final AtomicBoolean errorReporterToAm = new AtomicBoolean(false);

  private volatile boolean oobSignalErrorInProgress = false;
  private final Lock oobSignalLock = new ReentrantLock();
  private final Condition oobSignalCondition = oobSignalLock.newCondition();

  private volatile long taskKillStartTime  = 0;
  final Configuration taskConf;

  private final HadoopShim hadoopShim;

  // The callable which is being used to execute the task.
  private volatile TaskRunner2Callable taskRunnerCallable;

  // This instance is set only if the runner was not configured explicity and will be shutdown
  // when this task is finished.
  private final TezSharedExecutor localExecutor;

  @Deprecated
  public TezTaskRunner2(Configuration tezConf, UserGroupInformation ugi, String[] localDirs,
      TaskSpec taskSpec, int appAttemptNumber,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Map<String, String> serviceProviderEnvMap,
      Multimap<String, String> startedInputsMap,
      TaskReporterInterface taskReporter, ExecutorService executor,
      ObjectRegistry objectRegistry, String pid,
      ExecutionContext executionContext, long memAvailable,
      boolean updateSysCounters, HadoopShim hadoopShim) throws IOException {
    this(tezConf, ugi, localDirs, taskSpec, appAttemptNumber, serviceConsumerMetadata,
        serviceProviderEnvMap, startedInputsMap, taskReporter, executor, objectRegistry,
        pid, executionContext, memAvailable, updateSysCounters, hadoopShim, null);
  }

  public TezTaskRunner2(Configuration tezConf, UserGroupInformation ugi, String[] localDirs,
                        TaskSpec taskSpec, int appAttemptNumber,
                        Map<String, ByteBuffer> serviceConsumerMetadata,
                        Map<String, String> serviceProviderEnvMap,
                        Multimap<String, String> startedInputsMap,
                        TaskReporterInterface taskReporter, ExecutorService executor,
                        ObjectRegistry objectRegistry, String pid,
                        ExecutionContext executionContext, long memAvailable,
                        boolean updateSysCounters, HadoopShim hadoopShim,
                        TezExecutors sharedExecutor) throws
      IOException {
    this.ugi = ugi;
    this.taskReporter = taskReporter;
    this.executor = executor;
    this.umbilicalAndErrorHandler = new UmbilicalAndErrorHandler();
    this.hadoopShim = hadoopShim;
    this.taskConf = new Configuration(tezConf);
    if (taskSpec.getTaskConf() != null) {
      Iterator<Entry<String, String>> iter = taskSpec.getTaskConf().iterator();
      while (iter.hasNext()) {
        Entry<String, String> entry = iter.next();
        taskConf.set(entry.getKey(), entry.getValue());
      }
    }
    localExecutor = sharedExecutor == null ? new TezSharedExecutor(tezConf) : null;
    this.task = new LogicalIOProcessorRuntimeTask(taskSpec, appAttemptNumber, taskConf, localDirs,
        umbilicalAndErrorHandler, serviceConsumerMetadata, serviceProviderEnvMap, startedInputsMap,
        objectRegistry, pid, executionContext, memAvailable, updateSysCounters, hadoopShim,
        sharedExecutor == null ? localExecutor : sharedExecutor);
  }

  /**
   * Throws an exception only when there was a communication error reported by
   * the TaskReporter.
   *
   * Otherwise, this takes care of all communication with the AM for a a running task - which
   * includes informing the AM about Failures and Success.
   *
   * If a kill request is made to the task, it will not communicate this information to
   * the AM - since a task KILL is an external event, and whoever invoked it should
   * be able to track it.
   *
   * @return the taskRunner result
   */
  public TaskRunner2Result run() {
    try {
      Future<TaskRunner2CallableResult> future = null;
      synchronized (this) {
        // All running state changes must be made within a synchronized block to ensure
        // kills are issued or the task is not setup.
        if (isRunningState()) {
          // Safe to do this within a synchronized block because we're providing
          // the handler on which the Reporter will communicate back. Assuming
          // the register call doesn't end up hanging.
          taskRunnerCallable = new TaskRunner2Callable(task, ugi);
          taskReporter.registerTask(task, umbilicalAndErrorHandler);
          future = executor.submit(taskRunnerCallable);
        }
      }

      if (future == null) {
        return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
      }

      TaskRunner2CallableResult executionResult = null;
      // The task started. Wait for it to complete.
      try {
        executionResult = future.get();
      } catch (Throwable e) {
        if (e instanceof ExecutionException) {
          e = e.getCause();
        }
        synchronized (this) {
          if (isRunningState()) {
            trySettingEndReason(EndReason.TASK_ERROR);
            registerFirstException(TaskFailureType.NON_FATAL, e, null);
            LOG.warn("Exception from RunnerCallable", e);
          }
        }
      }
      processCallableResult(executionResult);

      switch (firstEndReason) {
        case SUCCESS:
          try {
            taskReporter.taskSucceeded(task.getTaskAttemptID());
            return logAndReturnEndResult(EndReason.SUCCESS, null, null, stopContainerRequested.get());
          } catch (IOException e) {
            // Comm failure. Task can't do much.
            handleFinalStatusUpdateFailure(e, "success");
            return logAndReturnEndResult(EndReason.COMMUNICATION_FAILURE, firstTaskFailureType, e, stopContainerRequested.get());
          } catch (TezException e) {
            // Failure from AM. Task can't do much.
            handleFinalStatusUpdateFailure(e, "success");
            return logAndReturnEndResult(EndReason.COMMUNICATION_FAILURE, firstTaskFailureType, e, stopContainerRequested.get());
          }
        case CONTAINER_STOP_REQUESTED:
          // Don't need to send any more communication updates to the AM.
          return logAndReturnEndResult(firstEndReason, firstTaskFailureType, null, stopContainerRequested.get());
        case KILL_REQUESTED:
          // This was an external kill called directly on the task runner
          return logAndReturnEndResult(firstEndReason, firstTaskFailureType, null, stopContainerRequested.get());
        case TASK_KILL_REQUEST:
          // Task reported a self kill
          return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
        case COMMUNICATION_FAILURE:
          // Already seen a communication failure. There's no point trying to report another one.
          return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
        case TASK_ERROR:
          // Don't report an error again if it was reported via signalFatalError
          if (errorReporterToAm.get()) {
            return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
          } else {
            String message;
            if (firstException instanceof FSError) {
              message = "Encountered an FSError while executing task: " + task.getTaskAttemptID();
            } else if (firstException instanceof Error) {
              message = "Encountered an Error while executing task: " + task.getTaskAttemptID();
            } else {
              message = "Error while running task ( failure ) : " + task.getTaskAttemptID();
            }
            try {
              taskReporter.taskFailed(task.getTaskAttemptID(), firstTaskFailureType, firstException, message, exceptionSourceInfo);
              return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
            } catch (IOException e) {
              // Comm failure. Task can't do much.
              handleFinalStatusUpdateFailure(e, "failure");
              return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
            } catch (TezException e) {
              // Failure from AM. Task can't do much.
              handleFinalStatusUpdateFailure(e, "failure");
              return logAndReturnEndResult(firstEndReason, firstTaskFailureType, firstException, stopContainerRequested.get());
            }
          }
        default:
          LOG.error("Unexpected EndReason. File a bug");
          return logAndReturnEndResult(EndReason.TASK_ERROR, firstTaskFailureType, new RuntimeException("Unexpected EndReason"), stopContainerRequested.get());

      }
    } finally {
      // Clear the interrupted status of the blocking thread, in case it is set after the
      // InterruptedException was invoked.
      oobSignalLock.lock();
      try {
        while (oobSignalErrorInProgress) {
          try {
            oobSignalCondition.await();
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for OOB fatal error to complete");
            Thread.currentThread().interrupt();
          }
        }
      } finally {
        oobSignalLock.unlock();
      }
      taskReporter.unregisterTask(task.getTaskAttemptID());
      if (taskKillStartTime != 0) {
        LOG.info("Time taken to interrupt task={}", (System.currentTimeMillis() - taskKillStartTime));
      }
      if (localExecutor != null) {
        localExecutor.shutdown();
      }
      Thread.interrupted();
    }
  }

  // It's possible for the task to actually complete, and an alternate signal such as killTask/killContainer
  // come in before the future has been processed by this thread. That condition is not handled - and
  // the result of the execution will be determind by the thread order.
  @VisibleForTesting
  void processCallableResult(TaskRunner2CallableResult executionResult) {
    if (executionResult != null) {
      synchronized (this) {
        if (isRunningState()) {
          if (executionResult.error != null) {
            trySettingEndReason(EndReason.TASK_ERROR);
            registerFirstException(TaskFailureType.NON_FATAL, executionResult.error, null);
          } else {
            trySettingEndReason(EndReason.SUCCESS);
            taskComplete.set(true);
          }
        }
      }
    }
  }

  /**
   * Attempt to kill the running task, if it hasn't already completed for some other reason.
   * @return true if the task kill was honored, false otherwise
   */
  public boolean killTask() {
    boolean isFirstError = false;
    synchronized (this) {
      if (isRunningState()) {
        if (trySettingEndReason(EndReason.KILL_REQUESTED)) {
          isFirstError = true;
          killTaskRequested.set(true);
        } else {
          logErrorIgnored("killTask", null);
        }
      } else {
        logErrorIgnored("killTask", null);
      }
    }
    if (isFirstError) {
      logAborting("killTask");
      killTaskInternal();
      return true;
    } else {
      return false;
    }
  }

  private void killTaskInternal() {
    abortTaskInternal();
    interruptTaskInternal();
  }

  private void abortTaskInternal() {
    if (taskRunnerCallable != null) {
      taskKillStartTime = System.currentTimeMillis();
      taskRunnerCallable.abortTask();
    }
  }

  private void interruptTaskInternal() {
    if (taskRunnerCallable != null) {
      taskRunnerCallable.interruptTask();
    }
  }

  // Checks and changes on these states should happen within a synchronized block,
  // to ensure the first event is the one that is captured and causes specific behaviour.
  private boolean isRunningState() {
    return !taskComplete.get() && !killTaskRequested.get() && !stopContainerRequested.get() &&
        !errorSeen.get();
  }

  class UmbilicalAndErrorHandler implements TezUmbilical, ErrorReporter {

    @Override
    public void addEvents(Collection<TezEvent> events) {
      // Incoming events from the running task.
      // Only add these if the task is running.
      if (isRunningState()) {
        taskReporter.addEvents(task.getTaskAttemptID(), events);
      }
    }

    @Override
    public void signalFailure(TezTaskAttemptID taskAttemptID, TaskFailureType taskFailureType, Throwable t, String message,
                              EventMetaData sourceInfo) {
      // Fatal error reported by the task.
      signalTerminationInternal(taskAttemptID, EndReason.TASK_ERROR, taskFailureType, t, message, sourceInfo, false);
    }

    @Override
    public void signalKillSelf(TezTaskAttemptID taskAttemptID, Throwable t, String message,
                               EventMetaData sourceInfo) {
      signalTerminationInternal(taskAttemptID, EndReason.TASK_KILL_REQUEST, null, t, message, sourceInfo, true);

    }


    @Override
    public boolean canCommit(TezTaskAttemptID taskAttemptID) throws IOException {
      // Task checking whether it can commit.

      // Not getting a lock here. It should be alright for the to check with the reporter
      // on whether a task can commit.
      if (isRunningState()) {
        return taskReporter.canCommit(taskAttemptID);
        // If there's a communication failure here, let it propagate through to the task.
        // which may throw it back or handle it appropriately.
      } else {
        // Don't throw an error since the task is already in the process of shutting down.
        LOG.info("returning canCommit=false since task is not in a running state");
        return false;
      }
    }


    @Override
    public void reportError(Throwable t) {
      // Umbilical reporting an error during heartbeat
      boolean isFirstError = false;
      synchronized (TezTaskRunner2.this) {
        if (isRunningState()) {
          LOG.info("TaskReporter reporter error which will cause the task to fail", t);
          if (trySettingEndReason(EndReason.COMMUNICATION_FAILURE)) {
            registerFirstException(TaskFailureType.NON_FATAL, t, null);
            isFirstError = true;
          } else {
            logErrorIgnored("umbilicalFatalError", null);
          }
          // A race is possible between a task succeeding, and a subsequent timed heartbeat failing.
          // These errors can be ignored, since a task can only succeed if the synchronous taskSucceeded
          // method does not throw an exception, in which case task success is registered with the AM.
          // Leave subsequent heartbeat errors to the next entity to communicate using the TaskReporter
        } else {
          logErrorIgnored("umbilicalFatalError", null);
        }
        // Since this error came from the taskReporter - there's no point attempting to report a failure back to it.
        // However, the task does need to be cleaned up
      }
      if (isFirstError) {
        logAborting("umbilicalFatalError");
        killTaskInternal();
      }
    }

    @Override
    public void shutdownRequested() {
      // Umbilical informing about a shutdown request for the container.
      boolean isFirstTerminate = false;
      synchronized (TezTaskRunner2.this) {
        isFirstTerminate = trySettingEndReason(EndReason.CONTAINER_STOP_REQUESTED);
        // Respect stopContainerRequested since it can come in at any point, despite a previous failure.
        stopContainerRequested.set(true);
      }
      if (isFirstTerminate) {
        logAborting("shutdownRequested");
        killTaskInternal();
      } else {
        logErrorIgnored("shutdownRequested", null);
      }
    }
  }


  private void signalTerminationInternal(TezTaskAttemptID taskAttemptID, EndReason endReason,
                                         TaskFailureType taskFailureType, Throwable t, String message,
                                         EventMetaData sourceInfo, boolean isKill) {
    boolean isFirstError = false;
    String typeString = isKill ? " kill " : " failure ";
    synchronized (TezTaskRunner2.this) {
      if (isRunningState()) {
        if (trySettingEndReason(endReason)) {
          if (t == null) {
            String errMessage = message;
            if (errMessage == null) {
              errMessage = typeString + " : No user message or exception specified";
            }
            t = new RuntimeException(errMessage);
          }
          registerFirstException(taskFailureType, t, sourceInfo);
          LOG.info("Received notification of a " + typeString +
              " which will cause the task to die", t);
          isFirstError = true;
          errorReporterToAm.set(true);
          oobSignalErrorInProgress = true;
        } else {
          logErrorIgnored(typeString, message);
        }
      } else {
        logErrorIgnored(typeString, message);
      }
    }

    // Informing the TaskReporter here because the running task may not be interruptable.
    // Has to be outside the lock.
    if (isFirstError) {
      logAborting(typeString);
      abortTaskInternal();
      try {
        if (isKill) {
          taskReporter
              .taskKilled(taskAttemptID, t, getTaskDiagnosticsString(t, message, typeString), sourceInfo);
        } else {
          taskReporter.taskFailed(taskAttemptID, taskFailureType, t,
              getTaskDiagnosticsString(t, message, typeString), sourceInfo);
        }
      } catch (IOException e) {
        // Comm failure. Task can't do much. The main exception is already registered.
        handleFinalStatusUpdateFailure(e, typeString);
      } catch (TezException e) {
        // Failure from AM. Task can't do much. The main exception is already registered.
        handleFinalStatusUpdateFailure(e, typeString);
      } catch (Exception e) {
        handleFinalStatusUpdateFailure(e, typeString);
      } finally {
        interruptTaskInternal();
        oobSignalLock.lock();
        try {
          // This message is being sent outside of the main thread, which may end up completing before
          // this thread runs. Make sure the main run thread does not end till this completes.
          oobSignalErrorInProgress = false;
          oobSignalCondition.signal();
        } finally {
          oobSignalLock.unlock();
        }
      }
    }
  }

  private synchronized boolean trySettingEndReason(EndReason endReason) {
    if (isRunningState()) {
      firstEndReason = endReason;
      return true;
    }
    return false;
  }


  private void registerFirstException(TaskFailureType taskFailureType, Throwable t, EventMetaData sourceInfo) {
    Preconditions.checkState(isRunningState());
    errorSeen.set(true);
    firstException = t;
    this.firstTaskFailureType = taskFailureType;
    this.exceptionSourceInfo = sourceInfo;
  }


  private String getTaskDiagnosticsString(Throwable t, String message, String typeString) {
    String diagnostics;
    if (t != null && message != null) {
      diagnostics = "Error while running task (" + typeString + ") : " + ExceptionUtils.getStackTrace(t) + ", errorMessage="
          + message;
    } else if (t == null && message == null) {
      diagnostics = "Unknown error";
    } else {
      diagnostics = t != null ? "Error while running task (" + typeString + ") : " + ExceptionUtils.getStackTrace(t)
          : " errorMessage=" + message;
    }
    return diagnostics;
  }

  private TaskRunner2Result logAndReturnEndResult(EndReason endReason,
                                                  TaskFailureType taskFailureType,
                                                  Throwable firstError,
                                                  boolean stopContainerRequested) {
    TaskRunner2Result result =
        new TaskRunner2Result(endReason, taskFailureType, firstError, stopContainerRequested);
    LOG.info("TaskRunnerResult for {} : {}  ", task.getTaskAttemptID(), result);
    return result;
  }

  private void handleFinalStatusUpdateFailure(Throwable t, String stateString) {
    // TODO Ideally differentiate between FAILED/KILLED
    LOG.warn("Failure while reporting state= {} to AM",
        stateString, t);
  }

  private void logErrorIgnored(String ignoredEndReason, String errorMessage) {
    LOG.info(
        "Ignoring {} request since the task with id {} has ended for reason: {}. IgnoredError: {} ",
        ignoredEndReason, task.getTaskAttemptID(),
        firstEndReason, (firstException == null ? (errorMessage == null ? "" : errorMessage) :
            firstException.getMessage()));
  }

  private void logAborting(String abortReason) {
    LOG.info("Attempting to abort {} due to an invocation of {}", task.getTaskAttemptID(),
        abortReason);
  }
}
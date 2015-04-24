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
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class TezTaskRunner implements TezUmbilical, ErrorReporter {

  private static final Logger LOG = LoggerFactory.getLogger(TezTaskRunner.class);

  private final Configuration tezConf;
  private final LogicalIOProcessorRuntimeTask task;
  private final UserGroupInformation ugi;

  private final TaskReporter taskReporter;
  private final ListeningExecutorService executor;
  private volatile ListenableFuture<Void> taskFuture;
  private volatile Thread waitingThread;
  private volatile Throwable firstException;

  // Effectively a duplicate check, since hadFatalError does the same thing.
  private final AtomicBoolean fatalErrorSent = new AtomicBoolean(false);
  private final AtomicBoolean taskRunning;
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  TezTaskRunner(Configuration tezConf, UserGroupInformation ugi, String[] localDirs,
      TaskSpec taskSpec, int appAttemptNumber,
      Map<String, ByteBuffer> serviceConsumerMetadata, Map<String, String> serviceProviderEnvMap,
      Multimap<String, String> startedInputsMap, TaskReporter taskReporter,
      ListeningExecutorService executor, ObjectRegistry objectRegistry, String pid,
      ExecutionContext executionContext, long memAvailable)
          throws IOException {
    this.tezConf = tezConf;
    this.ugi = ugi;
    this.taskReporter = taskReporter;
    this.executor = executor;
    task = new LogicalIOProcessorRuntimeTask(taskSpec, appAttemptNumber, tezConf, localDirs, this,
        serviceConsumerMetadata, serviceProviderEnvMap, startedInputsMap, objectRegistry, pid,
        executionContext, memAvailable);
    taskRunning = new AtomicBoolean(false);
  }

  /**
   * @return false if a shutdown message was received during task execution
   * @throws TezException
   * @throws IOException
   */
  public boolean run() throws InterruptedException, IOException, TezException {
    waitingThread = Thread.currentThread();
    taskRunning.set(true);
    taskReporter.registerTask(task, this);
    TaskRunnerCallable callable = new TaskRunnerCallable();
    Throwable failureCause = null;
    taskFuture = executor.submit(callable);
    try {
      taskFuture.get();

      // Task could signal a fatal error and return control, or a failure while registering success.
      failureCause = firstException;

    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for task to complete. Interrupting task");
      taskFuture.cancel(true);
      if (shutdownRequested.get()) {
        LOG.info("Shutdown requested... returning");
        return false;
      }
      if (firstException != null) {
        failureCause = firstException;
      } else {
        // Interrupted for some other reason.
        failureCause = e;
      }
    } catch (ExecutionException e) {
      // Exception thrown by the run() method itself.
      Throwable cause = e.getCause();
      if (cause instanceof FSError) {
        // Not immediately fatal, this is an error reported by Hadoop FileSystem
        failureCause = cause;
      } else if (cause instanceof Error) {
        LOG.error("Exception of type Error.", cause);
        sendFailure(cause, "Fatal Error cause TezChild exit.");
        throw new TezException("Fatal Error cause TezChild exit.", cause);
      } else {
        failureCause = cause;
      }
    } finally {
      // Clear the interrupted status of the blocking thread, in case it is set after the
      // InterruptedException was invoked.
      taskReporter.unregisterTask(task.getTaskAttemptID());
      Thread.interrupted();
    }

    if (failureCause != null) {
      if (failureCause instanceof FSError) {
        // Not immediately fatal, this is an error reported by Hadoop FileSystem
        LOG.info("Encountered an FSError while executing task: " + task.getTaskAttemptID(),
            failureCause);
        throw (FSError) failureCause;
      } else if (failureCause instanceof Error) {
        LOG.error("Exception of type Error.", failureCause);
        sendFailure(failureCause, "Fatal error cause TezChild exit.");
        throw new TezException("Fatal error cause TezChild exit.", failureCause);
      } else {
        if (failureCause instanceof IOException) {
          throw (IOException) failureCause;
        } else if (failureCause instanceof TezException) {
          throw (TezException) failureCause;
        } else if (failureCause instanceof InterruptedException) {
          throw (InterruptedException) failureCause;
        } else {
          throw new TezException(failureCause);
        }
      }
    }
    if (shutdownRequested.get()) {
      LOG.info("Shutdown requested... returning");
      return false;
    }
    return true;
  }

  private class TaskRunnerCallable extends CallableWithNdc<Void> {
    @Override
    protected Void callInternal() throws Exception {
      try {
        return ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              LOG.info("Initializing task" + ", taskAttemptId=" + task.getTaskAttemptID());
              task.initialize();
              if (!Thread.currentThread().isInterrupted() && firstException == null) {
                LOG.info("Running task, taskAttemptId=" + task.getTaskAttemptID());
                task.run();
                LOG.info("Closing task, taskAttemptId=" + task.getTaskAttemptID());
                task.close();
                task.setFrameworkCounters();
              }
              LOG.info("Task completed, taskAttemptId=" + task.getTaskAttemptID()
                  + ", fatalErrorOccurred=" + (firstException != null));
              if (firstException == null) {
                try {
                  taskReporter.taskSucceeded(task.getTaskAttemptID());
                } catch (IOException e) {
                  LOG.warn("Heartbeat failure caused by communication failure", e);
                  maybeRegisterFirstException(e);
                  // Falling off, since the runner thread checks for the registered exception.
                } catch (TezException e) {
                  LOG.warn("Heartbeat failure reported by AM", e);
                  maybeRegisterFirstException(e);
                  // Falling off, since the runner thread checks for the registered exception.
                }
              }
              return null;
            } catch (Throwable cause) {
              if (cause instanceof FSError) {
                // Not immediately fatal, this is an error reported by Hadoop FileSystem
                maybeRegisterFirstException(cause);
                LOG.info("Encountered an FSError while executing task: " + task.getTaskAttemptID(),
                    cause);
                try {
                  sendFailure(cause, "FS Error in Child JVM");
                } catch (Exception ignored) {
                  // Ignored since another cause is already known
                  LOG.info(
                      "Ignoring the following exception since a previous exception is already registered",
                      ignored.getClass().getName());
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Ignored exception is", ignored);
                  }
                }
                throw (FSError) cause;
              } else if (cause instanceof Error) {
                LOG.error("Exception of type Error.", cause);
                sendFailure(cause, "Fatal Error cause TezChild exit.");
                throw new TezException("Fatal Error cause TezChild exit.", cause);
              } else {
                if (cause instanceof UndeclaredThrowableException) {
                  cause = ((UndeclaredThrowableException) cause).getCause();
                }
                maybeRegisterFirstException(cause);
                LOG.info("Encounted an error while executing task: " + task.getTaskAttemptID(),
                    cause);
                try {
                  sendFailure(cause, "Failure while running task");
                } catch (Exception ignored) {
                  // Ignored since another cause is already known
                  LOG.info(
                      "Ignoring the following exception since a previous exception is already registered",
                      ignored.getClass().getName());
                  if (LOG.isTraceEnabled()) {
                    LOG.trace("Ignored exception is", ignored);
                  }
                }
                if (cause instanceof IOException) {
                  throw (IOException) cause;
                } else if (cause instanceof TezException) {
                  throw (TezException) cause;
                } else {
                  throw new TezException(cause);
                }
              }
            } finally {
              task.cleanup();
            }
          }
        });
      } finally {
        taskRunning.set(false);
      }
    }
  }

  // should wait until all messages are sent to AM before TezChild shutdown
  // if this method become async in future
  private void sendFailure(Throwable t, String message) throws IOException, TezException {
    if (!fatalErrorSent.getAndSet(true)) {
      task.setFatalError(t, message);
      task.setFrameworkCounters();
      try {
        taskReporter.taskFailed(task.getTaskAttemptID(), t, message, null);
      } catch (IOException e) {
        // A failure reason already exists, Comm error just logged.
        LOG.warn("Heartbeat failure caused by communication failure", e);
        throw e;
      } catch (TezException e) {
        // A failure reason already exists, Comm error just logged.
        LOG.warn("Heartbeat failure reported by AM", e);
        throw e;
      }
    } else {
      LOG.warn("Ignoring fatal error since another error has already been reported", t);
    }
  }

  @Override
  public void addEvents(Collection<TezEvent> events) {
    if (taskRunning.get()) {
      taskReporter.addEvents(task.getTaskAttemptID(), events);
    }
  }

  @Override
  public synchronized void signalFatalError(TezTaskAttemptID taskAttemptID, Throwable t,
      String message, EventMetaData sourceInfo) {
    // This can be called before a task throws an exception or after it.
    // If called before a task throws an exception
    // - ensure a heartbeat is sent with the diagnostics, and sent only once.
    // - interrupt the waiting thread, and make it throw the reported error.
    // If called after a task throws an exception, the waiting task has already returned, no point
    // interrupting it.
    // This case can be effectively ignored (log), as long as the run() method ends up throwing the
    // exception.
    //
    //
    if (!fatalErrorSent.getAndSet(true)) {
      maybeRegisterFirstException(t);
      try {
        taskReporter.taskFailed(taskAttemptID, t, getTaskDiagnosticsString(t, message), sourceInfo);
      } catch (IOException e) {
        // HeartbeatFailed. Don't need to propagate the heartbeat exception since a task exception
        // occurred earlier.
        LOG.warn("Heartbeat failure caused by communication failure", e);
      } catch (TezException e) {
        // HeartbeatFailed. Don't need to propagate the heartbeat exception since a task exception
        // occurred earlier.
        LOG.warn("Heartbeat failure reported by AM", e);
      } finally {
        // Wake up the waiting thread so that it can return control
        waitingThread.interrupt();
      }
    }
  }

  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptID) {
    if (taskRunning.get()) {
      try {
        return taskReporter.canCommit(taskAttemptID);
      } catch (IOException e) {
        LOG.warn("Communication failure while trying to commit", e);
        maybeRegisterFirstException(e);
        waitingThread.interrupt();
        // Not informing the task since it will be interrupted.
        // TODO: Should this be sent to the task as well, current Processors, etc do not handle
        // interrupts very well.
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public synchronized void reportError(Throwable t) {
   if (taskRunning.get()) {
      LOG.error("TaskReporter reported error", t);
      maybeRegisterFirstException(t);
      waitingThread.interrupt();
      // A race is possible between a task succeeding, and a subsequent timed heartbeat failing.
      // These errors can be ignored, since a task can only succeed if the synchronous taskSucceeded
      // method does not throw an exception, in which case task success is registered with the AM.
      // Leave this handling to the next getTask / actual task.
    } else {
      LOG.info("Ignoring Communication failure since task with id=" + task.getTaskAttemptID()
          + " is already complete");
    }
  }

  @Override
  public void shutdownRequested() {
    shutdownRequested.set(true);
    waitingThread.interrupt();
  }

  private String getTaskDiagnosticsString(Throwable t, String message) {
    String diagnostics;
    if (t != null && message != null) {
      diagnostics = "exceptionThrown=" + ExceptionUtils.getStackTrace(t) + ", errorMessage="
          + message;
    } else if (t == null && message == null) {
      diagnostics = "Unknown error";
    } else {
      diagnostics = t != null ? "exceptionThrown=" + ExceptionUtils.getStackTrace(t)
          : " errorMessage=" + message;
    }
    return diagnostics;
  }

  private synchronized void maybeRegisterFirstException(Throwable t) {
    if (firstException == null) {
      firstException = t;
    }
  }

}

/*
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

package org.apache.tez.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProgressHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(ProgressHelper.class);
  private static final float MIN_PROGRESS_VAL = 0.0f;
  private static final float MAX_PROGRESS_VAL = 1.0f;
  private final String processorName;
  protected final Map<String, LogicalInput> inputs;
  private final ProcessorContext processorContext;
  private final AtomicReference<ScheduledFuture<?>> periodicMonitorTaskRef;
  private long monitorExecPeriod;
  private volatile ScheduledExecutorService scheduledExecutorService;

  public static final float processProgress(float val) {
    return (Float.isNaN(val)) ? MIN_PROGRESS_VAL
        : Math.max(MIN_PROGRESS_VAL, Math.min(MAX_PROGRESS_VAL, val));
  }

  public static final boolean isProgressWithinRange(float val) {
    return (val <= MAX_PROGRESS_VAL && val >= MIN_PROGRESS_VAL);
  }

  public ProgressHelper(Map<String, LogicalInput> inputsParam,
      ProcessorContext context, String processorName) {
    this.periodicMonitorTaskRef = new AtomicReference<>(null);
    this.inputs = inputsParam;
    this.processorContext = context;
    this.processorName = processorName;
  }

  public void scheduleProgressTaskService(long delay, long period) {
    monitorExecPeriod = period;
    scheduledExecutorService =
        Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
                "TaskProgressService{" + processorName + ":" + processorContext
                    .getTaskVertexName()
                    + "} #%d").build());
    try {
      createPeriodicTask(delay);
    } catch (RejectedExecutionException | IllegalArgumentException ex) {
      LOG.error("Could not create periodic scheduled task for processor={}",
          processorName, ex);
    }
  }

  private Runnable createRunnableMonitor() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          float progSum = MIN_PROGRESS_VAL;
          int invalidInput = 0;
          float progressVal = MIN_PROGRESS_VAL;
          if (inputs != null && !inputs.isEmpty()) {
            for (LogicalInput input : inputs.values()) {
              if (!(input instanceof AbstractLogicalInput)) {
                /**
                 * According to javdoc in
                 * {@link org.apache.tez.runtime.api.AbstractLogicalInput} all
                 * implementations must extend AbstractLogicalInput.
                 */
                continue;
              }
              final float inputProgress =
                  ((AbstractLogicalInput) input).getProgress();
              if (!isProgressWithinRange(inputProgress)) {
                final int invalidSnapshot = ++invalidInput;
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      "progress update: Incorrect value in progress helper in "
                          + "processor={}, inputProgress={}, inputsSize={}, "
                          + "invalidInput={}",
                      processorName, inputProgress, inputs.size(),
                      invalidSnapshot);
                }
              }
              progSum += processProgress(inputProgress);
            }
            // No need to process the average within the valid range since the
            // processorContext validates the value before being set.
            progressVal = progSum / inputs.size();
          }
          // Report progress as 0.0f when if are errors.
          processorContext.setProgress(progressVal);
        } catch (Throwable th) {
          LOG.debug("progress update: Encountered InterruptedException during"
              + " Processor={}", processorName, th);
          if (th instanceof InterruptedException) {
            // set interrupt flag to true sand exit
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    };
  }

  private boolean createPeriodicTask(long delay)
      throws RejectedExecutionException, IllegalArgumentException {
    stopPeriodicMonitor();
    final Runnable runnableMonitor = createRunnableMonitor();
    ScheduledFuture<?> futureTask = scheduledExecutorService
        .scheduleWithFixedDelay(runnableMonitor, delay, monitorExecPeriod,
            TimeUnit.MILLISECONDS);
    periodicMonitorTaskRef.set(futureTask);
    return true;
  }

  private void stopPeriodicMonitor() {
    ScheduledFuture<?> scheduledMonitorRes =
        this.periodicMonitorTaskRef.get();
    if (scheduledMonitorRes != null && !scheduledMonitorRes.isCancelled()) {
      scheduledMonitorRes.cancel(true);
      this.periodicMonitorTaskRef.set(null);
    }
  }

  public void shutDownProgressTaskService() {
    stopPeriodicMonitor();
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
      try {
        if (!scheduledExecutorService.awaitTermination(monitorExecPeriod,
            TimeUnit.MILLISECONDS)) {
          scheduledExecutorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.debug("Interrupted exception while shutting down the "
            + "executor service for the processor name={}", processorName);
      }
      scheduledExecutorService.shutdownNow();
    }
    scheduledExecutorService = null;
  }
}

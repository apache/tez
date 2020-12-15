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

package org.apache.tez.dag.app.dag.speculation.legacy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.ProgressHelper;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.SpeculatorEvent;
import org.apache.tez.dag.app.dag.event.SpeculatorEventTaskAttemptStatusUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;

/**
 * Maintains runtime estimation statistics. Makes periodic updates
 * estimates based on progress and decides on when to trigger a 
 * speculative attempt. Speculation attempts are triggered when the 
 * estimated runtime is more than a threshold beyond the mean runtime
 * and the original task still has enough estimated runtime left that 
 * the speculative version is expected to finish sooner than that. If 
 * the original is close to completion then we dont start a speculation
 * because it may be likely a wasted attempt. There is a delay between
 * successive speculations.
 */
public class LegacySpeculator extends AbstractService {
  
  private static final long ON_SCHEDULE = Long.MIN_VALUE;
  private static final long ALREADY_SPECULATING = Long.MIN_VALUE + 1;
  private static final long TOO_NEW = Long.MIN_VALUE + 2;
  private static final long PROGRESS_IS_GOOD = Long.MIN_VALUE + 3;
  private static final long NOT_RUNNING = Long.MIN_VALUE + 4;
  private static final long TOO_LATE_TO_SPECULATE = Long.MIN_VALUE + 5;

  private final long soonestRetryAfterNoSpeculate;
  private final long soonestRetryAfterSpeculate;

  private final double proportionRunningTasksSpeculatable;
  private final double proportionTotalTasksSpeculatable;
  private final int  minimumAllowedSpeculativeTasks;
  private static final int VERTEX_SIZE_THRESHOLD_FOR_TIMEOUT_SPECULATION = 1;

  private static final Logger LOG = LoggerFactory.getLogger(LegacySpeculator.class);

  private final ConcurrentMap<TezTaskID, Boolean> runningTasks
      = new ConcurrentHashMap<TezTaskID, Boolean>();
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  // Used to track any TaskAttempts that aren't heart-beating for a while, so
  // that we can aggressively speculate instead of waiting for task-timeout.
  private final ConcurrentMap<TezTaskAttemptID, TaskAttemptHistoryStatistics>
      runningTaskAttemptStatistics = new ConcurrentHashMap<TezTaskAttemptID,
          TaskAttemptHistoryStatistics>();
  // Regular heartbeat from tasks is every 3 secs. So if we don't get a
  // heartbeat in 9 secs (3 heartbeats), we simulate a heartbeat with no change
  // in progress.
  private static final long MAX_WAITTING_TIME_FOR_HEARTBEAT = 9 * 1000;

  private final Set<TezTaskID> mayHaveSpeculated = new HashSet<TezTaskID>();

  private Vertex vertex;
  private TaskRuntimeEstimator estimator;
  private final long taskTimeout;
  private final Clock clock;
  private Thread speculationBackgroundThread = null;
  private volatile boolean stopped = false;

  @VisibleForTesting
  public int getMinimumAllowedSpeculativeTasks() { return minimumAllowedSpeculativeTasks;}

  @VisibleForTesting
  public double getProportionTotalTasksSpeculatable() { return proportionTotalTasksSpeculatable;}

  @VisibleForTesting
  public double getProportionRunningTasksSpeculatable() { return proportionRunningTasksSpeculatable;}

  @VisibleForTesting
  public long getSoonestRetryAfterNoSpeculate() { return soonestRetryAfterNoSpeculate;}

  @VisibleForTesting
  public long getSoonestRetryAfterSpeculate() { return soonestRetryAfterSpeculate;}

  public LegacySpeculator(Configuration conf, AppContext context, Vertex vertex) {
    this(conf, context.getClock(), vertex);
  }

  public LegacySpeculator(Configuration conf, Clock clock, Vertex vertex) {
    this(conf, getEstimator(conf, vertex), clock, vertex);
  }
  
  static private TaskRuntimeEstimator getEstimator
      (Configuration conf, Vertex vertex) {
    TaskRuntimeEstimator estimator;
    Class<? extends TaskRuntimeEstimator> estimatorClass =
        conf.getClass(TezConfiguration.TEZ_AM_TASK_ESTIMATOR_CLASS,
            LegacyTaskRuntimeEstimator.class,
            TaskRuntimeEstimator.class);
    try {
      Constructor<? extends TaskRuntimeEstimator> estimatorConstructor
          = estimatorClass.getConstructor();
      estimator = estimatorConstructor.newInstance();
      estimator.contextualize(conf, vertex);
    } catch (NoSuchMethodException e) {
      LOG.error("Can't make a speculation runtime estimator", e);
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      LOG.error("Can't make a speculation runtime estimator", e);
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      LOG.error("Can't make a speculation runtime estimator", e);
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Can't make a speculation runtime estimator", e);
      throw new RuntimeException(e);
    }
    return estimator;
  }

  @Override
  protected void serviceStart() throws Exception {
    lock.writeLock().lock();
    try {
      assert (speculationBackgroundThread == null);

      if (speculationBackgroundThread == null) {
        speculationBackgroundThread =
            new Thread(createThread(),
                "DefaultSpeculator background processing");
        speculationBackgroundThread.start();
      }
      super.serviceStart();
    } catch (Exception e) {
      LOG.warn("Speculator thread could not launch", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isStarted() {
    boolean result = false;
    lock.readLock().lock();
    try {
      if (this.speculationBackgroundThread != null) {
        result = getServiceState().equals(STATE.STARTED);
      }
    } finally {
      lock.readLock().unlock();
    }
    return result;
  }

  // This constructor is designed to be called by other constructors.
  //  However, it's public because we do use it in the test cases.
  // Normally we figure out our own estimator.
  public LegacySpeculator
      (Configuration conf, TaskRuntimeEstimator estimator, Clock clock, Vertex vertex) {
    super(LegacySpeculator.class.getName());
    this.vertex = vertex;
    this.estimator = estimator;
    this.clock = clock;
    taskTimeout = conf.getLong(
            TezConfiguration.TEZ_AM_LEGACY_SPECULATIVE_SINGLE_TASK_VERTEX_TIMEOUT,
            TezConfiguration.TEZ_AM_LEGACY_SPECULATIVE_SINGLE_TASK_VERTEX_TIMEOUT_DEFAULT);
    soonestRetryAfterNoSpeculate = conf.getLong(
            TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_NO_SPECULATE,
            TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_NO_SPECULATE_DEFAULT);
    soonestRetryAfterSpeculate = conf.getLong(
            TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_SPECULATE,
            TezConfiguration.TEZ_AM_SOONEST_RETRY_AFTER_SPECULATE_DEFAULT);
    proportionRunningTasksSpeculatable = conf.getDouble(
            TezConfiguration.TEZ_AM_PROPORTION_RUNNING_TASKS_SPECULATABLE,
            TezConfiguration.TEZ_AM_PROPORTION_RUNNING_TASKS_SPECULATABLE_DEFAULT);
    proportionTotalTasksSpeculatable = conf.getDouble(
            TezConfiguration.TEZ_AM_PROPORTION_TOTAL_TASKS_SPECULATABLE,
            TezConfiguration.TEZ_AM_PROPORTION_TOTAL_TASKS_SPECULATABLE_DEFAULT);
    minimumAllowedSpeculativeTasks = conf.getInt(
            TezConfiguration.TEZ_AM_MINIMUM_ALLOWED_SPECULATIVE_TASKS,
            TezConfiguration.TEZ_AM_MINIMUM_ALLOWED_SPECULATIVE_TASKS_DEFAULT);
  }

  @Override
  protected void serviceStop() throws Exception {
    lock.writeLock().lock();
    try {
      stopped = true;
      // this could be called before background thread is established
      if (speculationBackgroundThread != null) {
        speculationBackgroundThread.interrupt();
      }
      super.serviceStop();
      speculationBackgroundThread = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          long backgroundRunStartTime = clock.getTime();
          try {
            int speculations = computeSpeculations();
            long nextRecompTime = speculations > 0 ? soonestRetryAfterSpeculate
                : soonestRetryAfterNoSpeculate;
            long wait = Math.max(nextRecompTime, clock.getTime() - backgroundRunStartTime);
            if (speculations > 0) {
              LOG.info("We launched " + speculations
                  + " speculations.  Waiting " + wait + " milliseconds before next evaluation.");
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting {} milliseconds before next evaluation.", wait);
              }
            }
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            if (!stopped) {
              LOG.warn("Speculator thread interrupted", ie);
            }
          }
        }
      }
    };
  }

/*   *************************************************************    */

  public void notifyAttemptStarted(TezTaskAttemptID taId, long timestamp) {
    estimator.enrollAttempt(taId, timestamp);    
  }

  public void notifyAttemptStatusUpdate(TezTaskAttemptID taId,
      TaskAttemptState reportedState,
      long timestamp) {
    statusUpdate(taId, reportedState, timestamp);
  }

  /**
   * Absorbs one TaskAttemptStatus
   *
   * @param reportedStatus the status report that we got from a task attempt
   *        that we want to fold into the speculation data for this job
   * @param timestamp the time this status corresponds to.  This matters
   *        because statuses contain progress.
   */
  private void statusUpdate(TezTaskAttemptID attemptID,
      TaskAttemptState reportedState, long timestamp) {

    TezTaskID taskID = attemptID.getTaskID();
    Task task = vertex.getTask(taskID);

    if (task == null) {
      return;
    }

    estimator.updateAttempt(attemptID, reportedState, timestamp);

    if (reportedState == TaskAttemptState.RUNNING) {
      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
    } else {
      runningTasks.remove(taskID, Boolean.TRUE);
      if (reportedState == TaskAttemptState.STARTING) {
        runningTaskAttemptStatistics.remove(attemptID);
      }
    }
  }
  
  public void handle(SpeculatorEvent event) {
    SpeculatorEventTaskAttemptStatusUpdate updateEvent = ((SpeculatorEventTaskAttemptStatusUpdate) event);
    if (updateEvent.hasJustStarted()) {
      notifyAttemptStarted(updateEvent.getAttemptId(), updateEvent.getTimestamp());
    } else {
      notifyAttemptStatusUpdate(updateEvent.getAttemptId(), updateEvent.getTaskAttemptState(),
          updateEvent.getTimestamp());
    }
  }

/*   *************************************************************    */

// This is the code section that runs periodically and adds speculations for
//  those jobs that need them.


  // This can return a few magic values for tasks that shouldn't speculate:
  //  returns ON_SCHEDULE if thresholdRuntime(taskID) says that we should not
  //     considering speculating this task
  //  returns ALREADY_SPECULATING if that is true.  This has priority.
  //  returns TOO_NEW if our companion task hasn't gotten any information
  //  returns PROGRESS_IS_GOOD if the task is sailing through
  //  returns NOT_RUNNING if the task is not running
  //
  // All of these values are negative.  Any value that should be allowed to
  //  speculate is 0 or positive.
  //
  // If shouldUseTimeout is true, we will use timeout to decide on
  // speculation instead of the task statistics. This can be useful, for
  // example for single task vertices for which there are no tasks to compare
  // with
  private long speculationValue(Task task, long now, boolean shouldUseTimeout) {
    Map<TezTaskAttemptID, TaskAttempt> attempts = task.getAttempts();
    TezTaskID taskID = task.getTaskId();
    long acceptableRuntime = Long.MIN_VALUE;
    long result = Long.MIN_VALUE;

    // short circuit completed tasks. no need to spend time on them
    if (task.getState() == TaskState.SUCCEEDED) {
      // remove the task from may have speculated if it exists
      mayHaveSpeculated.remove(taskID);
      return NOT_RUNNING;
    }

    if (!mayHaveSpeculated.contains(taskID) && !shouldUseTimeout) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    TezTaskAttemptID runningTaskAttemptID;
    int numberRunningAttempts = 0;

    for (TaskAttempt taskAttempt : attempts.values()) {
      TaskAttemptState taskAttemptState = taskAttempt.getState();
      if (taskAttemptState == TaskAttemptState.RUNNING
          || taskAttemptState == TaskAttemptState.STARTING) {
        if (++numberRunningAttempts > 1) {
          return ALREADY_SPECULATING;
        }
        runningTaskAttemptID = taskAttempt.getID();

        long taskAttemptStartTime
            = estimator.attemptEnrolledTime(runningTaskAttemptID);
        if (taskAttemptStartTime > now) {
          // This background process ran before we could process the task
          //  attempt status change that chronicles the attempt start
          return TOO_NEW;
        }

        if (shouldUseTimeout) {
          if ((now - taskAttemptStartTime) > taskTimeout) {
            // If the task has timed out, then we want to schedule a speculation
            // immediately. However we cannot return immediately since we may
            // already have a speculation running.
            result = Long.MAX_VALUE;
          } else {
            // Task has not timed out so we are good
            return ON_SCHEDULE;
          }
        } else {
          long estimatedRunTime = estimator
              .estimatedRuntime(runningTaskAttemptID);

          long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;

          long estimatedReplacementEndTime
                  = now + estimator.newAttemptEstimatedRuntime();

          float progress = taskAttempt.getProgress();
          TaskAttemptHistoryStatistics data =
                  runningTaskAttemptStatistics.get(runningTaskAttemptID);
          if (data == null) {
            runningTaskAttemptStatistics.put(runningTaskAttemptID,
                new TaskAttemptHistoryStatistics(estimatedRunTime, progress,
                    now));
          } else {
            if (estimatedRunTime == data.getEstimatedRunTime()
                    && progress == data.getProgress()) {
              // Previous stats are same as same stats
              if (data.notHeartbeatedInAWhile(now)
                  || estimator
                  .hasStagnatedProgress(runningTaskAttemptID, now)) {
                // Stats have stagnated for a while, simulate heart-beat.
                // Now simulate the heart-beat
                statusUpdate(taskAttempt.getID(), taskAttempt.getState(),
                    clock.getTime());
              }
            } else {
              // Stats have changed - update our data structure
              data.setEstimatedRunTime(estimatedRunTime);
              data.setProgress(progress);
              data.resetHeartBeatTime(now);
            }
          }

          if (estimatedEndTime < now) {
            return PROGRESS_IS_GOOD;
          }

          if (estimatedReplacementEndTime >= estimatedEndTime) {
            return TOO_LATE_TO_SPECULATE;
          }

          result = estimatedEndTime - estimatedReplacementEndTime;
        }
      }
    }

    // If we are here, there's at most one task attempt.
    if (numberRunningAttempts == 0) {
      return NOT_RUNNING;
    }

    if ((acceptableRuntime == Long.MIN_VALUE) && !shouldUseTimeout) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    return result;
  }

  // Add attempt to a given Task.
  protected void addSpeculativeAttempt(TezTaskID taskID) {
    LOG.info("DefaultSpeculator.addSpeculativeAttempt -- we are speculating "
        + taskID);
    vertex.scheduleSpeculativeTask(taskID);
    mayHaveSpeculated.add(taskID);
  }

  int computeSpeculations() {
    int successes = 0;

    long now = clock.getTime();

    int numberSpeculationsAlready = 0;
    int numberRunningTasks = 0;

    Map<TezTaskID, Task> tasks = vertex.getTasks();

    int numberAllowedSpeculativeTasks
        = (int) Math.max(minimumAllowedSpeculativeTasks,
        proportionTotalTasksSpeculatable * tasks.size());
    TezTaskID bestTaskID = null;
    long bestSpeculationValue = -1L;
    boolean shouldUseTimeout =
        (tasks.size() <= VERTEX_SIZE_THRESHOLD_FOR_TIMEOUT_SPECULATION) &&
            (taskTimeout >= 0);

    // this loop is potentially pricey.
    // TODO track the tasks that are potentially worth looking at
    for (Map.Entry<TezTaskID, Task> taskEntry : tasks.entrySet()) {
      long mySpeculationValue = speculationValue(taskEntry.getValue(), now,
          shouldUseTimeout);

      if (mySpeculationValue == ALREADY_SPECULATING) {
        ++numberSpeculationsAlready;
      }

      if (mySpeculationValue != NOT_RUNNING) {
        ++numberRunningTasks;
      }

      if (mySpeculationValue > bestSpeculationValue) {
        bestTaskID = taskEntry.getKey();
        bestSpeculationValue = mySpeculationValue;
      }
    }
    numberAllowedSpeculativeTasks
        = (int) Math.max(numberAllowedSpeculativeTasks,
                        proportionRunningTasksSpeculatable * numberRunningTasks);

    // If we found a speculation target, fire it off
    if (bestTaskID != null
        && numberAllowedSpeculativeTasks > numberSpeculationsAlready) {
      addSpeculativeAttempt(bestTaskID);
      ++successes;
    }
    return successes;
  }

  static class TaskAttemptHistoryStatistics {

    private long estimatedRunTime;
    private float progress;
    private long lastHeartBeatTime;

    public TaskAttemptHistoryStatistics(long estimatedRunTime, float progress,
        long nonProgressStartTime) {
      this.estimatedRunTime = estimatedRunTime;
      this.progress = progress;
      resetHeartBeatTime(nonProgressStartTime);
    }

    public long getEstimatedRunTime() {
      return this.estimatedRunTime;
    }

    public float getProgress() {
      return this.progress;
    }

    public void setEstimatedRunTime(long estimatedRunTime) {
      this.estimatedRunTime = estimatedRunTime;
    }

    public void setProgress(float progress) {
      if (LOG.isDebugEnabled()) {
        if (!ProgressHelper.isProgressWithinRange(progress)) {
          LOG.debug("Progress update: speculator received progress in invalid "
              + "range={}", progress);
        }
      }
      this.progress = progress;
    }

    public boolean notHeartbeatedInAWhile(long now) {
      if (now - lastHeartBeatTime <= MAX_WAITTING_TIME_FOR_HEARTBEAT) {
        return false;
      } else {
        resetHeartBeatTime(now);
        return true;
      }
    }

    public void resetHeartBeatTime(long lastHeartBeatTime) {
      this.lastHeartBeatTime = lastHeartBeatTime;
    }
  }
}

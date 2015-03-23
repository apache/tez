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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

import com.google.common.base.Preconditions;

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
public class LegacySpeculator {
  
  private static final long ON_SCHEDULE = Long.MIN_VALUE;
  private static final long ALREADY_SPECULATING = Long.MIN_VALUE + 1;
  private static final long TOO_NEW = Long.MIN_VALUE + 2;
  private static final long PROGRESS_IS_GOOD = Long.MIN_VALUE + 3;
  private static final long NOT_RUNNING = Long.MIN_VALUE + 4;
  private static final long TOO_LATE_TO_SPECULATE = Long.MIN_VALUE + 5;

  private static final long SOONEST_RETRY_AFTER_NO_SPECULATE = 1000L * 1L;
  private static final long SOONEST_RETRY_AFTER_SPECULATE = 1000L * 15L;

  private static final double PROPORTION_RUNNING_TASKS_SPECULATABLE = 0.1;
  private static final double PROPORTION_TOTAL_TASKS_SPECULATABLE = 0.01;
  private static final int  MINIMUM_ALLOWED_SPECULATIVE_TASKS = 10;

  private static final Logger LOG = LoggerFactory.getLogger(LegacySpeculator.class);

  private final ConcurrentMap<TezTaskID, Boolean> runningTasks
      = new ConcurrentHashMap<TezTaskID, Boolean>();

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

  private final Clock clock;
  private long nextSpeculateTime = Long.MIN_VALUE;

  public LegacySpeculator(Configuration conf, AppContext context, Vertex vertex) {
    this(conf, context.getClock(), vertex);
  }

  public LegacySpeculator(Configuration conf, Clock clock, Vertex vertex) {
    this(conf, getEstimator(conf, vertex), clock, vertex);
  }
  
  static private TaskRuntimeEstimator getEstimator
      (Configuration conf, Vertex vertex) {
    TaskRuntimeEstimator estimator = new LegacyTaskRuntimeEstimator();
    estimator.contextualize(conf, vertex);
    
    return estimator;
  }

  // This constructor is designed to be called by other constructors.
  //  However, it's public because we do use it in the test cases.
  // Normally we figure out our own estimator.
  public LegacySpeculator
      (Configuration conf, TaskRuntimeEstimator estimator, Clock clock, Vertex vertex) {
    this.vertex = vertex;
    this.estimator = estimator;
    this.clock = clock;
  }

/*   *************************************************************    */

  void maybeSpeculate() {
    long now = clock.getTime();
    
    if (now < nextSpeculateTime) {
      return;
    }
    
    int speculations = maybeScheduleASpeculation();
    long mininumRecomp
        = speculations > 0 ? SOONEST_RETRY_AFTER_SPECULATE
                           : SOONEST_RETRY_AFTER_NO_SPECULATE;

    long wait = Math.max(mininumRecomp,
          clock.getTime() - now);
    nextSpeculateTime = now + wait;

    if (speculations > 0) {
      LOG.info("We launched " + speculations
          + " speculations.  Waiting " + wait + " milliseconds.");
    }
  }

/*   *************************************************************    */

  public void notifyAttemptStarted(TezTaskAttemptID taId, long timestamp) {
    estimator.enrollAttempt(taId, timestamp);    
  }
  
  public void notifyAttemptStatusUpdate(TezTaskAttemptID taId, TaskAttemptState reportedState,
      long timestamp) {
    statusUpdate(taId, reportedState, timestamp);
    maybeSpeculate();
  }

  /**
   * Absorbs one TaskAttemptStatus
   *
   * @param reportedStatus the status report that we got from a task attempt
   *        that we want to fold into the speculation data for this job
   * @param timestamp the time this status corresponds to.  This matters
   *        because statuses contain progress.
   */
  private void statusUpdate(TezTaskAttemptID attemptID, TaskAttemptState reportedState, long timestamp) {

    TezTaskID taskID = attemptID.getTaskID();
    Task task = vertex.getTask(taskID);

    Preconditions.checkState(task != null, "Null task for attempt: " + attemptID);

    estimator.updateAttempt(attemptID, reportedState, timestamp);

    //if (stateString.equals(TaskAttemptState.RUNNING.name())) {
    if (reportedState == TaskAttemptState.RUNNING) {
      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
    } else {
      runningTasks.remove(taskID, Boolean.TRUE);
      //if (!stateString.equals(TaskAttemptState.STARTING.name())) {
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
  private long speculationValue(Task task, long now) {
    Map<TezTaskAttemptID, TaskAttempt> attempts = task.getAttempts();
    TezTaskID taskID = task.getTaskId();
    long acceptableRuntime = Long.MIN_VALUE;
    long result = Long.MIN_VALUE;

    // short circuit completed tasks. no need to spend time on them
    if (task.getState() == TaskState.SUCCEEDED) {
      return NOT_RUNNING;
    }
    
    if (!mayHaveSpeculated.contains(taskID)) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    TezTaskAttemptID runningTaskAttemptID = null;

    int numberRunningAttempts = 0;

    for (TaskAttempt taskAttempt : attempts.values()) {
      if (taskAttempt.getState() == TaskAttemptState.RUNNING
          || taskAttempt.getState() == TaskAttemptState.STARTING) {
        if (++numberRunningAttempts > 1) {
          return ALREADY_SPECULATING;
        }
        runningTaskAttemptID = taskAttempt.getID();

        long estimatedRunTime = estimator.estimatedRuntime(runningTaskAttemptID);

        long taskAttemptStartTime
            = estimator.attemptEnrolledTime(runningTaskAttemptID);
        if (taskAttemptStartTime > now) {
          // This background process ran before we could process the task
          //  attempt status change that chronicles the attempt start
          return TOO_NEW;
        }

        long estimatedEndTime = estimatedRunTime + taskAttemptStartTime;

        long estimatedReplacementEndTime
            = now + estimator.newAttemptEstimatedRuntime();

        float progress = taskAttempt.getProgress();
        TaskAttemptHistoryStatistics data =
            runningTaskAttemptStatistics.get(runningTaskAttemptID);
        if (data == null) {
          runningTaskAttemptStatistics.put(runningTaskAttemptID,
            new TaskAttemptHistoryStatistics(estimatedRunTime, progress, now));
        } else {
          if (estimatedRunTime == data.getEstimatedRunTime()
              && progress == data.getProgress()) {
            // Previous stats are same as same stats
            if (data.notHeartbeatedInAWhile(now)) {
              // Stats have stagnated for a while, simulate heart-beat.
              // Now simulate the heart-beat
              statusUpdate(taskAttempt.getID(), taskAttempt.getState(), clock.getTime());
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

    // If we are here, there's at most one task attempt.
    if (numberRunningAttempts == 0) {
      return NOT_RUNNING;
    }



    if (acceptableRuntime == Long.MIN_VALUE) {
      acceptableRuntime = estimator.thresholdRuntime(taskID);
      if (acceptableRuntime == Long.MAX_VALUE) {
        return ON_SCHEDULE;
      }
    }

    return result;
  }

  //Add attempt to a given Task.
  protected void addSpeculativeAttempt(TezTaskID taskID) {
    LOG.info("DefaultSpeculator.addSpeculativeAttempt -- we are speculating " + taskID);
    vertex.scheduleSpeculativeTask(taskID);
    mayHaveSpeculated.add(taskID);
  }

  private int maybeScheduleASpeculation() {
    int successes = 0;

    long now = clock.getTime();

    int numberSpeculationsAlready = 0;
    int numberRunningTasks = 0;

    Map<TezTaskID, Task> tasks = vertex.getTasks();

    int numberAllowedSpeculativeTasks
        = (int) Math.max(MINIMUM_ALLOWED_SPECULATIVE_TASKS,
                         PROPORTION_TOTAL_TASKS_SPECULATABLE * tasks.size());

    TezTaskID bestTaskID = null;
    long bestSpeculationValue = -1L;

    // this loop is potentially pricey.
    // TODO track the tasks that are potentially worth looking at
    for (Map.Entry<TezTaskID, Task> taskEntry : tasks.entrySet()) {
      long mySpeculationValue = speculationValue(taskEntry.getValue(), now);

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
                         PROPORTION_RUNNING_TASKS_SPECULATABLE * numberRunningTasks);

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

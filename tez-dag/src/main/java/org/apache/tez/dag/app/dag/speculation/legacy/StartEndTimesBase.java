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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;

/**
 * Base class that uses the attempt runtime estimations from a derived class
 * and uses it to determine outliers based on deviating beyond the mean
 * estimated runtime by some threshold
 */
abstract class StartEndTimesBase implements TaskRuntimeEstimator {
  static final float MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE
      = 0.05F;
  static final int MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
      = 1;

  protected Vertex vertex;

  protected final Map<TezTaskAttemptID, Long> startTimes
      = new ConcurrentHashMap<TezTaskAttemptID, Long>();

  protected final DataStatistics taskStatistics = new DataStatistics();

  private float slowTaskRelativeTresholds;

  protected final Set<Task> doneTasks = new HashSet<Task>();

  @Override
  public void enrollAttempt(TezTaskAttemptID id, long timestamp) {
    startTimes.put(id, timestamp);
  }

  @Override
  public long attemptEnrolledTime(TezTaskAttemptID attemptID) {
    Long result = startTimes.get(attemptID);

    return result == null ? Long.MAX_VALUE : result;
  }

  @Override
  public void contextualize(Configuration conf, Vertex vertex) {
    slowTaskRelativeTresholds = conf.getFloat(
        TezConfiguration.TEZ_AM_LEGACY_SPECULATIVE_SLOWTASK_THRESHOLD, 1.0f);
    this.vertex = vertex;
  }

  protected DataStatistics dataStatisticsForTask(TezTaskID taskID) {
    return taskStatistics;
  }

  @Override
  public long thresholdRuntime(TezTaskID taskID) {
    int completedTasks = vertex.getCompletedTasks();

    int totalTasks = vertex.getTotalTasks();
    
    if (completedTasks < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
        || (((float)completedTasks) / totalTasks)
              < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE ) {
      return Long.MAX_VALUE;
    }
    
    long result = (long)taskStatistics.outlier(slowTaskRelativeTresholds);
    return result;
  }

  @Override
  public long newAttemptEstimatedRuntime() {
    return (long)taskStatistics.mean();
  }

  @Override
  public void updateAttempt(TezTaskAttemptID attemptID, TaskAttemptState state, long timestamp) {

    Task task = vertex.getTask(attemptID.getTaskID());

    if (task == null) {
      return;
    }

    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;
    
    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      boolean isNew = false;
      // is this  a new success?
      synchronized (doneTasks) {
        if (!doneTasks.contains(task)) {
          doneTasks.add(task);
          isNew = true;
        }
      }

      // It's a new completion
      // Note that if a task completes twice [because of a previous speculation
      //  and a race, or a success followed by loss of the machine with the
      //  local data] we only count the first one.
      if (isNew) {
        long finish = timestamp;
        if (start > 1L && finish > 1L && start <= finish) {
          long duration = finish - start;
          taskStatistics.add(duration);
        }
      }
    }
  }
}

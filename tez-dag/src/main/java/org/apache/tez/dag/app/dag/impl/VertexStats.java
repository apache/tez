/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.dag.app.dag.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezTaskID;

public class VertexStats {

  long firstTaskStartTime = -1;
  Set<TezTaskID> firstTasksToStart = new HashSet<TezTaskID>();
  long lastTaskFinishTime = -1;
  Set<TezTaskID> lastTasksToFinish = new HashSet<TezTaskID>();

  long minTaskDuration = -1;
  long maxTaskDuration = -1;
  double avgTaskDuration = -1;
  long numSuccessfulTasks = 0;

  Set<TezTaskID> shortestDurationTasks = new HashSet<TezTaskID>();
  Set<TezTaskID> longestDurationTasks = new HashSet<TezTaskID>();

  public long getFirstTaskStartTime() {
    return firstTaskStartTime;
  }

  public Set<TezTaskID> getFirstTaskToStart() {
    return Collections.unmodifiableSet(firstTasksToStart);
  }

  public long getLastTaskFinishTime() {
    return lastTaskFinishTime;
  }

  public Set<TezTaskID> getLastTaskToFinish() {
    return Collections.unmodifiableSet(lastTasksToFinish);
  }

  public long getMinTaskDuration() {
    return minTaskDuration;
  }

  public long getMaxTaskDuration() {
    return maxTaskDuration;
  }

  public double getAvgTaskDuration() {
    return avgTaskDuration;
  }

  public Set<TezTaskID> getShortestDurationTask() {
    return Collections.unmodifiableSet(shortestDurationTasks);
  }

  public Set<TezTaskID> getLongestDurationTask() {
    return Collections.unmodifiableSet(longestDurationTasks);
  }

  void updateStats(TaskReport taskReport) {
    if (firstTaskStartTime == -1
      || firstTaskStartTime >= taskReport.getStartTime()) {
      if (firstTaskStartTime != taskReport.getStartTime()) {
        firstTasksToStart.clear();
      }
      firstTasksToStart.add(taskReport.getTaskId());
      firstTaskStartTime = taskReport.getStartTime();
    }
    if ((taskReport.getFinishTime() > 0) &&
        (lastTaskFinishTime == -1
        || lastTaskFinishTime <= taskReport.getFinishTime())) {
      if (lastTaskFinishTime != taskReport.getFinishTime()) {
        lastTasksToFinish.clear();
      }
      lastTasksToFinish.add(taskReport.getTaskId());
      lastTaskFinishTime = taskReport.getFinishTime();
    }

    if (!taskReport.getTaskState().equals(
        TaskState.SUCCEEDED)) {
      // ignore non-successful tasks when calculating durations
      return;
    }

    long taskDuration = taskReport.getFinishTime() -
        taskReport.getStartTime();
    if (taskDuration < 0) {
      return;
    }

    ++numSuccessfulTasks;
    if (minTaskDuration == -1
      || minTaskDuration >= taskDuration) {
      if (minTaskDuration != taskDuration) {
        shortestDurationTasks.clear();
      }
      minTaskDuration = taskDuration;
      shortestDurationTasks.add(taskReport.getTaskId());
    }
    if (maxTaskDuration == -1
      || maxTaskDuration <= taskDuration) {
      if (maxTaskDuration != taskDuration) {
        longestDurationTasks.clear();
      }
      maxTaskDuration = taskDuration;
      longestDurationTasks.add(taskReport.getTaskId());
    }

    avgTaskDuration = ((avgTaskDuration * (numSuccessfulTasks-1)) + taskDuration)
        /numSuccessfulTasks;
  }

  private void appendTaskIdSet(StringBuilder sb,
      Set<TezTaskID> taskIDs) {
    sb.append("[ ");
    boolean first = true;
    if (taskIDs != null) {
      for (TezTaskID tezTaskID : taskIDs) {
        if (!first) {
          sb.append(",");
        } else {
          first = false;
        }
        sb.append(tezTaskID.toString());
      }
    }
    sb.append(" ]");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("firstTaskStartTime=").append(firstTaskStartTime)
       .append(", firstTasksToStart=");
    appendTaskIdSet(sb, firstTasksToStart);
    sb.append(", lastTaskFinishTime=").append(lastTaskFinishTime)
       .append(", lastTasksToFinish=");
    appendTaskIdSet(sb, lastTasksToFinish);
    sb.append(", minTaskDuration=").append(minTaskDuration)
       .append(", maxTaskDuration=").append(maxTaskDuration)
       .append(", avgTaskDuration=").append(avgTaskDuration)
       .append(", numSuccessfulTasks=").append(numSuccessfulTasks)
       .append(", shortestDurationTasks=");
    appendTaskIdSet(sb, shortestDurationTasks);
    sb.append(", longestDurationTasks=");
    appendTaskIdSet(sb, longestDurationTasks);
    return sb.toString();
  }

}

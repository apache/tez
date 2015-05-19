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

package org.apache.tez.history.parser.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
public class TaskInfo extends BaseInfo {

  private final long startTime;
  private final long endTime;
  private final String diagnostics;
  private final String successfulAttemptId;
  private final long scheduledTime;
  private final String status;
  private final String taskId;

  private VertexInfo vertexInfo;

  private Map<String, TaskAttemptInfo> attemptInfoMap = Maps
      .newHashMap();

  TaskInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    Preconditions.checkArgument(
        jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
            (Constants.TEZ_TASK_ID));

    taskId = jsonObject.optString(Constants.ENTITY);

    //Parse additional Info
    final JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);
    startTime = otherInfoNode.optLong(Constants.START_TIME);
    endTime = otherInfoNode.optLong(Constants.FINISH_TIME);
    diagnostics = otherInfoNode.optString(Constants.DIAGNOSTICS);
    successfulAttemptId = otherInfoNode.optString(Constants.SUCCESSFUL_ATTEMPT_ID);
    scheduledTime = otherInfoNode.optLong(Constants.SCHEDULED_TIME);
    status = otherInfoNode.optString(Constants.STATUS);
  }

  @Override
  public final long getStartTime() {
    return startTime - (vertexInfo.getDagInfo().getAbsStartTime());
  }

  public final long getAbsStartTime() {
    return startTime;
  }

  public final long getAbsFinishTime() {
    return endTime;
  }

  @Override
  public final long getFinishTime() {
    long taskFinishTime =  endTime - (vertexInfo.getDagInfo().getAbsStartTime());
    if (taskFinishTime < 0) {
      //probably vertex is not complete or failed in middle. get the last task attempt time
      for (TaskAttemptInfo attemptInfo : getTaskAttempts()) {
        taskFinishTime = (attemptInfo.getFinishTime() > taskFinishTime)
            ? attemptInfo.getFinishTime() : taskFinishTime;
      }
    }
    return taskFinishTime;
  }

  @Override
  public final String getDiagnostics() {
    return diagnostics;
  }

  public static TaskInfo create(JSONObject taskInfoObject) throws
      JSONException {
    return new TaskInfo(taskInfoObject);
  }

  void addTaskAttemptInfo(TaskAttemptInfo taskAttemptInfo) {
    attemptInfoMap.put(taskAttemptInfo.getTaskAttemptId(), taskAttemptInfo);
  }

  void setVertexInfo(VertexInfo vertexInfo) {
    Preconditions.checkArgument(vertexInfo != null, "Provide valid vertexInfo");
    this.vertexInfo = vertexInfo;
    //link it to vertex
    vertexInfo.addTaskInfo(this);
  }

  public final VertexInfo getVertexInfo() {
    return vertexInfo;
  }

  /**
   * Get all task attempts
   *
   * @return list of task attempt info
   */
  public final List<TaskAttemptInfo> getTaskAttempts() {
    List<TaskAttemptInfo> attemptsList = Lists.newLinkedList(attemptInfoMap.values());
    Collections.sort(attemptsList, orderingOnAttemptStartTime());
    return Collections.unmodifiableList(attemptsList);
  }

  /**
   * Get list of failed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskAttemptInfo> getFailedTaskAttempts() {
    return getTaskAttempts(TaskAttemptState.FAILED);
  }

  /**
   * Get list of killed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskAttemptInfo> getKilledTaskAttempts() {
    return getTaskAttempts(TaskAttemptState.KILLED);
  }

  /**
   * Get list of failed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskAttemptInfo> getSuccessfulTaskAttempts() {
    return getTaskAttempts(TaskAttemptState.SUCCEEDED);
  }

  /**
   * Get list of tasks belonging to a specific state
   *
   * @param state
   * @return Collection<TaskAttemptInfo>
   */
  public final List<TaskAttemptInfo> getTaskAttempts(final TaskAttemptState state) {
    return Collections.unmodifiableList(Lists.newLinkedList(Iterables.filter(Lists.newLinkedList
                    (attemptInfoMap.values()), new Predicate<TaskAttemptInfo>() {
                  @Override public boolean apply(TaskAttemptInfo input) {
                    return input.getStatus() != null && input.getStatus().equals(state.toString());
                  }
                }
            )
        )
    );
  }

  /**
   * Get the set of containers on which the task attempts ran for this task
   *
   * @return Multimap<Container, TaskAttemptInfo> task attempt details at container level
   */
  public final Multimap<Container, TaskAttemptInfo> getContainersMapping() {
    Multimap<Container, TaskAttemptInfo> containerMapping = LinkedHashMultimap.create();
    for (TaskAttemptInfo attemptInfo : getTaskAttempts()) {
      containerMapping.put(attemptInfo.getContainer(), attemptInfo);
    }
    return Multimaps.unmodifiableMultimap(containerMapping);
  }

  /**
   * Get the successful task attempt
   *
   * @return TaskAttemptInfo
   */
  public final TaskAttemptInfo getSuccessfulTaskAttempt() {
    for (TaskAttemptInfo attemptInfo : getTaskAttempts()) {
      if (attemptInfo.getStatus().equalsIgnoreCase(TaskAttemptState.SUCCEEDED.toString())) {
        return attemptInfo;
      }
    }
    return null;
  }

  /**
   * Get last task attempt to finish
   *
   * @return TaskAttemptInfo
   */
  public final TaskAttemptInfo getLastTaskAttemptToFinish() {
    List<TaskAttemptInfo> attemptsList = getTaskAttempts();
    if (attemptsList.isEmpty()) {
      return null;
    }

    return Ordering.from(new Comparator<TaskAttemptInfo>() {
      @Override public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getFinishTime() < o2.getFinishTime()) ? -1 :
            ((o1.getFinishTime() == o2.getFinishTime()) ?
                0 : 1);
      }
    }).max(attemptsList);
  }

  /**
   * Get average task attempt duration. Includes succesful and failed tasks
   *
   * @return float
   */
  public final float getAvgTaskAttemptDuration() {
    float totalTaskDuration = 0;
    List<TaskAttemptInfo> attemptsList = getTaskAttempts();
    if (attemptsList.size() == 0) {
      return 0;
    }
    for (TaskAttemptInfo attemptInfo : attemptsList) {
      totalTaskDuration += attemptInfo.getTimeTaken();
    }
    return ((totalTaskDuration * 1.0f) / attemptsList.size());
  }

  private Ordering<TaskAttemptInfo> orderingOnTimeTaken() {
    return Ordering.from(new Comparator<TaskAttemptInfo>() {
      @Override public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getTimeTaken() < o2.getTimeTaken()) ? -1 :
            ((o1.getTimeTaken() == o2.getTimeTaken()) ?
                0 : 1);
      }
    });
  }

  private Ordering<TaskAttemptInfo> orderingOnAttemptStartTime() {
    return Ordering.from(new Comparator<TaskAttemptInfo>() {
      @Override public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getStartTime() < o2.getStartTime()) ? -1 :
            ((o1.getStartTime() == o2.getStartTime()) ? 0 : 1);
      }
    });
  }

  /**
   * Get min task attempt duration.  This includes successful/failed task attempts as well
   *
   * @return long
   */
  public final long getMinTaskAttemptDuration() {
    List<TaskAttemptInfo> attemptsList = getTaskAttempts();
    if (attemptsList.isEmpty()) {
      return 0;
    }

    return orderingOnTimeTaken().min(attemptsList).getTimeTaken();
  }

  /**
   * Get max task attempt duration.  This includes successful/failed task attempts as well
   *
   * @return long
   */
  public final long getMaxTaskAttemptDuration() {
    List<TaskAttemptInfo> attemptsList = getTaskAttempts();
    if (attemptsList.isEmpty()) {
      return 0;
    }

    return orderingOnTimeTaken().max(attemptsList).getTimeTaken();
  }

  public final int getNumberOfTaskAttempts() {
    return getTaskAttempts().size();
  }

  public final String getStatus() {
    return status;
  }

  public final String getTaskId() {
    return taskId;
  }

  public final long getTimeTaken() {
    return getFinishTime() - getStartTime();
  }

  public final String getSuccessfulAttemptId() {
    return successfulAttemptId;
  }

  public final long getAbsoluteScheduleTime() {
    return scheduledTime;
  }

  public final long getScheduledTime() {
    return scheduledTime - this.getVertexInfo().getDagInfo().getAbsStartTime();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("taskId=").append(getTaskId()).append(", ");
    sb.append("scheduledTime=").append(getAbsoluteScheduleTime()).append(", ");
    sb.append("startTime=").append(getStartTime()).append(", ");
    sb.append("finishTime=").append(getFinishTime()).append(", ");
    sb.append("timeTaken=").append(getTimeTaken()).append(", ");
    sb.append("events=").append(getEvents()).append(", ");
    sb.append("diagnostics=").append(getDiagnostics()).append(", ");
    sb.append("successfulAttempId=").append(getSuccessfulAttemptId()).append(", ");
    sb.append("status=").append(getStatus()).append(", ");
    sb.append("vertexName=").append(getVertexInfo().getVertexName());
    sb.append("]");
    return sb.toString();
  }
}

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
import com.google.common.collect.Maps;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Map;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;
import static org.apache.hadoop.classification.InterfaceAudience.Public;

@Public
@Evolving
public class TaskAttemptInfo extends BaseInfo {

  private final String taskAttemptId;
  private final long startTime;
  private final long endTime;
  private final String diagnostics;
  private final String successfulAttemptId;
  private final long scheduledTime;
  private final String containerId;
  private final String nodeId;
  private final String status;
  private final String logUrl;

  private TaskInfo taskInfo;

  private Container container;

  TaskAttemptInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    Preconditions.checkArgument(
        jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
            (Constants.TEZ_TASK_ATTEMPT_ID));

    taskAttemptId = jsonObject.optString(Constants.ENTITY);

    //Parse additional Info
    final JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);
    startTime = otherInfoNode.optLong(Constants.START_TIME);
    endTime = otherInfoNode.optLong(Constants.FINISH_TIME);
    diagnostics = otherInfoNode.optString(Constants.DIAGNOSTICS);
    successfulAttemptId = otherInfoNode.optString(Constants.SUCCESSFUL_ATTEMPT_ID);
    scheduledTime = otherInfoNode.optLong(Constants.SCHEDULED_TIME);

    containerId = otherInfoNode.optString(Constants.CONTAINER_ID);
    String id = otherInfoNode.optString(Constants.NODE_ID);
    nodeId = (id != null) ? (id.split(":")[0]) : "";
    logUrl = otherInfoNode.optString(Constants.COMPLETED_LOGS_URL);

    status = otherInfoNode.optString(Constants.STATUS);
    container = new Container(containerId, nodeId);
  }

  void setTaskInfo(TaskInfo taskInfo) {
    Preconditions.checkArgument(taskInfo != null, "Provide valid taskInfo");
    this.taskInfo = taskInfo;
    taskInfo.addTaskAttemptInfo(this);
  }

  @Override
  public final long getStartTime() {
    return startTime - (getTaskInfo().getVertexInfo().getDagInfo().getAbsStartTime());
  }

  @Override
  public final long getFinishTime() {
    return endTime - (getTaskInfo().getVertexInfo().getDagInfo().getAbsStartTime());
  }

  public final long getAbsStartTime() {
    return startTime;
  }

  public final long getAbsFinishTime() {
    return endTime;
  }

  public final long getAbsoluteScheduledTime() {
    return scheduledTime;
  }

  public final long getTimeTaken() {
    return getFinishTime() - getStartTime();
  }

  public final long getScheduledTime() {
    return scheduledTime - (getTaskInfo().getVertexInfo().getDagInfo().getAbsStartTime());
  }

  @Override
  public final String getDiagnostics() {
    return diagnostics;
  }

  public static TaskAttemptInfo create(JSONObject taskInfoObject) throws JSONException {
    return new TaskAttemptInfo(taskInfoObject);
  }

  public final boolean isLocalityInfoAvailable() {
    Map<String, TezCounter> dataLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.DATA_LOCAL_TASKS.toString());
    Map<String, TezCounter> rackLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.RACK_LOCAL_TASKS.toString());

    Map<String, TezCounter> otherLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.OTHER_LOCAL_TASKS.toString());

    if (!dataLocalTask.isEmpty() || !rackLocalTask.isEmpty() || !otherLocalTask.isEmpty()) {
      return true;
    }
    return false;
  }

  public final TezCounter getLocalityInfo() {
    Map<String, TezCounter> dataLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.DATA_LOCAL_TASKS.toString());
    Map<String, TezCounter> rackLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.RACK_LOCAL_TASKS.toString());
    Map<String, TezCounter> otherLocalTask = getCounter(DAGCounter.class.getName(),
        DAGCounter.OTHER_LOCAL_TASKS.toString());

    if (!dataLocalTask.isEmpty()) {
      return dataLocalTask.get(DAGCounter.class.getName());
    }

    if (!rackLocalTask.isEmpty()) {
      return rackLocalTask.get(DAGCounter.class.getName());
    }

    if (!otherLocalTask.isEmpty()) {
      return otherLocalTask.get(DAGCounter.class.getName());
    }
    return null;
  }

  public final TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public final String getTaskAttemptId() {
    return taskAttemptId;
  }

  public final String getSuccessfulAttemptId() {
    return successfulAttemptId;
  }

  public final String getNodeId() {
    return nodeId;
  }

  public final String getStatus() {
    return status;
  }

  public final Container getContainer() {
    return container;
  }

  public final String getLogURL() {
    return logUrl;
  }

  /**
   * Get merge counter per source. Available in case of reducer task
   *
   * @return Map<String, TezCounter> merge phase time at every counter group level
   */
  public final Map<String, TezCounter> getMergePhaseTime() {
    return getCounter(null, TaskCounter.MERGE_PHASE_TIME.name());
  }

  /**
   * Get shuffle counter per source. Available in case of shuffle
   *
   * @return Map<String, TezCounter> shuffle phase time at every counter group level
   */
  public final Map<String, TezCounter> getShufflePhaseTime() {
    return getCounter(null, TaskCounter.SHUFFLE_PHASE_TIME.name());
  }

  /**
   * Get OUTPUT_BYTES counter per source. Available in case of map outputs
   *
   * @return Map<String, TezCounter> output bytes counter at every counter group
   */
  public final Map<String, TezCounter> getTaskOutputBytes() {
    return getCounter(null, TaskCounter.OUTPUT_BYTES.name());
  }

  /**
   * Get number of spills per source.  (SPILLED_RECORDS / OUTPUT_RECORDS)
   *
   * @return Map<String, Long> spill count details
   */
  public final Map<String, Float> getSpillCount() {
    Map<String, TezCounter> outputRecords = getCounter(null, "OUTPUT_RECORDS");
    Map<String, TezCounter> spilledRecords = getCounter(null, "SPILLED_RECORDS");
    Map<String, Float> result = Maps.newHashMap();
    for (Map.Entry<String, TezCounter> entry : spilledRecords.entrySet()) {
      String source = entry.getKey();
      long spilledVal = entry.getValue().getValue();
      long outputVal = outputRecords.get(source).getValue();
      result.put(source, (spilledVal * 1.0f) / (outputVal * 1.0f));
    }
    return result;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("taskAttemptId=").append(getTaskAttemptId()).append(", ");
    sb.append("scheduledTime=").append(getScheduledTime()).append(", ");
    sb.append("startTime=").append(getStartTime()).append(", ");
    sb.append("finishTime=").append(getFinishTime()).append(", ");
    sb.append("timeTaken=").append(getTimeTaken()).append(", ");
    sb.append("events=").append(getEvents()).append(", ");
    sb.append("diagnostics=").append(getDiagnostics()).append(", ");
    sb.append("successfulAttempId=").append(getSuccessfulAttemptId()).append(", ");
    sb.append("container=").append(getContainer()).append(", ");
    sb.append("nodeId=").append(getNodeId()).append(", ");
    sb.append("logURL=").append(getLogURL()).append(", ");
    sb.append("status=").append(getStatus());
    sb.append("]");
    return sb.toString();
  }
}

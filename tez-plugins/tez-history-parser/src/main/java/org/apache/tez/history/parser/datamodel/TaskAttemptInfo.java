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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.history.parser.utils.Utils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;
import static org.apache.hadoop.classification.InterfaceAudience.Public;

@Public
@Evolving
public class TaskAttemptInfo extends BaseInfo {

  private static final Log LOG = LogFactory.getLog(TaskAttemptInfo.class);

  private static final String SUCCEEDED = StringInterner.weakIntern(TaskAttemptState.SUCCEEDED.name());

  private final String taskAttemptId;
  private final long startTime;
  private final long endTime;
  private final String diagnostics;

  private final long creationTime;
  private final long allocationTime;
  private final String containerId;
  private final String nodeId;
  private final String status;
  private final String logUrl;
  private final String creationCausalTA;
  private final String terminationCause;
  private final long executionTimeInterval;
  // this list is in time order - array list for easy walking
  private final ArrayList<DataDependencyEvent> lastDataEvents = Lists.newArrayList();

  private TaskInfo taskInfo;

  private Container container;
  
  public static class DataDependencyEvent {
    String taId;
    long timestamp;
    public DataDependencyEvent(String id, long time) {
      taId = id;
      timestamp = time;
    }
    public long getTimestamp() {
      return timestamp;
    }
    public String getTaskAttemptId() {
      return taId;
    }
  }

  TaskAttemptInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    Preconditions.checkArgument(
        jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
            (Constants.TEZ_TASK_ATTEMPT_ID));

    taskAttemptId = StringInterner.weakIntern(jsonObject.optString(Constants.ENTITY));

    //Parse additional Info
    final JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);

    long sTime = otherInfoNode.optLong(Constants.START_TIME);
    long eTime = otherInfoNode.optLong(Constants.FINISH_TIME);
    if (eTime < sTime) {
      LOG.warn("TaskAttemptInfo has got wrong start/end values. "
          + "startTime=" + sTime + ", endTime=" + eTime + ". Will check "
          + "timestamps in DAG started/finished events");

      // Check if events TASK_STARTED, TASK_FINISHED can be made use of
      for(Event event : eventList) {
        switch (HistoryEventType.valueOf(event.getType())) {
        case TASK_ATTEMPT_STARTED:
          sTime = event.getAbsoluteTime();
          break;
        case TASK_ATTEMPT_FINISHED:
          eTime = event.getAbsoluteTime();
          break;
        default:
          break;
        }
      }

      if (eTime < sTime) {
        LOG.warn("TaskAttemptInfo has got wrong start/end values in events as well. "
            + "startTime=" + sTime + ", endTime=" + eTime);
      }
    }
    startTime = sTime;
    endTime = eTime;

    diagnostics = otherInfoNode.optString(Constants.DIAGNOSTICS);
    creationTime = otherInfoNode.optLong(Constants.CREATION_TIME);
    creationCausalTA = StringInterner.weakIntern(
        otherInfoNode.optString(Constants.CREATION_CAUSAL_ATTEMPT));
    allocationTime = otherInfoNode.optLong(Constants.ALLOCATION_TIME);
    containerId = StringInterner.weakIntern(otherInfoNode.optString(Constants.CONTAINER_ID));
    String id = otherInfoNode.optString(Constants.NODE_ID);
    nodeId = StringInterner.weakIntern((id != null) ? (id.split(":")[0]) : "");
    logUrl = otherInfoNode.optString(Constants.COMPLETED_LOGS_URL);

    status = StringInterner.weakIntern(otherInfoNode.optString(Constants.STATUS));
    container = new Container(containerId, nodeId);
    if (otherInfoNode.has(Constants.LAST_DATA_EVENTS)) {
      List<DataDependencyEvent> eventInfo = Utils.parseDataEventDependencyFromJSON(
          otherInfoNode.optJSONObject(Constants.LAST_DATA_EVENTS));
      long lastTime = 0;
      for (DataDependencyEvent item : eventInfo) {
        // check these are in time order
        Preconditions.checkState(lastTime < item.getTimestamp());
        lastTime = item.getTimestamp();
        lastDataEvents.add(item);
      }
    }
    terminationCause = StringInterner
        .weakIntern(otherInfoNode.optString(ATSConstants.TASK_ATTEMPT_ERROR_ENUM));
    executionTimeInterval = (endTime > startTime) ? (endTime - startTime) : 0;
  }
  
  public static Ordering<TaskAttemptInfo> orderingOnAllocationTime() {
    return Ordering.from(new Comparator<TaskAttemptInfo>() {
      @Override
      public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getAllocationTime() < o2.getAllocationTime() ? -1
            : o1.getAllocationTime() > o2.getAllocationTime() ? 1 : 0);
      }
    });
  }

  void setTaskInfo(TaskInfo taskInfo) {
    Preconditions.checkArgument(taskInfo != null, "Provide valid taskInfo");
    this.taskInfo = taskInfo;
    taskInfo.addTaskAttemptInfo(this);
  }

  @Override
  public final long getStartTimeInterval() {
    return startTime - (getTaskInfo().getVertexInfo().getDagInfo().getStartTime());
  }

  @Override
  public final long getFinishTimeInterval() {
    return endTime - (getTaskInfo().getVertexInfo().getDagInfo().getStartTime());
  }
  
  public final boolean isSucceeded() {
    return status.equals(SUCCEEDED);
  }
  
  public final List<DataDependencyEvent> getLastDataEvents() {
    return lastDataEvents;
  }
  
  public final long getExecutionTimeInterval() {
    return executionTimeInterval;
  }
  
  public final long getPostDataExecutionTimeInterval() {
    if (getStartTime() > 0 && getFinishTime() > 0) {
      // start time defaults to the actual start time
      long postDataStartTime = startTime;
      if (getLastDataEvents() != null && !getLastDataEvents().isEmpty()) {
        // if last data event is after the start time then use last data event time
        long lastEventTime = getLastDataEvents().get(getLastDataEvents().size()-1).getTimestamp();
        postDataStartTime = startTime > lastEventTime ? startTime : lastEventTime;
      }
      return (getFinishTime() - postDataStartTime);
    }
    return -1;
  }

  public final long getAllocationToEndTimeInterval() {
    return (endTime - allocationTime);
  }
  
  public final long getAllocationToStartTimeInterval() {
    return (startTime - allocationTime);
  }
  
  public final long getCreationToAllocationTimeInterval() {
    return (allocationTime - creationTime);
  }

  public final long getStartTime() {
    return startTime;
  }

  public final long getFinishTime() {
    return endTime;
  }

  public final long getCreationTime() {
    return creationTime;
  }
  
  public final DataDependencyEvent getLastDataEventInfo(long timeThreshold) {
    for (int i=lastDataEvents.size()-1; i>=0; i--) {
      // walk back in time until we get first event that happened before the threshold
      DataDependencyEvent item = lastDataEvents.get(i);
      if (item.getTimestamp() < timeThreshold) {
        return item;
      }
    }
    return null;
  }
  
  public final long getTimeTaken() {
    return getFinishTimeInterval() - getStartTimeInterval();
  }

  public final long getCreationTimeInterval() {
    return creationTime - (getTaskInfo().getVertexInfo().getDagInfo().getStartTime());
  }
  
  public final String getCreationCausalTA() {
    return creationCausalTA;
  }

  public final long getAllocationTime() {
    return allocationTime;
  }
  
  public final String getShortName() {
    return getTaskInfo().getVertexInfo().getVertexName() + " : " + 
    taskAttemptId.substring(taskAttemptId.lastIndexOf('_', taskAttemptId.lastIndexOf('_') - 1) + 1);
  }

  @Override
  public final String getDiagnostics() {
    return diagnostics;
  }
  
  public final String getTerminationCause() {
    return terminationCause;
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
  
  public final String getDetailedStatus() {
    if (!Strings.isNullOrEmpty(getTerminationCause())) {
      return getStatus() + ":" + getTerminationCause();
    }
    return getStatus();
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
    sb.append("creationTime=").append(getCreationTimeInterval()).append(", ");
    sb.append("startTime=").append(getStartTimeInterval()).append(", ");
    sb.append("finishTime=").append(getFinishTimeInterval()).append(", ");
    sb.append("timeTaken=").append(getTimeTaken()).append(", ");
    sb.append("events=").append(getEvents()).append(", ");
    sb.append("diagnostics=").append(getDiagnostics()).append(", ");
    sb.append("container=").append(getContainer()).append(", ");
    sb.append("nodeId=").append(getNodeId()).append(", ");
    sb.append("logURL=").append(getLogURL()).append(", ");
    sb.append("status=").append(getStatus());
    sb.append("]");
    return sb.toString();
  }
}

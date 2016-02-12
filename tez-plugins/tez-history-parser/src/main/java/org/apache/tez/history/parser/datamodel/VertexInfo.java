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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.history.HistoryEventType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
public class VertexInfo extends BaseInfo {

  private static final Log LOG = LogFactory.getLog(VertexInfo.class);

  private final String vertexId;
  private final String vertexName;
  private final long finishTime;
  private final long initTime;
  private final long initRequestedTime;
  private final long startTime;
  private final long startRequestedTime;
  
  private final String diagnostics;
  private final String processorClass;

  private final int numTasks;
  private final int failedTasks;
  private final int completedTasks;
  private final int succeededTasks;
  private final int killedTasks;
  private final int numFailedTaskAttempts;

  private final String status;

  //TaskID --> TaskInfo for internal reference
  private Map<String, TaskInfo> taskInfoMap;

  private final List<EdgeInfo> inEdgeList;
  private final List<EdgeInfo> outEdgeList;

  private final List<AdditionalInputOutputDetails> additionalInputInfoList;
  private final List<AdditionalInputOutputDetails> additionalOutputInfoList;
  
  private long avgPostDataExecutionTimeInterval = -1;

  private DagInfo dagInfo;

  VertexInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    Preconditions.checkArgument(
        jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
            (Constants.TEZ_VERTEX_ID));

    vertexId = StringInterner.weakIntern(jsonObject.optString(Constants.ENTITY));
    taskInfoMap = Maps.newHashMap();

    inEdgeList = Lists.newLinkedList();
    outEdgeList = Lists.newLinkedList();
    additionalInputInfoList = Lists.newLinkedList();
    additionalOutputInfoList = Lists.newLinkedList();

    //Parse additional Info
    JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);
    initRequestedTime = otherInfoNode.optLong(Constants.INIT_REQUESTED_TIME);
    startRequestedTime = otherInfoNode.optLong(Constants.START_REQUESTED_TIME);

    long sTime = otherInfoNode.optLong(Constants.START_TIME);
    long iTime = otherInfoNode.optLong(Constants.INIT_TIME);
    long eTime = otherInfoNode.optLong(Constants.FINISH_TIME);
    if (eTime < sTime) {
      LOG.warn("Vertex has got wrong start/end values. "
          + "startTime=" + sTime + ", endTime=" + eTime + ". Will check "
          + "timestamps in DAG started/finished events");

      // Check if events VERTEX_STARTED, VERTEX_FINISHED can be made use of
      for(Event event : eventList) {
        switch (HistoryEventType.valueOf(event.getType())) {
        case VERTEX_INITIALIZED:
          iTime = event.getAbsoluteTime();
          break;
        case VERTEX_STARTED:
          sTime = event.getAbsoluteTime();
          break;
        case VERTEX_FINISHED:
          eTime = event.getAbsoluteTime();
          break;
        default:
          break;
        }
      }

      if (eTime < sTime) {
        LOG.warn("Vertex has got wrong start/end values in events as well. "
            + "startTime=" + sTime + ", endTime=" + eTime);
      }
    }
    startTime = sTime;
    finishTime = eTime;
    initTime = iTime;


    diagnostics = otherInfoNode.optString(Constants.DIAGNOSTICS);
    numTasks = otherInfoNode.optInt(Constants.NUM_TASKS);
    failedTasks = otherInfoNode.optInt(Constants.NUM_FAILED_TASKS);
    succeededTasks =
        otherInfoNode.optInt(Constants.NUM_SUCCEEDED_TASKS);
    completedTasks =
        otherInfoNode.optInt(Constants.NUM_COMPLETED_TASKS);
    killedTasks = otherInfoNode.optInt(Constants.NUM_KILLED_TASKS);
    numFailedTaskAttempts =
        otherInfoNode.optInt(Constants.NUM_FAILED_TASKS_ATTEMPTS);
    vertexName = StringInterner.weakIntern(otherInfoNode.optString(Constants.VERTEX_NAME));
    processorClass = StringInterner.weakIntern(otherInfoNode.optString(Constants.PROCESSOR_CLASS_NAME));
    status = StringInterner.weakIntern(otherInfoNode.optString(Constants.STATUS));
  }

  public static VertexInfo create(JSONObject vertexInfoObject) throws
      JSONException {
    return new VertexInfo(vertexInfoObject);
  }

  /**
   * Update edge details with source and destination vertex objects.
   */
  private void updateEdgeInfo() {
    if (dagInfo.getNumVertices() == dagInfo.getVertices().size()) {
      //We can update EdgeInfo when all vertices are parsed
      Map<String, VertexInfo> vertexMapping = dagInfo.getVertexMapping();
      for (EdgeInfo edge : dagInfo.getEdges()) {
        VertexInfo sourceVertex = vertexMapping.get(edge.getInputVertexName());
        VertexInfo destinationVertex = vertexMapping.get(edge.getOutputVertexName());
        edge.setSourceVertex(sourceVertex);
        edge.setDestinationVertex(destinationVertex);
      }
    }
  }

  void addTaskInfo(TaskInfo taskInfo) {
    this.taskInfoMap.put(taskInfo.getTaskId(), taskInfo);
  }

  void setAdditionalInputInfoList(List<AdditionalInputOutputDetails> additionalInputInfoList) {
    this.additionalInputInfoList.clear();
    this.additionalInputInfoList.addAll(additionalInputInfoList);
  }
  
  void setAdditionalOutputInfoList(List<AdditionalInputOutputDetails> additionalOutputInfoList) {
    this.additionalOutputInfoList.clear();
    this.additionalOutputInfoList.addAll(additionalOutputInfoList);
  }

  void addInEdge(EdgeInfo edgeInfo) {
    this.inEdgeList.add(edgeInfo);
  }

  void addOutEdge(EdgeInfo edgeInfo) {
    this.outEdgeList.add(edgeInfo);
  }

  void setDagInfo(DagInfo dagInfo) {
    Preconditions.checkArgument(dagInfo != null, "Provide valid dagInfo");
    this.dagInfo = dagInfo;
    //link vertex to dagInfo
    dagInfo.addVertexInfo(this);
    updateEdgeInfo();
  }

  public List<AdditionalInputOutputDetails> getAdditionalInputInfoList() {
    return Collections.unmodifiableList(additionalInputInfoList);
  }

  public List<AdditionalInputOutputDetails> getAdditionalOutputInfoList() {
    return Collections.unmodifiableList(additionalOutputInfoList);
  }

  @Override
  public final long getStartTimeInterval() {
    return startTime - (dagInfo.getStartTime());
  }

  public final long getFirstTaskStartTimeInterval() {
    TaskInfo firstTask = getFirstTaskToStart();
    if (firstTask == null) {
      return 0;
    }
    return firstTask.getStartTimeInterval();
  }

  public final long getLastTaskFinishTimeInterval() {
    if (getLastTaskToFinish() == null || getLastTaskToFinish().getFinishTimeInterval() < 0) {
        return dagInfo.getFinishTimeInterval();
    }
    return getLastTaskToFinish().getFinishTimeInterval();
  }
  
  public final long getAvgPostDataExecutionTimeInterval() {
    if (avgPostDataExecutionTimeInterval == -1) {
      long totalExecutionTime = 0;
      long totalAttempts = 0;
      for (TaskInfo task : getTasks()) {
        TaskAttemptInfo attempt = task.getSuccessfulTaskAttempt();
        if (attempt != null) {
          // count only time after last data was received
          long execTime = attempt.getPostDataExecutionTimeInterval();
          if (execTime >= 0) {
            totalExecutionTime += execTime;
            totalAttempts++;
          }
        }
      }
      if (totalAttempts > 0) {
        avgPostDataExecutionTimeInterval = Math.round(totalExecutionTime*1.0/totalAttempts);
      }
    }
    return avgPostDataExecutionTimeInterval;
  }

  public final long getStartTime() {
    return startTime;
  }

  public final long getFinishTime() {
    return finishTime;
  }

  public final long getInitTime() {
    return initTime;
  }
  
  public final long getInitRequestedTime() {
    return initRequestedTime;
  }

  public final long getStartRequestedTime() {
    return startRequestedTime;
  }
  
  @Override
  public final long getFinishTimeInterval() {
    long vertexEndTime = finishTime - (dagInfo.getStartTime());
    if (vertexEndTime < 0) {
      //probably vertex is not complete or failed in middle. get the last task attempt time
      for (TaskInfo taskInfo : getTasks()) {
        vertexEndTime = (taskInfo.getFinishTimeInterval() > vertexEndTime)
            ? taskInfo.getFinishTimeInterval() : vertexEndTime;
      }
    }
    return vertexEndTime;
  }

  @Override
  public final String getDiagnostics() {
    return diagnostics;
  }

  public final String getVertexName() {
    return vertexName;
  }
  
  public final String getVertexId() {
    return vertexId;
  }

  //Quite possible that getFinishTime is not yet recorded for failed vertices (or killed vertices)
  //Start time of vertex infers that the dependencies are done and AM has inited it.
  public final long getTimeTaken() {
    return (getFinishTimeInterval() - getStartTimeInterval());
  }

  //Time taken for last task to finish  - time taken for first task to start
  public final long getTimeTakenForTasks() {
    return (getLastTaskFinishTimeInterval() - getFirstTaskStartTimeInterval());
  }

  public final long getInitTimeInterval() {
    return initTime - dagInfo.getStartTime();
  }

  public final int getNumTasks() {
    return numTasks;
  }

  public final int getFailedTasksCount() {
    return failedTasks;
  }

  public final int getKilledTasksCount() {
    return killedTasks;
  }

  public final int getCompletedTasksCount() {
    return completedTasks;
  }

  public final int getSucceededTasksCount() {
    return succeededTasks;
  }

  public final int getNumFailedTaskAttemptsCount() {
    return numFailedTaskAttempts;
  }

  public final String getProcessorClassName() {
    return processorClass;

  }


  private List<TaskInfo> getTasksInternal() {
    return Lists.newLinkedList(taskInfoMap.values());
  }

  /**
   * Get all tasks
   *
   * @return list of taskInfo
   */
  public final List<TaskInfo> getTasks() {
    return Collections.unmodifiableList(getTasksInternal());
  }

  /**
   * Get all tasks in sorted order
   *
   * @param sorted
   * @param ordering
   * @return list of TaskInfo
   */
  public final List<TaskInfo> getTasks(boolean sorted, @Nullable Ordering<TaskInfo> ordering) {
    List<TaskInfo> taskInfoList = getTasksInternal();
    if (sorted) {
      Collections.sort(taskInfoList, ((ordering == null) ? orderingOnStartTime() : ordering));
    }
    return Collections.unmodifiableList(taskInfoList);
  }

  /**
   * Get list of failed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskInfo> getFailedTasks() {
    return getTasks(TaskState.FAILED);
  }

  /**
   * Get list of killed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskInfo> getKilledTasks() {
    return getTasks(TaskState.KILLED);
  }

  /**
   * Get list of failed tasks
   *
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskInfo> getSuccessfulTasks() {
    return getTasks(TaskState.SUCCEEDED);
  }

  /**
   * Get list of tasks belonging to a specific state
   *
   * @param state
   * @return List<TaskAttemptInfo>
   */
  public final List<TaskInfo> getTasks(final TaskState state) {
    return Collections.unmodifiableList(Lists.newLinkedList(Iterables.filter(Lists.newLinkedList
                    (taskInfoMap.values()), new Predicate<TaskInfo>() {
                  @Override public boolean apply(TaskInfo input) {
                    return input.getStatus() != null && input.getStatus().equals(state.toString());
                  }
                }
            )
        )
    );
  }

  /**
   * Get source vertices for this vertex
   *
   * @return List<VertexInfo> list of incoming vertices to this vertex
   */
  public final List<VertexInfo> getInputVertices() {
    List<VertexInfo> inputVertices = Lists.newLinkedList();
    for (EdgeInfo edge : inEdgeList) {
      inputVertices.add(edge.getSourceVertex());
    }
    return Collections.unmodifiableList(inputVertices);
  }

  /**
   * Get destination vertices for this vertex
   *
   * @return List<VertexInfo> list of output vertices
   */
  public final List<VertexInfo> getOutputVertices() {
    List<VertexInfo> outputVertices = Lists.newLinkedList();
    for (EdgeInfo edge : outEdgeList) {
      outputVertices.add(edge.getDestinationVertex());
    }
    return Collections.unmodifiableList(outputVertices);
  }

  // expensive method to call for large DAGs as it creates big lists on every call
  private List<TaskAttemptInfo> getTaskAttemptsInternal() {
    List<TaskAttemptInfo> taskAttemptInfos = Lists.newLinkedList();
    for (TaskInfo taskInfo : getTasks()) {
      taskAttemptInfos.addAll(taskInfo.getTaskAttempts());
    }
    return taskAttemptInfos;
  }

  /**
   * Get all task attempts
   *
   * @return List<TaskAttemptInfo> list of attempts
   */
  public List<TaskAttemptInfo> getTaskAttempts() {
    return Collections.unmodifiableList(getTaskAttemptsInternal());
  }

  /**
   * Get all task attempts in sorted order
   *
   * @param sorted
   * @param ordering
   * @return list of TaskAttemptInfo
   */
  public final List<TaskAttemptInfo> getTaskAttempts(boolean sorted,
      @Nullable Ordering<TaskAttemptInfo> ordering) {
    List<TaskAttemptInfo> taskAttemptInfos = getTaskAttemptsInternal();
    if (sorted) {
      Collections.sort(taskAttemptInfos, ((ordering == null) ? orderingOnAttemptStartTime() : ordering));
    }
    return Collections.unmodifiableList(taskAttemptInfos);
  }

  public final TaskInfo getTask(String taskId) {
    return taskInfoMap.get(taskId);
  }

  /**
   * Get incoming edge information for a specific vertex
   *
   * @return List<EdgeInfo> list of input edges on this vertex
   */
  public final List<EdgeInfo> getInputEdges() {
    return Collections.unmodifiableList(inEdgeList);
  }

  /**
   * Get outgoing edge information for a specific vertex
   *
   * @return List<EdgeInfo> list of output edges on this vertex
   */
  public final List<EdgeInfo> getOutputEdges() {
    return Collections.unmodifiableList(outEdgeList);
  }

  public final Multimap<Container, TaskAttemptInfo> getContainersMapping() {
    Multimap<Container, TaskAttemptInfo> containerMapping = LinkedHashMultimap.create();
    for (TaskAttemptInfo attemptInfo : getTaskAttempts()) {
      containerMapping.put(attemptInfo.getContainer(), attemptInfo);
    }
    return Multimaps.unmodifiableMultimap(containerMapping);
  }

  /**
   * Get first task to start
   *
   * @return TaskInfo
   */
  public final TaskInfo getFirstTaskToStart() {
    List<TaskInfo> taskInfoList = Lists.newLinkedList(taskInfoMap.values());
    if (taskInfoList.size() == 0) {
      return null;
    }
    Collections.sort(taskInfoList, new Comparator<TaskInfo>() {
      @Override public int compare(TaskInfo o1, TaskInfo o2) {
        return (o1.getStartTimeInterval() < o2.getStartTimeInterval()) ? -1 :
            ((o1.getStartTimeInterval() == o2.getStartTimeInterval()) ?
                0 : 1);
      }
    });
    return taskInfoList.get(0);
  }

  /**
   * Get last task to finish
   *
   * @return TaskInfo
   */
  public final TaskInfo getLastTaskToFinish() {
    List<TaskInfo> taskInfoList = Lists.newLinkedList(taskInfoMap.values());
    if (taskInfoList.size() == 0) {
      return null;
    }
    Collections.sort(taskInfoList, new Comparator<TaskInfo>() {
      @Override public int compare(TaskInfo o1, TaskInfo o2) {
        return (o1.getFinishTimeInterval() > o2.getFinishTimeInterval()) ? -1 :
            ((o1.getStartTimeInterval() == o2.getStartTimeInterval()) ?
                0 : 1);
      }
    });
    return taskInfoList.get(0);
  }

  /**
   * Get average task duration
   *
   * @return long
   */
  public final float getAvgTaskDuration() {
    float totalTaskDuration = 0;
    List<TaskInfo> tasksList = getTasks();
    if (tasksList.size() == 0) {
      return 0;
    }
    for (TaskInfo taskInfo : tasksList) {
      totalTaskDuration += taskInfo.getTimeTaken();
    }
    return ((totalTaskDuration * 1.0f) / tasksList.size());
  }

  /**
   * Get min task duration in vertex
   *
   * @return long
   */
  public final long getMinTaskDuration() {
    TaskInfo taskInfo = getMinTaskDurationTask();
    return (taskInfo != null) ? taskInfo.getTimeTaken() : 0;
  }

  /**
   * Get max task duration in vertex
   *
   * @return long
   */
  public final long getMaxTaskDuration() {
    TaskInfo taskInfo = getMaxTaskDurationTask();
    return (taskInfo != null) ? taskInfo.getTimeTaken() : 0;
  }

  private Ordering<TaskInfo> orderingOnTimeTaken() {
    return Ordering.from(new Comparator<TaskInfo>() {
      @Override public int compare(TaskInfo o1, TaskInfo o2) {
        return (o1.getTimeTaken() < o2.getTimeTaken()) ? -1 :
            ((o1.getTimeTaken() == o2.getTimeTaken()) ? 0 : 1);
      }
    });
  }

  private Ordering<TaskInfo> orderingOnStartTime() {
    return Ordering.from(new Comparator<TaskInfo>() {
      @Override public int compare(TaskInfo o1, TaskInfo o2) {
        return (o1.getStartTimeInterval() < o2.getStartTimeInterval()) ? -1 :
            ((o1.getStartTimeInterval() == o2.getStartTimeInterval()) ? 0 : 1);
      }
    });
  }

  private Ordering<TaskAttemptInfo> orderingOnAttemptStartTime() {
    return Ordering.from(new Comparator<TaskAttemptInfo>() {
      @Override public int compare(TaskAttemptInfo o1, TaskAttemptInfo o2) {
        return (o1.getStartTimeInterval() < o2.getStartTimeInterval()) ? -1 :
            ((o1.getStartTimeInterval() == o2.getStartTimeInterval()) ? 0 : 1);
      }
    });
  }

  /**
   * Get min task duration in vertex
   *
   * @return TaskInfo
   */
  public final TaskInfo getMinTaskDurationTask() {
    List<TaskInfo> taskInfoList = getTasks();
    if (taskInfoList.size() == 0) {
      return null;
    }

    return orderingOnTimeTaken().min(taskInfoList);
  }

  /**
   * Get max task duration in vertex
   *
   * @return TaskInfo
   */
  public final TaskInfo getMaxTaskDurationTask() {
    List<TaskInfo> taskInfoList = getTasks();
    if (taskInfoList.size() == 0) {
      return null;
    }
    return orderingOnTimeTaken().max(taskInfoList);
  }

  public final String getStatus() {
    return status;
  }

  public final DagInfo getDagInfo() {
    return dagInfo;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("vertexName=").append(getVertexName()).append(", ");
    sb.append("events=").append(getEvents()).append(", ");
    sb.append("initTime=").append(getInitTimeInterval()).append(", ");
    sb.append("startTime=").append(getStartTimeInterval()).append(", ");
    sb.append("endTime=").append(getFinishTimeInterval()).append(", ");
    sb.append("timeTaken=").append(getTimeTaken()).append(", ");
    sb.append("diagnostics=").append(getDiagnostics()).append(", ");
    sb.append("numTasks=").append(getNumTasks()).append(", ");
    sb.append("processorClassName=").append(getProcessorClassName()).append(", ");
    sb.append("numCompletedTasks=").append(getCompletedTasksCount()).append(", ");
    sb.append("numFailedTaskAttempts=").append(getNumFailedTaskAttemptsCount()).append(", ");
    sb.append("numSucceededTasks=").append(getSucceededTasksCount()).append(", ");
    sb.append("numFailedTasks=").append(getFailedTasks()).append(", ");
    sb.append("numKilledTasks=").append(getKilledTasks()).append(", ");
    sb.append("tasksCount=").append(taskInfoMap.size()).append(", ");
    sb.append("status=").append(getStatus());
    sb.append("]");
    return sb.toString();
  }
}

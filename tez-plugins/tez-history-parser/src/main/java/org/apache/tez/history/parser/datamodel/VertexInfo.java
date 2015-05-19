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
import org.apache.tez.dag.api.oldrecords.TaskState;
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
public class VertexInfo extends BaseInfo {

  private final String vertexName;
  private final long endTime;
  private final long initTime;
  private final String diagnostics;
  private final String processorClass;

  private final int numTasks;
  private final int failedTasks;
  private final int completedTasks;
  private final int succeededTasks;
  private final int killedTasks;
  private final int numFailedTaskAttempts;

  private final String status;

  private final long startTime;

  //TaskID --> TaskInfo for internal reference
  private Map<String, TaskInfo> taskInfoMap;

  private final List<EdgeInfo> inEdgeList;
  private final List<EdgeInfo> outEdgeList;

  private final List<AdditionalInputOutputDetails> additionalInputInfoList;
  private final List<AdditionalInputOutputDetails> additionalOutputInfoList;

  private DagInfo dagInfo;

  VertexInfo(JSONObject jsonObject) throws JSONException {
    super(jsonObject);

    Preconditions.checkArgument(
        jsonObject.getString(Constants.ENTITY_TYPE).equalsIgnoreCase
            (Constants.TEZ_VERTEX_ID));

    taskInfoMap = Maps.newHashMap();

    inEdgeList = Lists.newLinkedList();
    outEdgeList = Lists.newLinkedList();
    additionalInputInfoList = Lists.newLinkedList();
    additionalOutputInfoList = Lists.newLinkedList();

    //Parse additional Info
    JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);
    startTime = otherInfoNode.optLong(Constants.START_TIME);
    initTime = otherInfoNode.optLong(Constants.INIT_TIME);
    endTime = otherInfoNode.optLong(Constants.FINISH_TIME);
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
    vertexName = otherInfoNode.optString(Constants.VERTEX_NAME);
    processorClass = otherInfoNode.optString(Constants.PROCESSOR_CLASS_NAME);
    status = otherInfoNode.optString(Constants.STATUS);
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

  @Override
  public final long getStartTime() {
    return startTime - (dagInfo.getAbsStartTime());
  }

  public final long getFirstTaskStartTime() {
    return getFirstTaskToStart().getStartTime();
  }

  public final long getLastTaskFinishTime() {
    if (getLastTaskToFinish() == null || getLastTaskToFinish().getFinishTime() < 0) {
        return dagInfo.getFinishTime();
    }
    return getLastTaskToFinish().getFinishTime();
  }

  public final long getAbsStartTime() {
    return startTime;
  }

  public final long getAbsFinishTime() {
    return endTime;
  }

  public final long getAbsoluteInitTime() {
    return initTime;
  }

  @Override
  public final long getFinishTime() {
    long vertexEndTime = endTime - (dagInfo.getAbsStartTime());
    if (vertexEndTime < 0) {
      //probably vertex is not complete or failed in middle. get the last task attempt time
      for (TaskInfo taskInfo : getTasks()) {
        vertexEndTime = (taskInfo.getFinishTime() > vertexEndTime)
            ? taskInfo.getFinishTime() : vertexEndTime;
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

  //Quite possible that getFinishTime is not yet recorded for failed vertices (or killed vertices)
  //Start time of vertex infers that the dependencies are done and AM has inited it.
  public final long getTimeTaken() {
    return (getFinishTime() - getStartTime());
  }

  //Time taken for last task to finish  - time taken for first task to start
  public final long getTimeTakenForTasks() {
    return (getLastTaskFinishTime() - getFirstTaskStartTime());
  }

  public final long getInitTime() {
    return initTime - dagInfo.getAbsStartTime();
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

  /**
   * Get all tasks
   *
   * @return list of taskInfo
   */
  public final List<TaskInfo> getTasks() {
    List<TaskInfo> taskInfoList = Lists.newLinkedList(taskInfoMap.values());
    Collections.sort(taskInfoList, orderingOnStartTime());
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

  public List<TaskAttemptInfo> getTaskAttempts() {
    List<TaskAttemptInfo> taskAttemptInfos = Lists.newLinkedList();
    for (TaskInfo taskInfo : getTasks()) {
      taskAttemptInfos.addAll(taskInfo.getTaskAttempts());
    }
    Collections.sort(taskAttemptInfos, orderingOnAttemptStartTime());
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
        return (o1.getStartTime() < o2.getStartTime()) ? -1 :
            ((o1.getStartTime() == o2.getStartTime()) ?
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
        return (o1.getFinishTime() > o2.getFinishTime()) ? -1 :
            ((o1.getStartTime() == o2.getStartTime()) ?
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
        return (o1.getStartTime() < o2.getStartTime()) ? -1 :
            ((o1.getStartTime() == o2.getStartTime()) ? 0 : 1);
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
    sb.append("initTime=").append(getInitTime()).append(", ");
    sb.append("startTime=").append(getStartTime()).append(", ");
    sb.append("endTime=").append(getFinishTime()).append(", ");
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

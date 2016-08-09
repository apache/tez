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

package org.apache.tez.dag.api.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.impl.TaskImpl;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.dag.api.DAGNotRunningException;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.records.TezDAGID;

public class DAGClientHandler {

  private Logger LOG = LoggerFactory.getLogger(DAGClientHandler.class);

  private DAGAppMaster dagAppMaster;
  
  public DAGClientHandler(DAGAppMaster dagAppMaster) {
    this.dagAppMaster = dagAppMaster;
  }

  private DAG getCurrentDAG() {
    return dagAppMaster.getContext().getCurrentDAG();
  }

  private Set<String> getAllDagIDs() {
    return dagAppMaster.getContext().getAllDAGIDs();
  }

  public List<String> getAllDAGs() throws TezException {
    return Collections.singletonList(getCurrentDAG().getID().toString());
  }

  public DAGStatus getDAGStatus(String dagIdStr,
      Set<StatusGetOpts> statusOptions) throws TezException {
    return getDAG(dagIdStr).getDAGStatus(statusOptions);
  }

  public DAGStatus getDAGStatus(String dagIdStr,
      Set<StatusGetOpts> statusOptions, long timeout) throws TezException {
    return getDAG(dagIdStr).getDAGStatus(statusOptions, timeout);
  }

  public DAGInformation getDAGInformation(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);

    DAGInformationBuilder dagInformationBuilder = new DAGInformationBuilder();
    dagInformationBuilder.setDagId(dagIdStr);
    dagInformationBuilder.setName(dag.getName());

    Map<TezVertexID, Vertex> vertexMap = dag.getVertices();
    List<VertexInformation> vertexInfoList = new ArrayList<>(vertexMap.size());
    for (Map.Entry<TezVertexID, Vertex> entry : vertexMap.entrySet()) {
      VertexInformationBuilder vertexBuilder = new VertexInformationBuilder();
      vertexBuilder.setName(entry.getValue().getName());
      vertexBuilder.setId(entry.getValue().getVertexId().toString());
      vertexInfoList.add(vertexBuilder);
    }

    dagInformationBuilder.setVertexInformationList(vertexInfoList);
    return dagInformationBuilder;
  }

  public TaskInformation getTaskInformation(String dagId, String vertexId, String taskId) throws TezException {
    DAG dag = getDAG(dagId);
    Vertex vertex = dag.getVertex(getVertexId(vertexId));
    if (vertex == null) {
      throw new TezException("Vertex not found: " + vertexId);
    }

    Task task = vertex.getTask(getTaskId(taskId));
    if (task == null) {
      throw new TezException("Task not found: " + taskId);
    }

    return getTaskInfoFromTask(task);
  }

  public List<TaskInformation> getTaskInformationList(String dagId, String vertexId, String startTaskId, int limit) throws TezException {
    DAG dag = getDAG(dagId);
    Vertex vertex = dag.getVertex(getVertexId(vertexId));
    if (vertex == null) {
      throw new TezException("Vertex not found: " + vertexId);
    }

    Iterable<Task> taskList;
    if (startTaskId != null) {
      Task startTask = vertex.getTask(getTaskId(startTaskId));
      if (startTask == null) {
        throw new TezException("Start task not found: " + startTaskId);
      }
      taskList = vertex.getTaskSubset(startTask, limit);
    } else {
      // need to start from the beginning
      taskList = vertex.getTaskSubset(limit);
    }

    List<TaskInformation> taskInformationList = new ArrayList<>(limit);
    for( Task task : taskList) {
      taskInformationList.add(getTaskInfoFromTask(task));
    }

    return taskInformationList;
  }

  private TaskInformation getTaskInfoFromTask(Task task) throws TezException {
    TaskInformationBuilder builder = new TaskInformationBuilder();
    TaskReport report = task.getReport();

    builder.setId(task.getTaskId().toString());
    builder.setDiagnostics(StringUtils.join(task.getDiagnostics(), TaskImpl.LINE_SEPARATOR));
    builder.setStartTime(report.getStartTime());
    builder.setScheduledTime(task.getScheduledTime());
    builder.setEndTime(task.getFinishTime());
    builder.setState(convertState(task.getState()));
    TaskAttempt successfulAttempt = task.getSuccessfulAttempt();
    if (successfulAttempt != null) {
      builder.setSuccessfulAttemptId(successfulAttempt.getID().toString());
    }
    if (task.getCounters() != null) {
      builder.setTaskCounters(task.getCounters());
    }

    return builder;
  }

  // we should switch the TaskState objects in Task to be consistent
  private TaskState convertState(org.apache.tez.dag.api.oldrecords.TaskState state) throws TezException {
    switch (state) {
      case NEW:
        return TaskState.NEW;
      case SCHEDULED:
        return TaskState.SCHEDULED;
      case RUNNING:
        return TaskState.RUNNING;
      case SUCCEEDED:
        return TaskState.SUCCEEDED;
      case FAILED:
        return TaskState.FAILED;
      case KILLED:
        return TaskState.KILLED;
      default:
        throw new TezException("Invalid enum value for TaskState: " + state);
    }
  }

  private TezVertexID getVertexId(String vertexId) throws TezException {
    try {
      return TezVertexID.fromString(vertexId);
    } catch (IllegalArgumentException e) {
      throw new TezException("Bad vertexId: " + vertexId);
    }
  }

  private TezTaskID getTaskId(String taskId) throws TezException {
    try {
      return TezTaskID.fromString(taskId);
    } catch (IllegalArgumentException e) {
      throw new TezException("Bad taskId: " + taskId);
    }
  }

  public VertexStatus getVertexStatus(String dagIdStr, String vertexName,
      Set<StatusGetOpts> statusOptions) throws TezException {
    VertexStatus status =
        getDAG(dagIdStr).getVertexStatus(vertexName, statusOptions);
    if (status == null) {
      throw new TezException("Unknown vertexName: " + vertexName);
    }

    return status;
  }

  DAG getDAG(String dagIdStr) throws TezException {
    TezDAGID dagId;
    try {
      dagId = TezDAGID.fromString(dagIdStr);
    } catch (IllegalArgumentException e) {
      throw new TezException("Bad dagId: " + dagIdStr, e);
    }

    DAG currentDAG = getCurrentDAG();
    if (currentDAG == null) {
      throw new TezException("No running dag at present");
    }

    final String currentDAGIdStr = currentDAG.getID().toString();
    if (!currentDAGIdStr.equals(dagIdStr)) {
      if (getAllDagIDs().contains(dagIdStr)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Looking for finished dagId " + dagIdStr + " current dag is " + currentDAGIdStr);
        }
        throw new DAGNotRunningException("DAG " + dagIdStr + " Not running, current dag is " +
            currentDAGIdStr);
      } else {
        LOG.warn("Current DAGID : " + currentDAGIdStr + ", Looking for string (not found): " +
            dagIdStr + ", dagIdObj: " + dagId);
        throw new TezException("Unknown dagId: " + dagIdStr);
      }
    }

    return currentDAG;
  }

  private String getClientInfo() throws TezException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw new TezException(ie);
    }
    String message = callerUGI.toString();
    if(null != Server.getRemoteAddress()) {
      message += " at " + Server.getRemoteAddress();
    }
    return message;
  }

  public void tryKillDAG(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    String message = "Sending client kill from " + getClientInfo() +
        " to dag " + dagIdStr;
    LOG.info(message);
    dagAppMaster.tryKillDAG(dag, message);
  }

  public synchronized String submitDAG(DAGPlan dagPlan,
      Map<String, LocalResource> additionalAmResources) throws TezException {
    return dagAppMaster.submitDAGToAppMaster(dagPlan, additionalAmResources);
  }

  // Only to be invoked by the DAGClient.
  public synchronized void shutdownAM() throws TezException {
    String message = "Received message to shutdown AM from " + getClientInfo();
    LOG.info(message);
    if (dagAppMaster != null) {
      dagAppMaster.shutdownTezAM(message);
    }
  }

  public synchronized TezAppMasterStatus getTezAppMasterStatus() throws TezException {
    switch (dagAppMaster.getState()) {
    case NEW:
    case INITED:
      return TezAppMasterStatus.INITIALIZING;
    case IDLE:
      return TezAppMasterStatus.READY;
    case RECOVERING:
    case RUNNING:
      return TezAppMasterStatus.RUNNING;
    case ERROR:
    case FAILED:
    case SUCCEEDED:
    case KILLED:
      return TezAppMasterStatus.SHUTDOWN;
    }
    return TezAppMasterStatus.INITIALIZING;
  }

  public ACLManager getACLManager() {
    return dagAppMaster.getACLManager();
  }

  public ACLManager getACLManager(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    return dag.getACLManager();
  }

}

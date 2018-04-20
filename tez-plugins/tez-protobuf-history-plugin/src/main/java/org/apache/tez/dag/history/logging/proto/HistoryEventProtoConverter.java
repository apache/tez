/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.proto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.records.DAGProtos.CallerContextProto;
import org.apache.tez.dag.app.web.AMWebController;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert history event into HistoryEventProto message.
 */
public class HistoryEventProtoConverter {
  private static final Logger log =
      LoggerFactory.getLogger(HistoryEventProtoConverter.class);

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Convert a given history event to HistoryEventProto message.
   */
  public HistoryEventProto convert(HistoryEvent historyEvent) {
    validateEvent(historyEvent);
    switch (historyEvent.getEventType()) {
    case APP_LAUNCHED:
      return convertAppLaunchedEvent((AppLaunchedEvent) historyEvent);
    case AM_LAUNCHED:
      return convertAMLaunchedEvent((AMLaunchedEvent) historyEvent);
    case AM_STARTED:
      return convertAMStartedEvent((AMStartedEvent) historyEvent);
    case CONTAINER_LAUNCHED:
      return convertContainerLaunchedEvent((ContainerLaunchedEvent) historyEvent);
    case CONTAINER_STOPPED:
      return convertContainerStoppedEvent((ContainerStoppedEvent) historyEvent);
    case DAG_SUBMITTED:
      return convertDAGSubmittedEvent((DAGSubmittedEvent) historyEvent);
    case DAG_INITIALIZED:
      return convertDAGInitializedEvent((DAGInitializedEvent) historyEvent);
    case DAG_STARTED:
      return convertDAGStartedEvent((DAGStartedEvent) historyEvent);
    case DAG_FINISHED:
      return convertDAGFinishedEvent((DAGFinishedEvent) historyEvent);
    case VERTEX_INITIALIZED:
      return convertVertexInitializedEvent((VertexInitializedEvent) historyEvent);
    case VERTEX_STARTED:
      return convertVertexStartedEvent((VertexStartedEvent) historyEvent);
    case VERTEX_FINISHED:
      return convertVertexFinishedEvent((VertexFinishedEvent) historyEvent);
    case TASK_STARTED:
      return convertTaskStartedEvent((TaskStartedEvent) historyEvent);
    case TASK_FINISHED:
      return convertTaskFinishedEvent((TaskFinishedEvent) historyEvent);
    case TASK_ATTEMPT_STARTED:
      return convertTaskAttemptStartedEvent((TaskAttemptStartedEvent) historyEvent);
    case TASK_ATTEMPT_FINISHED:
      return convertTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) historyEvent);
    case VERTEX_CONFIGURE_DONE:
      return convertVertexReconfigureDoneEvent((VertexConfigurationDoneEvent) historyEvent);
    case DAG_RECOVERED:
      return convertDAGRecoveredEvent((DAGRecoveredEvent) historyEvent);
    case VERTEX_COMMIT_STARTED:
    case VERTEX_GROUP_COMMIT_STARTED:
    case VERTEX_GROUP_COMMIT_FINISHED:
    case DAG_COMMIT_STARTED:
    case DAG_KILL_REQUEST:
      throw new UnsupportedOperationException("Invalid Event, does not support history, eventType="
          + historyEvent.getEventType());
      // Do not add default, if a new event type is added, we'll get a warning for the
      // switch.
    }
    throw new UnsupportedOperationException(
        "Unhandled Event, eventType=" + historyEvent.getEventType());
  }

  private void validateEvent(HistoryEvent event) {
    if (!event.isHistoryEvent()) {
      throw new UnsupportedOperationException(
          "Invalid Event, does not support history" + ", eventType=" + event.getEventType());
    }
  }

  private HistoryEventProto.Builder makeBuilderForEvent(HistoryEvent event, long time,
      TezDAGID dagId, ApplicationId appId, ApplicationAttemptId appAttemptId, TezVertexID vertexId,
      TezTaskID taskId, TezTaskAttemptID taskAttemptId, String user) {
    HistoryEventProto.Builder builder = HistoryEventProto.newBuilder();
    builder.setEventType(event.getEventType().name());
    builder.setEventTime(time);
    if (taskAttemptId != null) {
      builder.setTaskAttemptId(taskAttemptId.toString());
      taskId = taskAttemptId.getTaskID();
    }
    if (taskId != null) {
      builder.setTaskId(taskId.toString());
      vertexId = taskId.getVertexID();
    }
    if (vertexId != null) {
      builder.setVertexId(vertexId.toString());
      dagId = vertexId.getDAGId();
    }
    if (dagId != null) {
      builder.setDagId(dagId.toString());
      if (appId == null) {
        appId = dagId.getApplicationId();
      }
    }
    if (appAttemptId != null) {
      builder.setAppAttemptId(appAttemptId.toString());
      if (appId == null) {
        appId = appAttemptId.getApplicationId();
      }
    }
    if (appId != null) {
      builder.setAppId(appId.toString());
    }
    if (user != null) {
      builder.setUser(user);
    }
    return builder;
  }

  private void addEventData(HistoryEventProto.Builder builder, String key, String value) {
    if (value == null) {
      return;
    }
    builder.addEventData(KVPair.newBuilder().setKey(key).setValue(value));
  }

  private void addEventData(HistoryEventProto.Builder builder, String key, Number value) {
    builder.addEventData(KVPair.newBuilder().setKey(key).setValue(value.toString()));
  }

  private void addEventData(HistoryEventProto.Builder builder, String key,
      Map<String, Object> value) {
    try {
      builder.addEventData(
          KVPair.newBuilder().setKey(key).setValue(mapper.writeValueAsString(value)));
    } catch (IOException e) {
      log.error("Error converting value for key {} to json: ", key, e);
    }
  }

  private HistoryEventProto convertAppLaunchedEvent(AppLaunchedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getLaunchTime(), null,
        event.getApplicationId(), null, null, null, null, event.getUser());
    // This is ok as long as we do not modify the underlying map.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Map<String, Object> confMap = (Map)DAGUtils.convertConfigurationToATSMap(event.getConf());
    addEventData(builder, ATSConstants.CONFIG, confMap);
    if (event.getVersion() != null) {
      addEventData(builder, ATSConstants.TEZ_VERSION,
          DAGUtils.convertTezVersionToATSMap(event.getVersion()));
    }
    addEventData(builder, ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    return builder.build();
  }

  private HistoryEventProto convertAMLaunchedEvent(AMLaunchedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getLaunchTime(), null,
        null, event.getApplicationAttemptId(), null, null, null, event.getUser());
    addEventData(builder, ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());
    return builder.build();
  }

  private HistoryEventProto convertAMStartedEvent(AMStartedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStartTime(), null,
        null, event.getApplicationAttemptId(), null, null, null, event.getUser());
    return builder.build();
  }

  private HistoryEventProto convertContainerLaunchedEvent(ContainerLaunchedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getLaunchTime(), null,
        null, event.getApplicationAttemptId(), null, null, null, null);
    addEventData(builder, ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    return builder.build();
  }

  private HistoryEventProto convertContainerStoppedEvent(ContainerStoppedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStoppedTime(), null,
        null, event.getApplicationAttemptId(), null, null, null, null);
    addEventData(builder, ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    addEventData(builder, ATSConstants.EXIT_STATUS, event.getExitStatus());
    addEventData(builder, ATSConstants.FINISH_TIME, event.getStoppedTime());
    return builder.build();
  }

  private HistoryEventProto convertDAGSubmittedEvent(DAGSubmittedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getSubmitTime(),
        event.getDagID(), null, event.getApplicationAttemptId(), null, null, null,
        event.getUser());
    addEventData(builder, ATSConstants.DAG_NAME, event.getDAGName());
    if (event.getDAGPlan().hasCallerContext() &&
        event.getDAGPlan().getCallerContext().hasCallerId()) {
      CallerContextProto callerContext = event.getDagPlan().getCallerContext();
      addEventData(builder, ATSConstants.CALLER_CONTEXT_ID, callerContext.getCallerId());
      addEventData(builder, ATSConstants.CALLER_CONTEXT_TYPE, callerContext.getCallerType());
      addEventData(builder, ATSConstants.CALLER_CONTEXT, callerContext.getContext());
    }
    if (event.getQueueName() != null) {
      addEventData(builder, ATSConstants.DAG_QUEUE_NAME, event.getQueueName());
    }
    addEventData(builder, ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    addEventData(builder, ATSConstants.IN_PROGRESS_LOGS_URL + "_" +
        event.getApplicationAttemptId().getAttemptId(), event.getContainerLogs());
    try {
      addEventData(builder, ATSConstants.DAG_PLAN,
          DAGUtils.convertDAGPlanToATSMap(event.getDAGPlan()));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return builder.build();
  }

  private HistoryEventProto convertDAGInitializedEvent(DAGInitializedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getInitTime(),
        event.getDagID(), null, null, null, null, null, event.getUser());
    addEventData(builder, ATSConstants.DAG_NAME, event.getDagName());

    if (event.getVertexNameIDMap() != null) {
      Map<String, Object> nameIdStrMap = new TreeMap<String, Object>();
      for (Entry<String, TezVertexID> entry : event.getVertexNameIDMap().entrySet()) {
        nameIdStrMap.put(entry.getKey(), entry.getValue().toString());
      }
      addEventData(builder, ATSConstants.VERTEX_NAME_ID_MAPPING, nameIdStrMap);
    }
    return builder.build();
  }

  private HistoryEventProto convertDAGStartedEvent(DAGStartedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStartTime(),
        event.getDagID(), null, null, null, null, null, event.getUser());

    addEventData(builder, ATSConstants.DAG_NAME, event.getDagName());
    addEventData(builder, ATSConstants.STATUS, event.getDagState().name());

    return builder.build();
  }

  private HistoryEventProto convertDAGFinishedEvent(DAGFinishedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getFinishTime(),
        event.getDagID(), null, event.getApplicationAttemptId(), null, null, null,
        event.getUser());
    addEventData(builder, ATSConstants.DAG_NAME, event.getDagName());
    if (event.getDAGPlan().hasCallerContext()) {
      if (event.getDAGPlan().getCallerContext().hasCallerType()) {
        addEventData(builder, ATSConstants.CALLER_CONTEXT_TYPE,
            event.getDAGPlan().getCallerContext().getCallerType());
      }
      if (event.getDAGPlan().getCallerContext().hasCallerId()) {
        addEventData(builder, ATSConstants.CALLER_CONTEXT_ID,
            event.getDAGPlan().getCallerContext().getCallerId());
      }
    }
    addEventData(builder, ATSConstants.START_TIME, event.getStartTime());
    addEventData(builder, ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    addEventData(builder, ATSConstants.STATUS, event.getState().name());
    addEventData(builder, ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    addEventData(builder, ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    addEventData(builder, ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    Map<String, Integer> dagTaskStats = event.getDagTaskStats();
    if (dagTaskStats != null) {
      for (Entry<String, Integer> entry : dagTaskStats.entrySet()) {
        addEventData(builder, entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private HistoryEventProto convertTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStartTime(),
        null, null, null, null, null, event.getTaskAttemptID(), null);
    if (event.getInProgressLogsUrl() != null) {
      addEventData(builder, ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      addEventData(builder, ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    addEventData(builder, ATSConstants.NODE_ID, event.getNodeId().toString());
    addEventData(builder, ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    addEventData(builder, ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    addEventData(builder, ATSConstants.STATUS, TaskAttemptState.RUNNING.name());

    return builder.build();
  }

  private HistoryEventProto convertTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getFinishTime(),
        null, null, null, null, null, event.getTaskAttemptID(), null);

    addEventData(builder, ATSConstants.STATUS, event.getState().name());
    if (event.getTaskFailureType() != null) {
      addEventData(builder, ATSConstants.TASK_FAILURE_TYPE, event.getTaskFailureType().name());
    }

    addEventData(builder, ATSConstants.CREATION_TIME, event.getCreationTime());
    addEventData(builder, ATSConstants.ALLOCATION_TIME, event.getAllocationTime());
    addEventData(builder, ATSConstants.START_TIME, event.getStartTime());
    if (event.getCreationCausalTA() != null) {
      addEventData(builder, ATSConstants.CREATION_CAUSAL_ATTEMPT,
          event.getCreationCausalTA().toString());
    }
    addEventData(builder, ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    if (event.getTaskAttemptError() != null) {
      addEventData(builder, ATSConstants.TASK_ATTEMPT_ERROR_ENUM,
          event.getTaskAttemptError().name());
    }
    addEventData(builder, ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    addEventData(builder, ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getCounters()));
    if (event.getDataEvents() != null && !event.getDataEvents().isEmpty()) {
      addEventData(builder, ATSConstants.LAST_DATA_EVENTS,
          DAGUtils.convertDataEventDependecyInfoToATS(event.getDataEvents()));
    }
    if (event.getNodeId() != null) {
      addEventData(builder, ATSConstants.NODE_ID, event.getNodeId().toString());
    }
    if (event.getContainerId() != null) {
      addEventData(builder, ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    }
    if (event.getInProgressLogsUrl() != null) {
      addEventData(builder, ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      addEventData(builder, ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    if (event.getNodeHttpAddress() != null) {
      addEventData(builder, ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    }

    return builder.build();
  }

  private HistoryEventProto convertTaskFinishedEvent(TaskFinishedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getFinishTime(),
        null, null, null, null, event.getTaskID(), null, null);

    addEventData(builder, ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    addEventData(builder, ATSConstants.STATUS, event.getState().name());
    addEventData(builder, ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, event.getNumFailedAttempts());
    if (event.getSuccessfulAttemptID() != null) {
      addEventData(builder, ATSConstants.SUCCESSFUL_ATTEMPT_ID,
          event.getSuccessfulAttemptID().toString());
    }

    addEventData(builder, ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    addEventData(builder, ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    return builder.build();
  }

  private HistoryEventProto convertTaskStartedEvent(TaskStartedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStartTime(),
        null, null, null, null, event.getTaskID(), null, null);

    addEventData(builder, ATSConstants.SCHEDULED_TIME, event.getScheduledTime());
    addEventData(builder, ATSConstants.STATUS, event.getState().name());

    return builder.build();
  }

  private HistoryEventProto convertVertexFinishedEvent(VertexFinishedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getFinishTime(),
        null, null, null, event.getVertexID(), null, null, null);

    addEventData(builder, ATSConstants.STATUS, event.getState().name());
    addEventData(builder, ATSConstants.VERTEX_NAME, event.getVertexName());
    addEventData(builder, ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    addEventData(builder, ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    addEventData(builder, ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    addEventData(builder, ATSConstants.STATS,
        DAGUtils.convertVertexStatsToATSMap(event.getVertexStats()));
    if (event.getServicePluginInfo() != null) {
      addEventData(builder, ATSConstants.SERVICE_PLUGIN,
          DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }

    final Map<String, Integer> vertexTaskStats = event.getVertexTaskStats();
    if (vertexTaskStats != null) {
      for (Entry<String, Integer> entry : vertexTaskStats.entrySet()) {
        addEventData(builder, entry.getKey(), entry.getValue());
      }
    }

    return builder.build();
  }

  private HistoryEventProto convertVertexInitializedEvent(VertexInitializedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getInitedTime(),
        null, null, null, event.getVertexID(), null, null, null);
    addEventData(builder, ATSConstants.VERTEX_NAME, event.getVertexName());
    addEventData(builder, ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    addEventData(builder, ATSConstants.NUM_TASKS, event.getNumTasks());
    addEventData(builder, ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());
    if (event.getServicePluginInfo() != null) {
      addEventData(builder, ATSConstants.SERVICE_PLUGIN,
          DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }

    return builder.build();
  }

  private HistoryEventProto convertVertexStartedEvent(VertexStartedEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getStartTime(),
        null, null, null, event.getVertexID(), null, null, null);
    addEventData(builder, ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    addEventData(builder, ATSConstants.STATUS, event.getVertexState().name());
    return builder.build();
  }

  private HistoryEventProto convertVertexReconfigureDoneEvent(VertexConfigurationDoneEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getReconfigureDoneTime(),
        null, null, null, event.getVertexID(), null, null, null);
    if (event.getSourceEdgeProperties() != null && !event.getSourceEdgeProperties().isEmpty()) {
      Map<String, Object> updatedEdgeManagers = new HashMap<String, Object>();
      for (Entry<String, EdgeProperty> entry : event.getSourceEdgeProperties().entrySet()) {
        updatedEdgeManagers.put(entry.getKey(), DAGUtils.convertEdgeProperty(entry.getValue()));
      }
      addEventData(builder, ATSConstants.UPDATED_EDGE_MANAGERS, updatedEdgeManagers);
    }
    addEventData(builder, ATSConstants.NUM_TASKS, event.getNumTasks());
    return builder.build();
  }

  private HistoryEventProto convertDAGRecoveredEvent(DAGRecoveredEvent event) {
    HistoryEventProto.Builder builder = makeBuilderForEvent(event, event.getRecoveredTime(),
        event.getDagID(), null, event.getApplicationAttemptId(), null, null, null,
        event.getUser());
    addEventData(builder, ATSConstants.DAG_NAME, event.getDagName());
    if (event.getRecoveredDagState() != null) {
      addEventData(builder, ATSConstants.DAG_STATE, event.getRecoveredDagState().name());
    }
    if (event.getRecoveryFailureReason() != null) {
      addEventData(builder, ATSConstants.RECOVERY_FAILURE_REASON,
          event.getRecoveryFailureReason());
    }
    addEventData(builder, ATSConstants.IN_PROGRESS_LOGS_URL + "_" +
        event.getApplicationAttemptId().getAttemptId(), event.getContainerLogs());
    return builder.build();
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.ats;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.tez.common.ATSConstants;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.records.DAGProtos.CallerContextProto;
import org.apache.tez.dag.app.web.AMWebController;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
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
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezVertexID;

public class HistoryEventTimelineConversionAtsv2{

  public static final String TEZ_PREFIX = "tez_";

  private static void validateEvent(HistoryEvent event) {
    if (!event.isHistoryEvent()) {
      throw new UnsupportedOperationException(
        "Invalid Event, does not support history" + ", eventType=" + event
          .getEventType());
    }
  }

  public static TimelineEntity createTimelineEntity(HistoryEvent historyEvent) {
    validateEvent(historyEvent);

    switch (historyEvent.getEventType()) {
      case APP_LAUNCHED:
        return createAppLaunchedEntity((AppLaunchedEvent) historyEvent);
      case AM_LAUNCHED:
        return createAMLaunchedEntity((AMLaunchedEvent) historyEvent);
      case AM_STARTED:
        return createAMStartedEntity((AMStartedEvent) historyEvent);
      case CONTAINER_LAUNCHED:
        return createContainerLaunchedEntity(
          (ContainerLaunchedEvent) historyEvent);
      case CONTAINER_STOPPED:
        return createContainerStoppedEntity((ContainerStoppedEvent) historyEvent);
      case DAG_SUBMITTED:
        return createDAGSubmittedEntity((DAGSubmittedEvent) historyEvent);
      case DAG_INITIALIZED:
        return createDAGInitializedEntity((DAGInitializedEvent) historyEvent);
      case DAG_STARTED:
        return createDAGStartedEntity((DAGStartedEvent) historyEvent);
      case DAG_FINISHED:
        return createDAGFinishedEntity((DAGFinishedEvent) historyEvent);
      case VERTEX_INITIALIZED:
        return createVertexInitializedEntity(
          (VertexInitializedEvent) historyEvent);
      case VERTEX_STARTED:
        return createVertexStartedEntity((VertexStartedEvent) historyEvent);
      case VERTEX_FINISHED:
        return createVertexFinishedEntity((VertexFinishedEvent) historyEvent);
      case TASK_STARTED:
        return createTaskStartedEntity((TaskStartedEvent) historyEvent);
      case TASK_FINISHED:
        return createTaskFinishedEntity((TaskFinishedEvent) historyEvent);
      case TASK_ATTEMPT_STARTED:
        return createTaskAttemptStartedEntity(
          (TaskAttemptStartedEvent) historyEvent);
      case TASK_ATTEMPT_FINISHED:
        return createTaskAttemptFinishedEntity(
          (TaskAttemptFinishedEvent) historyEvent);
      case VERTEX_CONFIGURE_DONE:
        return createVertexReconfigureDoneEntity(
          (VertexConfigurationDoneEvent) historyEvent);
      case DAG_RECOVERED:
        return createDAGRecoveredEntity((DAGRecoveredEvent) historyEvent);
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_COMMIT_STARTED:
        throw new UnsupportedOperationException(
          "Invalid Event, does not support history" + ", eventType="
            + historyEvent.getEventType());
      default:
        throw new UnsupportedOperationException(
          "Unhandled Event" + ", eventType=" + historyEvent.getEventType());
    }

  }

  private static TimelineEntity createAppLaunchedEntity(
    AppLaunchedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getApplicationId().toString(),
        EntityTypes.TEZ_APPLICATION.name(), event.getLaunchTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getLaunchTime());
    entity.addEvent(tEvent);

    entity.addInfo(ATSConstants.CONFIG,
      DAGUtils.convertConfigurationToATSMap(event.getConf()));
    entity.addInfo(ATSConstants.APPLICATION_ID,
      event.getApplicationId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());
    if (event.getVersion() != null) {
      entity.addInfo(ATSConstants.TEZ_VERSION,
        DAGUtils.convertTezVersionToATSMap(event.getVersion()));
    }
    entity.addInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);


    return entity;
  }

  private static TimelineEntity createAMLaunchedEntity(AMLaunchedEvent event) {
    TimelineEntity entity = createBaseEntity(
      TEZ_PREFIX + event.getApplicationAttemptId().toString(),
      EntityTypes.TEZ_APPLICATION_ATTEMPT.name(), event.getAppSubmitTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getAppSubmitTime());
    entity.addEvent(tEvent);
    entity.addInfo(ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());
    entity.addInfo(ATSConstants.APPLICATION_ID,
      event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
      event.getApplicationAttemptId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());

    return entity;
  }

  private static TimelineEntity createAMStartedEntity(AMStartedEvent event) {
    TimelineEntity entity = createBaseEntity(
      TEZ_PREFIX + event.getApplicationAttemptId().toString(),
      EntityTypes.TEZ_APPLICATION_ATTEMPT.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getStartTime());
    entity.addEvent(tEvent);

    return entity;
  }

  private static TimelineEntity createContainerLaunchedEntity(
    ContainerLaunchedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getContainerId().toString(),
        EntityTypes.TEZ_CONTAINER_ID.name(), event.getLaunchTime());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
      "tez_" + event.getApplicationAttemptId().toString());


    entity.addInfo(ATSConstants.CONTAINER_ID,
      event.getContainerId().toString());
    entity.setCreatedTime(event.getLaunchTime());
    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getLaunchTime());
    entity.addEvent(tEvent);

    return entity;
  }

  private static TimelineEntity createContainerStoppedEntity(
    ContainerStoppedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getContainerId().toString(),
        EntityTypes.TEZ_CONTAINER_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getStoppedTime());
    entity.addEvent(tEvent);

    // In case, a container is stopped in a different attempt
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
      "tez_" + event.getApplicationAttemptId().toString());

    entity.addInfo(ATSConstants.EXIT_STATUS, event.getExitStatus());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getStoppedTime());

    return entity;
  }

  private static TimelineEntity createDAGSubmittedEntity(
    DAGSubmittedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getDagID().toString(),
        EntityTypes.TEZ_DAG_ID.name(), event.getSubmitTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getSubmitTime());
    entity.addEvent(tEvent);


    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION.name(),
      "tez_" + event.getApplicationAttemptId().getApplicationId().toString());
    entity.addIsRelatedToEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
      "tez_" + event.getApplicationAttemptId().toString());


    if (event.getDAGPlan().hasCallerContext()
      && event.getDAGPlan().getCallerContext().hasCallerId()) {
      CallerContextProto callerContext = event.getDagPlan().getCallerContext();
      entity.addInfo(ATSConstants.CALLER_CONTEXT_ID, callerContext.getCallerId());
      entity.addInfo(ATSConstants.CALLER_CONTEXT, callerContext.getContext());
    }

    entity.addInfo(ATSConstants.APPLICATION_ID,
      event.getApplicationAttemptId().getApplicationId().toString());
    entity.addInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
      event.getApplicationAttemptId().toString());
    entity.addInfo(ATSConstants.USER, event.getUser());
    entity.addInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
      + event.getApplicationAttemptId().getAttemptId(), event.getContainerLogs());
    if (event.getDAGPlan().hasCallerContext()
      && event.getDAGPlan().getCallerContext().hasCallerId()
      && event.getDAGPlan().getCallerContext().hasCallerType()) {
      entity.addInfo(ATSConstants.CALLER_CONTEXT_ID,
        event.getDAGPlan().getCallerContext().getCallerId());
      entity.addInfo(ATSConstants.CALLER_CONTEXT_TYPE,
        event.getDAGPlan().getCallerContext().getCallerType());
    }
    if (event.getQueueName() != null) {
      entity.addInfo(ATSConstants.DAG_QUEUE_NAME, event.getQueueName());
    }

    try {
      entity.addInfo(ATSConstants.DAG_PLAN,
        DAGUtils.convertDAGPlanToATSMap(event.getDAGPlan()));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return entity;
  }

  private static TimelineEntity createDAGInitializedEntity(
    DAGInitializedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getDagID().toString(),
        EntityTypes.TEZ_DAG_ID.name(), null);
    entity.addInfo(ATSConstants.INIT_TIME, event.getInitTime());
    if (event.getVertexNameIDMap() != null) {
      Map<String, String> nameIdStrMap = new TreeMap<String, String>();
      for (Entry<String, TezVertexID> entry : event.getVertexNameIDMap().entrySet()) {
        nameIdStrMap.put(entry.getKey(), entry.getValue().toString());
      }
      entity.addInfo(ATSConstants.VERTEX_NAME_ID_MAPPING, nameIdStrMap);
    }

    return entity;
  }

  private static TimelineEntity createDAGStartedEntity(DAGStartedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getDagID().toString(),
        EntityTypes.TEZ_DAG_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getStartTime());
    entity.addEvent(tEvent);
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getDagState().toString());
    return entity;
  }

  private static TimelineEntity createDAGFinishedEntity(
    DAGFinishedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getDagID().toString(),
        EntityTypes.TEZ_DAG_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getFinishTime());
    entity.addEvent(tEvent);

    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
      event.getApplicationAttemptId().toString());


    final Map<String, Integer> dagTaskStats = event.getDagTaskStats();
    if (dagTaskStats != null) {
      for(Entry<String, Integer> entry : dagTaskStats.entrySet()) {
        entity.addInfo(entry.getKey(), entry.getValue());
      }
    }

    entity.addInfo(ATSConstants.COUNTERS,
      DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    return entity;
  }

  private static TimelineEntity createVertexInitializedEntity(
    VertexInitializedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getVertexID().toString(),
        EntityTypes.TEZ_VERTEX_ID.name(), event.getInitedTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getInitedTime());
    entity.addEvent(tEvent);

    entity.addIsRelatedToEntity(EntityTypes.TEZ_DAG_ID.name(),
      event.getVertexID().getDAGId().toString());
    entity.addInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    entity.addInfo(ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    entity.addInfo(ATSConstants.INIT_TIME, event.getInitedTime());
    entity.addInfo(ATSConstants.NUM_TASKS, event.getNumTasks());
    entity.addInfo(ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());
    if (event.getServicePluginInfo() != null) {
      entity.addInfo(ATSConstants.SERVICE_PLUGIN,
        DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }
    return entity;
  }

  private static TimelineEntity createVertexStartedEntity(
    VertexStartedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getVertexID().toString(),
        EntityTypes.TEZ_VERTEX_ID.name(), null);

    TimelineEvent tEvent = craeteBaseEvent(event.getEventType().name(),
      event.getStartRequestedTime());
    entity.addEvent(tEvent);


    entity.addInfo(ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.STATUS, event.getVertexState().toString());

    return entity;
  }

  private static TimelineEntity createVertexFinishedEntity(
    VertexFinishedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getVertexID().toString(),
        EntityTypes.TEZ_VERTEX_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getFinishTime());
    entity.addEvent(tEvent);
    entity.addInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    entity.addInfo(ATSConstants.STATUS, event.getState().name());

    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS,
      DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    entity.addInfo(ATSConstants.STATS,
      DAGUtils.convertVertexStatsToATSMap(event.getVertexStats()));
    if (event.getServicePluginInfo() != null) {
      entity.addInfo(ATSConstants.SERVICE_PLUGIN,
        DAGUtils.convertServicePluginToATSMap(event.getServicePluginInfo()));
    }

    final Map<String, Integer> vertexTaskStats = event.getVertexTaskStats();
    if (vertexTaskStats != null) {
      for(Entry<String, Integer> entry : vertexTaskStats.entrySet()) {
        entity.addInfo(entry.getKey(), entry.getValue());
      }
    }

    return entity;
  }

  private static TimelineEntity createTaskStartedEntity(
    TaskStartedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getTaskID().toString(),
        EntityTypes.TEZ_TASK_ID.name(), event.getStartTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getStartTime());
    entity.addEvent(tEvent);

    entity.addIsRelatedToEntity(EntityTypes.TEZ_VERTEX_ID.name(),
      event.getTaskID().getVertexID().toString());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.SCHEDULED_TIME, event.getScheduledTime());
    entity.addInfo(ATSConstants.STATUS, event.getState().name());

    return entity;
  }

  private static TimelineEntity createTaskFinishedEntity(
    TaskFinishedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getTaskID().toString(),
        EntityTypes.TEZ_TASK_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getFinishTime());
    entity.addEvent(tEvent);
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    entity.addInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    entity.addInfo(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, event.getNumFailedAttempts());
    if (event.getSuccessfulAttemptID() != null) {
      entity.addInfo(ATSConstants.SUCCESSFUL_ATTEMPT_ID,
        event.getSuccessfulAttemptID().toString());
    }

    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS,
      DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    return entity;
  }

  private static TimelineEntity createTaskAttemptStartedEntity(
    TaskAttemptStartedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getTaskAttemptID().toString(),
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), event.getStartTime());

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getStartTime());
    entity.addEvent(tEvent);

    entity.addIsRelatedToEntity(EntityTypes.TEZ_TASK_ID.name(),
      event.getTaskAttemptID().getTaskID().toString());

    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    if (event.getInProgressLogsUrl() != null) {
      entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      entity.addInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    entity.addInfo(ATSConstants.NODE_ID, event.getNodeId().toString());
    entity.addInfo(ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    entity.addInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    entity.addInfo(ATSConstants.STATUS, TaskAttemptState.RUNNING.name());


    return entity;
  }

  private static TimelineEntity createTaskAttemptFinishedEntity(
    TaskAttemptFinishedEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getTaskAttemptID().toString(),
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getFinishTime());
    entity.addEvent(tEvent);

    if (event.getTaskFailureType() != null) {
      entity.addInfo(ATSConstants.TASK_FAILURE_TYPE, event.getTaskFailureType().name());
    }
    entity.addInfo(ATSConstants.CREATION_TIME, event.getCreationTime());
    entity.addInfo(ATSConstants.ALLOCATION_TIME, event.getAllocationTime());
    entity.addInfo(ATSConstants.START_TIME, event.getStartTime());
    entity.addInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    if (event.getCreationCausalTA() != null) {
      entity.addInfo(ATSConstants.CREATION_CAUSAL_ATTEMPT,
        event.getCreationCausalTA().toString());
    }
    entity.addInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    entity.addInfo(ATSConstants.STATUS, event.getState().name());
    if (event.getTaskAttemptError() != null) {
      entity.addInfo(ATSConstants.TASK_ATTEMPT_ERROR_ENUM, event.getTaskAttemptError().name());
    }
    entity.addInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    entity.addInfo(ATSConstants.COUNTERS,
      DAGUtils.convertCountersToATSMap(event.getCounters()));
    if (event.getDataEvents() != null && !event.getDataEvents().isEmpty()) {
      entity.addInfo(ATSConstants.LAST_DATA_EVENTS,
        DAGUtils.convertDataEventDependecyInfoToATS(event.getDataEvents()));
    }
    if (event.getNodeId() != null) {
      entity.addInfo(ATSConstants.NODE_ID, event.getNodeId().toString());
    }
    if (event.getContainerId() != null) {
      entity.addInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    }
    if (event.getInProgressLogsUrl() != null) {
      entity.addInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    }
    if (event.getCompletedLogsUrl() != null) {
      entity.addInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    }
    if (event.getNodeHttpAddress() != null) {
      entity.addInfo(ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    }

    return entity;
  }

  private static TimelineEntity createVertexReconfigureDoneEntity(
    VertexConfigurationDoneEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getVertexID().toString(),
        EntityTypes.TEZ_VERTEX_ID.name(), null);

    TimelineEvent tEvent = craeteBaseEvent(event.getEventType().name(),
      event.getReconfigureDoneTime());
    //entity.addEvent(tEvent);

    Map<String,Object> eventInfo = new HashMap<String, Object>();
    if (event.getSourceEdgeProperties() != null && !event.getSourceEdgeProperties().isEmpty()) {
      Map<String, Object> updatedEdgeManagers = new HashMap<String, Object>();
      for (Entry<String, EdgeProperty> entry :
        event.getSourceEdgeProperties().entrySet()) {
        updatedEdgeManagers.put(entry.getKey(),
          DAGUtils.convertEdgeProperty(entry.getValue()));
      }
      eventInfo.put(ATSConstants.UPDATED_EDGE_MANAGERS, updatedEdgeManagers);
    }
    eventInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);

    entity.addInfo(ATSConstants.NUM_TASKS, event.getNumTasks());

    return entity;
  }

  private static TimelineEntity createDAGRecoveredEntity(
    DAGRecoveredEvent event) {
    TimelineEntity entity =
      createBaseEntity(TEZ_PREFIX + event.getDagID().toString(),
        EntityTypes.TEZ_DAG_ID.name(), null);

    TimelineEvent tEvent =
      craeteBaseEvent(event.getEventType().name(), event.getRecoveredTime());
    entity.addEvent(tEvent);

    return entity;
  }

  private static TimelineEntity createBaseEntity(String entityId,
    String entityType, Long createdTime) {
    TimelineEntity entity = new TimelineEntity();
    entity.setId(entityId);
    entity.setType(entityType);
    entity.setCreatedTime(createdTime);
    return entity;
  }

  private static TimelineEvent craeteBaseEvent(String eventType,
    long createdTime) {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(eventType);
    tEvent.setTimestamp(createdTime);
    return tEvent;
  }
}

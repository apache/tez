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

package org.apache.tez.dag.history.logging.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.EdgeProperty;
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
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezVertexID;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class HistoryEventJsonConversion {

  public static JSONObject convertToJson(HistoryEvent historyEvent) throws JSONException {
    if (!historyEvent.isHistoryEvent()) {
      throw new UnsupportedOperationException("Invalid Event, does not support history"
          + ", eventType=" + historyEvent.getEventType());
    }
    JSONObject jsonObject = null;
    switch (historyEvent.getEventType()) {
      case APP_LAUNCHED:
        jsonObject = convertAppLaunchedEvent((AppLaunchedEvent) historyEvent);
        break;
      case AM_LAUNCHED:
        jsonObject = convertAMLaunchedEvent((AMLaunchedEvent) historyEvent);
        break;
      case AM_STARTED:
        jsonObject = convertAMStartedEvent((AMStartedEvent) historyEvent);
        break;
      case CONTAINER_LAUNCHED:
        jsonObject = convertContainerLaunchedEvent((ContainerLaunchedEvent) historyEvent);
        break;
      case CONTAINER_STOPPED:
        jsonObject = convertContainerStoppedEvent((ContainerStoppedEvent) historyEvent);
        break;
      case DAG_SUBMITTED:
        jsonObject = convertDAGSubmittedEvent((DAGSubmittedEvent) historyEvent);
        break;
      case DAG_INITIALIZED:
        jsonObject = convertDAGInitializedEvent((DAGInitializedEvent) historyEvent);
        break;
      case DAG_STARTED:
        jsonObject = convertDAGStartedEvent((DAGStartedEvent) historyEvent);
        break;
      case DAG_FINISHED:
        jsonObject = convertDAGFinishedEvent((DAGFinishedEvent) historyEvent);
        break;
      case VERTEX_INITIALIZED:
        jsonObject = convertVertexInitializedEvent((VertexInitializedEvent) historyEvent);
        break;
      case VERTEX_STARTED:
        jsonObject = convertVertexStartedEvent((VertexStartedEvent) historyEvent);
        break;
      case VERTEX_FINISHED:
        jsonObject = convertVertexFinishedEvent((VertexFinishedEvent) historyEvent);
      break;
      case TASK_STARTED:
        jsonObject = convertTaskStartedEvent((TaskStartedEvent) historyEvent);
        break;
      case TASK_FINISHED:
        jsonObject = convertTaskFinishedEvent((TaskFinishedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_STARTED:
        jsonObject = convertTaskAttemptStartedEvent((TaskAttemptStartedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_FINISHED:
        jsonObject = convertTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) historyEvent);
        break;
      case VERTEX_PARALLELISM_UPDATED:
        jsonObject = convertVertexParallelismUpdatedEvent((VertexParallelismUpdatedEvent) historyEvent);
        break;
      case DAG_RECOVERED:
        jsonObject = convertDAGRecoveredEvent((DAGRecoveredEvent) historyEvent);
        break;
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_COMMIT_STARTED:
        throw new UnsupportedOperationException("Invalid Event, does not support history"
            + ", eventType=" + historyEvent.getEventType());
      default:
        throw new UnsupportedOperationException("Unhandled Event"
            + ", eventType=" + historyEvent.getEventType());
    }
    return jsonObject;
  }

  private static JSONObject convertDAGRecoveredEvent(DAGRecoveredEvent event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        event.getDagID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    JSONArray events = new JSONArray();
    JSONObject recoverEvent = new JSONObject();
    recoverEvent.put(ATSConstants.TIMESTAMP, event.getRecoveredTime());
    recoverEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_RECOVERED.name());

    JSONObject recoverEventInfo = new JSONObject();
    recoverEventInfo.put(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    if (event.getRecoveredDagState() != null) {
      recoverEventInfo.put(ATSConstants.DAG_STATE, event.getRecoveredDagState().name());
    }
    if (event.getRecoveryFailureReason() != null) {
      recoverEventInfo.put(ATSConstants.RECOVERY_FAILURE_REASON,
          event.getRecoveryFailureReason());
    }

    recoverEvent.put(ATSConstants.EVENT_INFO, recoverEventInfo);
    events.put(recoverEvent);

    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;
  }

  private static JSONObject convertAppLaunchedEvent(AppLaunchedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + event.getApplicationId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION.name());

    // Other info to tag with Tez App
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.USER, event.getUser());
    otherInfo.put(ATSConstants.CONFIG, new JSONObject(
        DAGUtils.convertConfigurationToATSMap(event.getConf())));

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertAMLaunchedEvent(AMLaunchedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + event.getApplicationAttemptId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
            EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().getApplicationId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
            ATSConstants.APPLICATION_ATTEMPT_ID);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getLaunchTime());
    initEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.AM_LAUNCHED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info to tag with Tez AM
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);
    
    return jsonObject;
  }

  private static JSONObject convertAMStartedEvent(AMStartedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + event.getApplicationAttemptId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().getApplicationId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ATTEMPT_ID);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getStartTime());
    startEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.AM_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;  }

  private static JSONObject convertContainerLaunchedEvent(ContainerLaunchedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + event.getContainerId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_CONTAINER_ID.name());

    JSONArray relatedEntities = new JSONArray();
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, event.getContainerId().toString());
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(containerEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject launchEvent = new JSONObject();
    launchEvent.put(ATSConstants.TIMESTAMP, event.getLaunchTime());
    launchEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.CONTAINER_LAUNCHED.name());
    events.put(launchEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

    return jsonObject;
  }

  private static JSONObject convertContainerStoppedEvent(ContainerStoppedEvent event) throws JSONException {
    // structure is identical to ContainerLaunchedEvent
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + event.getContainerId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_CONTAINER_ID.name());

    JSONArray relatedEntities = new JSONArray();
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, event.getContainerId().toString());
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(containerEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject stopEvent = new JSONObject();
    stopEvent.put(ATSConstants.TIMESTAMP, event.getStoppedTime());
    stopEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.CONTAINER_STOPPED.name());
    events.put(stopEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.EXIT_STATUS, event.getExitStatus());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;  }

  private static JSONObject convertDAGFinishedEvent(DAGFinishedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        event.getDagID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getFinishTime());
    finishEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, event.getStartTime());
    otherInfo.put(ATSConstants.FINISH_TIME, event.getFinishTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    otherInfo.put(ATSConstants.STATUS, event.getState().name());
    otherInfo.put(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    otherInfo.put(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToJSON(event.getTezCounters()));
    otherInfo.put(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());

    final Map<String, Integer> dagTaskStats = event.getDagTaskStats();
    if (dagTaskStats != null) {
      for(Entry<String, Integer> entry : dagTaskStats.entrySet()) {
        otherInfo.put(entry.getKey(), entry.getValue().intValue());
      }
    }

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertDAGInitializedEvent(DAGInitializedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        event.getDagID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getInitTime());
    initEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_INITIALIZED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();

    if (event.getVertexNameIDMap() != null) {
      Map<String, String> nameIdStrMap = new TreeMap<String, String>();
      for (Entry<String, TezVertexID> entry : event.getVertexNameIDMap().entrySet()) {
        nameIdStrMap.put(entry.getKey(), entry.getValue().toString());
      }
      otherInfo.put(ATSConstants.VERTEX_NAME_ID_MAPPING, nameIdStrMap);
    }
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertDAGStartedEvent(DAGStartedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        event.getDagID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getStartTime());
    startEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;
  }

  private static JSONObject convertDAGSubmittedEvent(DAGSubmittedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        event.getDagID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject tezAppEntity = new JSONObject();
    tezAppEntity.put(ATSConstants.ENTITY,
        "tez_" + event.getApplicationAttemptId().getApplicationId().toString());
    tezAppEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION.name());
    JSONObject tezAppAttemptEntity = new JSONObject();
    tezAppAttemptEntity.put(ATSConstants.ENTITY,
        "tez_" + event.getApplicationAttemptId().toString());
    tezAppAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().getApplicationId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        event.getApplicationAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ATTEMPT_ID);
    JSONObject userEntity = new JSONObject();
    userEntity.put(ATSConstants.ENTITY,
        event.getUser());
    userEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.USER);

    relatedEntities.put(tezAppEntity);
    relatedEntities.put(tezAppAttemptEntity);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(userEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // filters
    JSONObject primaryFilters = new JSONObject();
    primaryFilters.put(ATSConstants.DAG_NAME,
        event.getDAGName());
    jsonObject.put(ATSConstants.PRIMARY_FILTERS, primaryFilters);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject submitEvent = new JSONObject();
    submitEvent.put(ATSConstants.TIMESTAMP, event.getSubmitTime());
    submitEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_SUBMITTED.name());
    events.put(submitEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info such as dag plan
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.DAG_PLAN,
        DAGUtils.generateSimpleJSONPlan(event.getDAGPlan()));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskAttemptID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getFinishTime());
    finishEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.TASK_ATTEMPT_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.CREATION_TIME, event.getCreationTime());
    otherInfo.put(ATSConstants.ALLOCATION_TIME, event.getAllocationTime());
    otherInfo.put(ATSConstants.START_TIME, event.getStartTime());
    otherInfo.put(ATSConstants.FINISH_TIME, event.getFinishTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    if (event.getCreationCausalTA() != null) {
      otherInfo.put(ATSConstants.CREATION_CAUSAL_ATTEMPT, event.getCreationCausalTA().toString());
    }
    otherInfo.put(ATSConstants.STATUS, event.getState().name());
    if (event.getTaskAttemptError() != null) {
      otherInfo.put(ATSConstants.TASK_ATTEMPT_ERROR_ENUM, event.getTaskAttemptError().name());
    }
    otherInfo.put(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    otherInfo.put(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToJSON(event.getCounters()));
    if (event.getDataEvents() != null && !event.getDataEvents().isEmpty()) {
      otherInfo.put(ATSConstants.LAST_DATA_EVENTS, 
          DAGUtils.convertDataEventDependencyInfoToJSON(event.getDataEvents()));
    }
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskAttemptStartedEvent(TaskAttemptStartedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskAttemptID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject nodeEntity = new JSONObject();
    nodeEntity.put(ATSConstants.ENTITY, event.getNodeId().toString());
    nodeEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.NODE_ID);

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, event.getContainerId().toString());
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    JSONObject taskEntity = new JSONObject();
    taskEntity.put(ATSConstants.ENTITY, event.getTaskAttemptID().getTaskID().toString());
    taskEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    relatedEntities.put(nodeEntity);
    relatedEntities.put(containerEntity);
    relatedEntities.put(taskEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getStartTime());
    startEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.TASK_ATTEMPT_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    otherInfo.put(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskFinishedEvent(TaskFinishedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getFinishTime());
    finishEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.TASK_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, event.getStartTime());
    otherInfo.put(ATSConstants.FINISH_TIME, event.getFinishTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    otherInfo.put(ATSConstants.STATUS, event.getState().name());
    otherInfo.put(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    otherInfo.put(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToJSON(event.getTezCounters()));
    if (event.getSuccessfulAttemptID() != null) {
      otherInfo.put(ATSConstants.SUCCESSFUL_ATTEMPT_ID, event.getSuccessfulAttemptID().toString());
    }

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskStartedEvent(TaskStartedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getTaskID().getVertexID().toString());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getStartTime());
    startEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.TASK_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix schedule/launch time to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, event.getStartTime());
    otherInfo.put(ATSConstants.SCHEDULED_TIME, event.getScheduledTime());

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexFinishedEvent(VertexFinishedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getFinishTime());
    finishEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.VERTEX_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.FINISH_TIME, event.getFinishTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    otherInfo.put(ATSConstants.STATUS, event.getState().name());
    otherInfo.put(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    otherInfo.put(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToJSON(event.getTezCounters()));

    otherInfo.put(ATSConstants.STATS,
        DAGUtils.convertVertexStatsToJSON(event.getVertexStats()));

    final Map<String, Integer> vertexTaskStats = event.getVertexTaskStats();
    if (vertexTaskStats != null) {
      for(Entry<String, Integer> entry : vertexTaskStats.entrySet()) {
        otherInfo.put(entry.getKey(), entry.getValue().intValue());
      }
    }

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexInitializedEvent(VertexInitializedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getVertexID().getDAGId().toString());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getInitedTime());
    initEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.VERTEX_INITIALIZED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix requested times to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.VERTEX_NAME, event.getVertexName());
    otherInfo.put(ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    otherInfo.put(ATSConstants.INIT_TIME, event.getInitedTime());
    otherInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    otherInfo.put(ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexStartedEvent(VertexStartedEvent event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getVertexID().getDAGId().toString());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getStartTime());
    startEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.VERTEX_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix requested times to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    otherInfo.put(ATSConstants.START_TIME, event.getStartTime());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexParallelismUpdatedEvent(
      VertexParallelismUpdatedEvent event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexID().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject updateEvent = new JSONObject();
    updateEvent.put(ATSConstants.TIMESTAMP, event.getUpdateTime());
    updateEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.VERTEX_PARALLELISM_UPDATED.name());

    JSONObject eventInfo = new JSONObject();
    eventInfo.put(ATSConstants.OLD_NUM_TASKS, event.getOldNumTasks());
    eventInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    if (event.getSourceEdgeProperties() != null && !event.getSourceEdgeProperties().isEmpty()) {
      JSONObject updatedEdgeManagers = new JSONObject();
      for (Entry<String, EdgeProperty> entry :
          event.getSourceEdgeProperties().entrySet()) {
        updatedEdgeManagers.put(entry.getKey(),
            new JSONObject(DAGUtils.convertEdgeProperty(entry.getValue())));
      }
      eventInfo.put(ATSConstants.UPDATED_EDGE_MANAGERS, updatedEdgeManagers);
    }
    updateEvent.put(ATSConstants.EVENT_INFO, eventInfo);
    events.put(updateEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    // TODO add more on all other updated information
    return jsonObject;
  }

}

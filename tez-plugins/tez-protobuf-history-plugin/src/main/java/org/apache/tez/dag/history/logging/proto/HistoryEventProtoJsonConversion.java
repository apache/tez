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

package org.apache.tez.dag.history.logging.proto;

import java.util.Iterator;
import java.util.Optional;

import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Convert HistoryEventProto into JSONObject for analyzers, which can already consume the output of
 * SimpleHistoryLoggingService's JSONs. This class is based on HistoryEventJsonConversion, and all
 * of the specific HistoryEvent calls were transformed info HistoryEventProto calls by taking the
 * corresponding HistoryEventProtoConverter methods into consideration.
 */
public final class HistoryEventProtoJsonConversion {

  private HistoryEventProtoJsonConversion() {
  }

  public static JSONObject convertToJson(HistoryEventProto historyEvent) throws JSONException {
    JSONObject jsonObject = null;

    switch (historyEvent.getEventType()) {
    case "APP_LAUNCHED":
      jsonObject = convertAppLaunchedEvent(historyEvent);
      break;
    case "AM_LAUNCHED":
      jsonObject = convertAMLaunchedEvent(historyEvent);
      break;
    case "AM_STARTED":
      jsonObject = convertAMStartedEvent(historyEvent);
      break;
    case "CONTAINER_LAUNCHED":
      jsonObject = convertContainerLaunchedEvent(historyEvent);
      break;
    case "CONTAINER_STOPPED":
      jsonObject = convertContainerStoppedEvent(historyEvent);
      break;
    case "DAG_SUBMITTED":
      jsonObject = convertDAGSubmittedEvent(historyEvent);
      break;
    case "DAG_INITIALIZED":
      jsonObject = convertDAGInitializedEvent(historyEvent);
      break;
    case "DAG_STARTED":
      jsonObject = convertDAGStartedEvent(historyEvent);
      break;
    case "DAG_FINISHED":
      jsonObject = convertDAGFinishedEvent(historyEvent);
      break;
    case "VERTEX_INITIALIZED":
      jsonObject = convertVertexInitializedEvent(historyEvent);
      break;
    case "VERTEX_STARTED":
      jsonObject = convertVertexStartedEvent(historyEvent);
      break;
    case "VERTEX_FINISHED":
      jsonObject = convertVertexFinishedEvent(historyEvent);
      break;
    case "TASK_STARTED":
      jsonObject = convertTaskStartedEvent(historyEvent);
      break;
    case "TASK_FINISHED":
      jsonObject = convertTaskFinishedEvent(historyEvent);
      break;
    case "TASK_ATTEMPT_STARTED":
      jsonObject = convertTaskAttemptStartedEvent(historyEvent);
      break;
    case "TASK_ATTEMPT_FINISHED":
      jsonObject = convertTaskAttemptFinishedEvent(historyEvent);
      break;
    case "VERTEX_CONFIGURE_DONE":
      jsonObject = convertVertexReconfigureDoneEvent(historyEvent);
      break;
    case "DAG_RECOVERED":
      jsonObject = convertDAGRecoveredEvent(historyEvent);
      break;
    case "VERTEX_COMMIT_STARTED":
    case "VERTEX_GROUP_COMMIT_STARTED":
    case "VERTEX_GROUP_COMMIT_FINISHED":
    case "DAG_COMMIT_STARTED":
      throw new UnsupportedOperationException(
          "Invalid Event, does not support history" + ", eventType=" + historyEvent.getEventType());
    default:
      throw new UnsupportedOperationException(
          "Unhandled Event" + ", eventType=" + historyEvent.getEventType());
    }
    return jsonObject;
  }

  private static JSONObject convertDAGRecoveredEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getDagId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    JSONArray events = new JSONArray();
    JSONObject recoverEvent = new JSONObject();
    recoverEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    recoverEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.DAG_RECOVERED.name());

    JSONObject recoverEventInfo = new JSONObject();
    recoverEventInfo.put(ATSConstants.APPLICATION_ATTEMPT_ID, event.getAppAttemptId().toString());
    recoverEventInfo.put(ATSConstants.DAG_STATE, getDataValueByKey(event, ATSConstants.DAG_STATE));
    recoverEventInfo.put(ATSConstants.RECOVERY_FAILURE_REASON,
        getDataValueByKey(event, ATSConstants.RECOVERY_FAILURE_REASON));

    recoverEvent.put(ATSConstants.EVENT_INFO, recoverEventInfo);
    events.put(recoverEvent);

    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;
  }

  private static JSONObject convertAppLaunchedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, "tez_" + event.getAppId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION.name());

    // Other info to tag with Tez App
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.USER, event.getUser());
    otherInfo.put(ATSConstants.CONFIG, new JSONObject()); // TODO: config from proto?

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertAMLaunchedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, "tez_" + event.getAppAttemptId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY, event.getAppId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY, event.getAppAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ATTEMPT_ID);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    initEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.AM_LAUNCHED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info to tag with Tez AM
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.APP_SUBMIT_TIME,
        getDataValueByKey(event, ATSConstants.APP_SUBMIT_TIME));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertAMStartedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, "tez_" + event.getAppAttemptId().toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY, event.getAppId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY, event.getAppAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ATTEMPT_ID);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    startEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.AM_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;
  }

  private static JSONObject convertContainerLaunchedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_CONTAINER_ID.name());

    JSONArray relatedEntities = new JSONArray();
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY, event.getAppAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(containerEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject launchEvent = new JSONObject();
    launchEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    launchEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.CONTAINER_LAUNCHED.name());
    events.put(launchEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

    return jsonObject;
  }

  private static JSONObject convertContainerStoppedEvent(HistoryEventProto event)
      throws JSONException {
    // structure is identical to ContainerLaunchedEvent
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_CONTAINER_ID.name());

    JSONArray relatedEntities = new JSONArray();
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY, event.getAppAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(containerEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject stopEvent = new JSONObject();
    stopEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    stopEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.CONTAINER_STOPPED.name());
    events.put(stopEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.EXIT_STATUS, getDataValueByKey(event, ATSConstants.EXIT_STATUS));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertDAGFinishedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getDagId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    finishEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.DAG_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();

    long startTime = getLongDataValueByKey(event, ATSConstants.START_TIME);

    otherInfo.put(ATSConstants.START_TIME, startTime);
    otherInfo.put(ATSConstants.FINISH_TIME, event.getEventTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, event.getEventTime() - startTime);
    otherInfo.put(ATSConstants.STATUS, getDataValueByKey(event, ATSConstants.STATUS));
    otherInfo.put(ATSConstants.DIAGNOSTICS, getDataValueByKey(event, ATSConstants.DIAGNOSTICS));
    otherInfo.put(ATSConstants.COUNTERS, getJSONDataValueByKey(event, ATSConstants.COUNTERS));
    otherInfo.put(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        event.getAppAttemptId().toString());

    // added all info to otherInfo in order to cover
    // all key/value pairs added from event.getDagTaskStats()
    Iterator<KVPair> it = event.getEventDataList().iterator();
    while (it.hasNext()) {
      KVPair pair = it.next();
      otherInfo.put(pair.getKey(), pair.getValue());
    }

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertDAGInitializedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getDagId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    initEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.DAG_INITIALIZED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.VERTEX_NAME_ID_MAPPING,
        getJSONDataValueByKey(event, ATSConstants.VERTEX_NAME_ID_MAPPING));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertDAGStartedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getDagId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    startEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.DAG_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    return jsonObject;
  }

  private static JSONObject convertDAGSubmittedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getDagId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject tezAppEntity = new JSONObject();
    tezAppEntity.put(ATSConstants.ENTITY, "tez_" + event.getAppId().toString());
    tezAppEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION.name());
    JSONObject tezAppAttemptEntity = new JSONObject();
    tezAppAttemptEntity.put(ATSConstants.ENTITY, "tez_" + event.getAppAttemptId().toString());
    tezAppAttemptEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_APPLICATION_ATTEMPT.name());
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY, event.getAppId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY, event.getAppAttemptId().toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.APPLICATION_ATTEMPT_ID);
    JSONObject userEntity = new JSONObject();
    userEntity.put(ATSConstants.ENTITY, event.getUser());
    userEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.USER);

    relatedEntities.put(tezAppEntity);
    relatedEntities.put(tezAppAttemptEntity);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(userEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // filters
    JSONObject primaryFilters = new JSONObject();
    primaryFilters.put(ATSConstants.DAG_NAME, getDataValueByKey(event, ATSConstants.DAG_NAME));
    primaryFilters.put(ATSConstants.CALLER_CONTEXT_ID,
        getDataValueByKey(event, ATSConstants.CALLER_CONTEXT_ID));
    primaryFilters.put(ATSConstants.CALLER_CONTEXT_TYPE,
        getDataValueByKey(event, ATSConstants.CALLER_CONTEXT_TYPE));
    primaryFilters.put(ATSConstants.DAG_QUEUE_NAME,
        getDataValueByKey(event, ATSConstants.DAG_QUEUE_NAME));

    jsonObject.put(ATSConstants.PRIMARY_FILTERS, primaryFilters);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject submitEvent = new JSONObject();
    submitEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    submitEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.DAG_SUBMITTED.name());
    events.put(submitEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info such as dag plan
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.DAG_PLAN, getJSONDataValueByKey(event, ATSConstants.DAG_PLAN));

    otherInfo.put(ATSConstants.CALLER_CONTEXT_ID,
        getDataValueByKey(event, ATSConstants.CALLER_CONTEXT_ID));
    otherInfo.put(ATSConstants.CALLER_CONTEXT_TYPE,
        getDataValueByKey(event, ATSConstants.CALLER_CONTEXT_TYPE));
    otherInfo.put(ATSConstants.DAG_QUEUE_NAME,
        getDataValueByKey(event, ATSConstants.DAG_QUEUE_NAME));

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskAttemptFinishedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskAttemptId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    finishEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.TASK_ATTEMPT_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    long startTime = getLongDataValueByKey(event, ATSConstants.START_TIME);

    otherInfo.put(ATSConstants.CREATION_TIME, getDataValueByKey(event, ATSConstants.CREATION_TIME));
    otherInfo.put(ATSConstants.ALLOCATION_TIME,
        getDataValueByKey(event, ATSConstants.ALLOCATION_TIME));
    otherInfo.put(ATSConstants.START_TIME, startTime);
    otherInfo.put(ATSConstants.FINISH_TIME, event.getEventTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, event.getEventTime() - startTime);

    otherInfo.put(ATSConstants.CREATION_CAUSAL_ATTEMPT,
        getDataValueByKey(event, ATSConstants.CREATION_CAUSAL_ATTEMPT));
    otherInfo.put(ATSConstants.STATUS, getDataValueByKey(event, ATSConstants.STATUS));

    otherInfo.put(ATSConstants.STATUS, getDataValueByKey(event, ATSConstants.STATUS));
    otherInfo.put(ATSConstants.TASK_ATTEMPT_ERROR_ENUM,
        getDataValueByKey(event, ATSConstants.TASK_ATTEMPT_ERROR_ENUM));
    otherInfo.put(ATSConstants.TASK_FAILURE_TYPE,
        getDataValueByKey(event, ATSConstants.TASK_FAILURE_TYPE));
    otherInfo.put(ATSConstants.DIAGNOSTICS, getDataValueByKey(event, ATSConstants.DIAGNOSTICS));
    otherInfo.put(ATSConstants.COUNTERS, getJSONDataValueByKey(event, ATSConstants.COUNTERS));
    otherInfo.put(ATSConstants.LAST_DATA_EVENTS,
        getJSONDataValueByKey(event, ATSConstants.LAST_DATA_EVENTS));
    otherInfo.put(ATSConstants.NODE_ID, getDataValueByKey(event, ATSConstants.NODE_ID));
    otherInfo.put(ATSConstants.CONTAINER_ID, getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    otherInfo.put(ATSConstants.IN_PROGRESS_LOGS_URL,
        getDataValueByKey(event, ATSConstants.IN_PROGRESS_LOGS_URL));
    otherInfo.put(ATSConstants.COMPLETED_LOGS_URL,
        getDataValueByKey(event, ATSConstants.COMPLETED_LOGS_URL));
    otherInfo.put(ATSConstants.NODE_HTTP_ADDRESS,
        getDataValueByKey(event, ATSConstants.NODE_HTTP_ADDRESS));

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskAttemptStartedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskAttemptId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject nodeEntity = new JSONObject();
    nodeEntity.put(ATSConstants.ENTITY, getDataValueByKey(event, ATSConstants.NODE_ID));
    nodeEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.NODE_ID);

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, getDataValueByKey(event, ATSConstants.CONTAINER_ID));
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    JSONObject taskEntity = new JSONObject();
    taskEntity.put(ATSConstants.ENTITY, event.getTaskAttemptId());
    taskEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    relatedEntities.put(nodeEntity);
    relatedEntities.put(containerEntity);
    relatedEntities.put(taskEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    startEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.TASK_ATTEMPT_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.IN_PROGRESS_LOGS_URL,
        getDataValueByKey(event, ATSConstants.IN_PROGRESS_LOGS_URL));
    otherInfo.put(ATSConstants.COMPLETED_LOGS_URL,
        getDataValueByKey(event, ATSConstants.COMPLETED_LOGS_URL));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskFinishedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    finishEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.TASK_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    long startTime = getLongDataValueByKey(event, ATSConstants.START_TIME);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, startTime);
    otherInfo.put(ATSConstants.FINISH_TIME, event.getEventTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, event.getEventTime() - startTime);

    otherInfo.put(ATSConstants.STATUS, getDataValueByKey(event, ATSConstants.STATUS));
    otherInfo.put(ATSConstants.DIAGNOSTICS, getDataValueByKey(event, ATSConstants.DIAGNOSTICS));
    otherInfo.put(ATSConstants.COUNTERS, getJSONDataValueByKey(event, ATSConstants.COUNTERS));
    otherInfo.put(ATSConstants.SUCCESSFUL_ATTEMPT_ID,
        getDataValueByKey(event, ATSConstants.SUCCESSFUL_ATTEMPT_ID));

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertTaskStartedEvent(HistoryEventProto event) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getTaskId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_TASK_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getVertexId());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    startEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.TASK_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix schedule/launch time to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, event.getEventTime());
    otherInfo.put(ATSConstants.SCHEDULED_TIME,
        getDataValueByKey(event, ATSConstants.SCHEDULED_TIME));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexFinishedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    finishEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.VERTEX_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    long startTime = getLongDataValueByKey(event, ATSConstants.START_TIME);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.FINISH_TIME, event.getEventTime());
    otherInfo.put(ATSConstants.TIME_TAKEN, (event.getEventTime() - startTime));
    otherInfo.put(ATSConstants.STATUS, getDataValueByKey(event, ATSConstants.STATUS));
    otherInfo.put(ATSConstants.DIAGNOSTICS, getDataValueByKey(event, ATSConstants.DIAGNOSTICS));
    otherInfo.put(ATSConstants.COUNTERS, getJSONDataValueByKey(event, ATSConstants.COUNTERS));

    otherInfo.put(ATSConstants.STATS, getJSONDataValueByKey(event, ATSConstants.STATS));

    // added all info to otherInfo in order to cover
    // all key/value pairs added from event.getVertexTaskStats()
    Iterator<KVPair> it = event.getEventDataList().iterator();
    while (it.hasNext()) {
      KVPair pair = it.next();
      otherInfo.put(pair.getKey(), pair.getValue());
    }

    otherInfo.put(ATSConstants.SERVICE_PLUGIN,
        getJSONDataValueByKey(event, ATSConstants.SERVICE_PLUGIN));

    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexReconfigureDoneEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Events
    JSONArray events = new JSONArray();
    JSONObject updateEvent = new JSONObject();
    updateEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    updateEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.VERTEX_CONFIGURE_DONE.name());

    JSONObject eventInfo = new JSONObject();
    eventInfo.put(ATSConstants.NUM_TASKS, getDataValueByKey(event, ATSConstants.NUM_TASKS));
    eventInfo.put(ATSConstants.UPDATED_EDGE_MANAGERS,
        getJSONDataValueByKey(event, ATSConstants.UPDATED_EDGE_MANAGERS));
    updateEvent.put(ATSConstants.EVENT_INFO, eventInfo);
    events.put(updateEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    JSONObject otherInfo = new JSONObject();
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    // TODO add more on all other updated information
    return jsonObject;
  }

  private static JSONObject convertVertexInitializedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getDagId());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    initEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.VERTEX_INITIALIZED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix requested times to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.VERTEX_NAME, getDataValueByKey(event, ATSConstants.VERTEX_NAME));
    otherInfo.put(ATSConstants.INIT_REQUESTED_TIME,
        getDataValueByKey(event, ATSConstants.INIT_REQUESTED_TIME));
    otherInfo.put(ATSConstants.INIT_TIME, getDataValueByKey(event, ATSConstants.INIT_TIME));
    otherInfo.put(ATSConstants.NUM_TASKS, getDataValueByKey(event, ATSConstants.NUM_TASKS));
    otherInfo.put(ATSConstants.PROCESSOR_CLASS_NAME,
        getDataValueByKey(event, ATSConstants.PROCESSOR_CLASS_NAME));
    otherInfo.put(ATSConstants.SERVICE_PLUGIN,
        getJSONDataValueByKey(event, ATSConstants.SERVICE_PLUGIN));
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static JSONObject convertVertexStartedEvent(HistoryEventProto event)
      throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, event.getVertexId());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, event.getDagId());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject startEvent = new JSONObject();
    startEvent.put(ATSConstants.TIMESTAMP, event.getEventTime());
    startEvent.put(ATSConstants.EVENT_TYPE, HistoryEventType.VERTEX_STARTED.name());
    events.put(startEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix requested times to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_REQUESTED_TIME,
        getDataValueByKey(event, ATSConstants.START_REQUESTED_TIME));
    otherInfo.put(ATSConstants.START_TIME, event.getEventTime());
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  private static String getDataValueByKey(HistoryEventProto event, String key) {
    Optional<KVPair> pair =
        event.getEventDataList().stream().filter(p -> p.getKey().equals(key)).findAny();
    return pair.isPresent() ? pair.get().getValue() : null;
  }

  private static long getLongDataValueByKey(HistoryEventProto event, String key) {
    String value = getDataValueByKey(event, key);
    return (value == null || value.isEmpty()) ? 0 : Long.parseLong(value);
  }

  private static JSONObject getJSONDataValueByKey(HistoryEventProto event, String key)
      throws JSONException {
    String value = getDataValueByKey(event, key);
    return (value == null || value.isEmpty()) ? null : new JSONObject(value);
  }
}

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

package org.apache.tez.dag.history.logging.ats;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.history.utils.DAGUtils;

public class HistoryEventTimelineConversion {

  public static TimelineEntity convertToTimelineEntity(HistoryEvent historyEvent) {
    if (!historyEvent.isHistoryEvent()) {
      throw new UnsupportedOperationException("Invalid Event, does not support history"
          + ", eventType=" + historyEvent.getEventType());
    }
    TimelineEntity timelineEntity = null;
    switch (historyEvent.getEventType()) {
      case AM_LAUNCHED:
        timelineEntity = convertAMLaunchedEvent((AMLaunchedEvent) historyEvent);
        break;
      case AM_STARTED:
        timelineEntity = convertAMStartedEvent((AMStartedEvent) historyEvent);
        break;
      case CONTAINER_LAUNCHED:
        timelineEntity = convertContainerLaunchedEvent((ContainerLaunchedEvent) historyEvent);
        break;
      case CONTAINER_STOPPED:
        timelineEntity = convertContainerStoppedEvent((ContainerStoppedEvent) historyEvent);
        break;
      case DAG_SUBMITTED:
        timelineEntity = convertDAGSubmittedEvent((DAGSubmittedEvent) historyEvent);
        break;
      case DAG_INITIALIZED:
        timelineEntity = convertDAGInitializedEvent((DAGInitializedEvent) historyEvent);
        break;
      case DAG_STARTED:
        timelineEntity = convertDAGStartedEvent((DAGStartedEvent) historyEvent);
        break;
      case DAG_FINISHED:
        timelineEntity = convertDAGFinishedEvent((DAGFinishedEvent) historyEvent);
        break;
      case VERTEX_INITIALIZED:
        timelineEntity = convertVertexInitializedEvent((VertexInitializedEvent) historyEvent);
        break;
      case VERTEX_STARTED:
        timelineEntity = convertVertexStartedEvent((VertexStartedEvent) historyEvent);
        break;
      case VERTEX_FINISHED:
        timelineEntity = convertVertexFinishedEvent((VertexFinishedEvent) historyEvent);
      break;
      case TASK_STARTED:
        timelineEntity = convertTaskStartedEvent((TaskStartedEvent) historyEvent);
        break;
      case TASK_FINISHED:
        timelineEntity = convertTaskFinishedEvent((TaskFinishedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_STARTED:
        timelineEntity = convertTaskAttemptStartedEvent((TaskAttemptStartedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_FINISHED:
        timelineEntity = convertTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) historyEvent);
        break;
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case VERTEX_PARALLELISM_UPDATED:
      case DAG_COMMIT_STARTED:
        throw new UnsupportedOperationException("Invalid Event, does not support history"
            + ", eventType=" + historyEvent.getEventType());
      default:
        throw new UnsupportedOperationException("Unhandled Event"
            + ", eventType=" + historyEvent.getEventType());
    }
    return timelineEntity;
  }

  private static TimelineEntity convertAMLaunchedEvent(AMLaunchedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getApplicationAttemptId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    atsEntity.addRelatedEntity(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addRelatedEntity(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    atsEntity.addRelatedEntity(ATSConstants.USER, event.getUser());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());

    atsEntity.setStartTime(event.getLaunchTime());

    TimelineEvent launchEvt = new TimelineEvent();
    launchEvt.setEventType(HistoryEventType.AM_LAUNCHED.name());
    launchEvt.setTimestamp(event.getLaunchTime());
    atsEntity.addEvent(launchEvt);

    atsEntity.addOtherInfo(ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());

    return atsEntity;
  }

  private static TimelineEntity convertAMStartedEvent(AMStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getApplicationAttemptId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.AM_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    return atsEntity;
  }

  private static TimelineEntity convertContainerLaunchedEvent(ContainerLaunchedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getContainerId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_CONTAINER_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());
    atsEntity.addRelatedEntity(ATSConstants.CONTAINER_ID,
        event.getContainerId().toString());

    atsEntity.setStartTime(event.getLaunchTime());

    TimelineEvent launchEvt = new TimelineEvent();
    launchEvt.setEventType(HistoryEventType.CONTAINER_LAUNCHED.name());
    launchEvt.setTimestamp(event.getLaunchTime());
    atsEntity.addEvent(launchEvt);

    return atsEntity;
  }

  private static TimelineEntity convertContainerStoppedEvent(ContainerStoppedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getContainerId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_CONTAINER_ID.name());

    // In case, a container is stopped in a different attempt
    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());

    TimelineEvent stoppedEvt = new TimelineEvent();
    stoppedEvt.setEventType(HistoryEventType.CONTAINER_STOPPED.name());
    stoppedEvt.setTimestamp(event.getStoppedTime());
    atsEntity.addEvent(stoppedEvt);

    atsEntity.addOtherInfo(ATSConstants.EXIT_STATUS, event.getExitStatus());
    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getStoppedTime());

    return atsEntity;
  }

  private static TimelineEntity convertDAGFinishedEvent(DAGFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.DAG_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());
    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    return atsEntity;
  }

  private static TimelineEntity convertDAGInitializedEvent(DAGInitializedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.DAG_INITIALIZED.name());
    finishEvt.setTimestamp(event.getInitTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    atsEntity.addOtherInfo(ATSConstants.INIT_TIME, event.getInitTime());

    return atsEntity;
  }

  private static TimelineEntity convertDAGStartedEvent(DAGStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.DAG_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());

    return atsEntity;
  }

  private static TimelineEntity convertDAGSubmittedEvent(DAGSubmittedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());
    atsEntity.addRelatedEntity(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addRelatedEntity(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    atsEntity.addRelatedEntity(ATSConstants.USER, event.getUser());

    TimelineEvent submitEvt = new TimelineEvent();
    submitEvt.setEventType(HistoryEventType.DAG_SUBMITTED.name());
    submitEvt.setTimestamp(event.getSubmitTime());
    atsEntity.addEvent(submitEvt);

    atsEntity.setStartTime(event.getSubmitTime());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDAGName());

    atsEntity.addOtherInfo(ATSConstants.DAG_PLAN,
        DAGUtils.convertDAGPlanToATSMap(event.getDAGPlan()));
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());

    return atsEntity;
  }

  private static TimelineEntity convertTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskAttemptID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.TASK_ATTEMPT_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());
    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getCounters()));

    return atsEntity;
  }

  private static TimelineEntity convertTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskAttemptID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    atsEntity.setStartTime(event.getStartTime());

    atsEntity.addRelatedEntity(ATSConstants.NODE_ID, event.getNodeId().toString());
    atsEntity.addRelatedEntity(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    atsEntity.addRelatedEntity(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.TASK_ATTEMPT_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    atsEntity.addOtherInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());

    return atsEntity;
  }

  private static TimelineEntity convertTaskFinishedEvent(TaskFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ID.name());

    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.TASK_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    return atsEntity;
  }

  private static TimelineEntity convertTaskStartedEvent(TaskStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.TASK_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.SCHEDULED_TIME, event.getScheduledTime());

    return atsEntity;
  }

  private static TimelineEntity convertVertexFinishedEvent(VertexFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.VERTEX_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    atsEntity.addOtherInfo(ATSConstants.STATS,
        DAGUtils.convertVertexStatsToATSMap(event.getVertexStats()));

    return atsEntity;
  }

  private static TimelineEntity convertVertexInitializedEvent(VertexInitializedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent initEvt = new TimelineEvent();
    initEvt.setEventType(HistoryEventType.VERTEX_INITIALIZED.name());
    initEvt.setTimestamp(event.getInitedTime());
    atsEntity.addEvent(initEvt);

    atsEntity.addOtherInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    atsEntity.addOtherInfo(ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    atsEntity.addOtherInfo(ATSConstants.INIT_TIME, event.getInitedTime());
    atsEntity.addOtherInfo(ATSConstants.NUM_TASKS, event.getNumTasks());
    atsEntity.addOtherInfo(ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());

    return atsEntity;
  }

  private static TimelineEntity convertVertexStartedEvent(VertexStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.VERTEX_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());

    return atsEntity;
  }

}

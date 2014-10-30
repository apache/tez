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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexRecoverableEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHistoryEventTimelineConversion {

  private ApplicationAttemptId applicationAttemptId;
  private ApplicationId applicationId;
  private String user = "user";
  private Random random = new Random();
  private TezDAGID tezDAGID;
  private TezVertexID tezVertexID;
  private TezTaskID tezTaskID;
  private TezTaskAttemptID tezTaskAttemptID;
  private DAGPlan dagPlan;
  private ContainerId containerId;
  private NodeId nodeId;

  @Before
  public void setup() {
    applicationId = ApplicationId.newInstance(9999l, 1);
    applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    tezDAGID = TezDAGID.getInstance(applicationId, random.nextInt());
    tezVertexID = TezVertexID.getInstance(tezDAGID, random.nextInt());
    tezTaskID = TezTaskID.getInstance(tezVertexID, random.nextInt());
    tezTaskAttemptID = TezTaskAttemptID.getInstance(tezTaskID, random.nextInt());
    dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock").build();
    containerId = ContainerId.newInstance(applicationAttemptId, 111);
    nodeId = NodeId.newInstance("node", 13435);
  }

  @Test
  public void testHandlerExists() throws JSONException {
    for (HistoryEventType eventType : HistoryEventType.values()) {
      HistoryEvent event = null;
      switch (eventType) {
        case APP_LAUNCHED:
          event = new AppLaunchedEvent(applicationId, random.nextInt(), random.nextInt(),
              user, new Configuration(false));
          break;
        case AM_LAUNCHED:
          event = new AMLaunchedEvent(applicationAttemptId, random.nextInt(), random.nextInt(),
              user);
          break;
        case AM_STARTED:
          event = new AMStartedEvent(applicationAttemptId, random.nextInt(), user);
          break;
        case DAG_SUBMITTED:
          event = new DAGSubmittedEvent(tezDAGID, random.nextInt(), dagPlan, applicationAttemptId,
              null, user);
          break;
        case DAG_INITIALIZED:
          event = new DAGInitializedEvent(tezDAGID, random.nextInt(), user, dagPlan.getName(), null);
          break;
        case DAG_STARTED:
          event = new DAGStartedEvent(tezDAGID, random.nextInt(), user, dagPlan.getName());
          break;
        case DAG_FINISHED:
          event = new DAGFinishedEvent(tezDAGID, random.nextInt(), random.nextInt(), DAGState.ERROR,
              null, null, user, dagPlan.getName(), null);
          break;
        case VERTEX_INITIALIZED:
          event = new VertexInitializedEvent(tezVertexID, "v1", random.nextInt(), random.nextInt(),
              random.nextInt(), "proc", null);
          break;
        case VERTEX_STARTED:
          event = new VertexStartedEvent(tezVertexID, random.nextInt(), random.nextInt());
          break;
        case VERTEX_PARALLELISM_UPDATED:
          event = new VertexParallelismUpdatedEvent(tezVertexID, 1, null, null, null, 1);
          break;
        case VERTEX_FINISHED:
          event = new VertexFinishedEvent(tezVertexID, "v1", random.nextInt(), random.nextInt(),
              random.nextInt(), random.nextInt(), random.nextInt(), VertexState.ERROR,
              null, null, null, null);
          break;
        case TASK_STARTED:
          event = new TaskStartedEvent(tezTaskID, "v1", random.nextInt(), random.nextInt());
          break;
        case TASK_FINISHED:
          event = new TaskFinishedEvent(tezTaskID, "v1", random.nextInt(), random.nextInt(),
              tezTaskAttemptID, TaskState.FAILED, null, null);
          break;
        case TASK_ATTEMPT_STARTED:
          event = new TaskAttemptStartedEvent(tezTaskAttemptID, "v1", random.nextInt(), containerId,
              nodeId, null, null, "nodeHttpAddress");
          break;
        case TASK_ATTEMPT_FINISHED:
          event = new TaskAttemptFinishedEvent(tezTaskAttemptID, "v1", random.nextInt(),
              random.nextInt(), TaskAttemptState.FAILED, null, null);
          break;
        case CONTAINER_LAUNCHED:
          event = new ContainerLaunchedEvent(containerId, random.nextInt(),
              applicationAttemptId);
          break;
        case CONTAINER_STOPPED:
          event = new ContainerStoppedEvent(containerId, random.nextInt(), -1, applicationAttemptId);
          break;
        case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
          event = new VertexRecoverableEventsGeneratedEvent();
          break;
        case DAG_COMMIT_STARTED:
          event = new DAGCommitStartedEvent();
          break;
        case VERTEX_COMMIT_STARTED:
          event = new VertexCommitStartedEvent();
          break;
        case VERTEX_GROUP_COMMIT_STARTED:
          event = new VertexGroupCommitStartedEvent();
          break;
        case VERTEX_GROUP_COMMIT_FINISHED:
          event = new VertexGroupCommitFinishedEvent();
          break;
        default:
          Assert.fail("Unhandled event type " + eventType);
      }
      if (event == null || !event.isHistoryEvent()) {
        continue;
      }
      HistoryEventTimelineConversion.convertToTimelineEntity(event);
    }
  }

  @Test
  public void testConvertAppLaunchedEvent() {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    conf.set("applicationId", "1234");


    AppLaunchedEvent event = new AppLaunchedEvent(applicationId, launchTime,
        submitTime, user, conf);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);

    Assert.assertEquals(EntityTypes.TEZ_APPLICATION.name(), timelineEntity.getEntityType());
    Assert.assertEquals("tez_" + applicationId.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(2, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(timelineEntity.getRelatedEntities().get(ATSConstants.USER).contains(user));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));

    Assert.assertEquals(1, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));

    Assert.assertEquals(1, timelineEntity.getOtherInfo().size());
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.CONFIG));

    Map<String, String> config =
        (Map<String, String>)timelineEntity.getOtherInfo().get(ATSConstants.CONFIG);
    Assert.assertEquals(conf.get("foo"), config.get("foo"));
    Assert.assertEquals(conf.get("applicationId"), config.get("applicationId"));
  }

  @Test
  public void testConvertContainerLaunchedEvent() {
    long launchTime = random.nextLong();
    ContainerLaunchedEvent event = new ContainerLaunchedEvent(containerId, launchTime,
        applicationAttemptId);
    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);

    Assert.assertEquals(EntityTypes.TEZ_CONTAINER_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals("tez_" + containerId.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(2, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(timelineEntity.getRelatedEntities().get(ATSConstants.CONTAINER_ID).contains(
        containerId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_APPLICATION_ATTEMPT.name()).contains(
            "tez_" + applicationAttemptId.toString()));

    Assert.assertEquals(1, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
        applicationAttemptId.getApplicationId().toString()));

    Assert.assertEquals(launchTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    Assert.assertEquals(HistoryEventType.CONTAINER_LAUNCHED.name(),
        timelineEntity.getEvents().get(0).getEventType());
    Assert.assertEquals(launchTime,
        timelineEntity.getEvents().get(0).getTimestamp());
  }

  @Test
  public void testConvertDAGSubmittedEvent() {
    long submitTime = random.nextLong();

    DAGSubmittedEvent event = new DAGSubmittedEvent(tezDAGID, submitTime, dagPlan,
        applicationAttemptId, null, user);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(4, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_APPLICATION_ATTEMPT.name()).contains(
            "tez_" + applicationAttemptId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.APPLICATION_ATTEMPT_ID).contains(
            applicationAttemptId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.APPLICATION_ID).contains(
            applicationAttemptId.getApplicationId().toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.USER).contains(user));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_SUBMITTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(submitTime, timelineEvent.getTimestamp());

    Assert.assertEquals(submitTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());

    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains(
            dagPlan.getName()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationAttemptId.getApplicationId().toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.DAG_PLAN));
    Assert.assertEquals(applicationId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.APPLICATION_ID));

  }

  @Test
  public void testConvertDAGInitializedEvent() {
    long initTime = random.nextLong();

    Map<String,TezVertexID> nameIdMap = new HashMap<String, TezVertexID>();
    nameIdMap.put("foo", tezVertexID);

    DAGInitializedEvent event = new DAGInitializedEvent(tezDAGID, initTime, "user", "dagName",
        nameIdMap);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_INITIALIZED.name(), timelineEvent.getEventType());
    Assert.assertEquals(initTime, timelineEvent.getTimestamp());

    Assert.assertEquals(2, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains("dagName"));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(
        ATSConstants.VERTEX_NAME_ID_MAPPING));
    Map<String, String> vIdMap = (Map<String, String>) timelineEntity.getOtherInfo().get(
        ATSConstants.VERTEX_NAME_ID_MAPPING);
    Assert.assertEquals(1, vIdMap.size());
    Assert.assertNotNull(vIdMap.containsKey("foo"));
    Assert.assertEquals(tezVertexID.toString(), vIdMap.get("foo"));

  }

  @Test
  public void testConvertDAGFinishedEvent() {
    long finishTime = random.nextLong();
    long startTime = random.nextLong();
    Map<String,Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put("FOO", 100);
    taskStats.put("BAR", 200);

    DAGFinishedEvent event = new DAGFinishedEvent(tezDAGID, startTime, finishTime, DAGState.ERROR,
        "diagnostics", null, user, dagPlan.getName(), taskStats);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_FINISHED.name(), timelineEvent.getEventType());
    Assert.assertEquals(finishTime, timelineEvent.getTimestamp());

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains(dagPlan.getName()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.STATUS).contains(
            DAGState.ERROR.name()));

    Assert.assertEquals(startTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.START_TIME)).longValue());
    Assert.assertEquals(finishTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.FINISH_TIME)).longValue());
    Assert.assertEquals(finishTime - startTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.TIME_TAKEN)).longValue());
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.COUNTERS));
    Assert.assertEquals(DAGState.ERROR.name(),
        timelineEntity.getOtherInfo().get(ATSConstants.STATUS));
    Assert.assertEquals("diagnostics",
        timelineEntity.getOtherInfo().get(ATSConstants.DIAGNOSTICS));

    Assert.assertEquals(100,
        ((Integer)timelineEntity.getOtherInfo().get("FOO")).intValue());
    Assert.assertEquals(200,
        ((Integer)timelineEntity.getOtherInfo().get("BAR")).intValue());
  }

  @Test
  public void testConvertVertexInitializedEvent() {
    long initRequestedTime = random.nextLong();
    long initedTime = random.nextLong();
    int numTasks = random.nextInt();
    VertexInitializedEvent event = new VertexInitializedEvent(tezVertexID, "v1", initRequestedTime,
        initedTime, numTasks, "proc", null);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_VERTEX_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezVertexID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_DAG_ID.name()).contains(
            tezDAGID.toString()));

    Assert.assertEquals(2, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_DAG_ID.name()).contains(
            tezDAGID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.VERTEX_INITIALIZED.name(), timelineEvent.getEventType());
    Assert.assertEquals(initedTime, timelineEvent.getTimestamp());

    Assert.assertEquals("v1", timelineEntity.getOtherInfo().get(ATSConstants.VERTEX_NAME));
    Assert.assertEquals("proc", timelineEntity.getOtherInfo().get(ATSConstants.PROCESSOR_CLASS_NAME));

    Assert.assertEquals(initedTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.INIT_TIME)).longValue());
    Assert.assertEquals(initRequestedTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.INIT_REQUESTED_TIME)).longValue());
    Assert.assertEquals(initedTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.INIT_TIME)).longValue());
    Assert.assertEquals(numTasks,
        ((Integer)timelineEntity.getOtherInfo().get(ATSConstants.NUM_TASKS)).intValue());
  }

  @Test
  public void testConvertVertexFinishedEvent() {
    long initRequestedTime = random.nextLong();
    long initedTime = random.nextLong();
    long startRequestedTime = random.nextLong();
    long startTime = random.nextLong();
    long finishTime = random.nextLong();
    Map<String,Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put("FOO", 100);
    taskStats.put("BAR", 200);
    VertexStats vertexStats = new VertexStats();

    VertexFinishedEvent event = new VertexFinishedEvent(tezVertexID, "v1", initRequestedTime,
        initedTime, startRequestedTime, startTime, finishTime, VertexState.ERROR,
        "diagnostics", null, vertexStats, taskStats);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_VERTEX_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezVertexID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(2, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_DAG_ID.name()).contains(
            tezDAGID.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.STATUS).contains(
            VertexState.ERROR.name()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.VERTEX_FINISHED.name(), timelineEvent.getEventType());
    Assert.assertEquals(finishTime, timelineEvent.getTimestamp());

    Assert.assertEquals(finishTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.FINISH_TIME)).longValue());
    Assert.assertEquals(finishTime - startTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.TIME_TAKEN)).longValue());
    Assert.assertEquals(VertexState.ERROR.name(),
        timelineEntity.getOtherInfo().get(ATSConstants.STATUS));
    Assert.assertEquals("diagnostics",
        timelineEntity.getOtherInfo().get(ATSConstants.DIAGNOSTICS));

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.STATS));

    Assert.assertEquals(100,
        ((Integer)timelineEntity.getOtherInfo().get("FOO")).intValue());
    Assert.assertEquals(200,
        ((Integer)timelineEntity.getOtherInfo().get("BAR")).intValue());
  }

  @Test
  public void testConvertTaskStartedEvent() {
    long scheduleTime = random.nextLong();
    long startTime = random.nextLong();
    TaskStartedEvent event = new TaskStartedEvent(tezTaskID, "v1", scheduleTime, startTime);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_TASK_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezTaskID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(startTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_VERTEX_ID.name()).contains(
            tezVertexID.toString()));

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_DAG_ID.name()).contains(
            tezDAGID.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_VERTEX_ID.name()).contains(
            tezVertexID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.TASK_STARTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(startTime, timelineEvent.getTimestamp());

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.SCHEDULED_TIME));
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.START_TIME));

    Assert.assertEquals(scheduleTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.SCHEDULED_TIME)).longValue());
    Assert.assertEquals(startTime,
        ((Long)timelineEntity.getOtherInfo().get(ATSConstants.START_TIME)).longValue());
  }

  @Test
  public void testConvertTaskAttemptStartedEvent() {
    long startTime = random.nextLong();
    TaskAttemptStartedEvent event = new TaskAttemptStartedEvent(tezTaskAttemptID, "v1",
        startTime, containerId, nodeId, "inProgressURL", "logsURL", "nodeHttpAddress");

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezTaskAttemptID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(startTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(3, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.NODE_ID).contains(nodeId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.CONTAINER_ID).contains(
            containerId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_TASK_ID.name()).contains(
            tezTaskID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.TASK_ATTEMPT_STARTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(startTime, timelineEvent.getTimestamp());

    Assert.assertEquals(4, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_DAG_ID.name()).contains(
            tezDAGID.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_VERTEX_ID.name()).contains(
            tezVertexID.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_TASK_ID.name()).contains(
            tezTaskID.toString()));

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.START_TIME));
    Assert.assertEquals("inProgressURL",
        timelineEntity.getOtherInfo().get(ATSConstants.IN_PROGRESS_LOGS_URL));
    Assert.assertEquals("logsURL",
        timelineEntity.getOtherInfo().get(ATSConstants.COMPLETED_LOGS_URL));
    Assert.assertEquals(nodeId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.NODE_ID));
    Assert.assertEquals(containerId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.CONTAINER_ID));
    Assert.assertEquals("nodeHttpAddress",
        timelineEntity.getOtherInfo().get(ATSConstants.NODE_HTTP_ADDRESS));
  }

  @Test
  public void testConvertVertexParallelismUpdatedEvent() {
    TezVertexID vId = tezVertexID;
    Map<String, EdgeManagerPluginDescriptor> edgeMgrs =
        new HashMap<String, EdgeManagerPluginDescriptor>();
    edgeMgrs.put("a", EdgeManagerPluginDescriptor.create("a.class").setHistoryText("text"));
    VertexParallelismUpdatedEvent event = new VertexParallelismUpdatedEvent(vId, 1, null,
        edgeMgrs, null, 10);

    TimelineEntity timelineEntity = HistoryEventTimelineConversion.convertToTimelineEntity(event);
    Assert.assertEquals(ATSConstants.TEZ_VERTEX_ID, timelineEntity.getEntityType());
    Assert.assertEquals(vId.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(1, timelineEntity.getEvents().size());

    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.VERTEX_PARALLELISM_UPDATED.name(), evt.getEventType());
    Assert.assertEquals(1, evt.getEventInfo().get(ATSConstants.NUM_TASKS));
    Assert.assertEquals(10, evt.getEventInfo().get(ATSConstants.OLD_NUM_TASKS));
    Assert.assertNotNull(evt.getEventInfo().get(ATSConstants.UPDATED_EDGE_MANAGERS));

    Map<String, Object> updatedEdgeMgrs = (Map<String, Object>)
        evt.getEventInfo().get(ATSConstants.UPDATED_EDGE_MANAGERS);
    Assert.assertEquals(1, updatedEdgeMgrs.size());
    Assert.assertTrue(updatedEdgeMgrs.containsKey("a"));
    Map<String, Object> updatedEdgeMgr = (Map<String, Object>) updatedEdgeMgrs.get("a");

    Assert.assertEquals("a.class", updatedEdgeMgr.get(DAGUtils.EDGE_MANAGER_CLASS_KEY));

    Assert.assertEquals(1, timelineEntity.getOtherInfo().get(ATSConstants.NUM_TASKS));

  }


}

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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.CallerContextProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.ServicePluginInfo;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl.DataEventDependencyInfo;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.app.web.AMWebController;
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
import org.apache.tez.dag.history.events.DAGKillRequestEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskFailureType;
import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

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
  private String containerLogs = "containerLogs";

  @SuppressWarnings("deprecation")
  @Before
  public void setup() {
    applicationId = ApplicationId.newInstance(9999l, 1);
    applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, 1);
    tezDAGID = TezDAGID.getInstance(applicationId, random.nextInt());
    tezVertexID = TezVertexID.getInstance(tezDAGID, random.nextInt());
    tezTaskID = TezTaskID.getInstance(tezVertexID, random.nextInt());
    tezTaskAttemptID = TezTaskAttemptID.getInstance(tezTaskID, random.nextInt());
    CallerContextProto.Builder callerContextProto = CallerContextProto.newBuilder();
    callerContextProto.setContext("ctxt");
    callerContextProto.setCallerId("Caller_ID");
    callerContextProto.setCallerType("Caller_Type");
    callerContextProto.setBlob("Desc_1");
    dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock")
        .setCallerContext(callerContextProto).build();
    containerId = ContainerId.newInstance(applicationAttemptId, 111);
    nodeId = NodeId.newInstance("node", 13435);
  }

  @Test(timeout = 5000)
  public void testHandlerExists() throws JSONException {
    for (HistoryEventType eventType : HistoryEventType.values()) {
      HistoryEvent event = null;
      switch (eventType) {
        case APP_LAUNCHED:
          event = new AppLaunchedEvent(applicationId, random.nextInt(), random.nextInt(),
              user, new Configuration(false), null);
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
              null, user, null, containerLogs, null);
          break;
        case DAG_INITIALIZED:
          event = new DAGInitializedEvent(tezDAGID, random.nextInt(), user, dagPlan.getName(), null);
          break;
        case DAG_STARTED:
          event = new DAGStartedEvent(tezDAGID, random.nextInt(), user, dagPlan.getName());
          break;
        case DAG_FINISHED:
          event = new DAGFinishedEvent(tezDAGID, random.nextInt(), random.nextInt(), DAGState.ERROR,
              null, null, user, dagPlan.getName(), null, applicationAttemptId, dagPlan);
          break;
        case VERTEX_INITIALIZED:
          event = new VertexInitializedEvent(tezVertexID, "v1", random.nextInt(), random.nextInt(),
              random.nextInt(), "proc", null, null, null);
          break;
        case VERTEX_STARTED:
          event = new VertexStartedEvent(tezVertexID, random.nextInt(), random.nextInt());
          break;
        case VERTEX_CONFIGURE_DONE:
          event = new VertexConfigurationDoneEvent(tezVertexID, 0L, 1, null, null, null, true);
          break;
        case VERTEX_FINISHED:
          event = new VertexFinishedEvent(tezVertexID, "v1", 1, random.nextInt(), random.nextInt(),
              random.nextInt(), random.nextInt(), random.nextInt(), VertexState.ERROR,
              null, null, null, null, null);
          break;
        case TASK_STARTED:
          event = new TaskStartedEvent(tezTaskID, "v1", random.nextInt(), random.nextInt());
          break;
        case TASK_FINISHED:
          event = new TaskFinishedEvent(tezTaskID, "v1", random.nextInt(), random.nextInt(),
              tezTaskAttemptID, TaskState.FAILED, null, null, 0);
          break;
        case TASK_ATTEMPT_STARTED:
          event = new TaskAttemptStartedEvent(tezTaskAttemptID, "v1", random.nextInt(), containerId,
              nodeId, null, null, "nodeHttpAddress");
          break;
        case TASK_ATTEMPT_FINISHED:
          event = new TaskAttemptFinishedEvent(tezTaskAttemptID, "v1", random.nextInt(),
              random.nextInt(), TaskAttemptState.FAILED, TaskFailureType.NON_FATAL, TaskAttemptTerminationCause.OUTPUT_LOST,
              null, null, null, null, 0, null, 0,
              containerId, nodeId, null, null, "nodeHttpAddress");
          break;
        case CONTAINER_LAUNCHED:
          event = new ContainerLaunchedEvent(containerId, random.nextInt(),
              applicationAttemptId);
          break;
        case CONTAINER_STOPPED:
          event = new ContainerStoppedEvent(containerId, random.nextInt(), -1, applicationAttemptId);
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
        case DAG_RECOVERED:
          event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID, dagPlan.getName(),
              user, random.nextLong(), containerLogs);
          break;
        case DAG_KILL_REQUEST:
          event = new DAGKillRequestEvent();
          break;
        default:
          Assert.fail("Unhandled event type " + eventType);
      }
      if (event == null || !event.isHistoryEvent()) {
        continue;
      }
      HistoryEventTimelineConversion.convertToTimelineEntities(event);
    }
  }

  static class MockVersionInfo extends VersionInfo {

    MockVersionInfo() {
      super("component", "1.1.0", "rev1", "20120101", "git.apache.org");
    }

  }

  @Test(timeout = 5000)
  public void testConvertAppLaunchedEventConcurrentModificationException()
      throws InterruptedException {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    final Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    conf.set("applicationId", "1234");
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    Thread confChanger = new Thread() {
      @Override
      public void run() {
        int i = 1;
        while (!shutdown.get()) {
          // trigger an actual change to test concurrency with Configuration#iterator
          conf.set("test" + i++, "test");
        }
      }
    };

    confChanger.start();
    try {
      MockVersionInfo mockVersionInfo = new MockVersionInfo();
      AppLaunchedEvent event = new AppLaunchedEvent(applicationId, launchTime,
          submitTime, user, conf, mockVersionInfo);
      HistoryEventTimelineConversion.convertToTimelineEntities(event);
    } finally {
      shutdown.set(true);
      confChanger.join();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertAppLaunchedEvent() {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    conf.set("applicationId", "1234");

    MockVersionInfo mockVersionInfo = new MockVersionInfo();
    AppLaunchedEvent event = new AppLaunchedEvent(applicationId, launchTime,
        submitTime, user, conf, mockVersionInfo);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(launchTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(EntityTypes.TEZ_APPLICATION.name(), timelineEntity.getEntityType());
    Assert.assertEquals("tez_" + applicationId.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));

    Assert.assertEquals(5, timelineEntity.getOtherInfo().size());
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.CONFIG));
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.TEZ_VERSION));
    Assert.assertEquals(user, timelineEntity.getOtherInfo().get(ATSConstants.USER));
    Assert.assertEquals(applicationId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.APPLICATION_ID));
    Assert.assertEquals(AMWebController.VERSION,
        timelineEntity.getOtherInfo().get(ATSConstants.DAG_AM_WEB_SERVICE_VERSION));

    Map<String, String> config =
        (Map<String, String>) timelineEntity.getOtherInfo().get(ATSConstants.CONFIG);
    Assert.assertEquals(conf.get("foo"), config.get("foo"));
    Assert.assertEquals(conf.get("applicationId"), config.get("applicationId"));

    Map<String, String> versionInfo =
        (Map<String, String>) timelineEntity.getOtherInfo().get(ATSConstants.TEZ_VERSION);
    Assert.assertEquals(mockVersionInfo.getVersion(),
        versionInfo.get(ATSConstants.VERSION));
    Assert.assertEquals(mockVersionInfo.getRevision(),
        versionInfo.get(ATSConstants.REVISION));
    Assert.assertEquals(mockVersionInfo.getBuildTime(),
        versionInfo.get(ATSConstants.BUILD_TIME));

  }

  @Test(timeout = 5000)
  public void testConvertAMLaunchedEvent() {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    AMLaunchedEvent event = new AMLaunchedEvent(applicationAttemptId, launchTime, submitTime, user);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals("tez_" + applicationAttemptId.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(), timelineEntity.getEntityType());

    final Map<String, Set<String>> relatedEntities = timelineEntity.getRelatedEntities();
    Assert.assertEquals(0, relatedEntities.size());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(2, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.USER).contains(user));
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));

    Assert.assertEquals(launchTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.AM_LAUNCHED.name(), evt.getEventType());
    Assert.assertEquals(launchTime, evt.getTimestamp());

    final Map<String, Object> otherInfo = timelineEntity.getOtherInfo();
    Assert.assertEquals(4, otherInfo.size());
    Assert.assertEquals(submitTime, otherInfo.get(ATSConstants.APP_SUBMIT_TIME));
    Assert.assertEquals(applicationId.toString(), otherInfo.get(ATSConstants.APPLICATION_ID));
    Assert.assertEquals(applicationAttemptId.toString(), otherInfo.get(ATSConstants.APPLICATION_ATTEMPT_ID));
    Assert.assertEquals(user, otherInfo.get(ATSConstants.USER));
  }

  @Test(timeout = 5000)
  public void testConvertAMStartedEvent() {
    long startTime = random.nextLong();

    AMStartedEvent event = new AMStartedEvent(applicationAttemptId, startTime, user);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals("tez_" + applicationAttemptId.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(), timelineEntity.getEntityType());

    final Map<String, Set<String>> relatedEntities = timelineEntity.getRelatedEntities();
    Assert.assertEquals(0, relatedEntities.size());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(2, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.USER).contains(user));
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
            .contains(applicationId.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.AM_STARTED.name(), evt.getEventType());
    Assert.assertEquals(startTime, evt.getTimestamp());

  }

  @Test(timeout = 5000)
  public void testConvertContainerLaunchedEvent() {
    long launchTime = random.nextLong();
    ContainerLaunchedEvent event = new ContainerLaunchedEvent(containerId, launchTime,
        applicationAttemptId);
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_CONTAINER_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals("tez_" + containerId.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_APPLICATION_ATTEMPT.name()).contains(
            "tez_" + applicationAttemptId.toString()));

    Assert.assertEquals(1, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
        applicationAttemptId.getApplicationId().toString()));

    Assert.assertEquals(containerId.toString(), timelineEntity.getOtherInfo().get(ATSConstants.CONTAINER_ID));

    Assert.assertEquals(launchTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    Assert.assertEquals(HistoryEventType.CONTAINER_LAUNCHED.name(),
        timelineEntity.getEvents().get(0).getEventType());
    Assert.assertEquals(launchTime,
        timelineEntity.getEvents().get(0).getTimestamp());
  }

  @Test(timeout = 5000)
  public void testConvertContainerStoppedEvent() {
    long stopTime = random.nextLong();
    int exitStatus = random.nextInt();
    ContainerStoppedEvent event = new ContainerStoppedEvent(containerId, stopTime, exitStatus,
        applicationAttemptId);
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals("tez_" + containerId.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_CONTAINER_ID.name(), timelineEntity.getEntityType());

    final Map<String, Set<String>> relatedEntities = timelineEntity.getRelatedEntities();
    Assert.assertEquals(1, relatedEntities.size());
    Assert.assertTrue(relatedEntities.get(EntityTypes.TEZ_APPLICATION_ATTEMPT.name())
        .contains("tez_" + applicationAttemptId.toString()));

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(2, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
    Assert.assertTrue(primaryFilters.get(ATSConstants.EXIT_STATUS).contains(exitStatus));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    final TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.CONTAINER_STOPPED.name(), evt.getEventType());
    Assert.assertEquals(stopTime, evt.getTimestamp());

    final Map<String, Object> otherInfo = timelineEntity.getOtherInfo();
    Assert.assertEquals(2, otherInfo.size());
    Assert.assertEquals(exitStatus, otherInfo.get(ATSConstants.EXIT_STATUS));
    Assert.assertEquals(stopTime, otherInfo.get(ATSConstants.FINISH_TIME));
  }

  @Test(timeout = 5000)
  public void testConvertDAGStartedEvent() {
    long startTime = random.nextLong();
    String dagName = "testDagName";
    DAGStartedEvent event = new DAGStartedEvent(tezDAGID, startTime, user, dagName);
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);


    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_STARTED.name(), evt.getEventType());
    Assert.assertEquals(startTime, evt.getTimestamp());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(3, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.USER).contains(user));
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
    Assert.assertTrue(primaryFilters.get(ATSConstants.DAG_NAME).contains(dagName));

    final Map<String, Object> otherInfo = timelineEntity.getOtherInfo();
    Assert.assertEquals(2, otherInfo.size());
    Assert.assertEquals(startTime, otherInfo.get(ATSConstants.START_TIME));
    Assert.assertEquals(DAGState.RUNNING.name(), otherInfo.get(ATSConstants.STATUS));
  }

  @Test(timeout = 5000)
  public void testConvertDAGSubmittedEvent() {
    long submitTime = random.nextLong();

    final String queueName = "TEST_DAG_SUBMITTED";
    DAGSubmittedEvent event = new DAGSubmittedEvent(tezDAGID, submitTime, dagPlan,
        applicationAttemptId, null, user, null, containerLogs, queueName);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(2, entities.size());


    if (entities.get(0).getEntityType().equals(EntityTypes.TEZ_DAG_ID.name())) {
      assertDagSubmittedEntity(submitTime, event, entities.get(0));
      assertDagSubmittedExtraInfoEntity(submitTime, event, entities.get(1));
    } else {
      assertDagSubmittedExtraInfoEntity(submitTime, event, entities.get(0));
      assertDagSubmittedEntity(submitTime, event, entities.get(1));
    }
  }

  private void assertDagSubmittedEntity(long submitTime, DAGSubmittedEvent event,
      TimelineEntity timelineEntity) {
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(2, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_APPLICATION.name()).contains(
            "tez_" + applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(EntityTypes.TEZ_APPLICATION_ATTEMPT.name()).contains(
            "tez_" + applicationAttemptId.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_SUBMITTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(submitTime, timelineEvent.getTimestamp());

    Assert.assertEquals(submitTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(5, timelineEntity.getPrimaryFilters().size());

    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains(
            dagPlan.getName()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.CALLER_CONTEXT_ID).contains(
            dagPlan.getCallerContext().getCallerId()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationAttemptId.getApplicationId().toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_QUEUE_NAME)
            .contains(event.getQueueName()));

    Assert.assertEquals(9, timelineEntity.getOtherInfo().size());
    Assert.assertEquals(applicationId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.APPLICATION_ID));
    Assert.assertEquals(applicationAttemptId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.APPLICATION_ATTEMPT_ID));
    Assert.assertEquals(applicationAttemptId.getApplicationId().toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.APPLICATION_ID));
    Assert.assertEquals(AMWebController.VERSION,
        timelineEntity.getOtherInfo().get(ATSConstants.DAG_AM_WEB_SERVICE_VERSION));
    Assert.assertEquals(user,
        timelineEntity.getOtherInfo().get(ATSConstants.USER));
    Assert.assertEquals(containerLogs,
        timelineEntity.getOtherInfo().get(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
            + applicationAttemptId.getAttemptId()));
    Assert.assertEquals(
        timelineEntity.getOtherInfo().get(ATSConstants.CALLER_CONTEXT_ID),
            dagPlan.getCallerContext().getCallerId());
    Assert.assertEquals(
        timelineEntity.getOtherInfo().get(ATSConstants.CALLER_CONTEXT_TYPE),
            dagPlan.getCallerContext().getCallerType());
    Assert.assertEquals(dagPlan.getCallerContext().getContext(),
        timelineEntity.getOtherInfo().get(ATSConstants.CALLER_CONTEXT));
    Assert.assertEquals(
        event.getQueueName(), timelineEntity.getOtherInfo().get(ATSConstants.DAG_QUEUE_NAME));

  }

  private void assertDagSubmittedExtraInfoEntity(long submitTime, DAGSubmittedEvent event,
      TimelineEntity timelineEntity) {
    Assert.assertEquals(EntityTypes.TEZ_DAG_EXTRA_INFO.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(timelineEntity.getRelatedEntities()
        .get(EntityTypes.TEZ_DAG_ID.name()).contains(tezDAGID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_SUBMITTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(submitTime, timelineEvent.getTimestamp());

    Assert.assertEquals(submitTime, timelineEntity.getStartTime().longValue());
    Assert.assertEquals(0, timelineEntity.getPrimaryFilters().size());
    Assert.assertEquals(1, timelineEntity.getOtherInfo().size());
    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.DAG_PLAN));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertTaskAttemptFinishedEvent() {
    String vertexName = "testVertex";
    long creationTime = random.nextLong();
    long startTime = creationTime + 1000;
    long allocationTime = creationTime + 1001;
    long finishTime = startTime + 1002;
    TaskAttemptState state = TaskAttemptState
        .values()[random.nextInt(TaskAttemptState.values().length)];
    TaskAttemptTerminationCause error = TaskAttemptTerminationCause
        .values()[random.nextInt(TaskAttemptTerminationCause.values().length)];
    String diagnostics = "random diagnostics message";
    TezCounters counters = new TezCounters();
    long lastDataEventTime = finishTime - 1;
    List<DataEventDependencyInfo> events = Lists.newArrayList();
    events.add(new DataEventDependencyInfo(lastDataEventTime, tezTaskAttemptID));
    events.add(new DataEventDependencyInfo(lastDataEventTime, tezTaskAttemptID));

    TaskAttemptFinishedEvent event = new TaskAttemptFinishedEvent(tezTaskAttemptID, vertexName,
        startTime, finishTime, state, TaskFailureType.FATAL, error, diagnostics, counters, events, null, creationTime,
        tezTaskAttemptID, allocationTime, containerId, nodeId, "inProgressURL", "logsURL", "nodeHttpAddress");
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(tezTaskAttemptID.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), timelineEntity.getEntityType());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(5, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_DAG_ID.name())
        .contains(tezDAGID.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_VERTEX_ID.name())
        .contains(tezVertexID.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_TASK_ID.name())
        .contains(tezTaskID.toString()));
    Assert.assertTrue(primaryFilters.get(ATSConstants.STATUS).contains(state.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.TASK_ATTEMPT_FINISHED.name(), evt.getEventType());
    Assert.assertEquals(finishTime, evt.getTimestamp());

    final Map<String, Object> otherInfo = timelineEntity.getOtherInfo();
    Assert.assertEquals(17, otherInfo.size());
    Assert.assertEquals(tezTaskAttemptID.toString(), 
        timelineEntity.getOtherInfo().get(ATSConstants.CREATION_CAUSAL_ATTEMPT));
    Assert.assertEquals(creationTime, timelineEntity.getOtherInfo().get(ATSConstants.CREATION_TIME));
    Assert.assertEquals(allocationTime, timelineEntity.getOtherInfo().get(ATSConstants.ALLOCATION_TIME));
    Assert.assertEquals(startTime, timelineEntity.getOtherInfo().get(ATSConstants.START_TIME));
    Assert.assertEquals(finishTime, otherInfo.get(ATSConstants.FINISH_TIME));
    Assert.assertEquals(finishTime - startTime, otherInfo.get(ATSConstants.TIME_TAKEN));
    Assert.assertEquals(state.name(), otherInfo.get(ATSConstants.STATUS));
    Assert.assertEquals(TaskFailureType.FATAL.name(), otherInfo.get(ATSConstants.TASK_FAILURE_TYPE));
    Assert.assertEquals(error.name(), otherInfo.get(ATSConstants.TASK_ATTEMPT_ERROR_ENUM));
    Assert.assertEquals(diagnostics, otherInfo.get(ATSConstants.DIAGNOSTICS));
    Map<String, Object> obj1 = (Map<String, Object>)otherInfo.get(ATSConstants.LAST_DATA_EVENTS);
    List<Object> obj2 = (List<Object>) obj1.get(ATSConstants.LAST_DATA_EVENTS);
    Assert.assertEquals(2, obj2.size());
    Map<String, Object> obj3 = (Map<String, Object>) obj2.get(0);
    Assert.assertEquals(events.get(0).getTimestamp(), obj3.get(ATSConstants.TIMESTAMP));
    Assert.assertTrue(otherInfo.containsKey(ATSConstants.COUNTERS));
    Assert.assertEquals("inProgressURL", otherInfo.get(ATSConstants.IN_PROGRESS_LOGS_URL));
    Assert.assertEquals("logsURL", otherInfo.get(ATSConstants.COMPLETED_LOGS_URL));
    Assert.assertEquals(nodeId.toString(), otherInfo.get(ATSConstants.NODE_ID));
    Assert.assertEquals(containerId.toString(), otherInfo.get(ATSConstants.CONTAINER_ID));
    Assert.assertEquals("nodeHttpAddress", otherInfo.get(ATSConstants.NODE_HTTP_ADDRESS));

    TaskAttemptFinishedEvent eventWithNullFailureType =
        new TaskAttemptFinishedEvent(tezTaskAttemptID, vertexName,
            startTime, finishTime, state, null, error, diagnostics, counters, events, null,
            creationTime,
            tezTaskAttemptID, allocationTime, containerId, nodeId, "inProgressURL", "logsURL",
            "nodeHttpAddress");
    List<TimelineEntity> evtEntities = HistoryEventTimelineConversion.convertToTimelineEntities(
        eventWithNullFailureType);
    Assert.assertEquals(1, evtEntities.size());
    TimelineEntity timelineEntityWithNullFailureType = evtEntities.get(0);

    Assert.assertNull(
        timelineEntityWithNullFailureType.getOtherInfo().get(ATSConstants.TASK_FAILURE_TYPE));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertDAGInitializedEvent() {
    long initTime = random.nextLong();

    Map<String, TezVertexID> nameIdMap = new HashMap<String, TezVertexID>();
    nameIdMap.put("foo", tezVertexID);

    DAGInitializedEvent event = new DAGInitializedEvent(tezDAGID, initTime, "user", "dagName",
        nameIdMap);


    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_INITIALIZED.name(), timelineEvent.getEventType());
    Assert.assertEquals(initTime, timelineEvent.getTimestamp());

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
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

  @Test(timeout = 5000)
  public void testConvertDAGFinishedEvent() {
    long finishTime = random.nextLong();
    long startTime = random.nextLong();
    Map<String, Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put("FOO", 100);
    taskStats.put("BAR", 200);

    DAGFinishedEvent event = new DAGFinishedEvent(tezDAGID, startTime, finishTime, DAGState.ERROR,
        "diagnostics", null, user, dagPlan.getName(), taskStats, applicationAttemptId, dagPlan);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(2, entities.size());

    if (entities.get(0).getEntityType().equals(EntityTypes.TEZ_DAG_ID.name())) {
      assertDagFinishedEntity(finishTime, startTime, event, entities.get(0));
      assertDagFinishedExtraInfoEntity(finishTime, entities.get(1));
    } else {
      assertDagFinishedExtraInfoEntity(finishTime, entities.get(0));
      assertDagFinishedEntity(finishTime, startTime, event, entities.get(1));
    }
  }

  private void assertDagFinishedEntity(long finishTime, long startTime, DAGFinishedEvent event,
      TimelineEntity timelineEntity) {
    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_FINISHED.name(), timelineEvent.getEventType());
    Assert.assertEquals(finishTime, timelineEvent.getTimestamp());

    Assert.assertEquals(5, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains(dagPlan.getName()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.STATUS).contains(
            DAGState.ERROR.name()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.CALLER_CONTEXT_ID).contains(
            dagPlan.getCallerContext().getCallerId()));

    Assert.assertEquals(startTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.START_TIME)).longValue());
    Assert.assertEquals(finishTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.FINISH_TIME)).longValue());
    Assert.assertEquals(finishTime - startTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.TIME_TAKEN)).longValue());
    Assert.assertEquals(DAGState.ERROR.name(),
        timelineEntity.getOtherInfo().get(ATSConstants.STATUS));
    Assert.assertEquals("diagnostics",
        timelineEntity.getOtherInfo().get(ATSConstants.DIAGNOSTICS));
    Assert.assertEquals(applicationAttemptId.toString(),
        timelineEntity.getOtherInfo().get(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID));

    Assert.assertEquals(100,
        ((Integer) timelineEntity.getOtherInfo().get("FOO")).intValue());
    Assert.assertEquals(200,
        ((Integer) timelineEntity.getOtherInfo().get("BAR")).intValue());
  }

  private void assertDagFinishedExtraInfoEntity(long finishTime, TimelineEntity timelineEntity) {
    Assert.assertEquals(EntityTypes.TEZ_DAG_EXTRA_INFO.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
    Assert.assertTrue(
        timelineEntity.getRelatedEntities().get(ATSConstants.TEZ_DAG_ID).contains(
            tezDAGID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_FINISHED.name(), timelineEvent.getEventType());
    Assert.assertEquals(finishTime, timelineEvent.getTimestamp());

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.COUNTERS));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertVertexInitializedEvent() {
    long initRequestedTime = random.nextLong();
    long initedTime = random.nextLong();
    int numTasks = random.nextInt();
    VertexInitializedEvent event = new VertexInitializedEvent(tezVertexID, "v1", initRequestedTime,
        initedTime, numTasks, "proc", null, null,
        new ServicePluginInfo().setContainerLauncherName("abc")
            .setTaskSchedulerName("def").setTaskCommunicatorName("ghi")
            .setContainerLauncherClassName("abc1")
            .setTaskSchedulerClassName("def1")
            .setTaskCommunicatorClassName("ghi1"));


    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_VERTEX_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezVertexID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(initedTime, timelineEntity.getStartTime().longValue());

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
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.INIT_TIME)).longValue());
    Assert.assertEquals(initRequestedTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.INIT_REQUESTED_TIME)).longValue());
    Assert.assertEquals(initedTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.INIT_TIME)).longValue());
    Assert.assertEquals(numTasks,
        ((Integer) timelineEntity.getOtherInfo().get(ATSConstants.NUM_TASKS)).intValue());
    Assert.assertNotNull(timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN));
    Assert.assertEquals("abc",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.CONTAINER_LAUNCHER_NAME));
    Assert.assertEquals("def",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_SCHEDULER_NAME));
    Assert.assertEquals("ghi",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_COMMUNICATOR_NAME));
    Assert.assertEquals("abc1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.CONTAINER_LAUNCHER_CLASS_NAME));
    Assert.assertEquals("def1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_SCHEDULER_CLASS_NAME));
    Assert.assertEquals("ghi1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_COMMUNICATOR_CLASS_NAME));
  }

  @Test(timeout = 5000)
  public void testConvertVertexStartedEvent() {
    long startRequestedTime = random.nextLong();
    long startTime = random.nextLong();

    VertexStartedEvent event = new VertexStartedEvent(tezVertexID, startRequestedTime, startTime);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_VERTEX_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezVertexID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(2, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID)
            .contains(applicationId.toString()));
    Assert.assertTrue(
            timelineEntity.getPrimaryFilters().get(EntityTypes.TEZ_DAG_ID.name()).contains(
                    tezDAGID.toString()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.VERTEX_STARTED.name(), timelineEvent.getEventType());
    Assert.assertEquals(startTime, timelineEvent.getTimestamp());

    Assert.assertEquals(3, timelineEntity.getOtherInfo().size());
    Assert.assertEquals(startRequestedTime,
            ((Long) timelineEntity.getOtherInfo().get(ATSConstants.START_REQUESTED_TIME)).longValue());
    Assert.assertEquals(startTime,
            ((Long) timelineEntity.getOtherInfo().get(ATSConstants.START_TIME)).longValue());
    Assert.assertEquals(VertexState.RUNNING.name(),
            timelineEntity.getOtherInfo().get(ATSConstants.STATUS));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertVertexFinishedEvent() {
    String vertexName = "v1";
    long initRequestedTime = random.nextLong();
    long initedTime = random.nextLong();
    long startRequestedTime = random.nextLong();
    long startTime = random.nextLong();
    long finishTime = random.nextLong();
    Map<String, Integer> taskStats = new HashMap<String, Integer>();
    taskStats.put("FOO", 100);
    taskStats.put("BAR", 200);
    VertexStats vertexStats = new VertexStats();

    VertexFinishedEvent event = new VertexFinishedEvent(tezVertexID, vertexName, 1, initRequestedTime,
        initedTime, startRequestedTime, startTime, finishTime, VertexState.ERROR,
        "diagnostics", null, vertexStats, taskStats,
        new ServicePluginInfo().setContainerLauncherName("abc")
            .setTaskSchedulerName("def").setTaskCommunicatorName("ghi")
            .setContainerLauncherClassName("abc1")
            .setTaskSchedulerClassName("def1")
            .setTaskCommunicatorClassName("ghi1"));

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_VERTEX_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezVertexID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
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

    Assert.assertEquals(vertexName,
        timelineEntity.getOtherInfo().get(ATSConstants.VERTEX_NAME));
    Assert.assertEquals(finishTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.FINISH_TIME)).longValue());
    Assert.assertEquals(finishTime - startTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.TIME_TAKEN)).longValue());
    Assert.assertEquals(VertexState.ERROR.name(),
        timelineEntity.getOtherInfo().get(ATSConstants.STATUS));
    Assert.assertEquals("diagnostics",
        timelineEntity.getOtherInfo().get(ATSConstants.DIAGNOSTICS));
    Assert.assertNotNull(timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN));
    Assert.assertEquals("abc",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.CONTAINER_LAUNCHER_NAME));
    Assert.assertEquals("def",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_SCHEDULER_NAME));
    Assert.assertEquals("ghi",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_COMMUNICATOR_NAME));
    Assert.assertEquals("abc1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.CONTAINER_LAUNCHER_CLASS_NAME));
    Assert.assertEquals("def1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_SCHEDULER_CLASS_NAME));
    Assert.assertEquals("ghi1",
        ((Map<String, Object>)timelineEntity.getOtherInfo().get(ATSConstants.SERVICE_PLUGIN)).get(
            ATSConstants.TASK_COMMUNICATOR_CLASS_NAME));

    Assert.assertTrue(timelineEntity.getOtherInfo().containsKey(ATSConstants.STATS));

    Assert.assertEquals(100,
        ((Integer) timelineEntity.getOtherInfo().get("FOO")).intValue());
    Assert.assertEquals(200,
        ((Integer) timelineEntity.getOtherInfo().get("BAR")).intValue());
  }

  @Test(timeout = 5000)
  public void testConvertTaskStartedEvent() {
    long scheduleTime = random.nextLong();
    long startTime = random.nextLong();
    TaskStartedEvent event = new TaskStartedEvent(tezTaskID, "v1", scheduleTime, startTime);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

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
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.SCHEDULED_TIME)).longValue());
    Assert.assertEquals(startTime,
        ((Long) timelineEntity.getOtherInfo().get(ATSConstants.START_TIME)).longValue());
    Assert.assertTrue(TaskState.SCHEDULED.name()
        .equals(timelineEntity.getOtherInfo().get(ATSConstants.STATUS)));
  }

  @Test(timeout = 5000)
  public void testConvertTaskAttemptStartedEvent() {
    long startTime = random.nextLong();
    TaskAttemptStartedEvent event = new TaskAttemptStartedEvent(tezTaskAttemptID, "v1",
        startTime, containerId, nodeId, "inProgressURL", "logsURL", "nodeHttpAddress");

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_TASK_ATTEMPT_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezTaskAttemptID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(startTime, timelineEntity.getStartTime().longValue());

    Assert.assertEquals(1, timelineEntity.getRelatedEntities().size());
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
    Assert.assertTrue(TaskAttemptState.RUNNING.name()
        .equals(timelineEntity.getOtherInfo().get(ATSConstants.STATUS)));
  }

  @Test(timeout = 5000)
  public void testConvertTaskFinishedEvent() {
    String vertexName = "testVertexName";
    long startTime = random.nextLong();
    long finishTime = random.nextLong();
    TaskState state = TaskState.values()[random.nextInt(TaskState.values().length)];
    String diagnostics = "diagnostics message";
    TezCounters counters = new TezCounters();

    TaskFinishedEvent event = new TaskFinishedEvent(tezTaskID, vertexName, startTime, finishTime,
        tezTaskAttemptID, state, diagnostics, counters, 3);
    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(tezTaskID.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(EntityTypes.TEZ_TASK_ID.name(), timelineEntity.getEntityType());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(4, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_DAG_ID.name())
        .contains(tezDAGID.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_VERTEX_ID.name())
        .contains(tezVertexID.toString()));
    Assert.assertTrue(primaryFilters.get(ATSConstants.STATUS).contains(state.name()));

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.TASK_FINISHED.name(), evt.getEventType());
    Assert.assertEquals(finishTime, evt.getTimestamp());

    final Map<String, Object> otherInfo = timelineEntity.getOtherInfo();
    Assert.assertEquals(7, otherInfo.size());
    Assert.assertEquals(finishTime, otherInfo.get(ATSConstants.FINISH_TIME));
    Assert.assertEquals(finishTime - startTime, otherInfo.get(ATSConstants.TIME_TAKEN));
    Assert.assertEquals(state.name(), otherInfo.get(ATSConstants.STATUS));
    Assert.assertEquals(tezTaskAttemptID.toString(),
        otherInfo.get(ATSConstants.SUCCESSFUL_ATTEMPT_ID));
    Assert.assertEquals(3, otherInfo.get(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS));
    Assert.assertEquals(diagnostics, otherInfo.get(ATSConstants.DIAGNOSTICS));
    Assert.assertTrue(otherInfo.containsKey(ATSConstants.COUNTERS));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testConvertVertexReconfigreDoneEvent() {
    TezVertexID vId = tezVertexID;
    Map<String, EdgeProperty> edgeMgrs =
        new HashMap<String, EdgeProperty>();
    
    edgeMgrs.put("a", EdgeProperty.create(EdgeManagerPluginDescriptor.create("a.class")
        .setHistoryText("text"), DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("Out"), InputDescriptor.create("In")));
    VertexConfigurationDoneEvent event = new VertexConfigurationDoneEvent(vId, 0L, 1, null,
        edgeMgrs, null, true);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(ATSConstants.TEZ_VERTEX_ID, timelineEntity.getEntityType());
    Assert.assertEquals(vId.toString(), timelineEntity.getEntityId());
    Assert.assertEquals(1, timelineEntity.getEvents().size());

    final Map<String, Set<Object>> primaryFilters = timelineEntity.getPrimaryFilters();
    Assert.assertEquals(2, primaryFilters.size());
    Assert.assertTrue(primaryFilters.get(ATSConstants.APPLICATION_ID)
        .contains(applicationId.toString()));
    Assert.assertTrue(primaryFilters.get(EntityTypes.TEZ_DAG_ID.name())
        .contains(tezDAGID.toString()));

    TimelineEvent evt = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.VERTEX_CONFIGURE_DONE.name(), evt.getEventType());
    Assert.assertEquals(1, evt.getEventInfo().get(ATSConstants.NUM_TASKS));
    Assert.assertNotNull(evt.getEventInfo().get(ATSConstants.UPDATED_EDGE_MANAGERS));

    Map<String, Object> updatedEdgeMgrs = (Map<String, Object>)
        evt.getEventInfo().get(ATSConstants.UPDATED_EDGE_MANAGERS);
    Assert.assertEquals(1, updatedEdgeMgrs.size());
    Assert.assertTrue(updatedEdgeMgrs.containsKey("a"));
    Map<String, Object> updatedEdgeMgr = (Map<String, Object>) updatedEdgeMgrs.get("a");

    Assert.assertEquals(DataMovementType.CUSTOM.name(),
        updatedEdgeMgr.get(DAGUtils.DATA_MOVEMENT_TYPE_KEY));
    Assert.assertEquals("In", updatedEdgeMgr.get(DAGUtils.EDGE_DESTINATION_CLASS_KEY));
    Assert.assertEquals("a.class", updatedEdgeMgr.get(DAGUtils.EDGE_MANAGER_CLASS_KEY));

    Assert.assertEquals(1, timelineEntity.getOtherInfo().get(ATSConstants.NUM_TASKS));
  }

  @Test(timeout = 5000)
  public void testConvertDAGRecoveredEvent() {
    long recoverTime = random.nextLong();

    DAGRecoveredEvent event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID,
        dagPlan.getName(), user, recoverTime, containerLogs);

    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_RECOVERED.name(), timelineEvent.getEventType());
    Assert.assertEquals(recoverTime, timelineEvent.getTimestamp());

    Assert.assertTrue(timelineEvent.getEventInfo().containsKey(ATSConstants.APPLICATION_ATTEMPT_ID));
    Assert.assertEquals(applicationAttemptId.toString(),
        timelineEvent.getEventInfo().get(ATSConstants.APPLICATION_ATTEMPT_ID));

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains("DAGPlanMock"));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));
    Assert.assertEquals(containerLogs,
        timelineEntity.getOtherInfo().get(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
            + applicationAttemptId.getAttemptId()));
  }

  @Test(timeout = 5000)
  public void testConvertDAGRecoveredEvent2() {
    long recoverTime = random.nextLong();

    DAGRecoveredEvent event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID,
        dagPlan.getName(), user, recoverTime, DAGState.ERROR, "mock reason", containerLogs);


    List<TimelineEntity> entities = HistoryEventTimelineConversion.convertToTimelineEntities(event);
    Assert.assertEquals(1, entities.size());
    TimelineEntity timelineEntity = entities.get(0);

    Assert.assertEquals(EntityTypes.TEZ_DAG_ID.name(), timelineEntity.getEntityType());
    Assert.assertEquals(tezDAGID.toString(), timelineEntity.getEntityId());

    Assert.assertEquals(0, timelineEntity.getRelatedEntities().size());

    Assert.assertEquals(1, timelineEntity.getEvents().size());
    TimelineEvent timelineEvent = timelineEntity.getEvents().get(0);
    Assert.assertEquals(HistoryEventType.DAG_RECOVERED.name(), timelineEvent.getEventType());
    Assert.assertEquals(recoverTime, timelineEvent.getTimestamp());

    Assert.assertTrue(timelineEvent.getEventInfo().containsKey(ATSConstants.APPLICATION_ATTEMPT_ID));
    Assert.assertEquals(applicationAttemptId.toString(),
        timelineEvent.getEventInfo().get(ATSConstants.APPLICATION_ATTEMPT_ID));
    Assert.assertEquals(DAGState.ERROR.name(),
        timelineEvent.getEventInfo().get(ATSConstants.DAG_STATE));
    Assert.assertEquals("mock reason",
        timelineEvent.getEventInfo().get(ATSConstants.RECOVERY_FAILURE_REASON));

    Assert.assertEquals(3, timelineEntity.getPrimaryFilters().size());
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.APPLICATION_ID).contains(
            applicationId.toString()));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.DAG_NAME).contains("DAGPlanMock"));
    Assert.assertTrue(
        timelineEntity.getPrimaryFilters().get(ATSConstants.USER).contains(user));
    Assert.assertEquals(containerLogs,
        timelineEntity.getOtherInfo().get(ATSConstants.IN_PROGRESS_LOGS_URL + "_"
            + applicationAttemptId.getAttemptId()));
  }


}

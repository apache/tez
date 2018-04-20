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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
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
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.KVPair;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskFailureType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestHistoryEventProtoConverter {
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
  private HistoryEventProtoConverter converter = new HistoryEventProtoConverter();

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
    containerId = ContainerId.newContainerId(applicationAttemptId, 111);
    nodeId = NodeId.newInstance("node", 13435);
  }

  @Test(timeout = 5000)
  public void testHandlerExists() {
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
      converter.convert(event);
    }
  }

  static class MockVersionInfo extends VersionInfo {
    MockVersionInfo() {
      super("component", "1.1.0", "rev1", "20120101", "git.apache.org");
    }
  }

  private String findEventData(HistoryEventProto proto, String key) {
    for (KVPair data : proto.getEventDataList()) {
      if (data.getKey().equals(key)) {
        return data.getValue();
      }
    }
    return null;
  }

  private void assertEventData(HistoryEventProto proto, String key, String value) {
    String evtVal = findEventData(proto, key);
    if (evtVal == null) {
      Assert.fail("Cannot find kv pair: " + key);
    }
    if (value != null) {
      Assert.assertEquals(value, evtVal);
    }
  }

  private void assertNoEventData(HistoryEventProto proto, String key) {
    for (KVPair data : proto.getEventDataList()) {
      if (data.getKey().equals(key)) {
        Assert.fail("Found find kv pair: " + key);
      }
    }
  }

  private String safeToString(Object obj) {
    return obj == null ? "" : obj.toString();
  }

  private void assertCommon(HistoryEventProto proto, HistoryEventType type, long eventTime,
      EntityTypes entityType, ApplicationAttemptId appAttemptId, String user, int numData) {
    Assert.assertEquals(type.name(), proto.getEventType());
    Assert.assertEquals(eventTime, proto.getEventTime());
    // Assert.assertEquals(safeToString(appId), proto.getAppId());
    Assert.assertEquals(safeToString(appAttemptId), proto.getAppAttemptId());
    Assert.assertEquals(safeToString(user), proto.getUser());
    if (entityType != null) {
      switch (entityType) { // Intentional fallthrough.
      case TEZ_TASK_ATTEMPT_ID:
        Assert.assertEquals(tezTaskAttemptID.toString(), proto.getTaskAttemptId());
      case TEZ_TASK_ID:
        Assert.assertEquals(tezTaskID.toString(), proto.getTaskId());
      case TEZ_VERTEX_ID:
        Assert.assertEquals(tezVertexID.toString(), proto.getVertexId());
      case TEZ_DAG_ID:
        Assert.assertEquals(tezDAGID.toString(), proto.getDagId());
      case TEZ_APPLICATION:
        Assert.assertEquals(applicationId.toString(), proto.getAppId());
        break;
      default:
        Assert.fail("Invalid type: " + entityType.name());
      }
    }
    Assert.assertEquals(numData, proto.getEventDataCount());
  }

  @Test(timeout = 5000)
  public void testConvertAppLaunchedEvent() {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    conf.set("applicationId", "1234");

    MockVersionInfo mockVersionInfo = new MockVersionInfo();
    AppLaunchedEvent event = new AppLaunchedEvent(applicationId, launchTime, submitTime, user,
        conf, mockVersionInfo);
    HistoryEventProto proto = converter.convert(event);

    assertCommon(proto, HistoryEventType.APP_LAUNCHED, launchTime, EntityTypes.TEZ_APPLICATION,
        null, user, 3);
    assertEventData(proto, ATSConstants.CONFIG, null);
    assertEventData(proto, ATSConstants.TEZ_VERSION, null);
    assertEventData(proto, ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
  }

  @Test(timeout = 5000)
  public void testConvertAMLaunchedEvent() {
    long launchTime = random.nextLong();
    long submitTime = random.nextLong();
    AMLaunchedEvent event = new AMLaunchedEvent(applicationAttemptId, launchTime, submitTime,
        user);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.AM_LAUNCHED, launchTime, EntityTypes.TEZ_APPLICATION,
        applicationAttemptId, user, 1);
    assertEventData(proto, ATSConstants.APP_SUBMIT_TIME, String.valueOf(submitTime));
  }

  @Test(timeout = 5000)
  public void testConvertAMStartedEvent() {
    long startTime = random.nextLong();
    AMStartedEvent event = new AMStartedEvent(applicationAttemptId, startTime, user);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.AM_STARTED, startTime, EntityTypes.TEZ_APPLICATION,
        applicationAttemptId, user, 0);
  }

  @Test(timeout = 5000)
  public void testConvertContainerLaunchedEvent() {
    long launchTime = random.nextLong();
    ContainerLaunchedEvent event = new ContainerLaunchedEvent(containerId, launchTime,
        applicationAttemptId);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.CONTAINER_LAUNCHED, launchTime, EntityTypes.TEZ_APPLICATION,
        applicationAttemptId, null, 1);
    assertEventData(proto, ATSConstants.CONTAINER_ID, containerId.toString());
  }

  @Test(timeout = 5000)
  public void testConvertContainerStoppedEvent() {
    long stopTime = random.nextLong();
    int exitStatus = random.nextInt();
    ContainerStoppedEvent event = new ContainerStoppedEvent(containerId, stopTime, exitStatus,
        applicationAttemptId);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.CONTAINER_STOPPED, stopTime, EntityTypes.TEZ_APPLICATION,
        applicationAttemptId, null, 3);
    assertEventData(proto, ATSConstants.CONTAINER_ID, containerId.toString());
    assertEventData(proto, ATSConstants.EXIT_STATUS, String.valueOf(exitStatus));
    assertEventData(proto, ATSConstants.FINISH_TIME, String.valueOf(stopTime));
  }

  @Test(timeout = 5000)
  public void testConvertDAGStartedEvent() {
    long startTime = random.nextLong();
    String dagName = "testDagName";
    DAGStartedEvent event = new DAGStartedEvent(tezDAGID, startTime, user, dagName);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_STARTED, startTime, EntityTypes.TEZ_DAG_ID, null,
        user, 2);
    assertEventData(proto, ATSConstants.DAG_NAME, dagName);
    assertEventData(proto, ATSConstants.STATUS, DAGState.RUNNING.name());
  }

  @Test(timeout = 5000)
  public void testConvertDAGSubmittedEvent() {
    long submitTime = random.nextLong();

    final String queueName = "TEST_DAG_SUBMITTED";
    DAGSubmittedEvent event = new DAGSubmittedEvent(tezDAGID, submitTime, dagPlan,
        applicationAttemptId, null, user, null, containerLogs, queueName);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_SUBMITTED, submitTime, EntityTypes.TEZ_DAG_ID,
        applicationAttemptId, user, 8);

    assertEventData(proto, ATSConstants.DAG_NAME, dagPlan.getName());
    assertEventData(proto, ATSConstants.DAG_QUEUE_NAME, event.getQueueName());
    assertEventData(proto, ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    assertEventData(proto, ATSConstants.IN_PROGRESS_LOGS_URL + "_"
        + applicationAttemptId.getAttemptId(), containerLogs);
    assertEventData(proto, ATSConstants.CALLER_CONTEXT_ID,
        dagPlan.getCallerContext().getCallerId());
    assertEventData(proto, ATSConstants.CALLER_CONTEXT_TYPE,
        dagPlan.getCallerContext().getCallerType());
    assertEventData(proto, ATSConstants.CALLER_CONTEXT, dagPlan.getCallerContext().getContext());
    assertEventData(proto, ATSConstants.DAG_PLAN, null);
  }

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
        startTime, finishTime, state, TaskFailureType.FATAL, error, diagnostics, counters, events,
        null, creationTime, tezTaskAttemptID, allocationTime, containerId, nodeId, "inProgressURL",
        "logsURL", "nodeHttpAddress");
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.TASK_ATTEMPT_FINISHED, finishTime,
        EntityTypes.TEZ_DAG_ID, null, null, 16);

    assertEventData(proto, ATSConstants.STATUS, state.name());
    assertEventData(proto, ATSConstants.CREATION_CAUSAL_ATTEMPT, tezTaskAttemptID.toString());
    assertEventData(proto, ATSConstants.CREATION_TIME, String.valueOf(creationTime));
    assertEventData(proto, ATSConstants.ALLOCATION_TIME, String.valueOf(allocationTime));
    assertEventData(proto, ATSConstants.START_TIME, String.valueOf(startTime));
    assertEventData(proto, ATSConstants.TIME_TAKEN, String.valueOf(finishTime - startTime));
    assertEventData(proto, ATSConstants.TASK_FAILURE_TYPE, TaskFailureType.FATAL.name());
    assertEventData(proto, ATSConstants.TASK_ATTEMPT_ERROR_ENUM, error.name());
    assertEventData(proto, ATSConstants.DIAGNOSTICS, diagnostics);
    assertEventData(proto, ATSConstants.LAST_DATA_EVENTS, null);
    assertEventData(proto, ATSConstants.COUNTERS, null);
    assertEventData(proto, ATSConstants.IN_PROGRESS_LOGS_URL, "inProgressURL");
    assertEventData(proto, ATSConstants.COMPLETED_LOGS_URL, "logsURL");
    assertEventData(proto, ATSConstants.NODE_ID, nodeId.toString());
    assertEventData(proto, ATSConstants.CONTAINER_ID, containerId.toString());
    assertEventData(proto, ATSConstants.NODE_HTTP_ADDRESS, "nodeHttpAddress");

    TaskAttemptFinishedEvent eventWithNullFailureType =
        new TaskAttemptFinishedEvent(tezTaskAttemptID, vertexName,
            startTime, finishTime, state, null, error, diagnostics, counters, events, null,
            creationTime,
            tezTaskAttemptID, allocationTime, containerId, nodeId, "inProgressURL", "logsURL",
            "nodeHttpAddress");
    proto = converter.convert(eventWithNullFailureType);
    assertNoEventData(proto, ATSConstants.TASK_FAILURE_TYPE);
  }

  @Test(timeout = 5000)
  public void testConvertDAGInitializedEvent() {
    long initTime = random.nextLong();

    Map<String, TezVertexID> nameIdMap = new HashMap<String, TezVertexID>();
    nameIdMap.put("foo", tezVertexID);

    DAGInitializedEvent event = new DAGInitializedEvent(tezDAGID, initTime, "user", "dagName",
        nameIdMap);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_INITIALIZED, initTime,
        EntityTypes.TEZ_DAG_ID, null, user, 2);
    assertEventData(proto, ATSConstants.DAG_NAME, "dagName");
    assertEventData(proto, ATSConstants.VERTEX_NAME_ID_MAPPING, null);
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
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_FINISHED, finishTime,
        EntityTypes.TEZ_DAG_ID, applicationAttemptId, user, 11);

    assertEventData(proto, ATSConstants.DAG_NAME, dagPlan.getName());
    assertEventData(proto, ATSConstants.STATUS, DAGState.ERROR.name());
    assertEventData(proto, ATSConstants.CALLER_CONTEXT_ID,
        dagPlan.getCallerContext().getCallerId());
    assertEventData(proto, ATSConstants.CALLER_CONTEXT_TYPE,
        dagPlan.getCallerContext().getCallerType());
    assertEventData(proto, ATSConstants.START_TIME, String.valueOf(startTime));
    assertEventData(proto, ATSConstants.TIME_TAKEN, String.valueOf(finishTime - startTime));
    assertEventData(proto, ATSConstants.DIAGNOSTICS, "diagnostics");
    assertEventData(proto, ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        applicationAttemptId.toString());
    assertEventData(proto, "FOO", String.valueOf(100));
    assertEventData(proto, "BAR", String.valueOf(200));
    assertEventData(proto, ATSConstants.COUNTERS, null);
  }

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

    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.VERTEX_INITIALIZED, initedTime,
        EntityTypes.TEZ_VERTEX_ID, null, null, 5);

    assertEventData(proto, ATSConstants.VERTEX_NAME, "v1");
    assertEventData(proto, ATSConstants.PROCESSOR_CLASS_NAME, "proc");
    assertEventData(proto, ATSConstants.INIT_REQUESTED_TIME, String.valueOf(initRequestedTime));
    assertEventData(proto, ATSConstants.NUM_TASKS, String.valueOf(numTasks));
    assertEventData(proto, ATSConstants.SERVICE_PLUGIN, null);

    /*
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
    */
  }

  @Test(timeout = 5000)
  public void testConvertVertexStartedEvent() {
    long startRequestedTime = random.nextLong();
    long startTime = random.nextLong();

    VertexStartedEvent event = new VertexStartedEvent(tezVertexID, startRequestedTime, startTime);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.VERTEX_STARTED, startTime,
        EntityTypes.TEZ_VERTEX_ID, null, null, 2);
    assertEventData(proto, ATSConstants.START_REQUESTED_TIME, String.valueOf(startRequestedTime));
    assertEventData(proto, ATSConstants.STATUS, VertexState.RUNNING.name());
  }

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

    VertexFinishedEvent event = new VertexFinishedEvent(tezVertexID, vertexName, 1,
        initRequestedTime, initedTime, startRequestedTime, startTime, finishTime,
        VertexState.ERROR, "diagnostics", null, vertexStats, taskStats,
        new ServicePluginInfo().setContainerLauncherName("abc")
            .setTaskSchedulerName("def").setTaskCommunicatorName("ghi")
            .setContainerLauncherClassName("abc1")
            .setTaskSchedulerClassName("def1")
            .setTaskCommunicatorClassName("ghi1"));
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.VERTEX_FINISHED, finishTime,
        EntityTypes.TEZ_VERTEX_ID, null, null, 9);

    assertEventData(proto, ATSConstants.VERTEX_NAME, vertexName);
    assertEventData(proto, ATSConstants.STATUS, VertexState.ERROR.name());

    assertEventData(proto, ATSConstants.TIME_TAKEN, String.valueOf(finishTime - startTime));
    assertEventData(proto, ATSConstants.DIAGNOSTICS, "diagnostics");
    assertEventData(proto, ATSConstants.COUNTERS, null);
    assertEventData(proto, ATSConstants.STATS, null);
    assertEventData(proto, "FOO", "100");
    assertEventData(proto, "BAR", "200");

    assertEventData(proto, ATSConstants.SERVICE_PLUGIN, null);
    /*
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
    */
  }

  @Test(timeout = 5000)
  public void testConvertTaskStartedEvent() {
    long scheduleTime = random.nextLong();
    long startTime = random.nextLong();
    TaskStartedEvent event = new TaskStartedEvent(tezTaskID, "v1", scheduleTime, startTime);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.TASK_STARTED, startTime,
        EntityTypes.TEZ_TASK_ID, null, null, 2);

    assertEventData(proto, ATSConstants.SCHEDULED_TIME, String.valueOf(scheduleTime));
    assertEventData(proto, ATSConstants.STATUS, TaskState.SCHEDULED.name());
  }

  @Test(timeout = 5000)
  public void testConvertTaskAttemptStartedEvent() {
    long startTime = random.nextLong();
    TaskAttemptStartedEvent event = new TaskAttemptStartedEvent(tezTaskAttemptID, "v1",
        startTime, containerId, nodeId, "inProgressURL", "logsURL", "nodeHttpAddress");
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.TASK_ATTEMPT_STARTED, startTime,
        EntityTypes.TEZ_TASK_ATTEMPT_ID, null, null, 6);

    assertEventData(proto, ATSConstants.STATUS, TaskAttemptState.RUNNING.name());
    assertEventData(proto, ATSConstants.IN_PROGRESS_LOGS_URL, "inProgressURL");
    assertEventData(proto, ATSConstants.COMPLETED_LOGS_URL, "logsURL");
    assertEventData(proto, ATSConstants.NODE_ID, nodeId.toString());
    assertEventData(proto, ATSConstants.CONTAINER_ID, containerId.toString());
    assertEventData(proto, ATSConstants.NODE_HTTP_ADDRESS, "nodeHttpAddress");
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
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.TASK_FINISHED, finishTime,
        EntityTypes.TEZ_TASK_ID, null, null, 6);

    assertEventData(proto, ATSConstants.STATUS, state.name());
    assertEventData(proto, ATSConstants.TIME_TAKEN, String.valueOf(finishTime - startTime));
    assertEventData(proto, ATSConstants.SUCCESSFUL_ATTEMPT_ID, tezTaskAttemptID.toString());
    assertEventData(proto, ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, "3");
    assertEventData(proto, ATSConstants.DIAGNOSTICS, diagnostics);
    assertEventData(proto, ATSConstants.COUNTERS, null);
  }

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
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.VERTEX_CONFIGURE_DONE, 0L,
        EntityTypes.TEZ_VERTEX_ID, null, null, 2);
    assertEventData(proto, ATSConstants.NUM_TASKS, "1");
    assertEventData(proto, ATSConstants.UPDATED_EDGE_MANAGERS, null);

    /*
    Map<String, Object> updatedEdgeMgrs = (Map<String, Object>)
        evt.getEventInfo().get(ATSConstants.UPDATED_EDGE_MANAGERS);
    Assert.assertEquals(1, updatedEdgeMgrs.size());
    Assert.assertTrue(updatedEdgeMgrs.containsKey("a"));
    Map<String, Object> updatedEdgeMgr = (Map<String, Object>) updatedEdgeMgrs.get("a");

    Assert.assertEquals(DataMovementType.CUSTOM.name(),
        updatedEdgeMgr.get(DAGUtils.DATA_MOVEMENT_TYPE_KEY));
    Assert.assertEquals("In", updatedEdgeMgr.get(DAGUtils.EDGE_DESTINATION_CLASS_KEY));
    Assert.assertEquals("a.class", updatedEdgeMgr.get(DAGUtils.EDGE_MANAGER_CLASS_KEY));
    */
  }

  @Test(timeout = 5000)
  public void testConvertDAGRecoveredEvent() {
    long recoverTime = random.nextLong();
    DAGRecoveredEvent event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID,
        dagPlan.getName(), user, recoverTime, containerLogs);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_RECOVERED, recoverTime,
        EntityTypes.TEZ_DAG_ID, applicationAttemptId, user, 2);
    assertEventData(proto, ATSConstants.IN_PROGRESS_LOGS_URL + "_"
        + applicationAttemptId.getAttemptId(), containerLogs);
    assertEventData(proto, ATSConstants.DAG_NAME, dagPlan.getName());
  }

  @Test(timeout = 5000)
  public void testConvertDAGRecoveredEvent2() {
    long recoverTime = random.nextLong();

    DAGRecoveredEvent event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID,
        dagPlan.getName(), user, recoverTime, DAGState.ERROR, "mock reason", containerLogs);
    HistoryEventProto proto = converter.convert(event);
    assertCommon(proto, HistoryEventType.DAG_RECOVERED, recoverTime,
        EntityTypes.TEZ_DAG_ID, applicationAttemptId, user, 4);
    assertEventData(proto, ATSConstants.DAG_STATE, DAGState.ERROR.name());
    assertEventData(proto, ATSConstants.RECOVERY_FAILURE_REASON, "mock reason");
    assertEventData(proto, ATSConstants.IN_PROGRESS_LOGS_URL + "_"
        + applicationAttemptId.getAttemptId(), containerLogs);
    assertEventData(proto, ATSConstants.DAG_NAME, dagPlan.getName());
  }
}

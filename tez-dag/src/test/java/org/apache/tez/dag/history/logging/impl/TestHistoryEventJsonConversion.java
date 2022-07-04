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

package org.apache.tez.dag.history.logging.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
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
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHistoryEventJsonConversion {

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

  @SuppressWarnings("deprecation")
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
              null, user, null, null, "Q_" + eventType.name());
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
              random.nextInt(), TaskAttemptState.KILLED, null, TaskAttemptTerminationCause.TERMINATED_BY_CLIENT,
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
          event = new DAGRecoveredEvent(applicationAttemptId, tezDAGID, dagPlan.getName(), user,
              1l, null);
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
      JSONObject json = HistoryEventJsonConversion.convertToJson(event);
      if (eventType == HistoryEventType.DAG_SUBMITTED) {
        try {
          Assert.assertEquals("Q_" + eventType.name(), json.getJSONObject(ATSConstants.OTHER_INFO)
              .getString(ATSConstants.DAG_QUEUE_NAME));
          Assert.assertEquals("Q_" + eventType.name(), json
              .getJSONObject(ATSConstants.PRIMARY_FILTERS).getString(ATSConstants.DAG_QUEUE_NAME));
        } catch (JSONException ex) {
          Assert.fail("Exception: " + ex.getMessage() + " for type: " + eventType);
        }
      }
    }
  }

  @Test(timeout = 5000)
  public void testConvertVertexReconfigureDoneEvent() throws JSONException {
    TezVertexID vId = TezVertexID.getInstance(
        TezDAGID.getInstance(
            ApplicationId.newInstance(1l, 1), 1), 1);
    Map<String, EdgeProperty> edgeMgrs =
        new HashMap<String, EdgeProperty>();

    edgeMgrs.put("a", EdgeProperty.create(EdgeManagerPluginDescriptor.create("a.class")
            .setHistoryText("text"), DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("Out"), InputDescriptor.create("In")));
    VertexConfigurationDoneEvent event = new VertexConfigurationDoneEvent(vId, 0L, 1, null,
        edgeMgrs, null, true);

    JSONObject jsonObject = HistoryEventJsonConversion.convertToJson(event);
    Assert.assertNotNull(jsonObject);
    Assert.assertEquals(vId.toString(), jsonObject.getString(ATSConstants.ENTITY));
    Assert.assertEquals(ATSConstants.TEZ_VERTEX_ID, jsonObject.get(ATSConstants.ENTITY_TYPE));

    JSONArray events = jsonObject.getJSONArray(ATSConstants.EVENTS);
    Assert.assertEquals(1, events.length());

    JSONObject evt = events.getJSONObject(0);
    Assert.assertEquals(HistoryEventType.VERTEX_CONFIGURE_DONE.name(),
        evt.getString(ATSConstants.EVENT_TYPE));

    JSONObject evtInfo = evt.getJSONObject(ATSConstants.EVENT_INFO);
    Assert.assertEquals(1, evtInfo.getInt(ATSConstants.NUM_TASKS));
    Assert.assertNotNull(evtInfo.getJSONObject(ATSConstants.UPDATED_EDGE_MANAGERS));

    JSONObject updatedEdgeMgrs = evtInfo.getJSONObject(ATSConstants.UPDATED_EDGE_MANAGERS);
    Assert.assertEquals(1, updatedEdgeMgrs.length());
    Assert.assertNotNull(updatedEdgeMgrs.getJSONObject("a"));
    JSONObject updatedEdgeMgr = updatedEdgeMgrs.getJSONObject("a");

    Assert.assertEquals(DataMovementType.CUSTOM.name(),
        updatedEdgeMgr.getString(DAGUtils.DATA_MOVEMENT_TYPE_KEY));
    Assert.assertEquals("In", updatedEdgeMgr.getString(DAGUtils.EDGE_DESTINATION_CLASS_KEY));
    Assert.assertEquals("a.class", updatedEdgeMgr.getString(DAGUtils.EDGE_MANAGER_CLASS_KEY));
  }
}

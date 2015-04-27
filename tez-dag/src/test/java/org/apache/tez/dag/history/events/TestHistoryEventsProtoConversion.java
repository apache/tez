/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License: Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing: software
 * distributed under the License is distributed on an "AS IS" BASIS:
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND: either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.events;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestHistoryEventsProtoConversion {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestHistoryEventsProtoConversion.class);


  private HistoryEvent testProtoConversion(HistoryEvent event) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    HistoryEvent deserializedEvent = null;
    event.toProtoStream(os);
    os.flush();
    os.close();
    deserializedEvent = ReflectionUtils.createClazzInstance(
        event.getClass().getName());
    LOG.info("Serialized event to byte array"
        + ", eventType=" + event.getEventType()
        + ", bufLen=" + os.toByteArray().length);
    deserializedEvent.fromProtoStream(
        new ByteArrayInputStream(os.toByteArray()));
    return deserializedEvent;
  }

  private HistoryEvent testSummaryProtoConversion(HistoryEvent historyEvent)
      throws IOException {
    SummaryEvent event = (SummaryEvent) historyEvent;
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    HistoryEvent deserializedEvent = null;
    event.toSummaryProtoStream(os);
    os.flush();
    os.close();
    LOG.info("Serialized event to byte array"
        + ", eventType=" + historyEvent.getEventType()
        + ", bufLen=" + os.toByteArray().length);
    SummaryEventProto summaryEventProto =
        SummaryEventProto.parseDelimitedFrom(
            new ByteArrayInputStream(os.toByteArray()));
    deserializedEvent = ReflectionUtils.createClazzInstance(
        event.getClass().getName());
    ((SummaryEvent)deserializedEvent).fromSummaryProtoStream(summaryEventProto);
    return deserializedEvent;
  }

  private void logEvents(HistoryEvent event,
      HistoryEvent deserializedEvent) {
    LOG.info("Initial Event toString: " + event.toString());
    LOG.info("Deserialized Event toString: " + deserializedEvent.toString());
  }

  private void testAppLaunchedEvent() throws Exception {
    AppLaunchedEvent event = new AppLaunchedEvent(ApplicationId.newInstance(0, 1),
        100, 100, null, new Configuration(false), null);
    try {
      testProtoConversion(event);
      fail("Expected to fail on conversion");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    LOG.info("Initial Event toString: " + event.toString());

  }

  private void testAMLaunchedEvent() throws Exception {
    AMLaunchedEvent event = new AMLaunchedEvent(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1),
        100, 100, null);
    AMLaunchedEvent deserializedEvent = (AMLaunchedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getApplicationAttemptId(),
        deserializedEvent.getApplicationAttemptId());
    Assert.assertEquals(event.getAppSubmitTime(),
        deserializedEvent.getAppSubmitTime());
    Assert.assertEquals(event.getLaunchTime(),
        deserializedEvent.getLaunchTime());
    logEvents(event, deserializedEvent);
  }

  private void testAMStartedEvent() throws Exception {
    AMStartedEvent event = new AMStartedEvent(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 100, "");
    AMStartedEvent deserializedEvent = (AMStartedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getApplicationAttemptId(),
        deserializedEvent.getApplicationAttemptId());
    Assert.assertEquals(event.getStartTime(),
        deserializedEvent.getStartTime());
    logEvents(event, deserializedEvent);
  }

  private void testDAGSubmittedEvent() throws Exception {
    DAGSubmittedEvent event = new DAGSubmittedEvent(TezDAGID.getInstance(
        ApplicationId.newInstance(0, 1), 1), 1001l,
        DAGPlan.newBuilder().setName("foo").build(),
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), null, "", null);
    DAGSubmittedEvent deserializedEvent = (DAGSubmittedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getApplicationAttemptId(),
        deserializedEvent.getApplicationAttemptId());
    Assert.assertEquals(event.getDagID(),
        deserializedEvent.getDagID());
    Assert.assertEquals(event.getDAGName(),
        deserializedEvent.getDAGName());
    Assert.assertEquals(event.getSubmitTime(),
        deserializedEvent.getSubmitTime());
    Assert.assertEquals(event.getDAGPlan(),
        deserializedEvent.getDAGPlan());
    logEvents(event, deserializedEvent);
  }

  private void testDAGInitializedEvent() throws Exception {
    DAGInitializedEvent event = new DAGInitializedEvent(
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 100334l,
        "user", "dagName", null);
    DAGInitializedEvent deserializedEvent = (DAGInitializedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getDagID(),
        deserializedEvent.getDagID());
    Assert.assertEquals(event.getInitTime(), deserializedEvent.getInitTime());
    logEvents(event, deserializedEvent);
  }

  private void testDAGStartedEvent() throws Exception {
    DAGStartedEvent event = new DAGStartedEvent(
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 100334l,
        "user", "dagName");
    DAGStartedEvent deserializedEvent = (DAGStartedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getDagID(),
        deserializedEvent.getDagID());
    Assert.assertEquals(event.getStartTime(), deserializedEvent.getStartTime());
    logEvents(event, deserializedEvent);
  }

  private void testDAGFinishedEvent() throws Exception {
    {
      DAGFinishedEvent event = new DAGFinishedEvent(
          TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 1000l, 20000l,
          DAGState.FAILED, null, null, "user", "dagName", null, null);
      DAGFinishedEvent deserializedEvent = (DAGFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(
          event.getDagID(),
          deserializedEvent.getDagID());
      Assert.assertEquals(event.getState(), deserializedEvent.getState());
      Assert.assertNotEquals(event.getStartTime(), deserializedEvent.getStartTime());
      Assert.assertEquals(event.getFinishTime(), deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      Assert.assertEquals(event.getTezCounters(), deserializedEvent.getTezCounters());
      logEvents(event, deserializedEvent);
    }
    {
      TezCounters tezCounters = new TezCounters();
      tezCounters.addGroup("foo", "bar");
      tezCounters.getGroup("foo").addCounter("c1", "c1", 100);
      tezCounters.getGroup("foo").findCounter("c1").increment(1);
      DAGFinishedEvent event = new DAGFinishedEvent(
          TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 1000l, 20000l,
          DAGState.FAILED, "bad diagnostics", tezCounters,
          "user", "dagName", null, null);
      DAGFinishedEvent deserializedEvent = (DAGFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(
          event.getDagID(),
          deserializedEvent.getDagID());
      Assert.assertEquals(event.getState(), deserializedEvent.getState());
      Assert.assertNotEquals(event.getStartTime(), deserializedEvent.getStartTime());
      Assert.assertEquals(event.getFinishTime(), deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      Assert.assertEquals(event.getTezCounters(), deserializedEvent.getTezCounters());
      Assert.assertEquals(101,
          deserializedEvent.getTezCounters().getGroup("foo").findCounter("c1").getValue());
      logEvents(event, deserializedEvent);
    }
  }

  private void testVertexInitializedEvent() throws Exception {
    VertexInitializedEvent event = new VertexInitializedEvent(
        TezVertexID.getInstance(
            TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
        "vertex1", 1000l, 15000l, 100, "procName", null);
    VertexInitializedEvent deserializedEvent = (VertexInitializedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
    Assert.assertEquals(event.getInitRequestedTime(),
        deserializedEvent.getInitRequestedTime());
    Assert.assertEquals(event.getInitedTime(),
        deserializedEvent.getInitedTime());
    Assert.assertEquals(event.getNumTasks(),
        deserializedEvent.getNumTasks());
    Assert.assertEquals(event.getAdditionalInputs(),
        deserializedEvent.getAdditionalInputs());
    Assert.assertNull(deserializedEvent.getProcessorName());
    logEvents(event, deserializedEvent);
  }

  private void testVertexStartedEvent() throws Exception {
    VertexStartedEvent event = new VertexStartedEvent(
        TezVertexID.getInstance(
            TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
        145553l, 12334455l);
    VertexStartedEvent deserializedEvent = (VertexStartedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
    Assert.assertEquals(event.getStartRequestedTime(),
        deserializedEvent.getStartRequestedTime());
    Assert.assertEquals(event.getStartTime(),
        deserializedEvent.getStartTime());
    logEvents(event, deserializedEvent);
  }

  private void testVertexParallelismUpdatedEvent() throws Exception {
    {
      InputSpecUpdate rootInputSpecUpdateBulk = InputSpecUpdate
          .createAllTaskInputSpecUpdate(2);
      InputSpecUpdate rootInputSpecUpdatePerTask = InputSpecUpdate
          .createPerTaskInputSpecUpdate(Lists.newArrayList(1, 2, 3));
      Map<String, InputSpecUpdate> rootInputSpecUpdates = new HashMap<String, InputSpecUpdate>();
      rootInputSpecUpdates.put("input1", rootInputSpecUpdateBulk);
      rootInputSpecUpdates.put("input2", rootInputSpecUpdatePerTask);
      VertexParallelismUpdatedEvent event =
          new VertexParallelismUpdatedEvent(
              TezVertexID.getInstance(
                  TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
              100, null, null, rootInputSpecUpdates, 1);
      VertexParallelismUpdatedEvent deserializedEvent = (VertexParallelismUpdatedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
      Assert.assertEquals(event.getNumTasks(), deserializedEvent.getNumTasks());
      Assert.assertEquals(event.getSourceEdgeProperties(),
          deserializedEvent.getSourceEdgeProperties());
      Assert.assertEquals(event.getVertexLocationHint(),
          deserializedEvent.getVertexLocationHint());
      Assert.assertEquals(event.getRootInputSpecUpdates().size(), deserializedEvent
          .getRootInputSpecUpdates().size());
      InputSpecUpdate deserializedBulk = deserializedEvent.getRootInputSpecUpdates().get("input1");
      InputSpecUpdate deserializedPerTask = deserializedEvent.getRootInputSpecUpdates().get("input2");
      Assert.assertEquals(rootInputSpecUpdateBulk.isForAllWorkUnits(),
          deserializedBulk.isForAllWorkUnits());
      Assert.assertEquals(rootInputSpecUpdateBulk.getAllNumPhysicalInputs(),
          deserializedBulk.getAllNumPhysicalInputs());
      Assert.assertEquals(rootInputSpecUpdatePerTask.isForAllWorkUnits(),
          deserializedPerTask.isForAllWorkUnits());
      Assert.assertEquals(rootInputSpecUpdatePerTask.getAllNumPhysicalInputs(),
          deserializedPerTask.getAllNumPhysicalInputs());
      logEvents(event, deserializedEvent);
    }
    {
      Map<String, EdgeProperty> sourceEdgeManagers
          = new LinkedHashMap<String, EdgeProperty>();
      // add standard and custom edge
      sourceEdgeManagers.put("foo", EdgeProperty.create(DataMovementType.SCATTER_GATHER, 
          DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
          OutputDescriptor.create("Out1"), InputDescriptor.create("in1")));
      sourceEdgeManagers.put("foo1", EdgeProperty.create(EdgeManagerPluginDescriptor.create("bar1")
          .setUserPayload(
              UserPayload.create(ByteBuffer.wrap(new String("payload").getBytes()), 100)), 
          DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
          OutputDescriptor.create("Out1"), InputDescriptor.create("in1")));
      VertexParallelismUpdatedEvent event =
          new VertexParallelismUpdatedEvent(
              TezVertexID.getInstance(
                  TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
              100, VertexLocationHint.create(Arrays.asList(TaskLocationHint.createTaskLocationHint(
              new HashSet<String>(Arrays.asList("h1")),
              new HashSet<String>(Arrays.asList("r1"))))),
              sourceEdgeManagers, null, 1);

      VertexParallelismUpdatedEvent deserializedEvent = 
          (VertexParallelismUpdatedEvent) testProtoConversion(event);
      Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
      Assert.assertEquals(event.getNumTasks(), deserializedEvent.getNumTasks());
      Assert.assertEquals(event.getSourceEdgeProperties().size(), deserializedEvent
          .getSourceEdgeProperties().size());
      Assert.assertEquals(event.getSourceEdgeProperties().get("foo").getDataMovementType(),
          deserializedEvent.getSourceEdgeProperties().get("foo").getDataMovementType());
      Assert.assertNull(deserializedEvent.getSourceEdgeProperties().get("foo")
          .getEdgeManagerDescriptor());
      Assert.assertEquals(event.getSourceEdgeProperties().get("foo1").getDataMovementType(),
          deserializedEvent.getSourceEdgeProperties().get("foo1").getDataMovementType());
      Assert.assertEquals(event.getSourceEdgeProperties().get("foo1").getEdgeManagerDescriptor()
          .getUserPayload().getVersion(), deserializedEvent.getSourceEdgeProperties().get("foo1")
          .getEdgeManagerDescriptor().getUserPayload().getVersion());
      Assert.assertArrayEquals(event.getSourceEdgeProperties().get("foo1")
          .getEdgeManagerDescriptor().getUserPayload().deepCopyAsArray(), deserializedEvent
          .getSourceEdgeProperties().get("foo1").getEdgeManagerDescriptor().getUserPayload()
          .deepCopyAsArray());
      Assert.assertEquals(event.getVertexLocationHint(), deserializedEvent.getVertexLocationHint());
      logEvents(event, deserializedEvent);
    }
  }

  private void testVertexFinishedEvent() throws Exception {
    {
      VertexFinishedEvent event =
          new VertexFinishedEvent(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
              "vertex1", 1, 1000l, 15000l, 16000l, 20000l, 1344400l, VertexState.ERROR,
              null, null, null, null);
      VertexFinishedEvent deserializedEvent = (VertexFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getState(), deserializedEvent.getState());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      logEvents(event, deserializedEvent);
    }
    {
      VertexFinishedEvent event =
          new VertexFinishedEvent(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111),
              "vertex1", 1, 1000l, 15000l, 16000l, 20000l, 1344400l, VertexState.ERROR,
              "diagnose", new TezCounters(), new VertexStats(), null);
      VertexFinishedEvent deserializedEvent = (VertexFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getState(), deserializedEvent.getState());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      logEvents(event, deserializedEvent);
    }
  }

  private void testTaskStartedEvent() throws Exception {
    TaskStartedEvent event = new TaskStartedEvent(
        TezTaskID.getInstance(TezVertexID.getInstance(
            TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1),
        "vertex1", 1000l, 100000l);
    TaskStartedEvent deserializedEvent = (TaskStartedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getTaskID(), deserializedEvent.getTaskID());
    Assert.assertEquals(event.getScheduledTime(),
        deserializedEvent.getScheduledTime());
    Assert.assertEquals(event.getStartTime(),
        deserializedEvent.getStartTime());
    logEvents(event, deserializedEvent);
  }

  private void testTaskFinishedEvent() throws Exception {
    {
      TaskFinishedEvent event = new TaskFinishedEvent(
          TezTaskID.getInstance(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1),
          "vertex1", 11000l, 1000000l, null, TaskState.FAILED, null, null);
      TaskFinishedEvent deserializedEvent = (TaskFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getTaskID(), deserializedEvent.getTaskID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getState(),
          deserializedEvent.getState());
      Assert.assertEquals(event.getSuccessfulAttemptID(),
          deserializedEvent.getSuccessfulAttemptID());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      logEvents(event, deserializedEvent);
    }
    {
      TaskFinishedEvent event = new TaskFinishedEvent(
          TezTaskID.getInstance(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1),
          "vertex1", 11000l, 1000000l,
          TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1), 1),
          TaskState.FAILED, "task_diagnostics", new TezCounters());
      TaskFinishedEvent deserializedEvent = (TaskFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getTaskID(), deserializedEvent.getTaskID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getState(),
          deserializedEvent.getState());
      Assert.assertEquals(event.getSuccessfulAttemptID(),
          deserializedEvent.getSuccessfulAttemptID());
      Assert.assertEquals(event.getDiagnostics(), deserializedEvent.getDiagnostics());
      logEvents(event, deserializedEvent);
    }
  }

  private void testTaskAttemptStartedEvent() throws Exception {
    TaskAttemptStartedEvent event = new TaskAttemptStartedEvent(
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
            TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1), 1),
        "vertex1", 10009l, ContainerId.newInstance(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1001), NodeId.newInstance(
        "host1", 19999), "inProgress", "Completed", "nodeHttpAddress");
    TaskAttemptStartedEvent deserializedEvent = (TaskAttemptStartedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getTaskAttemptID(),
        deserializedEvent.getTaskAttemptID());
    Assert.assertEquals(event.getContainerId(),
        deserializedEvent.getContainerId());
    Assert.assertEquals(event.getNodeId(),
        deserializedEvent.getNodeId());
    Assert.assertEquals(event.getStartTime(),
        deserializedEvent.getStartTime());
    logEvents(event, deserializedEvent);
  }

  private void testTaskAttemptFinishedEvent() throws Exception {
    {
      TaskAttemptFinishedEvent event = new TaskAttemptFinishedEvent(
          TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1), 1),
          "vertex1", 10001l, 1000434444l, TaskAttemptState.FAILED,
          null, null, null);
      TaskAttemptFinishedEvent deserializedEvent = (TaskAttemptFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getTaskAttemptID(),
          deserializedEvent.getTaskAttemptID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getDiagnostics(),
          deserializedEvent.getDiagnostics());
      Assert.assertEquals(event.getState(),
          deserializedEvent.getState());
      Assert.assertEquals(event.getCounters(),
          deserializedEvent.getCounters());
      logEvents(event, deserializedEvent);
    }
    {
      TaskAttemptFinishedEvent event = new TaskAttemptFinishedEvent(
          TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 111), 1), 1),
          "vertex1", 10001l, 1000434444l, TaskAttemptState.FAILED,
          TaskAttemptTerminationCause.APPLICATION_ERROR, "diagnose", new TezCounters());
      TaskAttemptFinishedEvent deserializedEvent = (TaskAttemptFinishedEvent)
          testProtoConversion(event);
      Assert.assertEquals(event.getTaskAttemptID(),
          deserializedEvent.getTaskAttemptID());
      Assert.assertEquals(event.getFinishTime(),
          deserializedEvent.getFinishTime());
      Assert.assertEquals(event.getDiagnostics(),
          deserializedEvent.getDiagnostics());
      Assert.assertEquals(event.getState(),
          deserializedEvent.getState());
      Assert.assertEquals(event.getCounters(),
          deserializedEvent.getCounters());
      Assert.assertEquals(event.getTaskAttemptError(),
          deserializedEvent.getTaskAttemptError());
      logEvents(event, deserializedEvent);
    }
  }

  private void testContainerLaunchedEvent() throws Exception {
    ContainerLaunchedEvent event = new ContainerLaunchedEvent(
        ContainerId.newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1001), 100034566,
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1));
    ContainerLaunchedEvent deserializedEvent = (ContainerLaunchedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getContainerId(),
        deserializedEvent.getContainerId());
    Assert.assertEquals(event.getLaunchTime(),
        deserializedEvent.getLaunchTime());
    Assert.assertEquals(event.getApplicationAttemptId(),
        deserializedEvent.getApplicationAttemptId());
    logEvents(event, deserializedEvent);
  }

  private void testContainerStoppedEvent() throws Exception {
    ContainerStoppedEvent event = new ContainerStoppedEvent(
        ContainerId.newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1001), 100034566,
        ContainerExitStatus.SUCCESS, ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1));
    ContainerStoppedEvent deserializedEvent = (ContainerStoppedEvent)
        testProtoConversion(event);
    Assert.assertEquals(event.getContainerId(),
        deserializedEvent.getContainerId());
    Assert.assertEquals(event.getStoppedTime(),
        deserializedEvent.getStoppedTime());
    Assert.assertEquals(event.getApplicationAttemptId(),
        deserializedEvent.getApplicationAttemptId());
    logEvents(event, deserializedEvent);
  }

  private void testVertexDataMovementEventsGeneratedEvent() throws Exception {
    VertexRecoverableEventsGeneratedEvent event;
    try {
      event = new VertexRecoverableEventsGeneratedEvent(
          TezVertexID.getInstance(
              TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 1), null);
      fail("Invalid creation should have errored out");
    } catch (RuntimeException e) {
      // Expected
    }
    List<TezEvent> events =
        Arrays.asList(new TezEvent(DataMovementEvent.create(1, null),
            new EventMetaData(EventProducerConsumerType.SYSTEM, "foo", "bar", null)));
    event = new VertexRecoverableEventsGeneratedEvent(
            TezVertexID.getInstance(
                TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 1), events);
    VertexRecoverableEventsGeneratedEvent deserializedEvent =
        (VertexRecoverableEventsGeneratedEvent) testProtoConversion(event);
    Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
    Assert.assertEquals(1,
        deserializedEvent.getTezEvents().size());
    Assert.assertEquals(event.getTezEvents().get(0).getEventType(),
        deserializedEvent.getTezEvents().get(0).getEventType());
    logEvents(event, deserializedEvent);
  }

  private void testDAGCommitStartedEvent() throws Exception {
    DAGCommitStartedEvent event = new DAGCommitStartedEvent(
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 100l);
    DAGCommitStartedEvent deserializedEvent =
        (DAGCommitStartedEvent) testProtoConversion(event);
    Assert.assertEquals(event.getDagID(), deserializedEvent.getDagID());
    logEvents(event, deserializedEvent);
  }

  private void testVertexCommitStartedEvent() throws Exception {
    VertexCommitStartedEvent event = new VertexCommitStartedEvent(
        TezVertexID.getInstance(
            TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1), 1), 100l);
    VertexCommitStartedEvent deserializedEvent =
        (VertexCommitStartedEvent) testProtoConversion(event);
    Assert.assertEquals(event.getVertexID(), deserializedEvent.getVertexID());
    logEvents(event, deserializedEvent);
  }

  private void testVertexGroupCommitStartedEvent() throws Exception {
    VertexGroupCommitStartedEvent event = new VertexGroupCommitStartedEvent(
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1),
        "fooGroup", 1000344l);
    {
      VertexGroupCommitStartedEvent deserializedEvent =
          (VertexGroupCommitStartedEvent) testProtoConversion(event);
      Assert.assertEquals(event.getDagID(), deserializedEvent.getDagID());
      Assert.assertEquals(event.getVertexGroupName(),
          deserializedEvent.getVertexGroupName());
      logEvents(event, deserializedEvent);
    }
    {
      VertexGroupCommitStartedEvent deserializedEvent =
          (VertexGroupCommitStartedEvent) testSummaryProtoConversion(event);
      Assert.assertEquals(event.getVertexGroupName(),
          deserializedEvent.getVertexGroupName());
      logEvents(event, deserializedEvent);
    }
  }

  private void testVertexGroupCommitFinishedEvent() throws Exception {
    VertexGroupCommitFinishedEvent event = new VertexGroupCommitFinishedEvent(
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1),
        "fooGroup", 1000344l);
    {
      VertexGroupCommitFinishedEvent deserializedEvent =
          (VertexGroupCommitFinishedEvent) testProtoConversion(event);
      Assert.assertEquals(event.getDagID(), deserializedEvent.getDagID());
      Assert.assertEquals(event.getVertexGroupName(),
          deserializedEvent.getVertexGroupName());
      logEvents(event, deserializedEvent);
    }
    {
      VertexGroupCommitFinishedEvent deserializedEvent =
          (VertexGroupCommitFinishedEvent) testSummaryProtoConversion(event);
      Assert.assertEquals(event.getVertexGroupName(),
          deserializedEvent.getVertexGroupName());
      logEvents(event, deserializedEvent);
    }
  }


  @Test(timeout = 5000)
  public void testDefaultProtoConversion() throws Exception {
    for (HistoryEventType eventType : HistoryEventType.values()) {
      switch (eventType) {
        case APP_LAUNCHED:
          testAppLaunchedEvent();
          break;
        case AM_LAUNCHED:
          testAMLaunchedEvent();
          break;
        case AM_STARTED:
          testAMStartedEvent();
          break;
        case DAG_SUBMITTED:
          testDAGSubmittedEvent();
          break;
        case DAG_INITIALIZED:
          testDAGInitializedEvent();
          break;
        case DAG_STARTED:
          testDAGStartedEvent();
          break;
        case DAG_FINISHED:
          testDAGFinishedEvent();
          break;
        case VERTEX_INITIALIZED:
          testVertexInitializedEvent();
          break;
        case VERTEX_STARTED:
          testVertexStartedEvent();
          break;
        case VERTEX_PARALLELISM_UPDATED:
          testVertexParallelismUpdatedEvent();
          break;
        case VERTEX_FINISHED:
          testVertexFinishedEvent();
          break;
        case TASK_STARTED:
          testTaskStartedEvent();
          break;
        case TASK_FINISHED:
          testTaskFinishedEvent();
          break;
        case TASK_ATTEMPT_STARTED:
          testTaskAttemptStartedEvent();
          break;
        case TASK_ATTEMPT_FINISHED:
          testTaskAttemptFinishedEvent();
          break;
        case CONTAINER_LAUNCHED:
          testContainerLaunchedEvent();
          break;
        case CONTAINER_STOPPED:
          testContainerStoppedEvent();
          break;
        case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
          testVertexDataMovementEventsGeneratedEvent();
          break;
        case DAG_COMMIT_STARTED:
          testDAGCommitStartedEvent();
          break;
        case VERTEX_COMMIT_STARTED:
          testVertexCommitStartedEvent();
          break;
        case VERTEX_GROUP_COMMIT_STARTED:
          testVertexGroupCommitStartedEvent();
          break;
        case VERTEX_GROUP_COMMIT_FINISHED:
          testVertexGroupCommitFinishedEvent();
          break;
        case DAG_RECOVERED:
          testDAGRecoveredEvent();
          break;
        default:
          throw new Exception("Unhandled Event type in Unit tests: " + eventType);
        }
      }
    }

  private void testDAGRecoveredEvent() {
    DAGRecoveredEvent dagRecoveredEvent = new DAGRecoveredEvent(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 1), 1),
        TezDAGID.getInstance(ApplicationId.newInstance(0, 1), 1),
        "mockDagname", "mockuser", 100334l);
    try {
      testProtoConversion(dagRecoveredEvent);
      Assert.fail("Proto conversion should have failed");
    } catch (UnsupportedOperationException e) {
      // Expected
    } catch (IOException e) {
      Assert.fail("Proto conversion should have failed with Unsupported Exception");
    }

  }

}

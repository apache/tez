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

package org.apache.tez.dag.app.dag.impl;

import static org.junit.Assert.*;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataMovementType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventRecoverTask;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventManagerUserCodeError;
import org.apache.tez.dag.app.dag.event.VertexEventRecoverVertex;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.CountingOutputCommitter;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.VertexRecoverableEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexFinishStateProto;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestVertexRecovery {

  private static final Log LOG = LogFactory.getLog(TestVertexRecovery.class);

  private DrainDispatcher dispatcher;

  private AppContext mockAppContext;
  private ApplicationId appId = ApplicationId.newInstance(
      System.currentTimeMillis(), 1);
  private DAGImpl dag;
  private TezDAGID dagId = TezDAGID.getInstance(appId, 1);
  private String user = "user";

  private long initRequestedTime = 100L;
  private long initedTime = initRequestedTime + 100L;

  /*
   * v1 v2 \ / v3
   */
  private DAGPlan createDAGPlan() {
    DAGPlan dag =
        DAGPlan
            .newBuilder()
            .setName("testverteximpl")
            .addVertex(
                VertexPlan
                    .newBuilder()
                    .setName("vertex1")
                    .setType(PlanVertexType.NORMAL)
                    .addTaskLocationHint(
                        PlanTaskLocationHint.newBuilder().addHost("host1")
                            .addRack("rack1").build())
                    .setTaskConfig(
                        PlanTaskConfiguration.newBuilder().setNumTasks(1)
                            .setVirtualCores(4).setMemoryMb(1024)
                            .setJavaOpts("").setTaskModule("x1.y1").build())
                    .addOutEdgeId("e1")
                    .addOutputs(
                        DAGProtos.RootInputLeafOutputProto
                            .newBuilder()
                            .setIODescriptor(
                                TezEntityDescriptorProto.newBuilder()
                                    .setClassName("output").build())
                            .setName("outputx")
                            .setControllerDescriptor(
                                TezEntityDescriptorProto
                                    .newBuilder()
                                    .setClassName(
                                        CountingOutputCommitter.class.getName())))
                    .build())
            .addVertex(
                VertexPlan
                    .newBuilder()
                    .setName("vertex2")
                    .setType(PlanVertexType.NORMAL)
                    .addTaskLocationHint(
                        PlanTaskLocationHint.newBuilder().addHost("host2")
                            .addRack("rack2").build())
                    .setTaskConfig(
                        PlanTaskConfiguration.newBuilder().setNumTasks(2)
                            .setVirtualCores(4).setMemoryMb(1024)
                            .setJavaOpts("").setTaskModule("x2.y2").build())
                    .addOutEdgeId("e2").build())
            .addVertex(
                VertexPlan
                    .newBuilder()
                    .setName("vertex3")
                    .setType(PlanVertexType.NORMAL)
                    .setProcessorDescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName(
                            "x3.y3"))
                    .addTaskLocationHint(
                        PlanTaskLocationHint.newBuilder().addHost("host3")
                            .addRack("rack3").build())
                    .setTaskConfig(
                        PlanTaskConfiguration.newBuilder().setNumTasks(2)
                            .setVirtualCores(4).setMemoryMb(1024)
                            .setJavaOpts("foo").setTaskModule("x3.y3").build())
                    .addInEdgeId("e1")
                    .addInEdgeId("e2")
                    .addOutputs(
                        DAGProtos.RootInputLeafOutputProto
                            .newBuilder()
                            .setIODescriptor(
                                TezEntityDescriptorProto.newBuilder()
                                    .setClassName("output").build())
                            .setName("outputx")
                            .setControllerDescriptor(
                                TezEntityDescriptorProto
                                    .newBuilder()
                                    .setClassName(
                                        CountingOutputCommitter.class.getName())))
                    .build()

            )

            .addEdge(
                EdgePlan
                    .newBuilder()
                    .setEdgeDestination(
                        TezEntityDescriptorProto.newBuilder().setClassName(
                            "i3_v1"))
                    .setInputVertexName("vertex1")
                    .setEdgeSource(
                        TezEntityDescriptorProto.newBuilder()
                            .setClassName("o1"))
                    .setOutputVertexName("vertex3")
                    .setDataMovementType(
                        PlanEdgeDataMovementType.SCATTER_GATHER).setId("e1")
                    .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                    .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                    .build())
            .addEdge(
                EdgePlan
                    .newBuilder()
                    .setEdgeDestination(
                        TezEntityDescriptorProto.newBuilder().setClassName(
                            "i3_v2"))
                    .setInputVertexName("vertex2")
                    .setEdgeSource(
                        TezEntityDescriptorProto.newBuilder()
                            .setClassName("o2"))
                    .setOutputVertexName("vertex3")
                    .setDataMovementType(
                        PlanEdgeDataMovementType.SCATTER_GATHER).setId("e2")
                    .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                    .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                    .build()).build();

    return dag;
  }

  private DAGPlan createDAGPlanSingleVertex() {
    DAGPlan dag =
        DAGPlan
            .newBuilder()
            .setName("testverteximpl")
            .addVertex(
                VertexPlan
                    .newBuilder()
                    .setName("vertex1")
                    .setType(PlanVertexType.NORMAL)
                    .addTaskLocationHint(
                        PlanTaskLocationHint.newBuilder().addHost("host1")
                            .addRack("rack1").build())
                    .setTaskConfig(
                        PlanTaskConfiguration.newBuilder().setNumTasks(-1)
                            .setVirtualCores(4).setMemoryMb(1024)
                            .setJavaOpts("").setTaskModule("x1.y1").build())
                    .addInputs(RootInputLeafOutputProto.newBuilder()
                            .setIODescriptor(
                                TezEntityDescriptorProto.newBuilder()
                                    .setClassName("input").build())
                            .setName("inputx")
                            .setControllerDescriptor(
                                TezEntityDescriptorProto
                                    .newBuilder()
                                    .setClassName("inputinitlizer"))
                            .build())
                    .addOutputs(
                        DAGProtos.RootInputLeafOutputProto
                            .newBuilder()
                            .setIODescriptor(
                                TezEntityDescriptorProto.newBuilder()
                                    .setClassName("output").build())
                            .setName("outputx")
                            .setControllerDescriptor(
                                TezEntityDescriptorProto
                                    .newBuilder()
                                    .setClassName(
                                        CountingOutputCommitter.class.getName())))
                    .build()).build();
    return dag;
  }

  class VertexEventHanlder implements EventHandler<VertexEvent> {

    private List<VertexEvent> events = new ArrayList<VertexEvent>();

    @Override
    public void handle(VertexEvent event) {
      events.add(event);
      ((VertexImpl) dag.getVertex(event.getVertexId())).handle(event);
    }

    public List<VertexEvent> getEvents() {
      return this.events;
    }
  }

  class TaskEventHandler implements EventHandler<TaskEvent> {

    private List<TaskEvent> events = new ArrayList<TaskEvent>();

    @Override
    public void handle(TaskEvent event) {
      events.add(event);
      ((TaskImpl) dag.getVertex(event.getTaskID().getVertexID()).getTask(
          event.getTaskID())).handle(event);
    }

    public List<TaskEvent> getEvents() {
      return events;
    }
  }

  class TaskAttemptEventHandler implements EventHandler<TaskAttemptEvent> {

    @Override
    public void handle(TaskAttemptEvent event) {
      // TezTaskID taskId = event.getTaskAttemptID().getTaskID();
      // ((TaskAttemptImpl) vertex1.getTask(taskId).getAttempt(
      // event.getTaskAttemptID())).handle(event);
    }
  }

  private VertexEventHanlder vertexEventHandler;
  private TaskEventHandler taskEventHandler;

  @Before
  public void setUp() throws IOException {

    dispatcher = new DrainDispatcher();
    dispatcher.register(DAGEventType.class, mock(EventHandler.class));
    vertexEventHandler = new VertexEventHanlder();
    dispatcher.register(VertexEventType.class, vertexEventHandler);
    taskEventHandler = new TaskEventHandler();
    dispatcher.register(TaskEventType.class, taskEventHandler);
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventHandler());
    dispatcher.init(new Configuration());
    dispatcher.start();

    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);

    DAGPlan dagPlan = createDAGPlan();
    dag =
        new DAGImpl(dagId, new Configuration(), dagPlan,
            dispatcher.getEventHandler(), mock(TaskAttemptListener.class),
            new Credentials(), new SystemClock(), user,
            mock(TaskHeartbeatHandler.class), mockAppContext);
    when(mockAppContext.getCurrentDAG()).thenReturn(dag);

    dag.handle(new DAGEvent(dagId, DAGEventType.DAG_INIT));
    LOG.info("finish setUp");
  }

  /**
   * vertex1(New) -> StartRecoveryTransition(SUCCEEDED)
   */
  @Test(timeout = 5000)
  public void testRecovery_Desired_SUCCEEDED() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexInitializedEvent(vertex1.getVertexId(),
        "vertex1", initRequestedTime, initedTime, vertex1.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.SUCCEEDED));
    dispatcher.await();
    assertEquals(VertexState.SUCCEEDED, vertex1.getState());
    assertEquals(vertex1.numTasks, vertex1.succeededTaskCount);
    assertEquals(vertex1.numTasks, vertex1.completedTaskCount);
    // recover its task
    assertTaskRecoveredEventSent(vertex1);

    // vertex3 is still in NEW, when the desiredState is
    // Completed State, each vertex recovery by itself, not depend on its parent
    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    assertEquals(VertexState.NEW, vertex3.getState());
    // no VertexEvent pass to downstream vertex
    assertEquals(0, vertexEventHandler.getEvents().size());

  }

  /**
   * vertex1(New) -> StartRecoveryTransition(SUCCEEDED)
   * @throws IOException 
   */
  @Test(timeout = 5000)
  public void testRecovery_Desired_SUCCEEDED_OnlySummaryLog() throws IOException {
    DAGPlan dagPlan = createDAGPlanSingleVertex();
    dag =
        new DAGImpl(dagId, new Configuration(), dagPlan,
            dispatcher.getEventHandler(), mock(TaskAttemptListener.class),
            new Credentials(), new SystemClock(), user,
            mock(TaskHeartbeatHandler.class), mockAppContext);
    when(mockAppContext.getCurrentDAG()).thenReturn(dag);
    dag.handle(new DAGEvent(dagId, DAGEventType.DAG_INIT));

    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexFinishedEvent vertexFinishEvent = new VertexFinishedEvent();
    vertexFinishEvent.fromSummaryProtoStream(SummaryEventProto.newBuilder()
        .setDagId(dag.getID().toString())
        .setEventType(HistoryEventType.VERTEX_FINISHED.ordinal())
        .setTimestamp(100L)
        .setEventPayload(VertexFinishStateProto.newBuilder()
            .setNumTasks(2)
            .setState(VertexState.SUCCEEDED.ordinal())
            .setVertexId(vertex1.getVertexId().toString()).build().toByteString())
        .build());
    VertexState recoveredState = vertex1.restoreFromEvent(vertexFinishEvent);
    // numTasks is recovered from summary log
    assertEquals(2, vertex1.numTasks);
    assertEquals(VertexState.SUCCEEDED, recoveredState);
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.SUCCEEDED));
    dispatcher.await();
    assertEquals(VertexState.SUCCEEDED, vertex1.getState());
    assertEquals(vertex1.numTasks, vertex1.succeededTaskCount);
    assertEquals(vertex1.numTasks, vertex1.completedTaskCount);
  }

  /**
   * vertex1(New) -> StartRecoveryTransition(FAILED)
   */
  @Test(timeout = 5000)
  public void testRecovery_Desired_FAILED() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexInitializedEvent(vertex1.getVertexId(),
        "vertex1", initRequestedTime, initedTime, vertex1.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.FAILED));
    dispatcher.await();
    assertEquals(VertexState.FAILED, vertex1.getState());
    assertEquals(vertex1.numTasks, vertex1.failedTaskCount);
    assertEquals(0, vertex1.completedTaskCount);
    // recover its task
    assertTaskRecoveredEventSent(vertex1);

    // vertex3 is still in NEW, when the desiredState is
    // Completed State, each vertex recovery by itself, not depend on its parent
    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    assertEquals(VertexState.NEW, vertex3.getState());
    // no VertexEvent pass to downstream vertex
    assertEquals(0, vertexEventHandler.getEvents().size());
  }

  /**
   * vertex1(New) -> StartRecoveryTransition(KILLED)
   */
  @Test(timeout = 5000)
  public void testRecovery_Desired_KILLED() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexInitializedEvent(vertex1.getVertexId(),
        "vertex1", initRequestedTime, initedTime, vertex1.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.KILLED));
    dispatcher.await();
    assertEquals(VertexState.KILLED, vertex1.getState());
    assertEquals(vertex1.numTasks, vertex1.killedTaskCount);
    assertEquals(0, vertex1.completedTaskCount);
    // recover its task
    assertTaskRecoveredEventSent(vertex1);

    // vertex3 is still in NEW, when the desiredState is
    // Completed State, each vertex recovery by itself, not depend on its parent
    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    assertEquals(VertexState.NEW, vertex3.getState());
    // no VertexEvent pass to downstream vertex
    assertEquals(0, vertexEventHandler.getEvents().size());
  }

  /**
   * vertex1(New) -> StartRecoveryTransition(ERROR)
   */
  @Test(timeout = 5000)
  public void testRecovery_Desired_ERROR() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexInitializedEvent(vertex1.getVertexId(),
        "vertex1", initRequestedTime, initedTime, vertex1.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.ERROR));
    dispatcher.await();
    assertEquals(VertexState.ERROR, vertex1.getState());
    assertEquals(vertex1.numTasks, vertex1.failedTaskCount);
    assertEquals(0, vertex1.completedTaskCount);
    // recover its task
    assertTaskRecoveredEventSent(vertex1);

    // vertex3 is still in NEW, when the desiredState is
    // Completed State, each vertex recovery by itself, not depend on its parent
    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    assertEquals(VertexState.NEW, vertex3.getState());
    // no VertexEvent pass to downstream vertex
    assertEquals(0, vertexEventHandler.getEvents().size());
  }

  private TezEvent createTezEvent() {
    return new TezEvent(InputDataInformationEvent.createWithSerializedPayload(0, ByteBuffer.allocate(0)),
        new EventMetaData(EventProducerConsumerType.INPUT, "vertex1", null,
            null));
  }

  /**
   * vertex1(New) -> restoreFromDataMovementEvent -> StartRecoveryTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_New_Desired_RUNNING() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    VertexState recoveredState =
        vertex1.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex1.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.NEW, recoveredState);
    assertEquals(1, vertex1.recoveredEvents.size());

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();

    // InputDataInformationEvent is removed
    assertEquals(0, vertex1.recoveredEvents.size());
    // V_INIT and V_START is sent
    assertEquals(VertexState.RUNNING, vertex1.getState());

    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());

  }

  private void assertTaskRecoveredEventSent(VertexImpl vertex) {
    int sentNum = 0;
    for (TaskEvent event : taskEventHandler.getEvents()) {
      if (event.getType() == TaskEventType.T_RECOVER) {
        TaskEventRecoverTask recoverEvent = (TaskEventRecoverTask)event;
        if (recoverEvent.getTaskID().getVertexID().equals(vertex.getVertexId())){
          sentNum++;
        }
      }
    }
    assertEquals("expect " + vertex.getTotalTasks()
        + " TaskEventTaskRecover sent for vertex:" + vertex.getVertexId() +
        "but actuall sent " + sentNum, vertex.getTotalTasks(), sentNum);
  }

  private void assertOutputCommitters(VertexImpl vertex){
    assertTrue(vertex.getOutputCommitters() != null);
    for (OutputCommitter c : vertex.getOutputCommitters().values()) {
      CountingOutputCommitter committer = (CountingOutputCommitter) c;
      assertEquals(0, committer.abortCounter);
      assertEquals(0, committer.commitCounter);
      assertEquals(1, committer.initCounter);
      assertEquals(1, committer.setupCounter);
    }
  }

  private void restoreFromInitializedEvent(VertexImpl vertex) {
    long initTimeRequested = 100L;
    long initedTime = initTimeRequested + 100L;
    VertexState recoveredState =
        vertex.restoreFromEvent(new VertexInitializedEvent(vertex
            .getVertexId(), "vertex1", initTimeRequested, initedTime, vertex.getTotalTasks(),
            "", null));
    assertEquals(VertexState.INITED, recoveredState);
    assertEquals(vertex.getTotalTasks(), vertex.getTasks().size());
    assertEquals(initTimeRequested, vertex.initTimeRequested);
    assertEquals(initedTime, vertex.initedTime);
  }

  /**
   * restoreFromVertexInitializedEvent -> StartRecoveryTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Inited_Desired_RUNNING() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);

    VertexState recoveredState =
        vertex1.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex1.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.INITED, recoveredState);

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();

    // InputDataInformationEvent is removed
    assertEquals(0, vertex1.recoveredEvents.size());
    assertEquals(VertexState.RUNNING, vertex1.getState());
    // task recovered event is sent
    assertTaskRecoveredEventSent(vertex1);
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
  }

  /**
   * restoreFromVertexInitializedEvent -> restoreFromVertexStartedEvent ->
   * StartRecoveryTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Started_Desired_RUNNING() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);

    long startTimeRequested = initedTime + 100L;
    long startedTime = startTimeRequested + 100L;
    VertexState recoveredState =
        vertex1.restoreFromEvent(new VertexStartedEvent(vertex1.getVertexId(),
            startTimeRequested, startedTime));
    assertEquals(VertexState.RUNNING, recoveredState);
    assertEquals(startTimeRequested, vertex1.startTimeRequested);
    assertEquals(startedTime, vertex1.startedTime);

    recoveredState =
        vertex1.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex1.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.RUNNING, recoveredState);
    assertEquals(1, vertex1.recoveredEvents.size());

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();

    // InputDataInformationEvent is removed
    assertEquals(0, vertex1.recoveredEvents.size());
    assertEquals(VertexState.RUNNING, vertex1.getState());
    // task recovered event is sent
    assertTaskRecoveredEventSent(vertex1);
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
  }

  /**
   * restoreFromVertexInitializedEvent -> restoreFromVertexStartedEvent ->
   * restoreFromVertexFinishedEvent -> StartRecoveryTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_Finished_Desired_RUNNING() {
    // v1: initFromInitializedEvent
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);

    // v1: initFromStartedEvent
    long startRequestedTime = initedTime + 100L;
    long startTime = startRequestedTime + 100L;
    VertexState recoveredState =
        vertex1.restoreFromEvent(new VertexStartedEvent(vertex1.getVertexId(),
            startRequestedTime, startTime));
    assertEquals(VertexState.RUNNING, recoveredState);

    // v1: initFromFinishedEvent
    long finishTime = startTime + 100L;
    recoveredState =
        vertex1.restoreFromEvent(new VertexFinishedEvent(vertex1.getVertexId(),
            "vertex1", 1, initRequestedTime, initedTime, startRequestedTime,
            startTime, finishTime, VertexState.SUCCEEDED, "",
            new TezCounters(), new VertexStats(), null));
    assertEquals(finishTime, vertex1.finishTime);
    assertEquals(VertexState.SUCCEEDED, recoveredState);
    assertEquals(false, vertex1.recoveryCommitInProgress);

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();

    // InputDataInformationEvent is removed
    assertEquals(0, vertex1.recoveredEvents.size());
    assertEquals(VertexState.RUNNING, vertex1.getState());
    // task recovered event is sent
    assertTaskRecoveredEventSent(vertex1);

    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
  }

  /**
   * vertex1 (New) -> StartRecoveryTransition <br>
   * vertex2 (New) -> StartRecoveryTransition vertex3 (New) -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_RecoveringFromNew() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(1, vertex1.getTasks().size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    VertexState recoveredState =
        vertex3.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex3.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.NEW, recoveredState);
    assertEquals(1, vertex3.recoveredEvents.size());

    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());

    VertexImpl vertex2 = (VertexImpl) dag.getVertex("vertex2");
    vertex2.handle(new VertexEventRecoverVertex(vertex2.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex2.getState());
    // no OutputCommitter for vertex2
    assertNull(vertex2.getOutputCommitters());

    // v3 go to RUNNING because v1 and v2 both start
    assertEquals(VertexState.RUNNING, vertex3.getState());
    assertEquals(2, vertex3.numRecoveredSourceVertices);
    assertEquals(2, vertex3.numInitedSourceVertices);
    assertEquals(2, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
    // RootInputDataInformation is removed
    assertEquals(0, vertex3.recoveredEvents.size());

    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex3);

  }
  
  
  @Test(timeout = 5000)
  public void testRecovery_VertexManagerErrorOnRecovery() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(vertex1.getTotalTasks(), vertex1.getTasks().size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    VertexState recoveredState =
        vertex3.restoreFromEvent(new VertexInitializedEvent(vertex3
            .getVertexId(), "vertex3", initRequestedTime, initedTime, 0, "",
            null));
    assertEquals(VertexState.INITED, recoveredState);
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    vertex3.handle(new VertexEventManagerUserCodeError(vertex3.getVertexId(), 
        new AMUserCodeException(Source.VertexManager, new TezUncheckedException("test"))));

    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
    VertexImpl vertex2 = (VertexImpl) dag.getVertex("vertex2");
    restoreFromInitializedEvent(vertex2);
    vertex2.handle(new VertexEventRecoverVertex(vertex2.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex2.getState());

    // v3 FAILED due to user code error
    assertEquals(VertexState.FAILED, vertex3.getState());
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, vertex3.getTerminationCause());
    assertEquals(2, vertex3.numRecoveredSourceVertices);
  }


  /**
   * vertex1 (New) -> restoreFromInitialized -> StartRecoveryTransition<br>
   * vertex2 (New) -> restoreFromInitialized -> StartRecoveryTransition<br>
   * vertex3 (New) -> restoreFromVertexInitedEvent -> RecoverTransition<br>
   */
  @Test(timeout = 5000)
  public void testRecovery_RecoveringFromInited() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);
    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(vertex1.getTotalTasks(), vertex1.getTasks().size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    VertexState recoveredState =
        vertex3.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex3.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.NEW, recoveredState);
    assertEquals(1, vertex3.recoveredEvents.size());
    recoveredState =
        vertex3.restoreFromEvent(new VertexInitializedEvent(vertex3
            .getVertexId(), "vertex3", initRequestedTime, initedTime, 2, "",
            null));
    assertEquals(VertexState.INITED, recoveredState);
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());

    VertexImpl vertex2 = (VertexImpl) dag.getVertex("vertex2");
    restoreFromInitializedEvent(vertex2);
    vertex2.handle(new VertexEventRecoverVertex(vertex2.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex2.getState());

    // v3 go to RUNNING because v1 and v2 both start
    assertEquals(VertexState.RUNNING, vertex3.getState());
    assertEquals(2, vertex3.numRecoveredSourceVertices);
    // numInitedSourceVertices is wrong but doesn't matter because v3 has
    // already initialized
    assertEquals(2, vertex3.numInitedSourceVertices);
    assertEquals(2, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
    // RootInputDataInformation is removed
    assertEquals(0, vertex3.recoveredEvents.size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex3);
    // 1 for vertex1, 2 for vertex2, the second 2 for vertex3
    assertTaskRecoveredEventSent(vertex1);
    assertTaskRecoveredEventSent(vertex2);
    assertTaskRecoveredEventSent(vertex3);
  }

  /**
   * vertex1 (New) -> restoreFromInitialized -> restoreFromVertexStarted ->
   * StartRecoveryTransition <br>
   * vertex2 (New) -> restoreFromInitialized -> restoreFromVertexStarted -> StartRecoveryTransition <br>
   * vertex3 (New) -> restoreFromInitialized -> restoreFromVertexStarted -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_RecoveringFromRunning() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexStartedEvent(vertex1.getVertexId(),
        initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(1, vertex1.getTasks().size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    recoveredState =
        vertex3.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex3.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.NEW, recoveredState);
    assertEquals(1, vertex3.recoveredEvents.size());
    recoveredState =
        vertex3.restoreFromEvent(new VertexInitializedEvent(vertex3
            .getVertexId(), "vertex3", initRequestedTime, initedTime, vertex3.getTotalTasks(), "",
            null));
    assertEquals(VertexState.INITED, recoveredState);
    recoveredState =
        vertex3.restoreFromEvent(new VertexStartedEvent(vertex3.getVertexId(),
            initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());

    VertexImpl vertex2 = (VertexImpl) dag.getVertex("vertex2");
    recoveredState = vertex2.restoreFromEvent(new VertexInitializedEvent(vertex2.getVertexId(),
        "vertex2", initRequestedTime, initedTime, vertex2.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    recoveredState = vertex2.restoreFromEvent(new VertexStartedEvent(vertex2.getVertexId(),
        initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);
    
    vertex2.handle(new VertexEventRecoverVertex(vertex2.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex2.getState());

    // v3 go to RUNNING because v1 and v2 both start
    assertEquals(VertexState.RUNNING, vertex3.getState());
    assertEquals(2, vertex3.numRecoveredSourceVertices);
    assertEquals(2, vertex3.numInitedSourceVertices);
    assertEquals(2, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
    // RootInputDataInformation is removed
    assertEquals(0, vertex3.recoveredEvents.size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex3);

    assertTaskRecoveredEventSent(vertex1);
    assertTaskRecoveredEventSent(vertex2);
    assertTaskRecoveredEventSent(vertex3);
  }

  /**
   * vertex1 (New) -> restoreFromInitialized -> restoreFromVertexStarted ->
   * restoreFromVertexFinished -> StartRecoveryTransition<br>
   * vertex2 (New) -> restoreFromInitialized -> restoreFromVertexStarted ->
   * restoreFromVertexFinished -> StartRecoveryTransition<br>
   * vertex3 (New) -> restoreFromInitialized -> restoreFromVertexStarted -> RecoverTransition
   */
  @Test(timeout = 5000)
  public void testRecovery_RecoveringFromSUCCEEDED() {
    VertexImpl vertex1 = (VertexImpl) dag.getVertex("vertex1");
    restoreFromInitializedEvent(vertex1);
    VertexState recoveredState = vertex1.restoreFromEvent(new VertexStartedEvent(vertex1.getVertexId(),
        initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);

    recoveredState = vertex1.restoreFromEvent(new VertexFinishedEvent(vertex1.getVertexId(),
        "vertex1", 1, initRequestedTime, initedTime, initRequestedTime + 300L,
        initRequestedTime + 400L, initRequestedTime + 500L,
        VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(), null));
    assertEquals(VertexState.SUCCEEDED, recoveredState);

    vertex1.handle(new VertexEventRecoverVertex(vertex1.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(1, vertex1.getTasks().size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex1);

    VertexImpl vertex3 = (VertexImpl) dag.getVertex("vertex3");
    recoveredState =
        vertex3.restoreFromEvent(new VertexRecoverableEventsGeneratedEvent(
            vertex3.getVertexId(), Lists.newArrayList(createTezEvent())));
    assertEquals(VertexState.NEW, recoveredState);
    assertEquals(1, vertex3.recoveredEvents.size());
    restoreFromInitializedEvent(vertex3);
    recoveredState =
        vertex3.restoreFromEvent(new VertexStartedEvent(vertex3.getVertexId(),
            initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);
    // wait for recovery of vertex2
    assertEquals(VertexState.RECOVERING, vertex3.getState());
    assertEquals(1, vertex3.numRecoveredSourceVertices);
    assertEquals(1, vertex3.numInitedSourceVertices);
    assertEquals(1, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());

    VertexImpl vertex2 = (VertexImpl) dag.getVertex("vertex2");
    recoveredState = vertex2.restoreFromEvent(new VertexInitializedEvent(vertex2.getVertexId(),
        "vertex2", initRequestedTime, initedTime, vertex2.getTotalTasks(), "", null));
    assertEquals(VertexState.INITED, recoveredState);
    recoveredState = vertex2.restoreFromEvent(new VertexStartedEvent(vertex2.getVertexId(),
        initRequestedTime + 100L, initRequestedTime + 200L));
    assertEquals(VertexState.RUNNING, recoveredState);
    vertex2.handle(new VertexEventRecoverVertex(vertex2.getVertexId(),
        VertexState.RUNNING));
    dispatcher.await();
    assertEquals(VertexState.RUNNING, vertex2.getState());

    // v3 go to RUNNING because v1 and v2 both start
    assertEquals(VertexState.RUNNING, vertex3.getState());
    assertEquals(2, vertex3.numRecoveredSourceVertices);
    assertEquals(2, vertex3.numInitedSourceVertices);
    assertEquals(2, vertex3.numStartedSourceVertices);
    assertEquals(1, vertex3.getDistanceFromRoot());
    // RootInputDataInformation is removed
    assertEquals(0, vertex3.recoveredEvents.size());
    // verify OutputCommitter is initialized
    assertOutputCommitters(vertex3);

    assertTaskRecoveredEventSent(vertex1);
    assertTaskRecoveredEventSent(vertex2);
    assertTaskRecoveredEventSent(vertex3);
  }
}

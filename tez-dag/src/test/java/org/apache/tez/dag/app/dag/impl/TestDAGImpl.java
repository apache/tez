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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeConnectionPattern;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate.UpdateType;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenSecretManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestDAGImpl {

  private static final Log LOG = LogFactory.getLog(TestDAGImpl.class);
  private DAGPlan dagPlan;
  private TezDAGID dagId;
  private Configuration conf;
  private DrainDispatcher dispatcher;
  private Credentials fsTokens;
  private AppContext appContext;
  private ApplicationAttemptId appAttemptId;
  private DAGImpl dag;
  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private DagEventDispatcher dagEventDispatcher;
  private TaskAttemptListener taskAttemptListener;
  private TaskHeartbeatHandler thh;
  private Clock clock = new SystemClock();
  private JobTokenSecretManager jobTokenSecretManager;
  private DAGFinishEventHandler dagFinishEventHandler;
  private AppContext mrrAppContext;
  private DAGPlan mrrDagPlan;
  private DAGImpl mrrDag;
  private TezDAGID mrrDagId;

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
      if (event.getDAGId().equals(dagId)) {
        dag.handle(event);
      }  else if (event.getDAGId().equals(mrrDagId)) {
        mrrDag.handle(event);
      } else {
        throw new RuntimeException("Invalid event, unknown dag"
            + ", dagId=" + event.getDAGId());
      }
    }
  }

  private class HistoryHandler implements EventHandler<DAGHistoryEvent> {
    @Override
    public void handle(DAGHistoryEvent event) {
    }
  }

  private class TaskEventHandler implements EventHandler<TaskEvent> {
    @Override
    public void handle(TaskEvent event) {
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      DAGImpl handler = event.getTaskID().getVertexID().getDAGId().equals(dagId) ?
          dag : mrrDag;
      Vertex vertex = handler.getVertex(event.getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class VertexEventDispatcher
      implements EventHandler<VertexEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      DAGImpl handler = event.getVertexId().getDAGId().equals(dagId) ?
          dag : mrrDag;
      Vertex vertex = handler.getVertex(event.getVertexId());
      ((EventHandler<VertexEvent>) vertex).handle(event);
    }
  }

  private class DAGFinishEventHandler
  implements EventHandler<DAGAppMasterEventDAGFinished> {
    public int dagFinishEvents = 0;

    @Override
    public void handle(DAGAppMasterEventDAGFinished event) {
      ++dagFinishEvents;
    }
  }

  private DAGPlan createTestMRRDAGPlan() {
    LOG.info("Setting up MRR dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testverteximpl")
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex1")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host1")
                .addRack("rack1")
                .build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(1)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x1.y1")
                    .build()
                    )
                    .addOutEdgeId("e1")
                    .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex2")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host2")
                .addRack("rack2")
                .build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(2)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x2.y2")
                    .build()
                    )
                    .addInEdgeId("e1")
                    .addOutEdgeId("e2")
                    .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex3")
            .setType(PlanVertexType.NORMAL)
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("x3.y3"))
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host3")
                .addRack("rack3")
                .build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(2)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("foo")
                    .setTaskModule("x3.y3")
                    .build()
                    )
                    .addInEdgeId("e2")
                    .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i2"))
            .setInputVertexName("vertex1")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o1"))
            .setOutputVertexName("vertex2")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e1")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
       .addEdge(
           EdgePlan.newBuilder()
           .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i3"))
           .setInputVertexName("vertex2")
           .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
           .setOutputVertexName("vertex3")
           .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
           .setId("e2")
           .setSourceType(PlanEdgeSourceType.STABLE)
           .build()
           )
      .build();

    return dag;
  }

  private DAGPlan createTestDAGPlan() {
    LOG.info("Setting up dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testverteximpl")
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex1")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host1")
                .addRack("rack1")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(1)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("x1.y1")
                .build()
                )
            .addOutEdgeId("e1")
            .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex2")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host2")
                .addRack("rack2")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("x2.y2")
                .build()
                )
            .addOutEdgeId("e2")
            .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex3")
            .setType(PlanVertexType.NORMAL)
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("x3.y3"))
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host3")
                .addRack("rack3")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("foo")
                .setTaskModule("x3.y3")
                .build()
                )
            .addInEdgeId("e1")
            .addInEdgeId("e2")
            .addOutEdgeId("e3")
            .addOutEdgeId("e4")
            .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex4")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host4")
                .addRack("rack4")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("x4.y4")
                .build()
                )
            .addInEdgeId("e3")
            .addOutEdgeId("e5")
            .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex5")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host5")
                .addRack("rack5")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("x5.y5")
                .build()
                )
            .addInEdgeId("e4")
            .addOutEdgeId("e6")
            .build()
            )
        .addVertex(
            VertexPlan.newBuilder()
            .setName("vertex6")
            .setType(PlanVertexType.NORMAL)
            .addTaskLocationHint(
                PlanTaskLocationHint.newBuilder()
                .addHost("host6")
                .addRack("rack6")
                .build()
                )
            .setTaskConfig(
                PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("x6.y6")
                .build()
                )
            .addInEdgeId("e5")
            .addInEdgeId("e6")
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i3_v1"))
            .setInputVertexName("vertex1")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o1"))
            .setOutputVertexName("vertex3")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e1")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i3_v2"))
            .setInputVertexName("vertex2")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
            .setOutputVertexName("vertex3")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e2")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i4_v3"))
            .setInputVertexName("vertex3")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o3_v4"))
            .setOutputVertexName("vertex4")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e3")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i5_v3"))
            .setInputVertexName("vertex3")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o3_v5"))
            .setOutputVertexName("vertex5")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e4")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i6_v4"))
            .setInputVertexName("vertex4")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o4"))
            .setOutputVertexName("vertex6")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e5")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i6_v5"))
            .setInputVertexName("vertex5")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o5"))
            .setOutputVertexName("vertex6")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e6")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .build();

    return dag;
  }

  @Before
  public void setup() {
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = new TezDAGID(appAttemptId.getApplicationId(), 1);
    Assert.assertNotNull(dagId);
    dagPlan = createTestDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    jobTokenSecretManager = new JobTokenSecretManager();
    appContext = mock(AppContext.class);
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(dagId).when(appContext).getDAGID();
    dag = new DAGImpl(dagId, conf, dagPlan,
        dispatcher.getEventHandler(),  taskAttemptListener,
        jobTokenSecretManager, fsTokens, clock, "user", thh, appContext);
    doReturn(dag).when(appContext).getDAG();
    mrrAppContext = mock(AppContext.class);
    mrrDagId = new TezDAGID(appAttemptId.getApplicationId(), 2);
    mrrDagPlan = createTestMRRDAGPlan();
    mrrDag = new DAGImpl(mrrDagId, conf, mrrDagPlan,
        dispatcher.getEventHandler(),  taskAttemptListener,
        jobTokenSecretManager, fsTokens, clock, "user", thh,
        mrrAppContext);
    doReturn(mrrDag).when(mrrAppContext).getDAG();
    doReturn(appAttemptId).when(mrrAppContext).getApplicationAttemptId();
    taskEventDispatcher = new TaskEventDispatcher();
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    vertexEventDispatcher = new VertexEventDispatcher();
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dagEventDispatcher = new DagEventDispatcher();
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(HistoryEventType.class,
        new HistoryHandler());
    dagFinishEventHandler = new DAGFinishEventHandler();
    dispatcher.register(DAGAppMasterEventType.class, dagFinishEventHandler);
    dispatcher.register(TaskEventType.class, new TaskEventHandler());
    dispatcher.init(conf);
    dispatcher.start();
  }

  @After
  public void teardown() {
    dispatcher.await();
    dispatcher.stop();
    dagPlan = null;
    dag = null;
  }

  private void initDAG(DAGImpl impl) {
    impl.handle(
        new DAGEvent(impl.getID(), DAGEventType.DAG_INIT));
    Assert.assertEquals(DAGState.INITED, impl.getState());
  }

  @SuppressWarnings("unchecked")
  private void startDAG(DAGImpl impl) {
    dispatcher.getEventHandler().handle(
        new DAGEvent(impl.getID(), DAGEventType.DAG_START));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, impl.getState());
  }

  @Test
  public void testDAGInit() {
    initDAG(dag);
    Assert.assertEquals(6, dag.getTotalVertices());
  }

  @Test
  public void testDAGStart() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    for (int i = 0 ; i < 6; ++i ) {
      TezVertexID vId = new TezVertexID(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.RUNNING, v.getState());
      if (i < 2) {
        Assert.assertEquals(0, v.getDistanceFromRoot());
      } else if (i == 2) {
        Assert.assertEquals(1, v.getDistanceFromRoot());
      } else if ( i > 2 && i < 5) {
        Assert.assertEquals(2, v.getDistanceFromRoot());
      } else if (i == 5) {
        Assert.assertEquals(3, v.getDistanceFromRoot());
      }
    }

    for (int i = 0 ; i < 6; ++i ) {
      TezVertexID vId = new TezVertexID(dagId, i);
      LOG.info("Distance from root: v" + i + ":"
          + dag.getVertex(vId).getDistanceFromRoot());
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testVertexCompletion() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    TezVertexID vId = new TezVertexID(dagId, 1);
    Vertex v = dag.getVertex(vId);
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        new TezTaskID(vId, 0), TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        new TezTaskID(vId, 1), TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  public void testKillStartedDAG() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(
        new DAGEvent(dagId, DAGEventType.DAG_KILL));
    dispatcher.await();

    Assert.assertEquals(DAGState.KILLED, dag.getState());
    for (int i = 0 ; i < 6; ++i ) {
      TezVertexID vId = new TezVertexID(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.KILLED, v.getState());
    }

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testKillRunningDAG() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    TezVertexID vId1 = new TezVertexID(dagId, 1);
    Vertex v1 = dag.getVertex(vId1);
    ((EventHandler<VertexEvent>) v1).handle(new VertexEventTaskCompleted(
        new TezTaskID(vId1, 0), TaskState.SUCCEEDED));
    TezVertexID vId0 = new TezVertexID(dagId, 0);
    Vertex v0 = dag.getVertex(vId0);
    ((EventHandler<VertexEvent>) v0).handle(new VertexEventTaskCompleted(
        new TezTaskID(vId0, 0), TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v0.getState());
    Assert.assertEquals(VertexState.RUNNING, v1.getState());

    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    dispatcher.await();

    Assert.assertEquals(DAGState.TERMINATING, dag.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v0.getState());
    Assert.assertEquals(VertexState.TERMINATING, v1.getState());
    for (int i = 2 ; i < 6; ++i ) {
      TezVertexID vId = new TezVertexID(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.KILLED, v.getState());
    }
    Assert.assertEquals(1, dag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInvalidEvent() {
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagId, DAGEventType.DAG_START));
    dispatcher.await();
    Assert.assertEquals(DAGState.ERROR, dag.getState());
  }

  @SuppressWarnings("unchecked")
  @Test
  @Ignore
  public void testVertexSuccessfulCompletionUpdates() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    for (int i = 0; i < 6; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          new TezVertexID(dagId, 0), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 2), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 3), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 4), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 5), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.SUCCEEDED, dag.getState());
    Assert.assertEquals(6, dag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testVertexFailureHandling() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 2), VertexState.FAILED));
    dispatcher.await();
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(2, dag.getSuccessfulVertices());

    // Expect running vertices to be killed on first failure
    for (int i = 3; i < 6; ++i) {
      TezVertexID vId = new TezVertexID(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.KILLED, v.getState());
    }
  }

  // a dag.kill() on an active DAG races with vertices all succeeding.
  // if a JOB_KILL is processed while dag is in running state, it should end in KILLED,
  // regardless of whether all vertices complete
  //
  // Final state:
  //   DAG is in KILLED state, with killTrigger = USER_KILL
  //   Each vertex had kill triggered but raced ahead and ends in SUCCEEDED state.
  @SuppressWarnings("unchecked")
  @Test
  public void testDAGKill() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    for (int i = 2; i < 6; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          new TezVertexID(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.DAG_KILL, dag.getTerminationCause());
    Assert.assertEquals(6, dag.getSuccessfulVertices());
    for (Vertex v : dag.getVertices().values()) {
      Assert.assertEquals(VertexTerminationCause.DAG_KILL, v.getTerminationCause());
    }
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  // job kill races with most vertices succeeding and one directly killed.
  // because the job.kill() happens before the direct kill, the vertex has kill_trigger=DAG_KILL
  @SuppressWarnings("unchecked")
  @Test
  public void testDAGKillPending() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));

    for (int i = 2; i < 5; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          new TezVertexID(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        new TezVertexID(dagId, 5), VertexState.KILLED));
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert.assertEquals(5, dag.getSuccessfulVertices());
    Assert.assertEquals(dag.getVertex(new TezVertexID(dagId, 5)).getTerminationCause(), VertexTerminationCause.DAG_KILL);
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  @Test
  public void testDiagnosticUpdates() {
    // FIXME need to implement
  }

  @Test
  public void testCounterUpdates() {
    // FIXME need to implement
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOutofBandFailureForMRRScheduler() {
    initDAG(mrrDag);
    dispatcher.await();
    Assert.assertTrue(mrrDag.dagScheduler instanceof DAGSchedulerMRR);
    startDAG(mrrDag);
    dispatcher.await();

    TaskAttempt attempt = mock(TaskAttempt.class);
    doReturn(
        mrrDag.getVertex("vertex1").getVertexId()).when(attempt).getVertexID();

    DAGEventSchedulerUpdate scheduleEvent =
        new DAGEventSchedulerUpdate(UpdateType.TA_SCHEDULE, attempt);
    dispatcher.getEventHandler().handle(scheduleEvent);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        mrrDag.getVertex("vertex2").getVertexId(), VertexState.FAILED));
    dispatcher.await();

    Assert.assertEquals(DAGState.FAILED, mrrDag.getState());
  }
}

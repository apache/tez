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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MRVertexOutputCommitter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.committer.NullVertexOutputCommitter;
import org.apache.tez.dag.api.committer.VertexContext;
import org.apache.tez.dag.api.committer.VertexOutputCommitter;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataMovementType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestVertexImpl {

  private static final Log LOG = LogFactory.getLog(TestVertexImpl.class);

  private TezDAGID dagId;
  private ApplicationAttemptId appAttemptId;
  private DAGPlan dagPlan;
  private Map<String, VertexImpl> vertices;
  private Map<TezVertexID, VertexImpl> vertexIdMap;
  private DrainDispatcher dispatcher;
  private TaskAttemptListener taskAttemptListener;
  private Credentials fsTokens;
  private Clock clock = new SystemClock();
  private TaskHeartbeatHandler thh;
  private AppContext appContext;
  private VertexLocationHint vertexLocationHint;
  private Configuration conf;
  private Map<String, Edge> edges;

  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;

  private DagEventDispatcher dagEventDispatcher;

  private class CountingVertexOutputCommitter extends
      VertexOutputCommitter {

    public int initCounter = 0;
    public int setupCounter = 0;
    public int commitCounter = 0;
    public int abortCounter = 0;
    private boolean throwError;
    private boolean throwErrorOnAbort;

    public CountingVertexOutputCommitter(boolean throwError,
        boolean throwOnAbort) {
      this.throwError = throwError;
      this.throwErrorOnAbort = throwOnAbort;
    }

    public CountingVertexOutputCommitter() {
      this(false, false);
    }

    @Override
    public void init(VertexContext context) throws IOException {
      ++initCounter;
    }

    @Override
    public void setupVertex() {
      ++setupCounter;
    }

    @Override
    public void commitVertex() throws IOException {
      ++commitCounter;
      if (throwError) {
        throw new IOException("I can throwz exceptions in commit");
      }
    }

    @Override
    public void abortVertex(VertexStatus.State finalState) throws IOException {
      ++abortCounter;
      if (throwErrorOnAbort) {
        throw new IOException("I can throwz exceptions in abort");
      }
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      VertexImpl vertex = vertexIdMap.get(event.getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    public Map<DAGEventType, Integer> eventCount =
        new HashMap<DAGEventType, Integer>();

    @Override
    public void handle(DAGEvent event) {
      int count = 1;
      if (eventCount.containsKey(event.getType())) {
        count = eventCount.get(event.getType()) + 1;
      }
      eventCount.put(event.getType(), count);
    }
  }

  private class HistoryHandler implements EventHandler<DAGHistoryEvent> {
    @Override
    public void handle(DAGHistoryEvent event) {
    }
  }

  private class VertexEventDispatcher
      implements EventHandler<VertexEvent> {

    @Override
    public void handle(VertexEvent event) {
      VertexImpl vertex = vertexIdMap.get(event.getVertexId());
      ((EventHandler<VertexEvent>) vertex).handle(event);
    }
  }

  private DAGPlan createInvalidDAGPlan() {
    LOG.info("Setting up invalid dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testverteximplinvalid")
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
            .setNumTasks(0)
            .setVirtualCores(4)
            .setMemoryMb(1024)
            .setJavaOpts("")
            .setTaskModule("x1.y1")
            .build()
            )
        .addOutEdgeId("e1")
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
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e1")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i3_v2"))
            .setInputVertexName("vertex2")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
            .setOutputVertexName("vertex3")
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e2")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i4_v3"))
            .setInputVertexName("vertex3")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o3_v4"))
            .setOutputVertexName("vertex4")
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e3")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i5_v3"))
            .setInputVertexName("vertex3")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o3_v5"))
            .setOutputVertexName("vertex5")
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e4")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i6_v4"))
            .setInputVertexName("vertex4")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o4"))
            .setOutputVertexName("vertex6")
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e5")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i6_v5"))
            .setInputVertexName("vertex5")
            .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o5"))
            .setOutputVertexName("vertex6")
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e6")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
        .build();

    return dag;
  }

  private void setupVertices() {
    int vCnt = dagPlan.getVertexCount();
    LOG.info("Setting up vertices from dag plan, verticesCnt=" + vCnt);
    vertices = new HashMap<String, VertexImpl>();
    vertexIdMap = new HashMap<TezVertexID, VertexImpl>();
    for (int i = 0; i < vCnt; ++i) {
      VertexPlan vPlan = dagPlan.getVertex(i);
      String vName = vPlan.getName();
      TezVertexID vertexId = new TezVertexID(dagId, i+1);
      VertexImpl v = new VertexImpl(vertexId, vPlan, vPlan.getName(), conf,
          dispatcher.getEventHandler(), taskAttemptListener, fsTokens,
          clock, thh, appContext, vertexLocationHint);
      vertices.put(vName, v);
      vertexIdMap.put(vertexId, v);
    }
  }

  private void parseVertexEdges() {
    LOG.info("Parsing edges from dag plan, edgeCount="
        + dagPlan.getEdgeCount());
    int vCnt = dagPlan.getVertexCount();
    Map<String, EdgePlan> edgePlans =
        DagTypeConverters.createEdgePlanMapFromDAGPlan(dagPlan.getEdgeList());

    // TODO - this test logic is tightly linked to impl DAGImpl code.
    for (int i = 0; i < vCnt; ++i) {
      VertexPlan vertexPlan = dagPlan.getVertex(i);
      Vertex vertex = vertices.get(vertexPlan.getName());
      Map<Vertex, Edge> inVertices =
          new HashMap<Vertex, Edge>();

      Map<Vertex, Edge> outVertices =
          new HashMap<Vertex, Edge>();

      for(String inEdgeId : vertexPlan.getInEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(inEdgeId);
        Vertex inVertex = this.vertices.get(edgePlan.getInputVertexName());
        Edge edge = this.edges.get(inEdgeId);
        edge.setSourceVertex(inVertex);
        edge.setDestinationVertex(vertex);
        inVertices.put(inVertex, edge);
      }

      for(String outEdgeId : vertexPlan.getOutEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(outEdgeId);
        Vertex outVertex = this.vertices.get(edgePlan.getOutputVertexName());
        Edge edge = this.edges.get(outEdgeId);
        edge.setSourceVertex(vertex);
        edge.setDestinationVertex(outVertex);
        outVertices.put(outVertex, edge);
      }
      LOG.info("Setting input vertices for vertex " + vertex.getName()
          + ", inputVerticesCnt=" + inVertices.size());
      vertex.setInputVertices(inVertices);
      LOG.info("Setting output vertices for vertex " + vertex.getName()
          + ", outputVerticesCnt=" + outVertices.size());
      vertex.setOutputVertices(outVertices);
    }
  }

  @Before
  public void setup() {
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = new TezDAGID(appAttemptId.getApplicationId(), 1);
    dagPlan = createTestDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    appContext = mock(AppContext.class);
    DAG dag = mock(DAG.class);
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(DAGPlan.getDefaultInstance()).when(dag).getJobPlan();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(dagId).when(dag).getID();
    setupVertices();
    
    // TODO - this test logic is tightly linked to impl DAGImpl code.
    edges = new HashMap<String, Edge>();
    for (EdgePlan edgePlan : dagPlan.getEdgeList()) {
      EdgeProperty edgeProperty = DagTypeConverters
          .createEdgePropertyMapFromDAGPlan(edgePlan);
      edges.put(edgePlan.getId(), new Edge(edgeProperty, dispatcher.getEventHandler()));
    }

    parseVertexEdges();
    taskEventDispatcher = new TaskEventDispatcher();
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    vertexEventDispatcher = new VertexEventDispatcher();
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dagEventDispatcher = new DagEventDispatcher();
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(HistoryEventType.class, new HistoryHandler());
    dispatcher.init(conf);
    dispatcher.start();

  }

  @After
  public void teardown() {
    dispatcher.await();
    dispatcher.stop();
    dispatcher = null;
    vertexEventDispatcher = null;
    dagEventDispatcher = null;
    dagPlan = null;
    this.vertices = null;
    this.edges = null;
    this.vertexIdMap = null;
  }

  private void initAllVertices() {
    for (int i = 1; i <= 6; ++i) {
      VertexImpl v = vertices.get("vertex" + i);
      initVertex(v);
    }
  }


  @SuppressWarnings("unchecked")
  private void initVertex(VertexImpl v) {
    Assert.assertEquals(VertexState.NEW, v.getState());
    dispatcher.getEventHandler().handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v.getState());
  }

  private void startVertex(VertexImpl v) {
    startVertex(v, true);
  }

  @SuppressWarnings("unchecked")
  private void killVertex(VertexImpl v) {
    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
    Assert.assertEquals(v.getTerminationCause(), VertexTerminationCause.DAG_KILL);
  }

  @SuppressWarnings("unchecked")
  private void startVertex(VertexImpl v,
      boolean checkRunningState) {
    Assert.assertEquals(VertexState.INITED, v.getState());
    dispatcher.getEventHandler().handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_START));
    dispatcher.await();
    if (checkRunningState) {
      Assert.assertEquals(VertexState.RUNNING, v.getState());
    }
  }

  @Test(timeout = 5000)
  public void testVertexInit() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);

    VertexImpl v3 = vertices.get("vertex3");
    initVertex(v3);

    Assert.assertEquals("x3.y3", v3.getProcessorName());
    Assert.assertEquals("foo", v3.getJavaOpts());

    Assert.assertEquals(2, v3.getInputSpecList(0).size());
    Assert.assertEquals(2, v3.getInputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());

    Assert.assertTrue("vertex1".equals(v3.getInputSpecList(0).get(0)
        .getSourceVertexName())
        || "vertex2".equals(v3.getInputSpecList(0).get(0)
            .getSourceVertexName()));
    Assert.assertTrue("vertex1".equals(v3.getInputSpecList(0).get(1)
        .getSourceVertexName())
        || "vertex2".equals(v3.getInputSpecList(0).get(1)
            .getSourceVertexName()));
    Assert.assertTrue("i3_v1".equals(v3.getInputSpecList(0).get(0)
        .getInputDescriptor().getClassName())
        || "i3_v2".equals(v3.getInputSpecList(0).get(0)
            .getInputDescriptor().getClassName()));
    Assert.assertTrue("i3_v1".equals(v3.getInputSpecList(0).get(1)
        .getInputDescriptor().getClassName())
        || "i3_v2".equals(v3.getInputSpecList(0).get(1)
            .getInputDescriptor().getClassName()));

    Assert.assertTrue("vertex4".equals(v3.getOutputSpecList(0).get(0)
        .getDestinationVertexName())
        || "vertex5".equals(v3.getOutputSpecList(0).get(0)
            .getDestinationVertexName()));
    Assert.assertTrue("vertex4".equals(v3.getOutputSpecList(0).get(1)
        .getDestinationVertexName())
        || "vertex5".equals(v3.getOutputSpecList(0).get(1)
            .getDestinationVertexName()));
    Assert.assertTrue("o3_v4".equals(v3.getOutputSpecList(0).get(0)
        .getOutputDescriptor().getClassName())
        || "o3_v5".equals(v3.getOutputSpecList(0).get(0)
            .getOutputDescriptor().getClassName()));
    Assert.assertTrue("o3_v4".equals(v3.getOutputSpecList(0).get(1)
        .getOutputDescriptor().getClassName())
        || "o3_v5".equals(v3.getOutputSpecList(0).get(1)
            .getOutputDescriptor().getClassName()));
  }

  @Test(timeout = 5000)
  public void testVertexStart() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);
  }

  @Test//(timeout = 5000)
  public void testVertexSetParallelism() {
    VertexImpl v3 = vertices.get("vertex3");
    initVertex(v3);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());
    TezTaskID firstTask = tasks.keySet().iterator().next();

    startVertex(v3);

    Vertex v1 = vertices.get("vertex1");
    EdgeManager mockEdgeManager = mock(EdgeManager.class);
    Map<Vertex, EdgeManager> edgeManager = Collections.singletonMap(
       v1, mockEdgeManager);
    v3.setParallelism(1, edgeManager);
    Assert.assertEquals(1, v3.getTotalTasks());
    Assert.assertEquals(1, tasks.size());
    // the last one is removed
    Assert.assertTrue(tasks.keySet().iterator().next().equals(firstTask));

    Assert.assertTrue(v3.sourceVertices.get(v1).getEdgeManager() == mockEdgeManager);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testBasicVertexCompletion() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(1, v.getCompletedTasks());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(2, v.getCompletedTasks());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  @Ignore // FIXME fix verteximpl for this test to work
  public void testDuplicateTaskCompletion() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
  }


  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexFailure() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.OWN_TASK_FAILURE, v.getTerminationCause());
    String diagnostics =
        StringUtils.join(",", v.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains("task failed " + t1.toString()));
  }

  @Test(timeout = 5000)
  public void testVertexKillDiagnostics() {
    VertexImpl v1 = vertices.get("vertex1");
    killVertex(v1);
    String diagnostics =
        StringUtils.join(",", v1.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in new state"));

    VertexImpl v2 = vertices.get("vertex2");
    initVertex(v2);
    killVertex(v2);
    diagnostics =
        StringUtils.join(",", v2.getDiagnostics()).toLowerCase();
    LOG.info("diagnostics v2: " + diagnostics);
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in inited state"));

    VertexImpl v3 = vertices.get("vertex3");
    VertexImpl v4 = vertices.get("vertex4");
    VertexImpl v5 = vertices.get("vertex5");
    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v3);
    initVertex(v4);
    initVertex(v5);
    initVertex(v6);

    startVertex(v3);
    killVertex(v3);
    diagnostics =
        StringUtils.join(",", v3.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill while in running state"));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexKillPending() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            new TezTaskID(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            new TezTaskID(v.getVertexId(), 1), TaskState.KILLED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testVertexKill() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            new TezTaskID(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            new TezTaskID(v.getVertexId(), 1), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testKilledTasksHandling() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.OWN_TASK_FAILURE, v.getTerminationCause());
    Assert.assertEquals(TaskState.KILLED, v.getTask(t2).getState());
  }

  @Test(timeout = 5000)
  public void testVertexCommitterInit() {
    VertexImpl v2 = vertices.get("vertex2");
    initVertex(v2);
    Assert.assertTrue(v2.getVertexOutputCommitter()
        instanceof NullVertexOutputCommitter);

    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v6);
    Assert.assertTrue(v6.getVertexOutputCommitter()
        instanceof MRVertexOutputCommitter);
  }

  @Test(timeout = 5000)
  public void testVertexSchedulerInit() {
    VertexImpl v2 = vertices.get("vertex2");
    initVertex(v2);
    Assert.assertTrue(v2.getVertexScheduler()
        instanceof ImmediateStartVertexScheduler);

    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v6);
    Assert.assertTrue(v6.getVertexScheduler()
        instanceof ShuffleVertexManager);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexTaskFailure() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");

    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.FAILED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.OWN_TASK_FAILURE, v.getTerminationCause());
    Assert.assertEquals(0, committer.commitCounter);
    Assert.assertEquals(1, committer.abortCounter);
  }

  @Test(timeout = 5000)
  public void testSourceVertexStartHandling() {
    LOG.info("Testing testSourceVertexStartHandling");
    initAllVertices();

    VertexImpl v4 = vertices.get("vertex4");
    VertexImpl v5 = vertices.get("vertex5");
    VertexImpl v6 = vertices.get("vertex6");

    startVertex(v4);
    startVertex(v5);
    dispatcher.await();
    LOG.info("Verifying v6 state " + v6.getState());
    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(1, v6.getDistanceFromRoot());
  }

  @Test(timeout = 5000)
  public void testCounters() {
    // FIXME need to test counters at vertex level
  }

  @Test(timeout = 5000)
  public void testDiagnostics() {
    // FIXME need to test diagnostics in various cases
  }

  @Test(timeout = 5000)
  public void testTaskAttemptCompletionEvents() {
    // FIXME need to test handling of task attempt events
  }

  @Test(timeout = 5000)
  public void testSourceTaskAttemptCompletionEvents() {
    LOG.info("Testing testSourceTaskAttemptCompletionEvents");
    initAllVertices();

    VertexImpl v4 = vertices.get("vertex4");
    VertexImpl v5 = vertices.get("vertex5");
    VertexImpl v6 = vertices.get("vertex6");

    startVertex(v4);
    startVertex(v5);
    dispatcher.await();
    LOG.info("Verifying v6 state " + v6.getState());
    Assert.assertEquals(VertexState.RUNNING, v6.getState());

    TezTaskID t1_v4 = new TezTaskID(v4.getVertexId(), 0);
    TezTaskID t2_v4 = new TezTaskID(v4.getVertexId(), 1);
    TezTaskID t1_v5 = new TezTaskID(v5.getVertexId(), 0);
    TezTaskID t2_v5 = new TezTaskID(v5.getVertexId(), 1);

    TezTaskAttemptID ta1_t1_v4 = new TezTaskAttemptID(t1_v4, 0);
    TezTaskAttemptID ta2_t1_v4 = new TezTaskAttemptID(t1_v4, 0);
    TezTaskAttemptID ta1_t2_v4 = new TezTaskAttemptID(t2_v4, 0);
    TezTaskAttemptID ta1_t1_v5 = new TezTaskAttemptID(t1_v5, 0);
    TezTaskAttemptID ta1_t2_v5 = new TezTaskAttemptID(t2_v5, 0);
    TezTaskAttemptID ta2_t2_v5 = new TezTaskAttemptID(t2_v5, 0);

    v4.handle(new VertexEventTaskAttemptCompleted(ta1_t1_v4, TaskAttemptStateInternal.FAILED));
    v4.handle(new VertexEventTaskAttemptCompleted(ta2_t1_v4, TaskAttemptStateInternal.SUCCEEDED));
    v4.handle(new VertexEventTaskAttemptCompleted(ta1_t2_v4, TaskAttemptStateInternal.SUCCEEDED));
    v5.handle(new VertexEventTaskAttemptCompleted(ta1_t1_v5, TaskAttemptStateInternal.SUCCEEDED));
    v5.handle(new VertexEventTaskAttemptCompleted(ta1_t2_v5, TaskAttemptStateInternal.FAILED));
    v5.handle(new VertexEventTaskAttemptCompleted(ta2_t2_v5, TaskAttemptStateInternal.SUCCEEDED));

    v4.handle(new VertexEventTaskCompleted(t1_v4, TaskState.SUCCEEDED));
    v4.handle(new VertexEventTaskCompleted(t2_v4, TaskState.SUCCEEDED));
    v5.handle(new VertexEventTaskCompleted(t1_v5, TaskState.SUCCEEDED));
    v5.handle(new VertexEventTaskCompleted(t2_v5, TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v4.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v5.getState());

    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(4, v6.numSuccessSourceAttemptCompletions);

  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testDAGEventGeneration() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_COMPLETED).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskReschedule() {
    // For downstream failures
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");

    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);

    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskReschedule(t1));
    // FIXME need to handle dups
    // dispatcher.getEventHandler().handle(new VertexEventTaskReschedule(t1));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(0, committer.commitCounter);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);

  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexCommit() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");

    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);

    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(0, committer.initCounter); // already done in init
    Assert.assertEquals(0, committer.setupCounter); // already done in init
  }

  @Test(timeout = 5000)
  public void testCommitterInitAndSetup() {
    // FIXME need to add a test for this
  }

  @Test(timeout = 5000)
  public void testTaskAttemptFetchFailureHandling() {
    // FIXME needs testing
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testBadCommitter() {
    initAllVertices();

    VertexImpl v = vertices.get("vertex2");

    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter(true, true);
    v.setVertexOutputCommitter(committer);

    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.COMMIT_FAILURE, v.getTerminationCause());
    Assert.assertEquals(1, committer.commitCounter);

    // FIXME need to verify whether abort needs to be called if commit fails
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(0, committer.initCounter); // already done in init
    Assert.assertEquals(0, committer.setupCounter); // already done in init
  }

  @Test(timeout = 5000)
  public void testHistoryEventGeneration() {
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testInvalidEvent() {
    VertexImpl v = vertices.get("vertex2");
    dispatcher.getEventHandler().handle(new VertexEvent(v.getVertexId(),
        VertexEventType.V_START));
    dispatcher.await();
    Assert.assertEquals(VertexState.ERROR, v.getState());
    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.INTERNAL_ERROR).intValue());
  }

  @Test
  public void testVertexWithNoTasks() {
    TezDAGID invalidDagId = new TezDAGID(
        dagId.getApplicationId(), 1000);
    DAGPlan dPlan = createInvalidDAGPlan();
    TezVertexID vId = new TezVertexID(invalidDagId, 1);
    VertexPlan vPlan = dPlan.getVertex(0);
    VertexImpl v = new VertexImpl(vId, vPlan, vPlan.getName(), conf,
        dispatcher.getEventHandler(), taskAttemptListener, fsTokens,
        clock, thh, appContext, vertexLocationHint);
    v.handle(new VertexEvent(vId, VertexEventType.V_INIT));
    Assert.assertEquals(VertexState.FAILED, v.getState());
  }

}

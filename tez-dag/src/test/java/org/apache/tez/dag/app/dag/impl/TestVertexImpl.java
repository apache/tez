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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.MRVertexOutputCommitter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeConnectionPattern;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.committer.NullVertexOutputCommitter;
import org.apache.tez.dag.api.committer.VertexContext;
import org.apache.tez.dag.api.committer.VertexOutputCommitter;
import org.apache.tez.dag.api.committer.VertexStatus.State;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent.Status;
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
  private Token<JobTokenIdentifier> jobToken;
  private Clock clock = new SystemClock();
  private TaskHeartbeatHandler thh;
  private AppContext appContext;
  private VertexLocationHint vertexLocationHint;
  private TezConfiguration conf = new TezConfiguration();
  private Map<String, EdgeProperty> edges;

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
    public void setupVertex() throws IOException {
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
    public void abortVertex(State finalState) throws IOException {
      ++abortCounter;
      if (throwErrorOnAbort) {
        throw new IOException("I can throwz exceptions in abort");
      }
    }    
  }
  
  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
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
            .setProcessorName("x3.y3")
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
            .setInputClass("i3_v1")
            .setInputVertexName("vertex1")
            .setOutputClass("o1")
            .setOutputVertexName("vertex3")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e1")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setInputClass("i3_v2")
            .setInputVertexName("vertex2")
            .setOutputClass("o2")
            .setOutputVertexName("vertex3")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e2")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setInputClass("i4_v3")
            .setInputVertexName("vertex3")
            .setOutputClass("o3_v4")
            .setOutputVertexName("vertex4")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e3")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setInputClass("i5_v3")
            .setInputVertexName("vertex3")
            .setOutputClass("o3_v5")
            .setOutputVertexName("vertex5")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e4")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setInputClass("i6_v4")
            .setInputVertexName("vertex4")
            .setOutputClass("o4")
            .setOutputVertexName("vertex6")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e5")
            .setSourceType(PlanEdgeSourceType.STABLE)
            .build()
            )
        .addEdge(
            EdgePlan.newBuilder()
            .setInputClass("i6_v5")
            .setInputVertexName("vertex5")
            .setOutputClass("o5")
            .setOutputVertexName("vertex6")
            .setConnectionPattern(PlanEdgeConnectionPattern.BIPARTITE)
            .setId("e6")
            .setSourceType(PlanEdgeSourceType.STABLE)
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
      TezVertexID vertexId = new TezVertexID(dagId, i+1);
      VertexImpl v = new VertexImpl(vertexId, vPlan, vPlan.getName(), conf,
          dispatcher.getEventHandler(), taskAttemptListener, jobToken, fsTokens,
          clock, thh, appContext, vertexLocationHint);
      vertices.put(vPlan.getName(), v);
      vertexIdMap.put(vertexId, v);
    }
  }

  private void parseVertexEdges() {
    LOG.info("Parsing edges from dag plan, edgeCount="
        + dagPlan.getEdgeCount());
    int vCnt = dagPlan.getVertexCount();
    Map<String, EdgePlan> edgePlans =
        DagTypeConverters.createEdgePlanMapFromDAGPlan(dagPlan.getEdgeList());

    for (int i = 0; i < vCnt; ++i) {
      VertexPlan vertexPlan = dagPlan.getVertex(i);
      Vertex vertex = vertices.get(vertexPlan.getName());
      Map<Vertex, EdgeProperty> inVertices =
          new HashMap<Vertex, EdgeProperty>();

      Map<Vertex, EdgeProperty> outVertices =
          new HashMap<Vertex, EdgeProperty>();
      
      for(String inEdgeId : vertexPlan.getInEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(inEdgeId);
        Vertex inVertex = this.vertices.get(edgePlan.getInputVertexName());
        EdgeProperty edgeProp = this.edges.get(inEdgeId);
        inVertices.put(inVertex, edgeProp);
      }

      for(String outEdgeId : vertexPlan.getOutEdgeIdList()){
        EdgePlan edgePlan = edgePlans.get(outEdgeId);
        Vertex outVertex = this.vertices.get(edgePlan.getOutputVertexName());
        EdgeProperty edgeProp = this.edges.get(outEdgeId);
        outVertices.put(outVertex, edgeProp);
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
    appAttemptId = BuilderUtils.newApplicationAttemptId(
        BuilderUtils.newApplicationId(100, 1), 1);
    dagId = new TezDAGID(appAttemptId.getApplicationId(), 1);
    dagPlan = createTestDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    jobToken = new Token<JobTokenIdentifier>();
    appContext = mock(AppContext.class);
    DAG dag = mock(DAG.class);
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(dag).when(appContext).getDAG();
    doReturn(dagId).when(appContext).getDAGID();
    doReturn(dagId).when(dag).getID();
    setupVertices();
    edges = DagTypeConverters.createEdgePropertyMapFromDAGPlan(
        dagPlan.getEdgeList());
    parseVertexEdges();
    vertexEventDispatcher = new VertexEventDispatcher();
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dagEventDispatcher = new DagEventDispatcher();
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(HistoryEventType.class,
        new HistoryHandler());
    dispatcher.init(conf);
    dispatcher.start();
  }

  @After
  public void teardown() {
    dagPlan = null;
    this.vertices = null;
    this.edges = null;
    dispatcher.await();
    dispatcher.stop();
  }

  private void initVertex(VertexImpl v) {
    Assert.assertEquals(VertexState.NEW, v.getState());
    v.handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v.getState());
  }

  private void startVertex(VertexImpl v) {
    startVertex(v, true);
  }

  private void killVertex(VertexImpl v, boolean checkKillWait) {
    v.handle(new VertexEvent(v.getVertexId(), VertexEventType.V_KILL));
    dispatcher.await();
    if (checkKillWait) {
      Assert.assertEquals(VertexState.KILL_WAIT, v.getState());
    } else {
      Assert.assertEquals(VertexState.KILLED, v.getState());      
    }
  }
  
  private void startVertex(VertexImpl v,
      boolean checkRunningState) {
    Assert.assertEquals(VertexState.INITED, v.getState());
    v.handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_START));
    dispatcher.await();
    if (checkRunningState) {
      Assert.assertEquals(VertexState.RUNNING, v.getState());
    }
  }

  @Test
  public void testVertexInit() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);

    VertexImpl v3 = vertices.get("vertex3");
    initVertex(v3);
    
    Assert.assertEquals("x3.y3", v3.getProcessorName());
    Assert.assertEquals("foo", v3.getJavaOpts());
    
    Assert.assertEquals(2, v3.getInputSpecList().size());
    Assert.assertEquals(2, v3.getInputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());

    Assert.assertTrue("vertex1".equals(v3.getInputSpecList().get(0)
        .getVertexName())
        || "vertex2".equals(v3.getInputSpecList().get(0)
            .getVertexName()));
    Assert.assertTrue("vertex1".equals(v3.getInputSpecList().get(1)
        .getVertexName())
        || "vertex2".equals(v3.getInputSpecList().get(1)
            .getVertexName()));
    Assert.assertTrue("i3_v1".equals(v3.getInputSpecList().get(0)
        .getInputClassName())
        || "i3_v2".equals(v3.getInputSpecList().get(0)
            .getInputClassName()));
    Assert.assertTrue("i3_v1".equals(v3.getInputSpecList().get(1)
        .getInputClassName())
        || "i3_v2".equals(v3.getInputSpecList().get(1)
            .getInputClassName()));
    
    Assert.assertTrue("vertex4".equals(v3.getOutputSpecList().get(0)
        .getVertexName())
        || "vertex5".equals(v3.getOutputSpecList().get(0)
            .getVertexName()));
    Assert.assertTrue("vertex4".equals(v3.getOutputSpecList().get(1)
        .getVertexName())
        || "vertex5".equals(v3.getOutputSpecList().get(1)
            .getVertexName()));
    Assert.assertTrue("o3_v4".equals(v3.getOutputSpecList().get(0)
        .getOutputClassName())
        || "o3_v5".equals(v3.getOutputSpecList().get(0)
            .getOutputClassName()));
    Assert.assertTrue("o3_v4".equals(v3.getOutputSpecList().get(1)
        .getOutputClassName())
        || "o3_v5".equals(v3.getOutputSpecList().get(1)
            .getOutputClassName()));
  }

  @Test
  public void testVertexStart() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);
  }

  @Test
  public void testBasicVertexCompletion() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(1, v.getCompletedTasks());

    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(2, v.getCompletedTasks());
  }

  @Test
  @Ignore // FIXME fix verteximpl for this test to work
  public void testDuplicateTaskCompletion() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    
    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
  }


  @Test
  public void testVertexFailure() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    String diagnostics =
        StringUtils.join(",", v.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains("task failed " + t1.toString()));
  }
  
  @Test
  public void testVertexWithNoTasks() {
    // FIXME a vertex with no tasks should not be allowed
    VertexImpl v = vertices.get("vertex1");
    initVertex(v);
    startVertex(v, false);
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
  }

  @Test
  public void testVertexKill() {
    VertexImpl v1 = vertices.get("vertex1");
    killVertex(v1, false);
    String diagnostics =
        StringUtils.join(",", v1.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in new state"));
    
    VertexImpl v2 = vertices.get("vertex2");
    initVertex(v2);
    killVertex(v2, false);
    diagnostics =
        StringUtils.join(",", v2.getDiagnostics()).toLowerCase();
    LOG.info("diagnostics v2: " + diagnostics);
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in inited state"));

    VertexImpl v3 = vertices.get("vertex3");
    initVertex(v3);
    startVertex(v3);
    killVertex(v3, true);
    diagnostics =
        StringUtils.join(",", v3.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill while in running state"));
  }

  @Test
  public void testKilledTasksHandling() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.KILLED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    v.handle(new VertexEventTaskCompleted(t2, TaskState.KILLED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
  }

  @Test
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

  @Test
  public void testVertexSchedulerInit() {
    VertexImpl v2 = vertices.get("vertex2");
    initVertex(v2);
    Assert.assertTrue(v2.getVertexScheduler()
        instanceof ImmediateStartVertexScheduler);
    
    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v6);
    Assert.assertTrue(v6.getVertexScheduler()
        instanceof BipartiteSlowStartVertexScheduler);
  }
  
  @Test
  public void testVertexTaskFailure() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    
    v.handle(new VertexEventTaskCompleted(t2, TaskState.FAILED));
    v.handle(new VertexEventTaskCompleted(t2, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(0, committer.commitCounter);
    Assert.assertEquals(1, committer.abortCounter);
  }

  @Test
  public void testSourceVertexStartHandling() {
    VertexImpl v4 = vertices.get("vertex4");
    initVertex(v4);
    VertexImpl v5 = vertices.get("vertex5");
    initVertex(v5);
    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v6);
    Assert.assertEquals(VertexState.INITED, v6.getState());

    startVertex(v4);
    startVertex(v5);
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(1, v6.getDistanceFromRoot());
  }

  @Test
  public void testCounters() {
    // FIXME need to test counters at vertex level
  }

  @Test
  public void testDiagnostics() {
    // FIXME need to test diagnostics in various cases
  }
  
  @Test
  public void testTaskAttemptCompletionEvents() {    
    // FIXME need to test handling of task attempt events
  }

  @Test
  public void testSourceTaskAttemptCompletionEvents() {
    VertexImpl v4 = vertices.get("vertex4");
    initVertex(v4);
    VertexImpl v5 = vertices.get("vertex5");
    initVertex(v5);
    VertexImpl v6 = vertices.get("vertex6");
    initVertex(v6);

    startVertex(v4);
    startVertex(v5);
    dispatcher.await();
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

    TezDependentTaskCompletionEvent cEvt1 =
        new TezDependentTaskCompletionEvent(1, ta1_t1_v4,
            Status.FAILED, "", 3);
    TezDependentTaskCompletionEvent cEvt2 =
        new TezDependentTaskCompletionEvent(2, ta2_t1_v4,
            Status.SUCCEEDED, "", 4);
    TezDependentTaskCompletionEvent cEvt3 =
        new TezDependentTaskCompletionEvent(2, ta1_t2_v4,
            Status.SUCCEEDED, "", 5);
    TezDependentTaskCompletionEvent cEvt4 =
        new TezDependentTaskCompletionEvent(2, ta1_t1_v5,
            Status.SUCCEEDED, "", 5);
    TezDependentTaskCompletionEvent cEvt5 =
        new TezDependentTaskCompletionEvent(1, ta1_t2_v5,
            Status.FAILED, "", 3);
    TezDependentTaskCompletionEvent cEvt6 =
        new TezDependentTaskCompletionEvent(2, ta2_t2_v5,
            Status.SUCCEEDED, "", 4);

    v4.handle(new VertexEventTaskAttemptCompleted(cEvt1));
    v4.handle(new VertexEventTaskAttemptCompleted(cEvt2));
    v4.handle(new VertexEventTaskAttemptCompleted(cEvt3));
    v5.handle(new VertexEventTaskAttemptCompleted(cEvt4));
    v5.handle(new VertexEventTaskAttemptCompleted(cEvt5));
    v5.handle(new VertexEventTaskAttemptCompleted(cEvt6));    

    v4.handle(new VertexEventTaskCompleted(t1_v4, TaskState.SUCCEEDED));
    v4.handle(new VertexEventTaskCompleted(t2_v4, TaskState.SUCCEEDED));
    v5.handle(new VertexEventTaskCompleted(t1_v5, TaskState.SUCCEEDED));
    v5.handle(new VertexEventTaskCompleted(t2_v5, TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v4.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v5.getState());

    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(4, v6.successSourceAttemptCompletionEventNoMap.size());
    Assert.assertEquals(6, v6.getTaskAttemptCompletionEvents(0, 100).length);
  }

  @Test
  public void testDAGEventGeneration() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());

  }

  @Test
  public void testTaskReschedule() {
    // For downstream failures
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);
    
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    v.handle(new VertexEventTaskReschedule(t1));
    // FIXME need to handle dups
    // v.handle(new VertexEventTaskReschedule(t1));
    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());    
    Assert.assertEquals(0, committer.commitCounter);
    
    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());    
    Assert.assertEquals(1, committer.commitCounter);
    
  }

  @Test
  public void testVertexCommit() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter();
    v.setVertexOutputCommitter(committer);
    
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());    
    Assert.assertEquals(1, committer.commitCounter);
    
    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());    
    Assert.assertEquals(1, committer.commitCounter);
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(0, committer.initCounter); // already done in init
    Assert.assertEquals(0, committer.setupCounter); // already done in init
  }
  
  @Test
  public void testCommitterInitAndSetup() {
    // FIXME need to add a test for this
  }
  
  @Test
  public void testTaskAttemptFetchFailureHandling() {
    // FIXME needs testing
  }

  @Test
  public void testBadCommitter() {
    VertexImpl v = vertices.get("vertex2");
    initVertex(v);
    CountingVertexOutputCommitter committer =
        new CountingVertexOutputCommitter(true, true);
    v.setVertexOutputCommitter(committer);
    
    startVertex(v);

    TezTaskID t1 = new TezTaskID(v.getVertexId(), 0);
    TezTaskID t2 = new TezTaskID(v.getVertexId(), 1);

    v.handle(new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    v.handle(new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());    
    Assert.assertEquals(1, committer.commitCounter);
    
    // FIXME need to verify whether abort needs to be called if commit fails
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(0, committer.initCounter); // already done in init
    Assert.assertEquals(0, committer.setupCounter); // already done in init  
  }
  
}

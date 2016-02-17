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
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.CallableEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventStartDag;
import org.apache.tez.dag.app.dag.event.DAGEventTerminateDag;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl.OutputKey;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.*;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 
 * The test case of commit here are different from that in TestDAGImpl &
 * TestVertexImpl in that the commits here are running in separated thread. So
 * should need to pay some special attention.
 * 
 * 2 kinds of commit 
 * <li> test XXX_OnDAGSuccess means TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS is true
 * <li> test XXX_OnVertexSuccess means TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS is false
 * 
 */
public class TestCommit {

  private static final Log LOG = LogFactory.getLog(TestCommit.class);
  private TezDAGID dagId;
  private static Configuration conf = new Configuration();
  private DrainDispatcher dispatcher;
  private Credentials fsTokens;
  private AppContext appContext;
  private ACLManager aclManager;
  private ApplicationAttemptId appAttemptId;
  private DAGImpl dag;
  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private DagEventDispatcher dagEventDispatcher;
  private TaskCommunicatorManagerInterface taskCommunicatorManagerInterface;
  private TaskHeartbeatHandler thh;
  private Clock clock = new SystemClock();
  private DAGFinishEventHandler dagFinishEventHandler;
  private MockHistoryEventHandler historyEventHandler;
  private TaskAttemptEventDispatcher taskAttemptEventDispatcher;

  private ExecutorService rawExecutor;
  private ListeningExecutorService execService;

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
      dag.handle(event);
    }
  }

  private class VertexEventDispatcher implements EventHandler<VertexEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      Vertex vertex = dag.getVertex(event.getVertexId());
      ((EventHandler<VertexEvent>) vertex).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Vertex vertex = dag.getVertex(event.getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskID());
      ((EventHandler<TaskEvent>) task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher implements
      EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      // Ignore
    }
  }

  private class DAGFinishEventHandler implements
      EventHandler<DAGAppMasterEventDAGFinished> {
    @Override
    public void handle(DAGAppMasterEventDAGFinished event) {
    }
  }

  public static class CountingOutputCommitter extends OutputCommitter {

    public volatile int initCounter = 0;
    public volatile int setupCounter = 0;
    public volatile int commitCounter = 0;
    public volatile int abortCounter = 0;
    private boolean throwError;
    private volatile boolean blockCommit;

    public CountingOutputCommitter(OutputCommitterContext context) {
      super(context);
      this.throwError = false;
    }

    @Override
    public void initialize() throws IOException {
      if (getContext().getUserPayload() != null
          && getContext().getUserPayload().hasPayload()) {
        CountingOutputCommitterConfig conf = new CountingOutputCommitterConfig(
            getContext().getUserPayload());
        this.throwError = conf.throwError;
        this.blockCommit = conf.blockCommit;
      }
      ++initCounter;
    }

    @Override
    public void setupOutput() throws IOException {
      ++setupCounter;
    }

    @Override
    public void commitOutput() throws IOException {
      ++commitCounter;
      while (blockCommit) {
        try {
          Thread.sleep(100);
          LOG.info("committing output:" + getContext().getOutputName());
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      if (throwError) {
        throw new RuntimeException("I can throwz exceptions in commit");
      }
    }

    public void unblockCommit() {
      blockCommit = false;
    }

    @Override
    public void abortOutput(VertexStatus.State finalState) throws IOException {
      ++abortCounter;
    }

    public static class CountingOutputCommitterConfig implements Writable {

      boolean throwError = false;
      boolean blockCommit = false;

      public CountingOutputCommitterConfig() {
      }

      public CountingOutputCommitterConfig(boolean throwError,
          boolean blockCommit) {
        this.throwError = throwError;
        this.blockCommit = blockCommit;
      }

      public CountingOutputCommitterConfig(UserPayload payload)
          throws IOException {
        DataInputByteBuffer in = new DataInputByteBuffer();
        in.reset(payload.getPayload());
        this.readFields(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        out.writeBoolean(throwError);
        out.writeBoolean(blockCommit);
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        throwError = in.readBoolean();
        blockCommit = in.readBoolean();
      }

      public byte[] toUserPayload() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bos);
        write(out);
        return bos.toByteArray();
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void setupDAG(DAGPlan dagPlan) {
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    Assert.assertNotNull(dagId);
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    appContext = mock(AppContext.class);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    rawExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("App Shared Pool - " + "#%d").build());
    execService = MoreExecutors.listeningDecorator(rawExecutor);

    doReturn(execService).when(appContext).getExecService();
    historyEventHandler = new MockHistoryEventHandler(appContext);
    aclManager = new ACLManager("amUser");
    doReturn(conf).when(appContext).getAMConf();
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(appContext)
        .getApplicationID();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(aclManager).when(appContext).getAMACLManager();
    dag = new DAGImpl(dagId, conf, dagPlan, dispatcher.getEventHandler(),
        taskCommunicatorManagerInterface, fsTokens, clock, "user", thh, appContext);
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(dispatcher.getEventHandler()).when(appContext).getEventHandler();
    ClusterInfo clusterInfo = new ClusterInfo(Resource.newInstance(8192,10));
    doReturn(clusterInfo).when(appContext).getClusterInfo();
    dispatcher.register(CallableEventType.class, new CallableEventDispatcher());
    taskEventDispatcher = new TaskEventDispatcher();
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    taskAttemptEventDispatcher = new TaskAttemptEventDispatcher();
    dispatcher.register(TaskAttemptEventType.class, taskAttemptEventDispatcher);
    vertexEventDispatcher = new VertexEventDispatcher();
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dagEventDispatcher = new DagEventDispatcher();
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dagFinishEventHandler = new DAGFinishEventHandler();
    dispatcher.register(DAGAppMasterEventType.class, dagFinishEventHandler);
    dispatcher.init(conf);
    dispatcher.start();
  }

  @After
  public void teardown() {
    if (dispatcher != null) {
      dispatcher.await();
      dispatcher.stop();
    }
    if (execService != null) {
      execService.shutdownNow();
    }
  }

  private void waitUntil(DAGImpl dag, DAGState state) {
    while (dag.getState() != state) {
      LOG.info("Wait for dag go to state:" + state);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void waitUntil(VertexImpl vertex, VertexState state) {
    while (vertex.getState() != state) {
      LOG.info("Wait for vertex " + vertex.getLogIdentifier() + " go to state:"
          + state);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void waitForCommitCompleted(VertexImpl vertex, String outputName) {
    while (vertex.commitFutures.containsKey(outputName)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Wait for vertex commit " + outputName + " to complete");
    }
  }

  private void waitForCommitCompleted(DAGImpl vertex, OutputKey outputKey) {
    while (vertex.commitFutures.containsKey(outputKey)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Wait for dag commit " + outputKey + " to complete");
    }
  }

  // v1->v3
  // v2->v3
  // vertex_group (v1, v2)
  private DAGPlan createDAGPlan(boolean vertexGroupCommitSucceeded,
      boolean v3CommitSucceeded) throws Exception {
    LOG.info("Setting up group dag plan");
    int dummyTaskCount = 1;
    Resource dummyTaskResource = Resource.newInstance(1, 1);
    org.apache.tez.dag.api.Vertex v1 = org.apache.tez.dag.api.Vertex.create(
        "vertex1", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);
    org.apache.tez.dag.api.Vertex v2 = org.apache.tez.dag.api.Vertex.create(
        "vertex2", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);
    org.apache.tez.dag.api.Vertex v3 = org.apache.tez.dag.api.Vertex.create(
        "vertex3", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);

    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    OutputCommitterDescriptor ocd1 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !vertexGroupCommitSucceeded, true).toUserPayload())));
    OutputCommitterDescriptor ocd2 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !v3CommitSucceeded, true).toUserPayload())));

    org.apache.tez.dag.api.VertexGroup uv12 = dag.createVertexGroup(groupName1,
        v1, v2);
    OutputDescriptor outDesc = OutputDescriptor.create("output.class");
    uv12.addDataSink("v12Out", DataSinkDescriptor.create(outDesc, ocd1, null));
    v3.addDataSink("v3Out", DataSinkDescriptor.create(outDesc, ocd2, null));

    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("dummy output class"),
        InputDescriptor.create("dummy input class")), InputDescriptor
        .create("merge.class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    return dag.createDag(conf, null, null, null, true);
  }

  // v1->v3
  // v2->v3
  // vertex_group (v1, v2) has 2 shared outputs
  private DAGPlan createDAGPlanWith2VertexGroupOutputs(boolean vertexGroupCommitSucceeded1,
    boolean vertexGroupCommitSucceeded2, boolean v3CommitSucceeded) throws Exception {
    LOG.info("Setting up group dag plan");
    int dummyTaskCount = 1;
    Resource dummyTaskResource = Resource.newInstance(1, 1);
    org.apache.tez.dag.api.Vertex v1 = org.apache.tez.dag.api.Vertex.create(
            "vertex1", ProcessorDescriptor.create("Processor"), dummyTaskCount,
            dummyTaskResource);
    org.apache.tez.dag.api.Vertex v2 = org.apache.tez.dag.api.Vertex.create(
        "vertex2", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);
    org.apache.tez.dag.api.Vertex v3 = org.apache.tez.dag.api.Vertex.create(
        "vertex3", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);

    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    OutputCommitterDescriptor ocd1 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !vertexGroupCommitSucceeded1, true).toUserPayload())));
    OutputCommitterDescriptor ocd2 = OutputCommitterDescriptor.create(
    CountingOutputCommitter.class.getName()).setUserPayload(
    UserPayload.create(ByteBuffer
        .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !vertexGroupCommitSucceeded2, true).toUserPayload())));
    OutputCommitterDescriptor ocd3 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !v3CommitSucceeded, true).toUserPayload())));

    org.apache.tez.dag.api.VertexGroup uv12 = dag.createVertexGroup(groupName1,
        v1, v2);
    OutputDescriptor outDesc = OutputDescriptor.create("output.class");
    uv12.addDataSink("v12Out1", DataSinkDescriptor.create(outDesc, ocd1, null));
    uv12.addDataSink("v12Out2", DataSinkDescriptor.create(outDesc, ocd2, null));
    v3.addDataSink("v3Out", DataSinkDescriptor.create(outDesc, ocd3, null));

    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("dummy output class"),
        InputDescriptor.create("dummy input class")), InputDescriptor
        .create("merge.class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    return dag.createDag(conf, null, null, null, true);
  }

  private DAGPlan createDAGPlan_SingleVertexWith2Committer(
      boolean commit1Succeed, boolean commit2Succeed) throws IOException {
    return createDAGPlan_SingleVertexWith2Committer(commit1Succeed, commit2Succeed, false);
  }

  // used for route event error in VM
  private DAGPlan createDAGPlan_SingleVertexWith2Committer
    (boolean commit1Succeed, boolean commit2Succeed, boolean customVM) throws IOException {
    LOG.info("Setting up group dag plan");
    int dummyTaskCount = 1;
    Resource dummyTaskResource = Resource.newInstance(1, 1);
    org.apache.tez.dag.api.Vertex v1 = org.apache.tez.dag.api.Vertex.create(
        "vertex1", ProcessorDescriptor.create("Processor"), dummyTaskCount,
        dummyTaskResource);
    if (customVM) {
      v1.setVertexManagerPlugin(
          VertexManagerPluginDescriptor.create(
              FailOnVMEventReceivedlVertexManager.class.getName()));
    }
    OutputCommitterDescriptor ocd1 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !commit1Succeed, true).toUserPayload())));
    OutputCommitterDescriptor ocd2 = OutputCommitterDescriptor.create(
        CountingOutputCommitter.class.getName()).setUserPayload(
        UserPayload.create(ByteBuffer
            .wrap(new CountingOutputCommitter.CountingOutputCommitterConfig(
                !commit2Succeed, true).toUserPayload())));

    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    OutputDescriptor outDesc = OutputDescriptor.create("output.class");
    v1.addDataSink("v1Out_1", DataSinkDescriptor.create(outDesc, ocd1, null));
    v1.addDataSink("v1Out_2", DataSinkDescriptor.create(outDesc, ocd2, null));
    return dag.createDag(conf, null, null, null, true);
  }

  private void initDAG(DAGImpl dag) {
    dag.handle(new DAGEvent(dag.getID(), DAGEventType.DAG_INIT));
    Assert.assertEquals(DAGState.INITED, dag.getState());
  }

  @SuppressWarnings("unchecked")
  private void startDAG(DAGImpl impl) {
    dispatcher.getEventHandler().handle(
        new DAGEventStartDag(impl.getID(), null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, impl.getState());
  }

  @Test(timeout = 5000)
  public void testVertexCommit_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertNull(v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    Assert.assertEquals(0, v1OutputCommitter_1.commitCounter);
    Assert.assertEquals(0, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    Assert.assertEquals(0, v1OutputCommitter_2.commitCounter);
    Assert.assertEquals(0, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testVertexCommit_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    v1OutputCommitter_1.unblockCommit();
    waitForCommitCompleted(v1, "v1Out_1");
    // still in COMMITTING due to another pending commit
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());

    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    v1OutputCommitter_2.unblockCommit();
    waitUntil(v1, VertexState.SUCCEEDED);
    Assert.assertNull(v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.commitCounter);
    Assert.assertEquals(0, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.commitCounter);
    Assert.assertEquals(0, v1OutputCommitter_2.abortCounter);
  }

  // the first commit fail which cause the second commit abort
  @Test(timeout = 5000)
  public void testVertexCommitFail1_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(false, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    v1OutputCommitter_1.unblockCommit();
    waitUntil(v1, VertexState.FAILED);

    Assert.assertEquals(VertexTerminationCause.COMMIT_FAILURE,
        v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");

    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.commitCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    // can't verify the commitCounter because v1OutputCommitter_2 may not be started
    Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  // the first commit succeed while the second fails
  @Test(timeout = 5000)
  public void testVertexCommitFail2_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, false));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    v1OutputCommitter_1.unblockCommit();
    waitForCommitCompleted(v1, "v1Out_1");
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());

    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    v1OutputCommitter_2.unblockCommit();
    waitUntil(v1, VertexState.FAILED);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    Assert.assertEquals(VertexTerminationCause.COMMIT_FAILURE,
        v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.commitCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.commitCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testVertexKilledWhileCommitting() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    // kill dag which will trigger the vertex killed event
    dag.handle(new DAGEventTerminateDag(dag.getID(), DAGTerminationCause.DAG_KILL, null));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    Assert.assertEquals(VertexTerminationCause.DAG_TERMINATED,
        v1.getTerminationCause());
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert
        .assertEquals(DAGTerminationCause.DAG_KILL, dag.getTerminationCause());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testVertexRescheduleWhileCommitting() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    // reschedule task
    v1.handle(new VertexEventTaskReschedule(TezTaskID.getInstance(
        v1.getVertexId(), 2)));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.VERTEX_RERUN_IN_COMMITTING,
        v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.VERTEX_FAILURE,
        dag.getTerminationCause());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testVertexRouteEventErrorWhileCommitting() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    // reschedule task
    VertexManagerEvent vmEvent = VertexManagerEvent.create("vertex1", ByteBuffer.wrap(new byte[0]));
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(v1.getVertexId(), 0), 0);
    TezEvent tezEvent = new TezEvent(vmEvent,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", 
            null, taId));
    v1.handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    waitUntil(dag, DAGState.FAILED);

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE,
        v1.getTerminationCause());
    Assert.assertTrue(v1.commitFutures.isEmpty());
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.VERTEX_FAILURE,
        dag.getTerminationCause());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testVertexInternalErrorWhileCommiting() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan_SingleVertexWith2Committer(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v1.getState());
    // internal error
    v1.handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INTERNAL_ERROR));
    dispatcher.await();
    Assert.assertEquals(VertexState.ERROR, v1.getState());
    Assert.assertEquals(VertexTerminationCause.INTERNAL_ERROR,
        v1.getTerminationCause());
    Assert.assertEquals(DAGState.ERROR, dag.getState());
    Assert.assertEquals(DAGTerminationCause.INTERNAL_ERROR,
        dag.getTerminationCause());
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);

    CountingOutputCommitter v1OutputCommitter_1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_1");
    CountingOutputCommitter v1OutputCommitter_2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v1Out_2");
    Assert.assertEquals(1, v1OutputCommitter_1.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_1.setupCounter);
    // commit may not have started, so can't verify commitCounter
    // TODO abort it when internal error happens TEZ-2250
    // Assert.assertEquals(1, v1OutputCommitter_1.abortCounter);

    Assert.assertEquals(1, v1OutputCommitter_2.initCounter);
    Assert.assertEquals(1, v1OutputCommitter_2.setupCounter);
    // commit may not have started, so can't verify commitCounter
    // TODO abort it when internal error happens TEZ-2250
    // Assert.assertEquals(1, v1OutputCommitter_2.abortCounter);
  }

  @Test(timeout = 5000)
  public void testDAGCommitSucceeded_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    // still in COMMITTING due to another pending commit
    waitUntil(dag, DAGState.COMMITTING);
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.SUCCEEDED);
    Assert.assertTrue(dag.commitFutures.isEmpty());
    Assert.assertNull(dag.getTerminationCause());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v3", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v3", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(0, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(0, v3OutputCommitter.abortCounter);
  }

  // first commit(v12Out) succeed and then the second commit(v3Out) fail
  @Test(timeout = 5000)
  public void testDAGCommitFail1_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, false));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    waitForCommitCompleted(dag, new OutputKey("v12Out", "uv12", true));
    // still in COMMITTING due to another pending commit
    Assert.assertEquals(DAGState.COMMITTING, dag.getState());
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v3.getState());
    Assert.assertEquals(DAGTerminationCause.COMMIT_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  // the first commit(v12Out) fail
  @Test(timeout = 5000)
  public void testDAGCommitFail2_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(false, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v12OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v3.getState());

    Assert.assertEquals(DAGTerminationCause.COMMIT_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  // commit of v3Out complete first then commit of v12Out complete
  @Test(timeout = 5000)
  public void testDAGCommitSucceeded1_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(v3, VertexState.SUCCEEDED);
    // dag go to COMMITTING due to the pending vertex group commit of v1,v2
    waitUntil(dag, DAGState.COMMITTING);
    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.SUCCEEDED);
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(0, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(0, v3OutputCommitter.abortCounter);
  }

  // commit of v12Out complete first then commit of v3Out
  @Test(timeout = 5000)
  public void testDAGCommitSucceeded2_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    // ugly (wait for commit event sent out)
    Thread.sleep(500);
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.SUCCEEDED);
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(0, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(0, v3OutputCommitter.abortCounter);
  }

  // test DAGCommitSucceeded when vertex group has multiple shared outputs
  @Test(timeout = 5000)
  public void testDAGCommitSucceeded3_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlanWith2VertexGroupOutputs(true, true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v12OutputCommitter1 = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out1");
    v12OutputCommitter1.unblockCommit();
    CountingOutputCommitter v12OutputCommitter2 = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out2");
    v12OutputCommitter2.unblockCommit();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.SUCCEEDED);
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("v1", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("v1", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter1.initCounter);
    Assert.assertEquals(1, v12OutputCommitter1.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter1.commitCounter);
    Assert.assertEquals(0, v12OutputCommitter1.abortCounter);

    Assert.assertEquals(1, v12OutputCommitter2.initCounter);
    Assert.assertEquals(1, v12OutputCommitter2.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter2.commitCounter);
    Assert.assertEquals(0, v12OutputCommitter2.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(0, v3OutputCommitter.abortCounter);
  }

  // commit of vertex group(v1,v2) fail and commit of v3 is not completed
  @Test(timeout = 5000)
  public void testDAGCommitFail1_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(false, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);
    // v3 is killed due to the commit failure of the vertex group (v1,v2)
    Assert.assertEquals(VertexState.KILLED, v3.getState());
    Assert.assertEquals(VertexTerminationCause.OTHER_VERTEX_FAILURE,
        v3.getTerminationCause());
    Assert.assertTrue(v3.commitFutures.isEmpty());
    Assert.assertEquals(DAGTerminationCause.COMMIT_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  // commit of vertex v3 fail and commit of vertex group (v1,v2) is not completed
  @Test(timeout = 5000)
  public void testDAGCommitFail2_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, false));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);

    Assert.assertEquals(VertexState.FAILED, v3.getState());
    Assert.assertEquals(VertexTerminationCause.COMMIT_FAILURE,
        v3.getTerminationCause());
    Assert.assertTrue(v3.commitFutures.isEmpty());
    Assert.assertEquals(DAGTerminationCause.VERTEX_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  // vertex group (v1,v2) succeeded first and then commit of vertex v3 fail
  @Test (timeout = 5000)
  public void testDAGCommitFail3_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, false));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();

    waitForCommitCompleted(dag, new OutputKey("v12Out", "uv12", true));

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);

    Assert.assertEquals(VertexState.FAILED, v3.getState());
    Assert.assertEquals(VertexTerminationCause.COMMIT_FAILURE,
        v3.getTerminationCause());
    Assert.assertTrue(v3.commitFutures.isEmpty());
    Assert.assertEquals(DAGTerminationCause.VERTEX_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  // commit of vertex v3 succeeded first and then commit of vertex group(v1,v2) fail
  @Test(timeout = 5000)
  public void testDAGCommitFail4_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(false, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");

    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    waitForCommitCompleted(dag, new OutputKey("v3Out", "vertex3", true));
    waitUntil(v3, VertexState.SUCCEEDED);
    Assert.assertTrue(v3.commitFutures.isEmpty());

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    v12OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);

    Assert.assertEquals(DAGTerminationCause.COMMIT_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test (timeout = 5000)
  public void testDAGInternalErrorWhileCommiting_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    dag.handle(new DAGEvent(dag.getID(), DAGEventType.INTERNAL_ERROR));
    waitUntil(dag, DAGState.ERROR);

    Assert.assertEquals(DAGTerminationCause.INTERNAL_ERROR, dag.getTerminationCause());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    // TODO abort it when internal error happens TEZ-2250
    // Assert.assertEquals(0, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    // TODO abort it when internal error happens TEZ-2250
    // Assert.assertEquals(0, v3OutputCommitter.abortCounter);
  }


  @Test(timeout = 5000)
  public void testDAGKilledWhileCommitting1_OnDAGSuccess() throws Exception {
    _testDAGTerminatedWhileCommitting1_OnDAGSuccess(DAGTerminationCause.DAG_KILL);
  }

  @Test(timeout = 5000)
  public void testServiceErrorWhileCommitting1_OnDAGSuccess() throws Exception {
    _testDAGTerminatedWhileCommitting1_OnDAGSuccess(DAGTerminationCause.SERVICE_PLUGIN_ERROR);
  }

  // Kill dag while it is in COMMITTING in the case of
  // TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS is true
  private void _testDAGTerminatedWhileCommitting1_OnDAGSuccess(DAGTerminationCause terminationCause) throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    dag.handle(new DAGEventTerminateDag(dag.getID(), terminationCause, null));
    waitUntil(dag, terminationCause.getFinishedState());

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v3.getState());
    Assert
        .assertEquals(terminationCause, dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }


  @Test(timeout = 5000)
  public void testDAGKilledWhileCommitting1_OnVertexSuccess() throws Exception {
    _testDAGTerminatedWhileCommitting1_OnVertexSuccess(DAGTerminationCause.DAG_KILL);
  }

  @Test(timeout = 5000)
  public void testServiceErrorWhileCommitting1_OnVertexSuccess() throws Exception {
    _testDAGTerminatedWhileCommitting1_OnVertexSuccess(DAGTerminationCause.SERVICE_PLUGIN_ERROR);
  }

  // Kill dag while it is in COMMITTING in the case of
  // TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS is false
  private void _testDAGTerminatedWhileCommitting1_OnVertexSuccess(DAGTerminationCause terminationCause) throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    // dag is still in RUNNING because v3 has not completed
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v3OutputCommitter.unblockCommit();
    // dag go to COMMITTING due to the pending commit of v12Out
    waitUntil(dag, DAGState.COMMITTING);
    dag.handle(new DAGEventTerminateDag(dag.getID(), terminationCause, null));
    waitUntil(dag, terminationCause.getFinishedState());

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v3.getState());
    Assert.assertEquals(terminationCause.getFinishedState(), dag.getState());
    Assert
        .assertEquals(terminationCause, dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    Assert.assertEquals(1, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test(timeout = 5000)
  public void testDAGKilledWhileRunning_OnVertexSuccess() throws Exception {
    _testDAGKilledWhileRunning_OnVertexSuccess(DAGTerminationCause.DAG_KILL);
  }

  @Test(timeout = 5000)
  public void testServiceErrorWhileRunning_OnVertexSuccess() throws Exception {
    _testDAGKilledWhileRunning_OnVertexSuccess(DAGTerminationCause.SERVICE_PLUGIN_ERROR);
  }

  // DAG killed while dag is still in RUNNING and vertex is in COMMITTING
  private void _testDAGKilledWhileRunning_OnVertexSuccess(DAGTerminationCause terminationCause) throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    Assert.assertEquals(VertexState.COMMITTING, v3.getState());
    // dag is still in RUNNING because v3 has not completed
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    dag.handle(new DAGEventTerminateDag(dag.getID(), terminationCause, null));
    waitUntil(dag, terminationCause.getFinishedState());

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());
    Assert.assertEquals(VertexState.KILLED, v3.getState());
    Assert.assertEquals(VertexTerminationCause.DAG_TERMINATED, v3.getTerminationCause());
    Assert.assertTrue(v3.commitFutures.isEmpty());
    Assert.assertEquals(terminationCause.getFinishedState(), dag.getState());
    Assert
        .assertEquals(terminationCause, dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    // commit uv12 may not have started, so can't verify the VertexGroupCommitStartedEvent
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");

    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test(timeout = 5000)
  public void testDAGCommitVertexRerunWhileCommitting_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    TezTaskID newTaskId = TezTaskID.getInstance(v1.getVertexId(), 1);
    v1.handle(new VertexEventTaskReschedule(newTaskId));
    // dag is in TERMINATING, wait for the complete of its rescheduled tasks
    waitUntil(dag, DAGState.TERMINATING);
    waitUntil(v1, VertexState.TERMINATING);
    // reschedueled task is killed
    v1.handle(new VertexEventTaskCompleted(newTaskId, TaskState.KILLED));
    waitUntil(dag, DAGState.FAILED);
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(VertexTerminationCause.VERTEX_RERUN_IN_COMMITTING, v1.getTerminationCause());

    Assert.assertEquals(DAGTerminationCause.VERTEX_RERUN_IN_COMMITTING, dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    // VertexFinishedEvent is logged twice due to vertex-rerun
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 2);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test(timeout = 5000)
  public void testDAGCommitInternalErrorWhileCommiting_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    dag.handle(new DAGEvent(dag.getID(), DAGEventType.INTERNAL_ERROR));
    waitUntil(dag, DAGState.ERROR);

    Assert.assertEquals(DAGTerminationCause.INTERNAL_ERROR, dag.getTerminationCause());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test (timeout = 5000)
  public void testVertexGroupCommitFinishedEventFail_OnVertexSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        false);
    setupDAG(createDAGPlan(true, true));
    historyEventHandler.failVertexGroupCommitFinishedEvent = true;
    
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    v12OutputCommitter.unblockCommit();
    waitUntil(dag, DAGState.FAILED);
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 1);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.RECOVERY_FAILURE,
        dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    Assert.assertEquals(VertexState.KILLED, v3.getState());
    Assert.assertEquals(VertexTerminationCause.OTHER_VERTEX_FAILURE, v3.getTerminationCause());
    Assert.assertTrue(v3.commitFutures.isEmpty());
    
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    Assert.assertEquals(1, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit may not have started, so can't verify commitCounter
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test(timeout = 5000)
  public void testDAGCommitStartedEventFail_OnDAGSuccess() throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    historyEventHandler.failDAGCommitStartedEvent = true;
    
    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.FAILED);
    Assert.assertEquals(DAGTerminationCause.RECOVERY_FAILURE, dag.getTerminationCause());
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 0);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit has not started
    Assert.assertEquals(0, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);

    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit has not started
    Assert.assertEquals(0, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);
  }

  @Test(timeout = 5000)
  public void testCommitCanceled_OnDAGSuccess() throws Exception {
    _testCommitCanceled_OnDAGSuccess(DAGTerminationCause.DAG_KILL);
  }

  @Test(timeout = 5000)
  public void testCommitCanceled_OnDAGSuccess2() throws Exception {
    _testCommitCanceled_OnDAGSuccess(DAGTerminationCause.SERVICE_PLUGIN_ERROR);
  }

  // test commit will be canceled no matter it is started or still in the threadpool
  // ControlledThreadPoolExecutor is used for to not schedule the commits
  private void _testCommitCanceled_OnDAGSuccess(DAGTerminationCause terminationCause) throws Exception {
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS,
        true);
    setupDAG(createDAGPlan(true, true));
    // create customized ThreadPoolExecutor to wait before schedule new task
    rawExecutor = new ControlledThreadPoolExecutor(1);
    execService = MoreExecutors.listeningDecorator(rawExecutor);
    doReturn(execService).when(appContext).getExecService();

    initDAG(dag);
    startDAG(dag);
    VertexImpl v1 = (VertexImpl) dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl) dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl) dag.getVertex("vertex3");
    // need to make vertices to go to SUCCEEDED
    v1.handle(new VertexEventTaskCompleted(v1.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    v3.handle(new VertexEventTaskCompleted(v3.getTask(0).getTaskId(),
        TaskState.SUCCEEDED));
    waitUntil(dag, DAGState.COMMITTING);
    // mean the commits have been submitted to ThreadPool
    Assert.assertEquals(2, dag.commitFutures.size());

    dag.handle(new DAGEventTerminateDag(dag.getID(), terminationCause, null));
    waitUntil(dag, terminationCause.getFinishedState());
    
    Assert.assertEquals(terminationCause, dag.getTerminationCause());
    // mean the commits have been canceled
    Assert.assertTrue(dag.commitFutures.isEmpty());
    historyEventHandler.verifyVertexGroupCommitStartedEvent("uv12", 0);
    historyEventHandler.verifyVertexGroupCommitFinishedEvent("uv12", 0);
    historyEventHandler.verifyVertexCommitStartedEvent(v1.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v1.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v2.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v2.getVertexId(), 1);
    historyEventHandler.verifyVertexCommitStartedEvent(v3.getVertexId(), 0);
    historyEventHandler.verifyVertexFinishedEvent(v3.getVertexId(), 1);
    historyEventHandler.verifyDAGCommitStartedEvent(dag.getID(), 1);
    historyEventHandler.verifyDAGFinishedEvent(dag.getID(), 1);

    CountingOutputCommitter v12OutputCommitter = (CountingOutputCommitter) v1
        .getOutputCommitter("v12Out");
    CountingOutputCommitter v3OutputCommitter = (CountingOutputCommitter) v3
        .getOutputCommitter("v3Out");
    Assert.assertEquals(1, v12OutputCommitter.initCounter);
    Assert.assertEquals(1, v12OutputCommitter.setupCounter);
    // commit is not started because ControlledThreadPoolExecutor wait before schedule tasks
    Assert.assertEquals(0, v12OutputCommitter.commitCounter);
    Assert.assertEquals(1, v12OutputCommitter.abortCounter);
    
    Assert.assertEquals(1, v3OutputCommitter.initCounter);
    Assert.assertEquals(1, v3OutputCommitter.setupCounter);
    // commit is not started because ControlledThreadPoolExecutor  wait before schedule tasks
    Assert.assertEquals(0, v3OutputCommitter.commitCounter);
    Assert.assertEquals(1, v3OutputCommitter.abortCounter);

  }

  public static class FailOnVMEventReceivedlVertexManager extends ImmediateStartVertexManager {

    public FailOnVMEventReceivedlVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
      super.onVertexManagerEventReceived(vmEvent);
      throw new RuntimeException("fail vm");
    }

  }

  private static class MockHistoryEventHandler extends HistoryEventHandler {

    public boolean failVertexGroupCommitFinishedEvent = false;
    public boolean failDAGCommitStartedEvent = false;
    public List<HistoryEvent> historyEvents = new ArrayList<HistoryEvent>();
    public MockHistoryEventHandler(AppContext context) {
      super(context);
    }
    
    @Override
    public void handleCriticalEvent(DAGHistoryEvent event) throws IOException {
      if (event.getHistoryEvent().getEventType() == HistoryEventType.VERTEX_GROUP_COMMIT_FINISHED
          && failVertexGroupCommitFinishedEvent) {
        throw new IOException("fail VertexGroupCommitFinishedEvent");
      }
      if (event.getHistoryEvent().getEventType() == HistoryEventType.DAG_COMMIT_STARTED
          && failDAGCommitStartedEvent) {
        throw new IOException("fail DAGCommitStartedEvent");
      }
      historyEvents.add(event.getHistoryEvent());
    }

    public void verifyVertexGroupCommitStartedEvent(String groupName, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.VERTEX_GROUP_COMMIT_STARTED) {
          VertexGroupCommitStartedEvent startedEvent = (VertexGroupCommitStartedEvent)event;
          if (startedEvent.getVertexGroupName().equals(groupName)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }

    public void verifyVertexGroupCommitFinishedEvent(String groupName, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.VERTEX_GROUP_COMMIT_FINISHED) {
          VertexGroupCommitFinishedEvent finishedEvent = (VertexGroupCommitFinishedEvent)event;
          if (finishedEvent.getVertexGroupName().equals(groupName)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }

    public void verifyVertexCommitStartedEvent(TezVertexID vertexId, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.VERTEX_COMMIT_STARTED) {
          VertexCommitStartedEvent startedEvent = (VertexCommitStartedEvent)event;
          if (startedEvent.getVertexID().equals(vertexId)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }

    public void verifyVertexFinishedEvent(TezVertexID vertexId, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.VERTEX_FINISHED) {
          VertexFinishedEvent finishedEvent = (VertexFinishedEvent)event;
          if (finishedEvent.getVertexID().equals(vertexId)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }

    public void verifyDAGCommitStartedEvent(TezDAGID dagId, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.DAG_COMMIT_STARTED) {
          DAGCommitStartedEvent startedEvent = (DAGCommitStartedEvent)event;
          if (startedEvent.getDagID().equals(dagId)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }

    public void verifyDAGFinishedEvent(TezDAGID dagId, int expectedTimes) {
      int actualTimes = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == HistoryEventType.DAG_FINISHED) {
          DAGFinishedEvent startedEvent = (DAGFinishedEvent)event;
          if (startedEvent.getDagID().equals(dagId)) {
            actualTimes ++;
          }
        }
      }
      Assert.assertEquals(expectedTimes, actualTimes);
    }
  }
  
  private static class ControlledThreadPoolExecutor extends ThreadPoolExecutor {

    public ControlledThreadPoolExecutor(int poolSize) {
      this(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>());
    }

    public ControlledThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
        long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public boolean startFlag = false;
    
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      while(!startFlag) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      super.beforeExecute(t, r);
    }
  }
}

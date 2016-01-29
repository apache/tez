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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.VertexStatus.State;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
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
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.RecoveryParser.DAGRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.TaskAttemptRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.TaskRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.VertexRecoveryData;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TaskStateInternal;
import org.apache.tez.dag.app.dag.TestStateChangeNotifier.StateChangeNotifierForTest;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.CallableEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventRecoverEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventScheduleTask;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.CountingOutputCommitter;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.EventHandlingRootInputInitializer;
import org.apache.tez.dag.app.rm.AMSchedulerEvent;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class TestDAGRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(TestDAGImpl.class);
  private static Configuration conf;
  private DrainDispatcher dispatcher;
  private ListeningExecutorService execService;
  private Credentials fsTokens;
  private AppContext appContext;
  private ACLManager aclManager;
  private ApplicationAttemptId appAttemptId;
  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private DagEventDispatcher dagEventDispatcher;
  private TaskCommunicatorManagerInterface taskCommunicatorManagerInterface;
  private TaskHeartbeatHandler thh;
  private Clock clock = new SystemClock();
  private DAGFinishEventHandler dagFinishEventHandler;
  private DAGPlan dagPlan;
  private DAGImpl dag;
  private TezDAGID dagId;
  private UserGroupInformation ugi;
  private MockHistoryEventHandler historyEventHandler;
  private TaskAttemptEventDispatcher taskAttemptEventDispatcher;
  private ClusterInfo clusterInfo = new ClusterInfo(Resource.newInstance(8192,
      10));
  private DAGRecoveryData dagRecoveryData = mock(DAGRecoveryData.class);

  private TezVertexID v1Id;   // first vertex
  private TezTaskID t1v1Id;     // first task of v1
  private TezTaskAttemptID ta1t1v1Id;   // first task attempt of the first task of v1
  private TezVertexID v2Id;
  private TezTaskID t1v2Id;
  private TezTaskAttemptID ta1t1v2Id;

  ////////////////////////
  private Random rand = new Random();
  private long dagInitedTime = System.currentTimeMillis() + rand.nextInt(100);
  private long dagStartedTime = dagInitedTime + rand.nextInt(100);
  private long v1InitedTime = dagStartedTime + rand.nextInt(100);
  private long v1StartedTime = v1InitedTime + rand.nextInt(100);
  private int v1NumTask = 10;
  private long t1StartedTime = v1StartedTime + rand.nextInt(100);
  private long t1FinishedTime = t1StartedTime + rand.nextInt(100);
  private long ta1LaunchTime = t1StartedTime + rand.nextInt(100);
  private long ta1FinishedTime = ta1LaunchTime + rand.nextInt(100);
  
  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
      dag.handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      TaskImpl task = (TaskImpl) dag.getVertex(event.getTaskID().getVertexID())
          .getTask(event.getTaskID());
      task.handle(event);
    }
  }

  @SuppressWarnings("unchecked")
  private class TaskAttemptEventDispatcher implements
      EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      Vertex vertex = dag.getVertex(event.getTaskAttemptID().getTaskID()
          .getVertexID());
      Task task = vertex.getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt ta = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) ta).handle(event);
    }
  }

  private class VertexEventDispatcher implements EventHandler<VertexEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      VertexImpl vertex = (VertexImpl) dag.getVertex(event.getVertexId());
      vertex.handle(event);
    }
  }

  private class DAGFinishEventHandler implements
      EventHandler<DAGAppMasterEventDAGFinished> {
    public int dagFinishEvents = 0;

    @Override
    public void handle(DAGAppMasterEventDAGFinished event) {
      ++dagFinishEvents;
    }
  }

  private class AMSchedulerEventDispatcher implements EventHandler<AMSchedulerEvent> {
    @Override
    public void handle(AMSchedulerEvent event) {
    }
  }
  
  private static class MockHistoryEventHandler extends HistoryEventHandler {

    private List<HistoryEvent> historyEvents = new ArrayList<HistoryEvent>();

    public MockHistoryEventHandler(AppContext context) {
      super(context);
    }

    @Override
    public void handleCriticalEvent(DAGHistoryEvent event) throws IOException {
      this.historyEvents.add(event.getHistoryEvent());
    }

    public List<HistoryEvent> getHistoryEvents() {
      return historyEvents;
    }

    public void verifyHistoryEvent(int expectedTimes, HistoryEventType eventType) {
      int actualCount = 0;
      for (HistoryEvent event : historyEvents) {
        if (event.getEventType() == eventType) {
          actualCount ++;
        }
      }
      assertEquals(expectedTimes, actualCount);
    }
  }

  public static class MockInputInitializer extends InputInitializer {

    public MockInputInitializer(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      // sleep forever, block the initialization
      while(true) {
        Thread.sleep(1000);
      }
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events)
        throws Exception {
    }
  }

  @BeforeClass
  public static void beforeClass() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Before
  public void setup() {
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    Assert.assertNotNull(dagId);
    dagPlan = createDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    appContext = mock(AppContext.class);
    execService = mock(ListeningExecutorService.class);
    thh = mock(TaskHeartbeatHandler.class);
    final ListenableFuture<Void> mockFuture = mock(ListenableFuture.class);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appAttemptId.getApplicationId());

    Mockito.doAnswer(new Answer() {
      public ListenableFuture<Void> answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        CallableEvent e = (CallableEvent) args[0];
        dispatcher.getEventHandler().handle(e);
        return mockFuture;
      }
    }).when(execService).submit((Callable<Void>) any());

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
    doReturn(dagRecoveryData).when(appContext).getDAGRecoveryData();
    dag = new DAGImpl(dagId, conf, dagPlan, dispatcher.getEventHandler(),
        taskCommunicatorManagerInterface, fsTokens, clock, "user", thh,
        appContext);
    dag.entityUpdateTracker = new StateChangeNotifierForTest(dag);
    doReturn(dag).when(appContext).getCurrentDAG();
    ugi = mock(UserGroupInformation.class);
    UserGroupInformation ugi =dag.getDagUGI();
    doReturn(clusterInfo).when(appContext).getClusterInfo();
    TaskSchedulerManager mockTaskScheduler = mock(TaskSchedulerManager.class);
    doReturn(mockTaskScheduler).when(appContext).getTaskScheduler();
    v1Id = TezVertexID.getInstance(dagId, 0);
    t1v1Id = TezTaskID.getInstance(v1Id, 0);
    ta1t1v1Id = TezTaskAttemptID.getInstance(t1v1Id, 0);
    v2Id = TezVertexID.getInstance(dagId, 1);
    t1v2Id = TezTaskID.getInstance(v2Id, 0);
    ta1t1v2Id = TezTaskAttemptID.getInstance(t1v2Id, 0);

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
    dispatcher.register(AMSchedulerEventType.class, new AMSchedulerEventDispatcher());
    dispatcher.init(conf);
    dispatcher.start();
    doReturn(dispatcher.getEventHandler()).when(appContext).getEventHandler();
    LogManager.getRootLogger().setLevel(Level.DEBUG);
  }


  public static class RecoveryNotSupportedOutputCommitter extends OutputCommitter {

    public RecoveryNotSupportedOutputCommitter(
        OutputCommitterContext committerContext) {
      super(committerContext);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void setupOutput() throws Exception {
    }

    @Override
    public void commitOutput() throws Exception {
    }

    @Override
    public void abortOutput(State finalState) throws Exception {
    }
    
    @Override
    public boolean isTaskRecoverySupported() {
      return false;
    }
  }

  /**
   * v1     v2 
   *   \    / 
   *    \  / 
   *     v3
   */
  private DAGPlan createDAGPlan() {
    DAGPlan dag = DAGPlan
        .newBuilder()
        .setName("testverteximpl")
        .addVertex(
            VertexPlan
                .newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(RootInputLeafOutputProto.newBuilder().setName("input1")
                    .setControllerDescriptor(TezEntityDescriptorProto.newBuilder()
                        .setClassName(MockInputInitializer.class.getName()).build()))
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host1")
                        .addRack("rack1").build())
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder().setNumTasks(-1)
                        .setVirtualCores(4).setMemoryMb(1024).setJavaOpts("")
                        .setTaskModule("x1.y1").build())
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto
                        .newBuilder()
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("output1").build())
                        .setName("output1")
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                CountingOutputCommitter.class.getName())))
                .addOutEdgeId("e1").build())
        .addVertex(
            VertexPlan
                .newBuilder()
                .setName("vertex2")
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host2")
                        .addRack("rack2").build())
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder().setNumTasks(1)
                        .setVirtualCores(4).setMemoryMb(1024).setJavaOpts("")
                        .setTaskModule("x2.y2").build())
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto
                        .newBuilder()
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("output2").build())
                        .setName("output2")
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                RecoveryNotSupportedOutputCommitter.class.getName())))
                .addOutEdgeId("e2").build())
        .addVertex(
            VertexPlan
                .newBuilder()
                .setName("vertex3")
                .setType(PlanVertexType.NORMAL)
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("x3.y3"))
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host3")
                        .addRack("rack3").build())
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder().setNumTasks(1)
                        .setVirtualCores(4).setMemoryMb(1024)
                        .setJavaOpts("foo").setTaskModule("x3.y3").build())
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto
                        .newBuilder()
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("output3").build())
                        .setName("output3")
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                CountingOutputCommitter.class.getName())))
                .addInEdgeId("e1").addInEdgeId("e2").build())
        .addEdge(
            EdgePlan
                .newBuilder()
                .setEdgeDestination(
                    TezEntityDescriptorProto.newBuilder().setClassName("i2"))
                .setInputVertexName("vertex1")
                .setEdgeSource(
                    TezEntityDescriptorProto.newBuilder().setClassName("o1"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL).build())
        .addEdge(
            EdgePlan
                .newBuilder()
                .setEdgeDestination(
                    TezEntityDescriptorProto.newBuilder().setClassName("i3"))
                .setInputVertexName("vertex2")
                .setEdgeSource(
                    TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("e2")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL).build())
        .build();

    return dag;
  }

 
  @After
  public void teardown() {
    dispatcher.await();
    dispatcher.stop();
    execService.shutdownNow();
    dagPlan = null;
    if (dag != null) {
      dag.entityUpdateTracker.stop();
    }
    dag = null;
  }
  
  
  ////////////////////////////////// DAG Recovery ///////////////////////////////////////////////////
  /**
   * RecoveryEvents: SummaryEvent_DAGFinishedEvent(SUCCEEDED)
   * Recover dag to SUCCEEDED and all of its vertices to SUCCEEDED
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromDesiredSucceeded() {
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, DAGState.SUCCEEDED, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.SUCCEEDED, dag.getState());
    assertEquals(3, dag.getVertices().size());
    assertEquals(VertexState.SUCCEEDED, dag.getVertex("vertex1").getState());
    assertEquals(VertexState.SUCCEEDED, dag.getVertex("vertex2").getState());
    assertEquals(VertexState.SUCCEEDED, dag.getVertex("vertex3").getState());
    // DAG#initTime, startTime is not guaranteed to be recovered in this case
  }
  
  /**
   * RecoveryEvents: SummaryEvent_DAGFinishedEvent(FAILED)
   * Recover dag to FAILED and all of its vertices to FAILED
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromDesiredFailed() {
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, DAGState.FAILED, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.FAILED, dag.getState());
    assertEquals(3, dag.getVertices().size());
    assertEquals(VertexState.FAILED, dag.getVertex("vertex1").getState());
    assertEquals(VertexState.FAILED, dag.getVertex("vertex2").getState());
    assertEquals(VertexState.FAILED, dag.getVertex("vertex3").getState());
    // DAG#initTime, startTime is not guaranteed to be recovered in this case
  }
  
  /**
   * RecoveryEvents: SummaryEvent_DAGFinishedEvent(KILLED)
   * Recover dag to KILLED and all of its vertices to KILLED
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromDesiredKilled() {
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, DAGState.KILLED, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.KILLED, dag.getState());
    assertEquals(3, dag.getVertices().size());
    assertEquals(VertexState.KILLED, dag.getVertex("vertex1").getState());
    assertEquals(VertexState.KILLED, dag.getVertex("vertex2").getState());
    assertEquals(VertexState.KILLED, dag.getVertex("vertex3").getState());
    // DAG#initTime, startTime is not guaranteed to be recovered in this case
  }
  
  /**
   * RecoveryEvents: SummaryEvent_DAGFinishedEvent(ERROR)
   * Recover dag to ERROR and all of its vertices to ERROR
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromDesiredError() {
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, DAGState.ERROR, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.ERROR, dag.getState());
    assertEquals(3, dag.getVertices().size());
    assertEquals(VertexState.ERROR, dag.getVertex("vertex1").getState());
    assertEquals(VertexState.ERROR, dag.getVertex("vertex2").getState());
    assertEquals(VertexState.ERROR, dag.getVertex("vertex3").getState());
    // DAG#initTime, startTime is not guaranteed to be recovered in this case
  }
  
  /**
   * RecoveryEvents: DAGSubmittedEvent
   * Recover it as normal dag execution
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromNew() {
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.RUNNING, dag.getState());
  }
  
  /**
   * RecoveryEvents: DAGSubmittedEvent, DAGInitializedEvent
   * Recover it as normal dag execution
   */
  @Test(timeout=5000)
  public void testDAGRecoverFromInited() {
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagId, dagInitedTime, 
        "user", "dagName", null);
    doReturn(dagInitedEvent).when(dagRecoveryData).getDAGInitializedEvent();
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.RUNNING, dag.getState());
    assertEquals(dagInitedTime, dag.initTime);
  }
  
  @Test(timeout=5000)
  public void testDAGRecoverFromStarted() {
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagId, dagInitedTime, 
        "user", "dagName", null);
    doReturn(dagInitedEvent).when(dagRecoveryData).getDAGInitializedEvent();
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagId, dagStartedTime, "user", "dagName");
    doReturn(dagStartedEvent).when(dagRecoveryData).getDAGStartedEvent();

    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.RUNNING, dag.getState());
    assertEquals(dagInitedTime, dag.initTime);
    assertEquals(dagStartedTime, dag.startTime);
  }
 
  /////////////////////////////// Vertex Recovery /////////////////////////////////////////
  
  private void initMockDAGRecoveryDataForVertex() {    
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagId, dagInitedTime, 
        "user", "dagName", null);
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagId, dagStartedTime, "user", "dagName");
    doReturn(dagInitedEvent).when(dagRecoveryData).getDAGInitializedEvent();
    doReturn(dagStartedEvent).when(dagRecoveryData).getDAGStartedEvent();
  }

  /**
   * RecoveryEvents:
   *  DAG:  DAGInitedEvent -> DAGStartedEvent 
   *  V1:   No any event
   * 
   * Reinitialize V1 again. 
   */
  @Test(timeout=5000)
  public void testVertexRecoverFromNew() {
    initMockDAGRecoveryDataForVertex();
    
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.RUNNING, dag.getState());
    // reinitialize v1 again
    VertexImpl v1 = (VertexImpl)dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl)dag.getVertex("vertex3");
    assertEquals(VertexState.INITIALIZING, v1.getState());
    assertEquals(VertexState.RUNNING, v2.getState());
    assertEquals(VertexState.INITED, v3.getState());
  }
  
  /**
   * RecoveryEvents:
   *  DAG:  DAGInitedEvent -> DAGStartedEvent 
   *  V1:   VertexInitializedEvent
   * 
   * Reinitialize V1 again. 
   */
  @Test(timeout=5000)
  public void testVertexRecoverFromInited() {
    initMockDAGRecoveryDataForVertex();
    List<TezEvent> inputGeneratedTezEvents = new ArrayList<TezEvent>();
    VertexInitializedEvent v1InitedEvent = new VertexInitializedEvent(v1Id, 
        "vertex1", 0L, v1InitedTime, 
        v1NumTask, "", null, inputGeneratedTezEvents);
    VertexRecoveryData vertexRecoveryData = new VertexRecoveryData(v1InitedEvent,
        null, null, null, null, false);
    doReturn(vertexRecoveryData).when(dagRecoveryData).getVertexRecoveryData(v1Id);
    
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    assertEquals(DAGState.RUNNING, dag.getState());
    // reinitialize v1 again because its VertexManager is not completed
    VertexImpl v1 = (VertexImpl)dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl)dag.getVertex("vertex3");
    assertEquals(VertexState.INITIALIZING, v1.getState());
    assertEquals(VertexState.RUNNING, v2.getState());
    assertEquals(VertexState.INITED, v3.getState());
  }
  
  /**
   * RecoveryEvents:
   *  DAG:  DAGInitedEvent -> DAGStartedEvent 
   *  V1:   VertexReconfigrationDoneEvent -> VertexInitializedEvent
   * 
   * V1 skip initialization. 
   */
  @Test//(timeout=5000)
  public void testVertexRecoverFromInitedAndReconfigureDone() {
    initMockDAGRecoveryDataForVertex();
    List<TezEvent> inputGeneratedTezEvents = new ArrayList<TezEvent>();
    VertexInitializedEvent v1InitedEvent = new VertexInitializedEvent(v1Id, 
        "vertex1", 0L, v1InitedTime, 
        v1NumTask, "", null, inputGeneratedTezEvents);
    VertexConfigurationDoneEvent v1ReconfigureDoneEvent = new VertexConfigurationDoneEvent(v1Id, 
        0L, v1NumTask, null, null, null, true);
    VertexRecoveryData vertexRecoveryData = new VertexRecoveryData(v1InitedEvent,
        v1ReconfigureDoneEvent, null, null, new HashMap<TezTaskID, TaskRecoveryData>(), false);
    doReturn(vertexRecoveryData).when(dagRecoveryData).getVertexRecoveryData(v1Id);
    
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    VertexImpl v1 = (VertexImpl)dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl)dag.getVertex("vertex3");
    assertEquals(DAGState.RUNNING, dag.getState());
    // v1 skip initialization
    assertEquals(VertexState.RUNNING, v1.getState());
    assertEquals(v1InitedTime, v1.initedTime);
    assertEquals(v1NumTask, v1.getTotalTasks());
    assertEquals(VertexState.RUNNING, v2.getState());
    assertEquals(VertexState.RUNNING, v3.getState());
  }
  
  /**
   * RecoveryEvents:
   *  DAG:  DAGInitedEvent -> DAGStartedEvent 
   *  V1:   VertexReconfigrationDoneEvent -> VertexInitializedEvent -> VertexStartedEvent
   * 
   * V1 skip initialization. 
   */
  @Test(timeout=5000)
  public void testVertexRecoverFromStart() {
    initMockDAGRecoveryDataForVertex();   
    List<TezEvent> inputGeneratedTezEvents = new ArrayList<TezEvent>();
    VertexInitializedEvent v1InitedEvent = new VertexInitializedEvent(v1Id, 
        "vertex1", 0L, v1InitedTime, 
        v1NumTask, "", null, inputGeneratedTezEvents);
    VertexConfigurationDoneEvent v1ReconfigureDoneEvent = new VertexConfigurationDoneEvent(v1Id, 
        0L, v1NumTask, null, null, null, true);
    VertexStartedEvent v1StartedEvent = new VertexStartedEvent(v1Id, 0L, v1StartedTime);
    VertexRecoveryData vertexRecoveryData = new VertexRecoveryData(v1InitedEvent,
        v1ReconfigureDoneEvent, v1StartedEvent, null, new HashMap<TezTaskID, TaskRecoveryData>(), false);
    doReturn(vertexRecoveryData).when(dagRecoveryData).getVertexRecoveryData(v1Id);
    
    DAGEventRecoverEvent recoveryEvent = new DAGEventRecoverEvent(dagId, dagRecoveryData);
    dag.handle(recoveryEvent);
    dispatcher.await();

    VertexImpl v1 = (VertexImpl)dag.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dag.getVertex("vertex2");
    VertexImpl v3 = (VertexImpl)dag.getVertex("vertex3");
    assertEquals(DAGState.RUNNING, dag.getState());
    // v1 skip initialization
    assertEquals(VertexState.RUNNING, v1.getState());
    assertEquals(v1InitedTime, v1.initedTime);
    assertEquals(v1StartedTime, v1.startedTime);
    assertEquals(v1NumTask, v1.getTotalTasks());
    assertEquals(VertexState.RUNNING, v2.getState());
    assertEquals(VertexState.RUNNING, v3.getState());
  }
  
  /////////////////////////////// Task ////////////////////////////////////////////////////////////
  
  private void initMockDAGRecoveryDataForTask() {
    List<TezEvent> inputGeneratedTezEvents = new ArrayList<TezEvent>();
    VertexInitializedEvent v1InitedEvent = new VertexInitializedEvent(v1Id, 
        "vertex1", 0L, v1InitedTime, 
        v1NumTask, "", null, inputGeneratedTezEvents);
    Map<String, InputSpecUpdate> rootInputSpecs = new HashMap<String, InputSpecUpdate>();
    VertexConfigurationDoneEvent v1ReconfigureDoneEvent = new VertexConfigurationDoneEvent(v1Id, 
        0L, v1NumTask, null, null, rootInputSpecs, true);
    VertexStartedEvent v1StartedEvent = new VertexStartedEvent(v1Id, 0L, v1StartedTime);
    VertexRecoveryData v1RecoveryData = new VertexRecoveryData(v1InitedEvent,
        v1ReconfigureDoneEvent, v1StartedEvent, null, new HashMap<TezTaskID, TaskRecoveryData>(), false);
    
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagId, dagInitedTime, 
        "user", "dagName", null);
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagId, dagStartedTime, "user", "dagName");
    doReturn(v1RecoveryData).when(dagRecoveryData).getVertexRecoveryData(v1Id);
    doReturn(dagInitedEvent).when(dagRecoveryData).getDAGInitializedEvent();
    doReturn(dagStartedEvent).when(dagRecoveryData).getDAGStartedEvent();
  }

  /**
   * RecoveryEvent: TaskFinishedEvent(KILLED)
   * Recover it to KILLED
   */
  @Test(timeout=5000)
  public void testTaskRecoverFromKilled() {
    initMockDAGRecoveryDataForTask();
    TaskFinishedEvent taskFinishedEvent = new TaskFinishedEvent(t1v1Id, "v1",
        0L, 0L, null, TaskState.KILLED, "", null, 4);
    TaskRecoveryData taskRecoveryData = new TaskRecoveryData(null, taskFinishedEvent, null);
    doReturn(taskRecoveryData).when(dagRecoveryData).getTaskRecoveryData(t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();

    VertexImpl vertex1 = (VertexImpl) dag.getVertex(v1Id);
    TaskImpl task = (TaskImpl)vertex1.getTask(t1v1Id);
    assertEquals(TaskStateInternal.KILLED, task.getInternalState());
    assertEquals(1, vertex1.getCompletedTasks());
  }
  
  /**
   * RecoveryEvent: TaskStartedEvent
   * Recover it to Scheduled
   */
  @Test(timeout=5000)
  public void testTaskRecoverFromStarted() {
    initMockDAGRecoveryDataForTask();
    TaskStartedEvent taskStartedEvent = new TaskStartedEvent(t1v1Id, "v1", 0L, 0L);
    TaskRecoveryData taskRecoveryData = new TaskRecoveryData(taskStartedEvent, null, null);
    doReturn(taskRecoveryData).when(dagRecoveryData).getTaskRecoveryData(t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    VertexImpl vertex1 = (VertexImpl) dag.getVertex(v1Id);
    TaskImpl task = (TaskImpl)vertex1.getTask(t1v1Id);
    assertEquals(TaskStateInternal.SCHEDULED, task.getInternalState());
  }
  
  /**
   * RecoveryEvent: TaskStartedEvent -> TaskFinishedEvent
   * Recover it to Scheduled
   */
  @Test(timeout=5000)
  public void testTaskRecoverFromSucceeded() {
    initMockDAGRecoveryDataForTask();
    TaskStartedEvent taskStartedEvent = new TaskStartedEvent(t1v1Id, "v1", 0L, 0L);
    TaskFinishedEvent taskFinishedEvent = new TaskFinishedEvent(t1v1Id, "v1",
        0L, 0L, null, TaskState.SUCCEEDED, "", null, 4);
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v1Id, "v1", 0L, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    List<TezEvent> taGeneratedEvents = new ArrayList<TezEvent>();
    EventMetaData metadata = new EventMetaData(EventProducerConsumerType.OUTPUT,
        "vertex1", "vertex3", ta1t1v2Id);
    taGeneratedEvents.add(new TezEvent(DataMovementEvent.create(ByteBuffer.wrap(new byte[0])), metadata));
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", 0L, 0L, 
        TaskAttemptState.SUCCEEDED, null, "", null, 
        null, taGeneratedEvents, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, taFinishedEvent);
    Map<TezTaskAttemptID, TaskAttemptRecoveryData> taRecoveryDataMap =
        new HashMap<TezTaskAttemptID, TaskAttemptRecoveryData>();
    taRecoveryDataMap.put(ta1t1v1Id, taRecoveryData);
    TaskRecoveryData taskRecoveryData = new TaskRecoveryData(taskStartedEvent, taskFinishedEvent, taRecoveryDataMap);
    doReturn(taskRecoveryData).when(dagRecoveryData).getTaskRecoveryData(t1v1Id);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);

    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    VertexImpl vertex1 = (VertexImpl) dag.getVertex(v1Id);
    TaskImpl task = (TaskImpl)vertex1.getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(VertexState.RUNNING, vertex1.getState());
    assertEquals(1, vertex1.getCompletedTasks());
    assertEquals(TaskStateInternal.SUCCEEDED, task.getInternalState());
    assertEquals(TaskAttemptStateInternal.SUCCEEDED, taskAttempt.getInternalState());
  }

  /////////////////////////////// TaskAttempt Recovery /////////////////////////////////////////////////////
  
  private void initMockDAGRecoveryDataForTaskAttempt() {
    TaskStartedEvent t1StartedEvent = new TaskStartedEvent(t1v1Id, "vertex1", 0L, t1StartedTime);
    TaskRecoveryData taskRecoveryData = new TaskRecoveryData(t1StartedEvent, null, null);
    Map<TezTaskID, TaskRecoveryData> taskRecoveryDataMap = new HashMap<TezTaskID, TaskRecoveryData>();
    taskRecoveryDataMap.put(t1v1Id, taskRecoveryData);
    
    List<TezEvent> inputGeneratedTezEvents = new ArrayList<TezEvent>();
    VertexInitializedEvent v1InitedEvent = new VertexInitializedEvent(v1Id, 
        "vertex1", 0L, v1InitedTime, 
        v1NumTask, "", null, inputGeneratedTezEvents);
    Map<String, InputSpecUpdate> rootInputSpecs = new HashMap<String, InputSpecUpdate>();
    VertexConfigurationDoneEvent v1ReconfigureDoneEvent = new VertexConfigurationDoneEvent(v1Id, 
        0L, v1NumTask, null, null, rootInputSpecs, true);
    VertexStartedEvent v1StartedEvent = new VertexStartedEvent(v1Id, 0L, v1StartedTime);
    VertexRecoveryData v1RecoveryData = new VertexRecoveryData(v1InitedEvent,
        v1ReconfigureDoneEvent, v1StartedEvent, null, taskRecoveryDataMap, false);
    
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagId, dagInitedTime, 
        "user", "dagName", null);
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagId, dagStartedTime, "user", "dagName");
    doReturn(v1RecoveryData).when(dagRecoveryData).getVertexRecoveryData(v1Id);
    doReturn(dagInitedEvent).when(dagRecoveryData).getDAGInitializedEvent();
    doReturn(dagStartedEvent).when(dagRecoveryData).getDAGStartedEvent();
  }

  /**
   * RecoveryEvents: TaskAttemptFinishedEvent (FAILED)
   * Recover it to FAILED
   */
  @Test(timeout=5000)
  public void testTARecoverFromNewToFailed() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, ta1FinishedTime, 
        TaskAttemptState.FAILED, TaskAttemptTerminationCause.CONTAINER_LAUNCH_FAILED, "", null, 
        null, null, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(null, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();

    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.FAILED, taskAttempt.getInternalState());
    assertEquals(TaskAttemptTerminationCause.CONTAINER_LAUNCH_FAILED, taskAttempt.getTerminationCause());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_STARTED);
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(1, task.failedAttempts);
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }
  
  /**
   * RecoveryEvents: TaskAttemptFinishedEvent (KILLED)
   * Recover it to KILLED
   */
  @Test(timeout=5000)
  public void testTARecoverFromNewToKilled() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, ta1FinishedTime, 
        TaskAttemptState.KILLED, TaskAttemptTerminationCause.TERMINATED_BY_CLIENT, "", null, 
        null, null, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(null, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();

    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.KILLED, taskAttempt.getInternalState());
    assertEquals(TaskAttemptTerminationCause.TERMINATED_BY_CLIENT, taskAttempt.getTerminationCause());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_STARTED);
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(0, task.failedAttempts);
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }
  
  /**
   * RecoveryEvents: TaskAttemptStartedEvent
   * Recover it to KILLED
   */
  @Test(timeout=5000)
  public void testTARecoverFromRunning() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, null);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();

    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.KILLED, taskAttempt.getInternalState());
    assertEquals(TaskAttemptTerminationCause.TERMINATED_AT_RECOVERY, taskAttempt.getTerminationCause());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_STARTED);
    historyEventHandler.verifyHistoryEvent(1, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(ta1LaunchTime, taskAttempt.getLaunchTime());
  }

  /**
   * RecoveryEvents: TaskAttemptStartedEvent -> TaskAttemptFinishedEvent (SUCCEEDED)
   * Recover it to SUCCEEDED
   */
  @Test(timeout=5000)
  public void testTARecoverFromSucceeded() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    List<TezEvent> taGeneratedEvents = new ArrayList<TezEvent>();
    EventMetaData sourceInfo = new EventMetaData(EventProducerConsumerType.INPUT, "vertex1",
        "vertex3", ta1t1v1Id);
    taGeneratedEvents.add(new TezEvent(DataMovementEvent.create(ByteBuffer.wrap(new byte[0])),
        sourceInfo));
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, ta1FinishedTime, 
        TaskAttemptState.SUCCEEDED, null, "", null, 
        null, taGeneratedEvents, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);

    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.SUCCEEDED, taskAttempt.getInternalState());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(TaskStateInternal.SUCCEEDED, task.getInternalState());
    assertEquals(ta1LaunchTime, taskAttempt.getLaunchTime());
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }

  /**
   * RecoveryEvents: TaskAttemptStartedEvent -> TaskAttemptFinishedEvent (SUCCEEDED)
   * Recovered it SUCCEEDED, but task schedule new task attempt
   * V2's committer is not recovery supported
   */
  @Test//(timeout=5000)
  public void testTARecoverFromSucceeded_OutputCommitterRecoveryNotSupported() {
    initMockDAGRecoveryDataForTaskAttempt();
    // set up v2 recovery data
    // ta1t1v2: TaskAttemptStartedEvent -> TaskAttemptFinishedEvent(SUCCEEDED)
    // t1v2: TaskStartedEvent
    // v2: VertexInitializedEvent -> VertexConfigurationDoneEvent -> VertexStartedEvent
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v2Id, "vertex2", ta1LaunchTime, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    List<TezEvent> taGeneratedEvents = new ArrayList<TezEvent>();
    EventMetaData metadata = new EventMetaData(EventProducerConsumerType.OUTPUT,
        "vertex2", "vertex3", ta1t1v2Id);
    taGeneratedEvents.add(new TezEvent(DataMovementEvent.create(ByteBuffer.wrap(new byte[0])), metadata));
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v2Id, "vertex2", ta1LaunchTime, ta1FinishedTime, 
        TaskAttemptState.SUCCEEDED, null, "", null, 
        null, taGeneratedEvents, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v2Id);   
    Map<TezTaskAttemptID, TaskAttemptRecoveryData> taRecoveryDataMap =
        new HashMap<TezTaskAttemptID, TaskAttemptRecoveryData>();
    taRecoveryDataMap.put(ta1t1v2Id, taRecoveryData);
 
    TaskStartedEvent t1StartedEvent = new TaskStartedEvent(t1v2Id, "vertex2", 0L, t1StartedTime);
    TaskRecoveryData taskRecoveryData = new TaskRecoveryData(t1StartedEvent, null, taRecoveryDataMap);
    Map<TezTaskID, TaskRecoveryData> taskRecoveryDataMap = new HashMap<TezTaskID, TaskRecoveryData>();
    taskRecoveryDataMap.put(t1v2Id, taskRecoveryData);
    doReturn(taskRecoveryData).when(dagRecoveryData).getTaskRecoveryData(t1v2Id);

    VertexInitializedEvent v2InitedEvent = new VertexInitializedEvent(v2Id, 
        "vertex2", 0L, v1InitedTime, 
        v1NumTask, "", null, null);
    VertexConfigurationDoneEvent v2ReconfigureDoneEvent = new VertexConfigurationDoneEvent(v2Id, 
        0L, v1NumTask, null, null, null, false);
    VertexStartedEvent v2StartedEvent = new VertexStartedEvent(v2Id, 0L, v1StartedTime);
    VertexRecoveryData v2RecoveryData = new VertexRecoveryData(v2InitedEvent,
        v2ReconfigureDoneEvent, v2StartedEvent, null, taskRecoveryDataMap, false);
    doReturn(v2RecoveryData).when(dagRecoveryData).getVertexRecoveryData(v2Id);


    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    TaskImpl task = (TaskImpl)dag.getVertex(v2Id).getTask(t1v2Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v2Id);
    assertEquals(TaskAttemptStateInternal.SUCCEEDED, taskAttempt.getInternalState());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(TaskStateInternal.RUNNING, task.getInternalState());
    // new task attempt is scheduled
    assertEquals(2, task.getAttempts().size());
    assertEquals(ta1LaunchTime, taskAttempt.getLaunchTime());
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }

  /**
   * RecoveryEvents: TaskAttemptStartedEvent -> TaskAttemptFinishedEvent (FAILED)
   * Recover it to FAILED
   */
  @Test(timeout=5000)
  public void testTARecoverFromFailed() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, ta1FinishedTime, 
        TaskAttemptState.FAILED, TaskAttemptTerminationCause.INPUT_READ_ERROR, "", null, 
        null, null, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.FAILED, taskAttempt.getInternalState());
    assertEquals(TaskAttemptTerminationCause.INPUT_READ_ERROR, taskAttempt.getTerminationCause());
    assertEquals(TaskStateInternal.SCHEDULED, task.getInternalState());
    assertEquals(2, task.getAttempts().size());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(ta1LaunchTime, taskAttempt.getLaunchTime());
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }

  /**
   * RecoveryEvents: TaskAttemptStartedEvent -> TaskAttemptFinishedEvent (KILLED)
   * Recover it to KILLED
   */
  @Test(timeout=5000)
  public void testTARecoverFromKilled() {
    initMockDAGRecoveryDataForTaskAttempt();
    TaskAttemptStartedEvent taStartedEvent = new TaskAttemptStartedEvent(
        ta1t1v1Id, "v1", ta1LaunchTime, mock(ContainerId.class), 
        mock(NodeId.class), "", "", "");
    TaskAttemptFinishedEvent taFinishedEvent = new TaskAttemptFinishedEvent(
        ta1t1v1Id, "v1", ta1FinishedTime, ta1FinishedTime, 
        TaskAttemptState.KILLED, TaskAttemptTerminationCause.TERMINATED_BY_CLIENT, "", null, 
        null, null, 0L, null, 0L);
    TaskAttemptRecoveryData taRecoveryData = new TaskAttemptRecoveryData(taStartedEvent, taFinishedEvent);
    doReturn(taRecoveryData).when(dagRecoveryData).getTaskAttemptRecoveryData(ta1t1v1Id);
    
    dag.handle(new DAGEventRecoverEvent(dagId, dagRecoveryData));
    dispatcher.await();
    
    TaskImpl task = (TaskImpl)dag.getVertex(v1Id).getTask(t1v1Id);
    TaskAttemptImpl taskAttempt = (TaskAttemptImpl)task.getAttempt(ta1t1v1Id);
    assertEquals(TaskAttemptStateInternal.KILLED, taskAttempt.getInternalState());
    assertEquals(TaskAttemptTerminationCause.TERMINATED_BY_CLIENT, taskAttempt.getTerminationCause());
    historyEventHandler.verifyHistoryEvent(0, HistoryEventType.TASK_ATTEMPT_FINISHED);
    assertEquals(ta1LaunchTime, taskAttempt.getLaunchTime());
    assertEquals(ta1FinishedTime, taskAttempt.getFinishTime());
  }
}

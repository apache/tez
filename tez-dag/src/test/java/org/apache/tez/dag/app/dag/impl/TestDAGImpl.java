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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
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
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate.UpdateType;
import org.apache.tez.dag.app.dag.event.DAGEventStartDag;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.CountingOutputCommitter;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestDAGImpl {

  private static final Log LOG = LogFactory.getLog(TestDAGImpl.class);
  private DAGPlan dagPlan;
  private TezDAGID dagId;
  private static Configuration conf;
  private DrainDispatcher dispatcher;
  private Credentials fsTokens;
  private AppContext appContext;
  private ACLManager aclManager;
  private ApplicationAttemptId appAttemptId;
  private DAGImpl dag;
  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private DagEventDispatcher dagEventDispatcher;
  private TaskAttemptListener taskAttemptListener;
  private TaskHeartbeatHandler thh;
  private Clock clock = new SystemClock();
  private DAGFinishEventHandler dagFinishEventHandler;
  private AppContext mrrAppContext;
  private DAGPlan mrrDagPlan;
  private DAGImpl mrrDag;
  private TezDAGID mrrDagId;
  private AppContext groupAppContext;
  private DAGPlan groupDagPlan;
  private DAGImpl groupDag;
  private TezDAGID groupDagId;
  private HistoryEventHandler historyEventHandler;
  private TaskAttemptEventDispatcher taskAttemptEventDispatcher;

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
      if (event.getDAGId().equals(dagId)) {
        dag.handle(event);
      } else if (event.getDAGId().equals(mrrDagId)) {
        mrrDag.handle(event);
      } else if (event.getDAGId().equals(groupDagId)) {
        groupDag.handle(event);
      } else {
        throw new RuntimeException("Invalid event, unknown dag"
            + ", dagId=" + event.getDAGId());
      }
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
      TezDAGID id = event.getTaskID().getVertexID().getDAGId();
      DAGImpl handler = id.equals(dagId) ? dag : (id.equals(mrrDagId) ? mrrDag : groupDag);
      Vertex vertex = handler.getVertex(event.getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher implements EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      // Ignore
    }
  }

  private class VertexEventDispatcher
      implements EventHandler<VertexEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      TezDAGID id = event.getVertexId().getDAGId();
      DAGImpl handler = id.equals(dagId) ? dag : (id.equals(mrrDagId) ? mrrDag : groupDag);
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
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto.newBuilder()
                    .setIODescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName("output1").build()
                    )
                    .setName("output1")
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                CountingOutputCommitter.class.getName()))
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
                    .setNumTasks(1)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x2.y2")
                    .build()
                    )
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto.newBuilder()
                    .setIODescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName("output2").build()
                    )
                    .setName("output2")
                    .setControllerDescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName(
                            CountingOutputCommitter.class.getName()))
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
                    .setNumTasks(1)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("foo")
                    .setTaskModule("x3.y3")
                    .build()
                    )
               .addOutputs(
                    DAGProtos.RootInputLeafOutputProto.newBuilder()
                    .setIODescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName("output3").build()
                    )
                    .setName("output3")
                    .setControllerDescriptor(
                        TezEntityDescriptorProto.newBuilder().setClassName(
                            CountingOutputCommitter.class.getName()))
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
            .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
            .setId("e1")
            .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
            .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
            .build()
            )
       .addEdge(
           EdgePlan.newBuilder()
           .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("i3"))
           .setInputVertexName("vertex2")
           .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
           .setOutputVertexName("vertex3")
           .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
           .setId("e2")
           .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
           .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
           .build()
           )
      .build();

    return dag;
  }

  public static class TotalCountingOutputCommitter extends CountingOutputCommitter {
    static int totalCommitCounter = 0;
    public TotalCountingOutputCommitter(OutputCommitterContext context) {
      super(context);
    }
    @Override
    public void commitOutput() throws IOException {
      ++totalCommitCounter;
      super.commitOutput();
    }
  }

  // Create a plan with 3 vertices: A, B, C. Group(A,B)->C
  static DAGPlan createGroupDAGPlan() {
    LOG.info("Setting up group dag plan");
    int dummyTaskCount = 1;
    Resource dummyTaskResource = Resource.newInstance(1, 1);
    org.apache.tez.dag.api.Vertex v1 = org.apache.tez.dag.api.Vertex.create("vertex1",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    org.apache.tez.dag.api.Vertex v2 = org.apache.tez.dag.api.Vertex.create("vertex2",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    org.apache.tez.dag.api.Vertex v3 = org.apache.tez.dag.api.Vertex.create("vertex3",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    OutputCommitterDescriptor ocd = OutputCommitterDescriptor.create(
        TotalCountingOutputCommitter.class.getName());
    org.apache.tez.dag.api.VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = OutputDescriptor.create("output.class");
    uv12.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, ocd, null));
    v3.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, ocd, null));

    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("merge.class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    return dag.createDag(conf);
  }

  public static DAGPlan createTestDAGPlan() {
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

  @Before
  public void setup() {
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    Assert.assertNotNull(dagId);
    dagPlan = createTestDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    appContext = mock(AppContext.class);
    historyEventHandler = mock(HistoryEventHandler.class);
    aclManager = new ACLManager("amUser");
    doReturn(conf).when(appContext).getAMConf();
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(appContext).getApplicationID();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(aclManager).when(appContext).getAMACLManager();
    dag = new DAGImpl(dagId, conf, dagPlan,
        dispatcher.getEventHandler(),  taskAttemptListener,
        fsTokens, clock, "user", thh, appContext);
    doReturn(dag).when(appContext).getCurrentDAG();
    mrrAppContext = mock(AppContext.class);
    doReturn(aclManager).when(mrrAppContext).getAMACLManager();
    mrrDagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 2);
    mrrDagPlan = createTestMRRDAGPlan();
    mrrDag = new DAGImpl(mrrDagId, conf, mrrDagPlan,
        dispatcher.getEventHandler(),  taskAttemptListener,
        fsTokens, clock, "user", thh,
        mrrAppContext);
    doReturn(conf).when(mrrAppContext).getAMConf();
    doReturn(mrrDag).when(mrrAppContext).getCurrentDAG();
    doReturn(appAttemptId).when(mrrAppContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(mrrAppContext).getApplicationID();
    doReturn(historyEventHandler).when(mrrAppContext).getHistoryHandler();
    groupAppContext = mock(AppContext.class);
    doReturn(aclManager).when(groupAppContext).getAMACLManager();
    groupDagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 3);
    groupDagPlan = createGroupDAGPlan();
    groupDag = new DAGImpl(groupDagId, conf, groupDagPlan,
        dispatcher.getEventHandler(),  taskAttemptListener,
        fsTokens, clock, "user", thh,
        groupAppContext);
    doReturn(conf).when(groupAppContext).getAMConf();
    doReturn(groupDag).when(groupAppContext).getCurrentDAG();
    doReturn(appAttemptId).when(groupAppContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId())
        .when(groupAppContext).getApplicationID();
    doReturn(historyEventHandler).when(groupAppContext).getHistoryHandler();
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
        new DAGEventStartDag(impl.getID(), null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, impl.getState());
  }

  @Test(timeout = 5000)
  public void testDAGInit() {
    initDAG(dag);
    Assert.assertEquals(6, dag.getTotalVertices());
  }

  @Test(timeout = 5000)
  public void testDAGStart() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    for (int i = 0 ; i < 6; ++i ) {
      TezVertexID vId = TezVertexID.getInstance(dagId, i);
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
      TezVertexID vId = TezVertexID.getInstance(dagId, i);
      LOG.info("Distance from root: v" + i + ":"
          + dag.getVertex(vId).getDistanceFromRoot());
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexCompletion() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    TezVertexID vId = TezVertexID.getInstance(dagId, 1);
    Vertex v = dag.getVertex(vId);
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId, 0), TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId, 1), TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGroupDAGCompletionWithCommitSuccess() {
    // should have only 2 commits. 1 vertex3 commit and 1 group commit.
    initDAG(groupDag);
    startDAG(groupDag);
    dispatcher.await();

    for (int i=0; i<3; ++i) {
      Vertex v = groupDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, groupDag.getSuccessfulVertices());
    }
    
    Assert.assertEquals(3, groupDag.getSuccessfulVertices());
    Assert.assertEquals(DAGState.SUCCEEDED, groupDag.getState());
    Assert.assertEquals(2, TotalCountingOutputCommitter.totalCommitCounter);
  }  
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testDAGCompletionWithCommitSuccess() {
    // all vertices completed -> DAG completion and commit
    initDAG(mrrDag);
    dispatcher.await();
    startDAG(mrrDag);
    dispatcher.await();
    for (int i=0; i<2; ++i) {
      Vertex v = mrrDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, mrrDag.getSuccessfulVertices());
    }
    
    // no commit yet
    for (Vertex v : mrrDag.vertices.values()) {
      for (OutputCommitter c : v.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(0, committer.abortCounter);
        Assert.assertEquals(0, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
    
    // dag completion and commit
    Vertex v = mrrDag.getVertex("vertex3");
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(3, mrrDag.getSuccessfulVertices());
    Assert.assertEquals(DAGState.SUCCEEDED, mrrDag.getState());
    
    for (Vertex vertex : mrrDag.vertices.values()) {
      for (OutputCommitter c : vertex.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(0, committer.abortCounter);
        Assert.assertEquals(1, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testDAGCompletionWithCommitFailure() throws IOException {
    // all vertices completed -> DAG completion and commit
    initDAG(mrrDag);
    dispatcher.await();
    // committer for bad vertex will throw exception
    Vertex badVertex = mrrDag.getVertex("vertex3");
    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto
                .newBuilder()
                .setClassName(CountingOutputCommitter.class.getName())
                .setUserPayload(
                    ByteString
                        .copyFrom(new CountingOutputCommitter.CountingOutputCommitterConfig(
                            true, false, false).toUserPayload())).build())
        .setName("output3")
        .setIODescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName("output.class")
                )
        .build());
    badVertex.setAdditionalOutputs(outputs);
    
    startDAG(mrrDag);
    dispatcher.await();
    for (int i=0; i<2; ++i) {
      Vertex v = mrrDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, mrrDag.getSuccessfulVertices());
    }
    
    // no commit yet
    for (Vertex v : mrrDag.vertices.values()) {
      for (OutputCommitter c : v.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(0, committer.abortCounter);
        Assert.assertEquals(0, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
    
    // dag completion and commit. Exception causes all outputs to be aborted
    Vertex v = mrrDag.getVertex("vertex3");
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(3, mrrDag.getSuccessfulVertices());
    Assert.assertEquals(DAGState.FAILED, mrrDag.getState());
    Assert.assertEquals(DAGTerminationCause.COMMIT_FAILURE, mrrDag.getTerminationCause());
    
    for (Vertex vertex : mrrDag.vertices.values()) {
      for (OutputCommitter c : vertex.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(1, committer.abortCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testDAGErrorAbortAllOutputs() {
    // error on a vertex -> dag error -> all outputs aborted.
    initDAG(mrrDag);
    dispatcher.await();
    startDAG(mrrDag);
    dispatcher.await();
    for (int i=0; i<2; ++i) {
      Vertex v = mrrDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, mrrDag.getSuccessfulVertices());
    }
    
    // no commit yet
    for (Vertex v : mrrDag.vertices.values()) {
      for (OutputCommitter c : v.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(0, committer.abortCounter);
        Assert.assertEquals(0, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
    
    // vertex error -> dag error -> abort all outputs
    Vertex v = mrrDag.getVertex("vertex3");
    dispatcher.getEventHandler().handle(new VertexEvent(
        v.getVertexId(), VertexEventType.V_INTERNAL_ERROR));
    dispatcher.await();
    Assert.assertEquals(VertexState.ERROR, v.getState());
    Assert.assertEquals(DAGState.ERROR, mrrDag.getState());
    
    for (Vertex vertex : mrrDag.vertices.values()) {
      for (OutputCommitter c : vertex.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(1, committer.abortCounter);
        Assert.assertEquals(0, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testDAGErrorAbortNonSuccessfulOutputs() {
    // vertex success -> vertex output commit. failed dag aborts only non-successful vertices
    conf.setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false);
    initDAG(mrrDag);
    dispatcher.await();
    startDAG(mrrDag);
    dispatcher.await();
    for (int i=0; i<2; ++i) {
      Vertex v = mrrDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, mrrDag.getSuccessfulVertices());
      for (OutputCommitter c : v.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        Assert.assertEquals(0, committer.abortCounter);
        Assert.assertEquals(1, committer.commitCounter);
        Assert.assertEquals(1, committer.initCounter);
        Assert.assertEquals(1, committer.setupCounter);
      }
    }
    
    // error on vertex -> dag error -> successful vertex output not aborted
    Vertex errorVertex = mrrDag.getVertex("vertex3");
    dispatcher.getEventHandler().handle(new VertexEvent(
        errorVertex.getVertexId(), VertexEventType.V_INTERNAL_ERROR));
    dispatcher.await();
    Assert.assertEquals(VertexState.ERROR, errorVertex.getState());
    
    dispatcher.await();
    Assert.assertEquals(DAGState.ERROR, mrrDag.getState());
    
    for (Vertex vertex : mrrDag.vertices.values()) {
      for (OutputCommitter c : vertex.getOutputCommitters().values()) {
        CountingOutputCommitter committer= (CountingOutputCommitter) c;
        if (vertex == errorVertex) {
          Assert.assertEquals(1, committer.abortCounter);
          Assert.assertEquals(0, committer.commitCounter);
          Assert.assertEquals(1, committer.initCounter);
          Assert.assertEquals(1, committer.setupCounter);
        } else {
          Assert.assertEquals(0, committer.abortCounter);
          Assert.assertEquals(1, committer.commitCounter);
          Assert.assertEquals(1, committer.initCounter);
          Assert.assertEquals(1, committer.setupCounter);          
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testVertexReRunning() {
    initDAG(dag);
    dag.dagScheduler = mock(DAGScheduler.class);
    startDAG(dag);
    dispatcher.await();

    TezVertexID vId = TezVertexID.getInstance(dagId, 1);
    Vertex v = dag.getVertex(vId);
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId, 0), TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId, 1), TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());
    Assert.assertEquals(1, dag.numCompletedVertices);
    verify(dag.dagScheduler, times(1)).vertexCompleted(v);
    
    dispatcher.getEventHandler().handle(
        new VertexEventTaskReschedule(TezTaskID.getInstance(vId, 0)));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(0, dag.getSuccessfulVertices());
    Assert.assertEquals(0, dag.numCompletedVertices);
    
    dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId, 0), TaskState.SUCCEEDED));
          dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());
    Assert.assertEquals(1, dag.numCompletedVertices);
    
    // re-completion is not notified again
    verify(dag.dagScheduler, times(1)).vertexCompleted(v);

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
      TezVertexID vId = TezVertexID.getInstance(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.KILLED, v.getState());
    }

  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testKillRunningDAG() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    TezVertexID vId1 = TezVertexID.getInstance(dagId, 1);
    Vertex v1 = dag.getVertex(vId1);
    ((EventHandler<VertexEvent>) v1).handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId1, 0), TaskState.SUCCEEDED));
    TezVertexID vId0 = TezVertexID.getInstance(dagId, 0);
    Vertex v0 = dag.getVertex(vId0);
    ((EventHandler<VertexEvent>) v0).handle(new VertexEventTaskCompleted(
        TezTaskID.getInstance(vId0, 0), TaskState.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v0.getState());
    Assert.assertEquals(VertexState.RUNNING, v1.getState());

    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    dispatcher.await();

    Assert.assertEquals(DAGState.TERMINATING, dag.getState());
    Assert.assertEquals(VertexState.SUCCEEDED, v0.getState());
    Assert.assertEquals(VertexState.TERMINATING, v1.getState());
    for (int i = 2 ; i < 6; ++i ) {
      TezVertexID vId = TezVertexID.getInstance(dagId, i);
      Vertex v = dag.getVertex(vId);
      Assert.assertEquals(VertexState.KILLED, v.getState());
    }
    Assert.assertEquals(1, dag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testInvalidEvent() {
    dispatcher.getEventHandler().handle(
        new DAGEventStartDag(dagId, null));
    dispatcher.await();
    Assert.assertEquals(DAGState.ERROR, dag.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  @Ignore // Duplicate completions from a vertex would be a bug. Invalid test.
  public void testVertexSuccessfulCompletionUpdates() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    for (int i = 0; i < 6; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, 0), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    Assert.assertEquals(1, dag.getSuccessfulVertices());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 2), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 3), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 4), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 5), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.SUCCEEDED, dag.getState());
    Assert.assertEquals(6, dag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexFailureHandling() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 2), VertexState.FAILED));
    dispatcher.await();
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(2, dag.getSuccessfulVertices());

    // Expect running vertices to be killed on first failure
    for (int i = 3; i < 6; ++i) {
      TezVertexID vId = TezVertexID.getInstance(dagId, i);
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
  @Test(timeout = 5000)
  public void testDAGKill() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    for (int i = 2; i < 6; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, i), VertexState.SUCCEEDED));
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
  @Test(timeout = 5000)
  public void testDAGKillPending() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 0), VertexState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 1), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));

    for (int i = 2; i < 5; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 5), VertexState.KILLED));
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert.assertEquals(5, dag.getSuccessfulVertices());
    Assert.assertEquals(dag.getVertex(TezVertexID.getInstance(dagId, 5)).getTerminationCause(), VertexTerminationCause.DAG_KILL);
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  @Test(timeout = 5000)
  public void testDiagnosticUpdates() {
    // FIXME need to implement
  }

  @Test(timeout = 5000)
  public void testCounterUpdates() {
    // FIXME need to implement
  }

  @Ignore
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
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

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testDAGKillInternalError() {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    dispatcher.getEventHandler().handle(new DAGEvent(dagId,
        DAGEventType.INTERNAL_ERROR));

    dispatcher.await();
    Assert.assertEquals(DAGState.ERROR, dag.getState());
    Assert.assertEquals(0, dag.getSuccessfulVertices());
    Assert.assertEquals(dag.getVertex(TezVertexID.getInstance(dagId, 2)).getTerminationCause(),
        VertexTerminationCause.INTERNAL_ERROR);
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

}

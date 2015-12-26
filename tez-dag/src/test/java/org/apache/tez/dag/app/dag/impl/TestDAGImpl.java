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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
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
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatusBuilder;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataMovementType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeDataSourceType;
import org.apache.tez.dag.api.records.DAGProtos.PlanEdgeSchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ClusterInfo;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGScheduler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TestStateChangeNotifier.StateChangeNotifierForTest;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.CallableEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventStartDag;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DAGEventVertexCompleted;
import org.apache.tez.dag.app.dag.event.DAGEventVertexReRunning;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.CountingOutputCommitter;
import org.apache.tez.dag.app.rm.AMSchedulerEvent;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;

public class TestDAGImpl {

  private static final Logger LOG = LoggerFactory.getLogger(TestDAGImpl.class);
  private DAGPlan dagPlan;
  private TezDAGID dagId;
  private static Configuration conf;
  private DrainDispatcher dispatcher;
  private ListeningExecutorService execService;
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
  private AppContext mrrAppContext;
  private DAGPlan mrrDagPlan;
  private DAGImpl mrrDag;
  private TezDAGID mrrDagId;
  private AppContext groupAppContext;
  private DAGPlan groupDagPlan;
  private DAGImpl groupDag;
  private TezDAGID groupDagId;
  private DAGPlan dagPlanWithCustomEdge;
  private DAGImpl dagWithCustomEdge;
  private TezDAGID dagWithCustomEdgeId;
  private AppContext dagWithCustomEdgeAppContext;
  private HistoryEventHandler historyEventHandler;
  private TaskAttemptEventDispatcher taskAttemptEventDispatcher;
  private ClusterInfo clusterInfo = new ClusterInfo(Resource.newInstance(8192,10));
  private HadoopShim defaultShim = new DefaultHadoopShim();

  static {
    Limits.reset();
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_COUNTERS_MAX, 100);
    conf.setInt(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS, 100);
    Limits.setConfiguration(conf);
  }

  private DAGImpl chooseDAG(TezDAGID curDAGId) {
    if (curDAGId.equals(dagId)) {
      return dag;
    } else if (curDAGId.equals(mrrDagId)) {
      return mrrDag;
    } else if (curDAGId.equals(groupDagId)) {
      return groupDag;
    } else if (curDAGId.equals(dagWithCustomEdgeId)) {
      return dagWithCustomEdge;
    } else {
      throw new RuntimeException("Invalid event, unknown dag"
          + ", dagId=" + curDAGId);
    }
  }
  
  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @Override
    public void handle(DAGEvent event) {
      DAGImpl dag = chooseDAG(event.getDAGId());
      dag.handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      TezDAGID id = event.getTaskID().getVertexID().getDAGId();
      DAGImpl handler = chooseDAG(id);
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

  @SuppressWarnings("unchecked")
  private class TaskAttemptEventDisptacher2 implements EventHandler<TaskAttemptEvent> {
    @Override
    public void handle(TaskAttemptEvent event) {
      TezDAGID id = event.getTaskAttemptID().getTaskID().getVertexID().getDAGId();
      DAGImpl handler = chooseDAG(id);
      Vertex vertex = handler.getVertex(event.getTaskAttemptID().getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt ta = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>)ta).handle(event);
    }
  }
  
  private class VertexEventDispatcher
      implements EventHandler<VertexEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      TezDAGID id = event.getVertexId().getDAGId();
      DAGImpl handler = chooseDAG(id);
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
    return dag.createDag(conf, null, null, null, true);
  }

  public static DAGPlan createTestDAGPlan() {
    LOG.info("Setting up dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testverteximpl")
        .setDagConf(ConfigurationProto.newBuilder()
            .addConfKeyValues(PlanKeyValuePair.newBuilder()
                .setKey(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS)
                .setValue(3 + "")))
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
            .setVertexConf(ConfigurationProto.newBuilder()
                .addConfKeyValues(PlanKeyValuePair.newBuilder()
                    .setKey(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS)
                    .setValue(2+"")))
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

  // v1 -> v2
  private DAGPlan createDAGWithCustomEdge(ExceptionLocation exLocation, boolean useLegacy) {
    LOG.info("Setting up custome edge dag plan " + exLocation + " " + useLegacy);
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
                  .setProcessorDescriptor(
                      TezEntityDescriptorProto.newBuilder().setClassName("x2.y2"))
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
                          .setJavaOpts("foo")
                          .setTaskModule("x2.y2")
                          .build()
                  )
                  .addInEdgeId("e1")
                  .build()
          )
         .addEdge(
             EdgePlan.newBuilder()
                 .setEdgeManager(TezEntityDescriptorProto.newBuilder()
                         .setClassName(useLegacy ? CustomizedEdgeManagerLegacy.class.getName() :
                           CustomizedEdgeManager.class.getName())
                         .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                            .setUserPayload(ByteString.copyFromUtf8(exLocation.name())))
                 )
                 .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                 .setInputVertexName("vertex1")
                 .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o1"))
                 .setOutputVertexName("vertex2")
                 .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                 .setId("e1")
                 .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                 .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                 .build()
         )
          .build();
    return dag;
  }

  // v1 -> v2
  private DAGPlan createDAGWithNonExistEdgeManager() {
    LOG.info("Setting up dag plan with non-exist edgemanager");
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
                  .setProcessorDescriptor(
                      TezEntityDescriptorProto.newBuilder().setClassName("x2.y2"))
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
                          .setJavaOpts("foo")
                          .setTaskModule("x2.y2")
                          .build()
                  )
                  .addInEdgeId("e1")
                  .build()
          )
         .addEdge(
             EdgePlan.newBuilder()
                 .setEdgeManager(TezEntityDescriptorProto.newBuilder()
                         .setClassName("non-exist-edge-manager")
                 )
                 .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                 .setInputVertexName("vertex1")
                 .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o1"))
                 .setOutputVertexName("vertex2")
                 .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                 .setId("e1")
                 .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                 .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                 .build()
         )
          .build();
    return dag;
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
    dagPlan = createTestDAGPlan();
    dispatcher = new DrainDispatcher();
    fsTokens = new Credentials();
    appContext = mock(AppContext.class);
    execService = mock(ListeningExecutorService.class);
    final ListenableFuture<Void> mockFuture = mock(ListenableFuture.class);
    when(appContext.getHadoopShim()).thenReturn(defaultShim);
    when(appContext.getApplicationID()).thenReturn(appAttemptId.getApplicationId());
    
    Mockito.doAnswer(new Answer() {
      public ListenableFuture<Void> answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          CallableEvent e = (CallableEvent) args[0];
          dispatcher.getEventHandler().handle(e);
          return mockFuture;
      }})
    .when(execService).submit((Callable<Void>) any());
    
    doReturn(execService).when(appContext).getExecService();
    historyEventHandler = mock(HistoryEventHandler.class);
    aclManager = new ACLManager("amUser");
    doReturn(conf).when(appContext).getAMConf();
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(appContext).getApplicationID();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(aclManager).when(appContext).getAMACLManager();
    doReturn(defaultShim).when(appContext).getHadoopShim();
    dag = new DAGImpl(dagId, conf, dagPlan,
        dispatcher.getEventHandler(), taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh, appContext);
    dag.entityUpdateTracker = new StateChangeNotifierForTest(dag);
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(clusterInfo).when(appContext).getClusterInfo();
    mrrAppContext = mock(AppContext.class);
    doReturn(aclManager).when(mrrAppContext).getAMACLManager();
    doReturn(execService).when(mrrAppContext).getExecService();
    doReturn(defaultShim).when(mrrAppContext).getHadoopShim();

    mrrDagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 2);
    mrrDagPlan = createTestMRRDAGPlan();
    mrrDag = new DAGImpl(mrrDagId, conf, mrrDagPlan,
        dispatcher.getEventHandler(), taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh,
        mrrAppContext);
    mrrDag.entityUpdateTracker = new StateChangeNotifierForTest(mrrDag);
    doReturn(conf).when(mrrAppContext).getAMConf();
    doReturn(mrrDag).when(mrrAppContext).getCurrentDAG();
    doReturn(appAttemptId).when(mrrAppContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(mrrAppContext).getApplicationID();
    doReturn(historyEventHandler).when(mrrAppContext).getHistoryHandler();
    doReturn(clusterInfo).when(mrrAppContext).getClusterInfo();
    groupAppContext = mock(AppContext.class);
    doReturn(aclManager).when(groupAppContext).getAMACLManager();
    doReturn(execService).when(groupAppContext).getExecService();
    doReturn(defaultShim).when(groupAppContext).getHadoopShim();

    groupDagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 3);
    groupDagPlan = createGroupDAGPlan();
    groupDag = new DAGImpl(groupDagId, conf, groupDagPlan,
        dispatcher.getEventHandler(), taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh,
        groupAppContext);
    groupDag.entityUpdateTracker = new StateChangeNotifierForTest(groupDag);
    doReturn(conf).when(groupAppContext).getAMConf();
    doReturn(groupDag).when(groupAppContext).getCurrentDAG();
    doReturn(appAttemptId).when(groupAppContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId())
        .when(groupAppContext).getApplicationID();
    doReturn(historyEventHandler).when(groupAppContext).getHistoryHandler();
    doReturn(clusterInfo).when(groupAppContext).getClusterInfo();

    // reset totalCommitCounter to 0
    TotalCountingOutputCommitter.totalCommitCounter = 0;
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
    dispatcher.await();
    dispatcher.stop();
    execService.shutdownNow();
    dagPlan = null;
    if (dag != null) {
      dag.entityUpdateTracker.stop();
    }
    if (mrrDag != null) {
      mrrDag.entityUpdateTracker.stop();
    }
    if (groupDag != null) {
      groupDag.entityUpdateTracker.stop();
    }
    if (dagWithCustomEdge != null) {
      dagWithCustomEdge.entityUpdateTracker.stop();
    }
    dag = null;
    mrrDag = null;
    groupDag = null;
    dagWithCustomEdge = null;
  }

  private class AMSchedulerEventHandler implements EventHandler<AMSchedulerEvent> {
    @Override
    public void handle(AMSchedulerEvent event) {
      // do nothing
    }
  }

  private void setupDAGWithCustomEdge(ExceptionLocation exLocation) {
    setupDAGWithCustomEdge(exLocation, false);
  }
  
  private void setupDAGWithCustomEdge(ExceptionLocation exLocation, boolean useLegacy) {
    dagWithCustomEdgeId =  TezDAGID.getInstance(appAttemptId.getApplicationId(), 4);
    dagPlanWithCustomEdge = createDAGWithCustomEdge(exLocation, useLegacy);
    dagWithCustomEdgeAppContext = mock(AppContext.class);
    doReturn(aclManager).when(dagWithCustomEdgeAppContext).getAMACLManager();
    when(dagWithCustomEdgeAppContext.getHadoopShim()).thenReturn(defaultShim);
    dagWithCustomEdge = new DAGImpl(dagWithCustomEdgeId, conf, dagPlanWithCustomEdge,
        dispatcher.getEventHandler(), taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh, dagWithCustomEdgeAppContext);
    dagWithCustomEdge.entityUpdateTracker = new StateChangeNotifierForTest(dagWithCustomEdge);
    doReturn(conf).when(dagWithCustomEdgeAppContext).getAMConf();
    doReturn(execService).when(dagWithCustomEdgeAppContext).getExecService();
    doReturn(dagWithCustomEdge).when(dagWithCustomEdgeAppContext).getCurrentDAG();
    doReturn(appAttemptId).when(dagWithCustomEdgeAppContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(dagWithCustomEdgeAppContext).getApplicationID();
    doReturn(historyEventHandler).when(dagWithCustomEdgeAppContext).getHistoryHandler();
    doReturn(clusterInfo).when(dagWithCustomEdgeAppContext).getClusterInfo();
    dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDisptacher2());
    dispatcher.register(AMSchedulerEventType.class, new AMSchedulerEventHandler());
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
  public void testDAGInitFailed() {
    setupDAGWithCustomEdge(ExceptionLocation.Initialize);
    dagWithCustomEdge.handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    Assert.assertEquals(DAGState.FAILED, dagWithCustomEdge.getState());
    // START event is followed after INIT event
    dagWithCustomEdge.handle(new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_START));
    dispatcher.await();
    Assert.assertEquals(DAGState.FAILED, dagWithCustomEdge.getState());
  }

  @Test(timeout = 5000)
  public void testDAGInitFailedDuetoInvalidResource() {
    // cluster maxContainerCapability is less than the vertex resource request
    ClusterInfo clusterInfo = new ClusterInfo(Resource.newInstance(512,10));
    doReturn(clusterInfo).when(appContext).getClusterInfo();
    dag.handle(
        new DAGEvent(dag.getID(), DAGEventType.DAG_INIT));
    dispatcher.await();
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.INIT_FAILURE, dag.getTerminationCause());
    Assert.assertTrue(StringUtils.join(dag.getDiagnostics(), ",")
        .contains("Vertex's TaskResource is beyond the cluster container capability"));
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

  @Test(timeout = 5000)
  public void testNonExistEdgeManagerPlugin() {
    dagPlan = createDAGWithNonExistEdgeManager();
    dag = new DAGImpl(dagId, conf, dagPlan,
        dispatcher.getEventHandler(),  taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh, appContext);
    dag.entityUpdateTracker = new StateChangeNotifierForTest(dag);
    doReturn(dag).when(appContext).getCurrentDAG();

    dag.handle(new DAGEvent(dagId, DAGEventType.DAG_INIT));
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.INIT_FAILURE, dag.getTerminationCause());
    Assert.assertTrue(StringUtils.join(dag.getDiagnostics(), "")
        .contains("java.lang.ClassNotFoundException: non-exist-edge-manager"));
  }

  @Test (timeout = 5000)
  public void testNonExistDAGScheduler() {
    conf.set(TezConfiguration.TEZ_AM_DAG_SCHEDULER_CLASS, "non-exist-dag-scheduler");
    dag = new DAGImpl(dagId, conf, dagPlan,
        dispatcher.getEventHandler(),  taskCommunicatorManagerInterface,
        fsTokens, clock, "user", thh, appContext);
    dag.entityUpdateTracker = new StateChangeNotifierForTest(dag);
    doReturn(dag).when(appContext).getCurrentDAG();

    dag.handle(new DAGEvent(dag.getID(), DAGEventType.DAG_INIT));
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGState.FAILED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.INIT_FAILURE, dag.getTerminationCause());
    Assert.assertTrue(StringUtils.join(dag.getDiagnostics(), "")
        .contains("java.lang.ClassNotFoundException: non-exist-dag-scheduler"));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexCompletion() {
    initDAG(dag);
    Assert.assertTrue(0.0f == dag.getCompletedTaskProgress());
    startDAG(dag);
    Assert.assertTrue(0.0f == dag.getCompletedTaskProgress());
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

    // 2 tasks completed, total plan has 11 vertices
    Assert.assertEquals((float) 2 / 11,
        dag.getCompletedTaskProgress(), 0.05);
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_GetNumDestinationTaskPhysicalInputs() {
    setupDAGWithCustomEdge(ExceptionLocation.GetNumDestinationTaskPhysicalInputs);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(),
        null));
    dispatcher.await();

    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.GetNumDestinationTaskPhysicalInputs.name()));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_GetNumSourceTaskPhysicalOutputs() {
    setupDAGWithCustomEdge(ExceptionLocation.GetNumSourceTaskPhysicalOutputs);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(),
        null));
    dispatcher.await();
    // After TEZ-1711, all task attempts of v1 fail which result in task fail, and finally
    // dag failed.
    Assert.assertEquals(DAGState.FAILED, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    String diag = StringUtils.join(v1.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.GetNumSourceTaskPhysicalOutputs.name()));
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_RouteDataMovementEventToDestination() {
    setupDAGWithCustomEdge(ExceptionLocation.RouteDataMovementEventToDestination);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(), 
        null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");
    dispatcher.await();
    Task t1= v2.getTask(0);
    TaskAttemptImpl ta1= (TaskAttemptImpl)t1.getAttempt(TezTaskAttemptID.getInstance(t1.getTaskId(), 0));

    DataMovementEvent daEvent = DataMovementEvent.create(ByteBuffer.wrap(new byte[0]));
    TezEvent tezEvent = new TezEvent(daEvent, 
        new EventMetaData(EventProducerConsumerType.INPUT, "vertex1", "vertex2", ta1.getID()));
    dispatcher.getEventHandler().handle(new VertexEventRouteEvent(v2.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();
    v2.getTaskAttemptTezEvents(ta1.getID(), 0, 0, 1000);
    dispatcher.await();

    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.RouteDataMovementEventToDestination.name()));
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_RouteDataMovementEventToDestinationWithLegacyRouting() {
    // Remove after legacy routing is removed
    setupDAGWithCustomEdge(ExceptionLocation.RouteDataMovementEventToDestination, true);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(),
        null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");

    dispatcher.await();
    Task t1= v2.getTask(0);
    TaskAttemptImpl ta1= (TaskAttemptImpl)t1.getAttempt(TezTaskAttemptID.getInstance(t1.getTaskId(), 0));

    DataMovementEvent daEvent = DataMovementEvent.create(ByteBuffer.wrap(new byte[0]));
    TezEvent tezEvent = new TezEvent(daEvent, 
        new EventMetaData(EventProducerConsumerType.INPUT, "vertex1", "vertex2", ta1.getID()));
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v2.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();

    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.RouteDataMovementEventToDestination.name()));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_RouteInputSourceTaskFailedEventToDestinationLegacyRouting() {
    // Remove after legacy routing is removed
    setupDAGWithCustomEdge(ExceptionLocation.RouteInputSourceTaskFailedEventToDestination, true);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(), 
        null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");
    dispatcher.await();

    Task t1= v2.getTask(0);
    TaskAttemptImpl ta1= (TaskAttemptImpl)t1.getAttempt(TezTaskAttemptID.getInstance(t1.getTaskId(), 0));
    InputFailedEvent ifEvent = InputFailedEvent.create(0, 1);
    TezEvent tezEvent = new TezEvent(ifEvent, 
        new EventMetaData(EventProducerConsumerType.INPUT,"vertex1", "vertex2", ta1.getID()));
    dispatcher.getEventHandler().handle(new VertexEventRouteEvent(v2.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();
    v2.getTaskAttemptTezEvents(ta1.getID(), 0, 0, 1000);
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.RouteInputSourceTaskFailedEventToDestination.name()));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_GetNumDestinationConsumerTasks() {
    setupDAGWithCustomEdge(ExceptionLocation.GetNumDestinationConsumerTasks);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(),
        null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");
    dispatcher.await();

    Task t1= v2.getTask(0);
    TaskAttemptImpl ta1= (TaskAttemptImpl)t1.getAttempt(TezTaskAttemptID.getInstance(t1.getTaskId(), 0));

    InputReadErrorEvent ireEvent = InputReadErrorEvent.create("", 0, 0);
    TezEvent tezEvent = new TezEvent(ireEvent, 
        new EventMetaData(EventProducerConsumerType.INPUT,"vertex2", "vertex1", ta1.getID()));
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v2.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();
    // 
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.GetNumDestinationConsumerTasks.name()));
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEdgeManager_RouteInputErrorEventToSource() {
    setupDAGWithCustomEdge(ExceptionLocation.RouteInputErrorEventToSource);
    dispatcher.getEventHandler().handle(
        new DAGEvent(dagWithCustomEdge.getID(), DAGEventType.DAG_INIT));
    dispatcher.getEventHandler().handle(new DAGEventStartDag(dagWithCustomEdge.getID(), 
        null));
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dagWithCustomEdge.getState());

    VertexImpl v1 = (VertexImpl)dagWithCustomEdge.getVertex("vertex1");
    VertexImpl v2 = (VertexImpl)dagWithCustomEdge.getVertex("vertex2");
    dispatcher.await();

    Task t1= v2.getTask(0);
    TaskAttemptImpl ta1= (TaskAttemptImpl)t1.getAttempt(TezTaskAttemptID.getInstance(t1.getTaskId(), 0));
    InputReadErrorEvent ireEvent = InputReadErrorEvent.create("", 0, 0);
    TezEvent tezEvent = new TezEvent(ireEvent, 
        new EventMetaData(EventProducerConsumerType.INPUT,"vertex2", "vertex1", ta1.getID()));
    dispatcher.getEventHandler().handle(new VertexEventRouteEvent(v2.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();
    // 
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(VertexState.KILLED, v1.getState());
    String diag = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diag.contains(ExceptionLocation.RouteInputErrorEventToSource.name()));
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
    Assert.assertTrue(1.0f == groupDag.getCompletedTaskProgress());
    Assert.assertEquals(DAGState.SUCCEEDED, groupDag.getState());
    Assert.assertEquals(2, TotalCountingOutputCommitter.totalCommitCounter);
  }  

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGroupDAGWithVertexReRunning() {
    groupDag.getConf().setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false);
    initDAG(groupDag);
    startDAG(groupDag);
    dispatcher.await();

    Vertex v1 = groupDag.getVertex("vertex1");
    Vertex v2 = groupDag.getVertex("vertex2");
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(v1.getVertexId(), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexReRunning(v1.getVertexId()));
    dispatcher.getEventHandler().handle(
        new DAGEventVertexCompleted(v2.getVertexId(), VertexState.SUCCEEDED));
    dispatcher.await();
    // commit should not happen due to vertex-rerunning
    Assert.assertEquals(0, TotalCountingOutputCommitter.totalCommitCounter);

    dispatcher.getEventHandler().handle(
        new DAGEventVertexCompleted(v1.getVertexId(), VertexState.SUCCEEDED));
    dispatcher.await();
    // commit happen
    Assert.assertEquals(1, TotalCountingOutputCommitter.totalCommitCounter);
    Assert.assertEquals(2, groupDag.getSuccessfulVertices());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testGroupDAGWithVertexReRunningAfterCommit() {
    groupDag.getConf().setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false);
    initDAG(groupDag);
    startDAG(groupDag);
    dispatcher.await();

    Vertex v1 = groupDag.getVertex("vertex1");
    Vertex v2 = groupDag.getVertex("vertex2");
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(v1.getVertexId(), VertexState.SUCCEEDED));
    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(v2.getVertexId(), VertexState.SUCCEEDED));
    dispatcher.await();
    // vertex group commit happens
    Assert.assertEquals(1, TotalCountingOutputCommitter.totalCommitCounter);

    // dag failed when vertex re-run happens after vertex group commit is done.
    dispatcher.getEventHandler().handle(new DAGEventVertexReRunning(v1.getVertexId()));
    dispatcher.await();
    Assert.assertEquals(DAGState.FAILED, groupDag.getState());
    Assert.assertEquals(DAGTerminationCause.VERTEX_RERUN_AFTER_COMMIT, groupDag.getTerminationCause());
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
                .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                    .setUserPayload(
                        ByteString
                            .copyFrom(new CountingOutputCommitter.CountingOutputCommitterConfig(
                                true, false, false).toUserPayload())).build()))
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
    mrrDag.getConf().setBoolean(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false);
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
    
    // error on vertex -> dag error
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
          // abort operation should take no side effort on the successful commit
          Assert.assertEquals(1, committer.abortCounter);
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
  @Test(timeout = 10000)
  public void testGetDAGStatusWithWait() throws TezException {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    // All vertices except one succeed
    for (int i = 0; i < dag.getVertices().size() - 1; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    Assert.assertEquals(5, dag.getSuccessfulVertices());

    long dagStatusStartTime = System.currentTimeMillis();
    DAGStatusBuilder dagStatus = dag.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000l);
    long dagStatusEndTime = System.currentTimeMillis();
    long diff = dagStatusEndTime - dagStatusStartTime;
    Assert.assertTrue(diff > 1500 && diff < 2500);
    Assert.assertEquals(DAGStatusBuilder.State.RUNNING, dagStatus.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 20000)
  public void testGetDAGStatusReturnOnDagSucceeded() throws InterruptedException, TezException {
    runTestGetDAGStatusReturnOnDagFinished(DAGStatus.State.SUCCEEDED);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 20000)
  public void testGetDAGStatusReturnOnDagFailed() throws InterruptedException, TezException {
    runTestGetDAGStatusReturnOnDagFinished(DAGStatus.State.FAILED);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 20000)
  public void testGetDAGStatusReturnOnDagKilled() throws InterruptedException, TezException {
    runTestGetDAGStatusReturnOnDagFinished(DAGStatus.State.KILLED);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 20000)
  public void testGetDAGStatusReturnOnDagError() throws InterruptedException, TezException {
    runTestGetDAGStatusReturnOnDagFinished(DAGStatus.State.ERROR);
  }


  @SuppressWarnings("unchecked")
  public void runTestGetDAGStatusReturnOnDagFinished(DAGStatusBuilder.State testState) throws TezException, InterruptedException {
    initDAG(dag);
    startDAG(dag);
    dispatcher.await();

    // All vertices except one succeed
    for (int i = 0; i < dag.getVertices().size() - 1; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, 0), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    Assert.assertEquals(DAGState.RUNNING, dag.getState());
    Assert.assertEquals(5, dag.getSuccessfulVertices());

    ReentrantLock lock = new ReentrantLock();
    Condition startCondition = lock.newCondition();
    Condition endCondition = lock.newCondition();
    DagStatusCheckRunnable statusCheckRunnable =
        new DagStatusCheckRunnable(lock, startCondition, endCondition);
    Thread t1 = new Thread(statusCheckRunnable);
    t1.start();
    lock.lock();
    try {
      while (!statusCheckRunnable.started.get()) {
        startCondition.await();
      }
    } finally {
      lock.unlock();
    }

    // Sleep for 2 seconds. Then mark the last vertex is successful.
    Thread.sleep(2000l);
    if (testState == DAGStatus.State.SUCCEEDED) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, 5), VertexState.SUCCEEDED));
    } else if (testState == DAGStatus.State.FAILED) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, 5), VertexState.FAILED));
    } else if (testState == DAGStatus.State.KILLED) {
      dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    } else if (testState == DAGStatus.State.ERROR) {
      dispatcher.getEventHandler().handle(new DAGEventStartDag(dagId, new LinkedList<URL>()));
    } else {
      throw new UnsupportedOperationException("Unsupported state for test: " + testState);
    }
    dispatcher.await();

    // Wait for the dag status to return
    lock.lock();
    try {
      while (!statusCheckRunnable.ended.get()) {
        endCondition.await();
      }
    } finally {
      lock.unlock();
    }

    long diff = statusCheckRunnable.dagStatusEndTime - statusCheckRunnable.dagStatusStartTime;
    Assert.assertNotNull(statusCheckRunnable.dagStatus);
    Assert.assertTrue(diff > 1000 && diff < 3500);
    Assert.assertEquals(testState, statusCheckRunnable.dagStatus.getState());
    t1.join();
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

  // Couple of vertices succeed. DAG_KILLED processed, which causes the rest of the vertices to be
  // marked as KILLED.
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
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert.assertEquals(DAGTerminationCause.DAG_KILL, dag.getTerminationCause());
    Assert.assertEquals(2, dag.getSuccessfulVertices());

    int killedCount = 0;
    for (Map.Entry<TezVertexID, Vertex> vEntry : dag.getVertices().entrySet()) {
      if (vEntry.getValue().getState() == VertexState.KILLED) {
        killedCount++;
      }
    }
    Assert.assertEquals(4, killedCount);

    for (Vertex v : dag.getVertices().values()) {
      Assert.assertEquals(VertexTerminationCause.DAG_KILL, v.getTerminationCause());
    }

    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  // Vertices succeed after a DAG kill has been processed. Should be ignored.
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testDAGKillVertexSuccessAfterKill() {
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
    dispatcher.await();

    Assert.assertEquals(DAGState.KILLED, dag.getState());

    // Vertex SUCCESS gets processed after the DAG has reached the KILLED state. Should be ignored.
    for (int i = 2; i < 6; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();

    int killedCount = 0;
    for (Map.Entry<TezVertexID, Vertex> vEntry : dag.getVertices().entrySet()) {
      if (vEntry.getValue().getState() == VertexState.KILLED) {
        killedCount++;
      }
    }
    Assert.assertEquals(4, killedCount);

    Assert.assertEquals(DAGTerminationCause.DAG_KILL, dag.getTerminationCause());
    Assert.assertEquals(2, dag.getSuccessfulVertices());
    for (Vertex v : dag.getVertices().values()) {
      Assert.assertEquals(VertexTerminationCause.DAG_KILL, v.getTerminationCause());
    }
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  // Vertex KILLED after a DAG_KILLED is issued. Termination reason should be DAG_KILLED
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

    for (int i = 2; i < 5; ++i) {
      dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
          TezVertexID.getInstance(dagId, i), VertexState.SUCCEEDED));
    }
    dispatcher.await();
    dispatcher.getEventHandler().handle(new DAGEvent(dagId, DAGEventType.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());

    dispatcher.getEventHandler().handle(new DAGEventVertexCompleted(
        TezVertexID.getInstance(dagId, 5), VertexState.KILLED));
    dispatcher.await();
    Assert.assertEquals(DAGState.KILLED, dag.getState());
    Assert.assertEquals(5, dag.getSuccessfulVertices());
    Assert.assertEquals(dag.getVertex(TezVertexID.getInstance(dagId, 5)).getTerminationCause(),
        VertexTerminationCause.DAG_KILL);
    Assert.assertEquals(1, dagFinishEventHandler.dagFinishEvents);
  }

  @Test(timeout = 5000)
  public void testConfiguration() throws AMUserCodeException {
    initDAG(dag);
    // dag override the default configuration
    Assert.assertEquals(3, dag.getConf().getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
        TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT));
    Vertex v1 = dag.getVertex("vertex1");
    Vertex v2 = dag.getVertex("vertex2");
    // v1 override the dagConfiguration
    Assert.assertEquals(2, v1.getConf().getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
        TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT));
    // v2 inherit the configuration from dag
    Assert.assertEquals(3, v2.getConf().getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS,
        TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT));
  }

  public static enum ExceptionLocation {
    Initialize,
    GetNumDestinationTaskPhysicalInputs,
    GetNumSourceTaskPhysicalOutputs,
    RouteDataMovementEventToDestination,
    RouteInputSourceTaskFailedEventToDestination,
    GetNumDestinationConsumerTasks,
    RouteInputErrorEventToSource
  }

  public static class CustomizedEdgeManagerLegacy extends EdgeManagerPlugin {
    private ExceptionLocation exLocation;

    public static EdgeManagerPluginDescriptor getUserPayload(ExceptionLocation exLocation) {
      return EdgeManagerPluginDescriptor.create(CustomizedEdgeManager.class.getName())
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(exLocation.name().getBytes())));
    }

    public CustomizedEdgeManagerLegacy(EdgeManagerPluginContext context) {
      super(context);
      this.exLocation = ExceptionLocation.valueOf(
          new String(context.getUserPayload().deepCopyAsArray()));
    }

    @Override
    public void initialize() throws Exception {
      if (exLocation == ExceptionLocation.Initialize) {
        throw new Exception(exLocation.name());
      }
    }

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumDestinationTaskPhysicalInputs) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumSourceTaskPhysicalOutputs) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public void routeDataMovementEventToDestination(DataMovementEvent event,
        int sourceTaskIndex, int sourceOutputIndex,
        Map<Integer, List<Integer>> destinationTaskAndInputIndices)
        throws Exception {
      if (exLocation == ExceptionLocation.RouteDataMovementEventToDestination) {
        throw new Exception(exLocation.name());
      }
    }

    @Override
    public void routeInputSourceTaskFailedEventToDestination(
        int sourceTaskIndex,
        Map<Integer, List<Integer>> destinationTaskAndInputIndices)
        throws Exception {
      if (exLocation == ExceptionLocation.RouteInputSourceTaskFailedEventToDestination) {
        throw new Exception(exLocation.name());
      }
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumDestinationConsumerTasks) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public int routeInputErrorEventToSource(InputReadErrorEvent event,
        int destinationTaskIndex, int destinationFailedInputIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.RouteInputErrorEventToSource) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }
  }

  public static class CustomizedEdgeManager extends EdgeManagerPluginOnDemand {
    private ExceptionLocation exLocation;

    public static EdgeManagerPluginDescriptor getUserPayload(ExceptionLocation exLocation) {
      return EdgeManagerPluginDescriptor.create(CustomizedEdgeManager.class.getName())
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(exLocation.name().getBytes())));
    }

    public CustomizedEdgeManager(EdgeManagerPluginContext context) {
      super(context);
      this.exLocation = ExceptionLocation.valueOf(
          new String(context.getUserPayload().deepCopyAsArray()));
    }

    @Override
    public void initialize() throws Exception {
      if (exLocation == ExceptionLocation.Initialize) {
        throw new Exception(exLocation.name());
      }
    }

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumDestinationTaskPhysicalInputs) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumSourceTaskPhysicalOutputs) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.GetNumDestinationConsumerTasks) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public int routeInputErrorEventToSource(int destinationTaskIndex,
        int destinationFailedInputIndex) throws Exception {
      if (exLocation == ExceptionLocation.RouteInputErrorEventToSource) {
        throw new Exception(exLocation.name());
      }
      return 0;
    }

    @Override
    public EventRouteMetadata routeDataMovementEventToDestination(int sourceTaskIndex,
        int sourceOutputIndex, int destinationTaskIndex) throws Exception {
      if (exLocation == ExceptionLocation.RouteDataMovementEventToDestination) {
        throw new Exception(exLocation.name());
      }
      return null;
    }

    @Override
    public EventRouteMetadata routeCompositeDataMovementEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex)
        throws Exception {
      if (exLocation == ExceptionLocation.RouteDataMovementEventToDestination) {
        throw new Exception(exLocation.name());
      }
      return null;
    }

    @Override
    public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex) throws Exception {
      if (exLocation == ExceptionLocation.RouteInputSourceTaskFailedEventToDestination) {
        throw new Exception(exLocation.name());
      }
      return null;
    }

    @Override
    public void prepareForRouting() throws Exception {
    }
  }


  // Specificially for testGetDAGStatusReturnOnDagSuccess
  private class DagStatusCheckRunnable implements Runnable {

    private volatile DAGStatusBuilder dagStatus;
    private volatile long dagStatusStartTime = -1;
    private volatile long dagStatusEndTime = -1;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean ended = new AtomicBoolean(false);

    private final ReentrantLock lock;
    private final Condition startCondition;
    private final Condition endCondition;

    public DagStatusCheckRunnable(ReentrantLock lock,
                                  Condition startCondition,
                                  Condition endCondition) {
      this.lock = lock;
      this.startCondition = startCondition;
      this.endCondition = endCondition;
    }

    @Override
    public void run() {
      started.set(true);
      lock.lock();
      try {
        startCondition.signal();
      } finally {
        lock.unlock();
      }
      try {
        dagStatusStartTime = System.currentTimeMillis();
        dagStatus = dag.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 10000l);
        dagStatusEndTime = System.currentTimeMillis();
      } catch (TezException e) {

      }
      lock.lock();
      ended.set(true);
      try {
        endCondition.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testCounterLimits() {
    initDAG(mrrDag);
    dispatcher.await();
    startDAG(mrrDag);
    dispatcher.await();
    for (int i=0; i<3; ++i) {
      Vertex v = mrrDag.getVertex("vertex"+(i+1));
      dispatcher.getEventHandler().handle(new VertexEventTaskCompleted(
          TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
      TezCounters ctrs = new TezCounters();
      for (int j = 0; j < 50; ++j) {
        ctrs.findCounter("g", "c" + i + "_" + j).increment(1);
      }
      ((VertexImpl) v).setCounters(ctrs);
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
      Assert.assertEquals(i+1, mrrDag.getSuccessfulVertices());
    }

    Assert.assertEquals(3, mrrDag.getSuccessfulVertices());
    Assert.assertEquals(DAGState.FAILED, mrrDag.getState());
    Assert.assertTrue("Diagnostics should contain counter limits error message",
        StringUtils.join(mrrDag.getDiagnostics(), ",").contains("Counters limit exceeded"));

  }

}

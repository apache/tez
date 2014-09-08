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

import java.nio.ByteBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanGroupInputEdgeInfo;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexGroupInfo;
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
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.RootInputInitializerManager;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputFailed;
import org.apache.tez.dag.app.dag.event.VertexEventRootInputInitialized;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.VertexEventTaskAttemptCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskCompleted;
import org.apache.tez.dag.app.dag.event.VertexEventTaskReschedule;
import org.apache.tez.dag.app.dag.event.VertexEventTermination;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl.VertexGroupInfo;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.InputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.test.EdgeManagerForTest;
import org.apache.tez.test.VertexManagerPluginForTest;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestVertexImpl {

  private static final Log LOG = LogFactory.getLog(TestVertexImpl.class);

  private boolean useCustomInitializer = false;
  private InputInitializer customInitializer = null;
  
  private TezDAGID dagId;
  private ApplicationAttemptId appAttemptId;
  private DAGPlan dagPlan;
  private DAGPlan invalidDagPlan;
  private Map<String, VertexImpl> vertices;
  private Map<TezVertexID, VertexImpl> vertexIdMap;
  private DrainDispatcher dispatcher;
  private TaskAttemptListener taskAttemptListener;
  private Clock clock = new SystemClock();
  private TaskHeartbeatHandler thh;
  private AppContext appContext;
  private VertexLocationHint vertexLocationHint = null;
  private Configuration conf;
  private Map<String, Edge> edges;
  private Map<String, VertexGroupInfo> vertexGroups;
  private byte[] edgePayload = "EP".getBytes();

  private TaskAttemptEventDispatcher taskAttemptEventDispatcher;
  private TaskEventDispatcher taskEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private DagEventDispatcher dagEventDispatcher;
  private HistoryEventHandler historyEventHandler;
  private StateChangeNotifier updateTracker;
  private static TaskSpecificLaunchCmdOption taskSpecificLaunchCmdOption;

  public static class CountingOutputCommitter extends OutputCommitter {

    public int initCounter = 0;
    public int setupCounter = 0;
    public int commitCounter = 0;
    public int abortCounter = 0;
    private boolean throwError;
    private boolean throwErrorOnAbort;
    private boolean throwRuntimeException;

    public CountingOutputCommitter(OutputCommitterContext context) {
      super(context);
      this.throwError = false;
      this.throwErrorOnAbort = false;
      this.throwRuntimeException = false;
    }

    @Override
    public void initialize() throws IOException {
      if (getContext().getUserPayload().hasPayload()) {
        CountingOutputCommitterConfig conf =
            new CountingOutputCommitterConfig(getContext().getUserPayload());
        this.throwError = conf.throwError;
        this.throwErrorOnAbort = conf.throwErrorOnAbort;
        this.throwRuntimeException = conf.throwRuntimeException;
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
      if (throwError) {
        if (!throwRuntimeException) {
          throw new IOException("I can throwz exceptions in commit");
        } else {
          throw new RuntimeException("I can throwz exceptions in commit");
        }
      }
    }

    @Override
    public void abortOutput(VertexStatus.State finalState) throws IOException {
      ++abortCounter;
      if (throwErrorOnAbort) {
        if (!throwRuntimeException) {
          throw new IOException("I can throwz exceptions in abort");
        } else {
          throw new RuntimeException("I can throwz exceptions in abort");
        }
      }
    }

    public static class CountingOutputCommitterConfig implements Writable {

      boolean throwError = false;
      boolean throwErrorOnAbort = false;
      boolean throwRuntimeException = false;

      public CountingOutputCommitterConfig() {
      }

      public CountingOutputCommitterConfig(boolean throwError,
          boolean throwErrorOnAbort, boolean throwRuntimeException) {
        this.throwError = throwError;
        this.throwErrorOnAbort = throwErrorOnAbort;
        this.throwRuntimeException = throwRuntimeException;
      }

      public CountingOutputCommitterConfig(UserPayload payload) throws IOException {
        DataInputByteBuffer in  = new DataInputByteBuffer();
        in.reset(payload.getPayload());
        this.readFields(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        out.writeBoolean(throwError);
        out.writeBoolean(throwErrorOnAbort);
        out.writeBoolean(throwRuntimeException);
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        throwError = in.readBoolean();
        throwErrorOnAbort = in.readBoolean();
        throwRuntimeException = in.readBoolean();
      }

      public byte[] toUserPayload() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(bos);
        write(out);
        return bos.toByteArray();
      }

    }

  }
  
  private class TaskAttemptEventDispatcher implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      VertexImpl vertex = vertexIdMap.get(
        event.getTaskAttemptID().getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskAttemptID().getTaskID());
      ((EventHandler<TaskAttemptEvent>)task.getAttempt(
          event.getTaskAttemptID())).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      VertexImpl vertex = vertexIdMap.get(event.getTaskID().getVertexID());
      Task task = vertex.getTask(event.getTaskID());
      if (task != null) {
        ((EventHandler<TaskEvent>)task).handle(event);
      } else {
        LOG.warn("Task null for vertex: " + vertex.getName() + " taskId: " +
            event.getTaskID() + ". Please check if this is important for the test");
      }
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
        .build()
        )
        .build();
    return dag;
  }
  
  private DAGPlan createDAGPlanWithMixedEdges() {
    LOG.info("Setting up mixed edge dag plan");
    org.apache.tez.dag.api.DAG dag = org.apache.tez.dag.api.DAG.create("MixedEdges");
    org.apache.tez.dag.api.Vertex v1 = org.apache.tez.dag.api.Vertex.create("vertex1",
        ProcessorDescriptor.create("v1.class"), 1, Resource.newInstance(0, 0));
    org.apache.tez.dag.api.Vertex v2 = org.apache.tez.dag.api.Vertex.create("vertex2",
        ProcessorDescriptor.create("v2.class"), 1, Resource.newInstance(0, 0));
    org.apache.tez.dag.api.Vertex v3 = org.apache.tez.dag.api.Vertex.create("vertex3",
        ProcessorDescriptor.create("v3.class"), 1, Resource.newInstance(0, 0));
    org.apache.tez.dag.api.Vertex v4 = org.apache.tez.dag.api.Vertex.create("vertex4",
        ProcessorDescriptor.create("v4.class"), 1, Resource.newInstance(0, 0));
    org.apache.tez.dag.api.Vertex v5 = org.apache.tez.dag.api.Vertex.create("vertex5",
        ProcessorDescriptor.create("v5.class"), 1, Resource.newInstance(0, 0));
    org.apache.tez.dag.api.Vertex v6 = org.apache.tez.dag.api.Vertex.create("vertex6",
        ProcessorDescriptor.create("v6.class"), 1, Resource.newInstance(0, 0));
    dag.addVertex(v1).addVertex(v2).addVertex(v3).addVertex(v4).addVertex(v5).addVertex(v6);
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.BROADCAST, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v1, v3,
        EdgeProperty.create(DataMovementType.BROADCAST, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v4, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v5, v3,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v4, v6,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
    dag.addEdge(org.apache.tez.dag.api.Edge.create(v5, v6,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE, DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, OutputDescriptor.create("out.class"),
            InputDescriptor.create("out.class"))));
   
    return dag.createDag(conf);
  }

  private DAGPlan createDAGPlanWithInitializer0Tasks(String initializerClassName) {
    LOG.info("Setting up dag plan with input initializer and 0 tasks");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("initializerWith0Tasks")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                        .setName("input1")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("InputClazz")
                                .build()
                        ).build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
                        .build()
                )
                .addInEdgeId("e1")
                .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex2")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x2.y2")
                        .build()
                )
                .addOutEdgeId("e1")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                .setInputVertexName("vertex2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex1")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .build();
    return dag;
  }

  private DAGPlan createDAGPlanWithInputInitializer(String initializerClassName) {
    LOG.info("Setting up dag plan with input initializer");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testVertexWithInitializer")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                    .setName("input1")
                    .setIODescriptor(
                        TezEntityDescriptorProto.newBuilder()
                            .setClassName("InputClazz")
                            .build()
                    ).build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(-1)
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
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                        .setName("input2")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                              .setClassName("InputClazz")
                              .build()
                        )
                        .build()
                    )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x2.y2")
                        .build()
                )
                .addInEdgeId("e1")
              .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex3")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                        .setName("input3")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                              .setClassName("InputClazz")
                              .build()
                        )
                        .build()
                    )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x3.y3")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(RootInputSpecUpdaterVertexManager.class.getName())
                    .setUserPayload(ByteString.copyFrom(new byte[] {0})))
              .build()
        )
                .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex4")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                        .setName("input4")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                              .setClassName("InputClazz")
                              .build()
                        )
                        .build()
                    )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x3.y3")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(RootInputSpecUpdaterVertexManager.class.getName())
                    .setUserPayload(ByteString.copyFrom(new byte[] {1})))
              .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex2")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
            )
        .build();
    return dag;
  }

  private DAGPlan createDAGPlanWithRunningInitializer() {
    LOG.info("Setting up dag plan with running input initializer");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("DagWithInputInitializer2")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(10)
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
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                "IrrelevantInitializerClassName"))
                        .setName("input1")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("InputClazz")
                                .build()
                        )
                        .build()
                )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(20)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x2.y2")
                        .build()
                )
                .addInEdgeId("e1")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex2")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .build();
    return dag;
  }
  
  private DAGPlan createDAGPlanWithInputDistributor(String initializerClassName) {
    LOG.info("Setting up invalid dag plan with input distributor");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testVertexWithInitializer")
        .addVertex( // simulates split distribution with known number of tasks
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
                        .setName("input1")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                              .setClassName("InputClazz")
                              .build()
                        )
                        .build()
                    )
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(2)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x3.y3")
                        .build()
                )
                .addOutEdgeId("e1")
              .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex2")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(2)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x1.y1")
                    .build()
                )
                .addInEdgeId("e1")
            .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v5"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
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
  
  private DAGPlan createDAGPlanForOneToOneSplit(String initializerClassName, 
      int numTasks, boolean addNullEdge) {
    VertexPlan.Builder v1Builder = VertexPlan.newBuilder();
    v1Builder.setName("vertex1")
    .setType(PlanVertexType.NORMAL)
    .addOutEdgeId("e1")
    .addOutEdgeId("e2");
    if (addNullEdge) {
      v1Builder.addOutEdgeId("e5");
    }
    if (initializerClassName != null) {
      numTasks = -1;
      v1Builder.addInputs(
          RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                initializerClassName))
          .setName("input1")
          .setIODescriptor(
              TezEntityDescriptorProto.newBuilder()
                  .setClassName("InputClazz")
                  .build()
          ).build()
      ).setTaskConfig(
          PlanTaskConfiguration.newBuilder()
          .setNumTasks(-1)
          .setVirtualCores(4)
          .setMemoryMb(1024)
          .setJavaOpts("")
          .setTaskModule("x1.y1")
          .build()
      );
    } else {
      v1Builder.setTaskConfig(
          PlanTaskConfiguration.newBuilder()
          .setNumTasks(numTasks)
          .setVirtualCores(4)
          .setMemoryMb(1024)
          .setJavaOpts("")
          .setTaskModule("x1.y1")
          .build()
      );
    }
    VertexPlan v1Plan = v1Builder.build();
    
    VertexPlan.Builder v4Builder = VertexPlan.newBuilder();
    v4Builder
    .setName("vertex4")
    .setType(PlanVertexType.NORMAL)
    .setTaskConfig(
        PlanTaskConfiguration.newBuilder()
        .setNumTasks(numTasks)
        .setVirtualCores(4)
        .setMemoryMb(1024)
        .setJavaOpts("")
        .setTaskModule("x4.y4")
        .build()
    )
    .addInEdgeId("e3")
    .addInEdgeId("e4");
    if (addNullEdge) {
      v4Builder.addOutEdgeId("e6");
    }
    VertexPlan v4Plan = v4Builder.build();
    
    LOG.info("Setting up one to one dag plan");
    DAGPlan.Builder dagBuilder = DAGPlan.newBuilder()
        .setName("testVertexOneToOneSplit")
        .addVertex(v1Plan)
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex2")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(numTasks)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x2.y2")
                    .build()
                )
                .addInEdgeId("e1")
                .addOutEdgeId("e3")
            .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex3")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(numTasks)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x3.y3")
                    .build()
                )
                .addInEdgeId("e2")
                .addOutEdgeId("e4")
            .build()
        )
        .addVertex(v4Plan)
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v2"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex2")
                .setDataMovementType(PlanEdgeDataMovementType.ONE_TO_ONE)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v3"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.ONE_TO_ONE)
                .setId("e2")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v2_v4"))
                .setInputVertexName("vertex2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex4")
                .setDataMovementType(PlanEdgeDataMovementType.ONE_TO_ONE)
                .setId("e3")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v3_v4"))
                .setInputVertexName("vertex3")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex4")
                .setDataMovementType(PlanEdgeDataMovementType.ONE_TO_ONE)
                .setId("e4")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                
            );
    if (addNullEdge) {
      dagBuilder.addVertex(
              VertexPlan.newBuilder()
              .setName("vertex5")
              .setType(PlanVertexType.NORMAL)
              .setTaskConfig(
                  PlanTaskConfiguration.newBuilder()
                  .setNumTasks(1)
                  .setVirtualCores(4)
                  .setMemoryMb(1024)
                  .setJavaOpts("")
                  .setTaskModule("x4.y4")
                  .build()
              )
              .addInEdgeId("e5")
              .build()
          ).addVertex(
              VertexPlan.newBuilder()
              .setName("vertex6")
              .setType(PlanVertexType.NORMAL)
              .setTaskConfig(
                  PlanTaskConfiguration.newBuilder()
                  .setNumTasks(1)
                  .setVirtualCores(4)
                  .setMemoryMb(1024)
                  .setJavaOpts("")
                  .setTaskModule("x4.y4")
                  .build()
              )
              .addInEdgeId("e6")
              .build()
          )
          .addEdge(
              EdgePlan.newBuilder()
                  .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v5"))
                  .setInputVertexName("vertex1")
                  .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                  .setOutputVertexName("vertex5")
                  .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                  .setId("e5")
                  .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                  .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                  .build()
          )
          .addEdge(
              EdgePlan.newBuilder()
                  .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v4_v6"))
                  .setInputVertexName("vertex4")
                  .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                  .setOutputVertexName("vertex6")
                  .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                  .setId("e6")
                  .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                  .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                  .build()
          );
    }
    DAGPlan dag = dagBuilder.build();
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
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto.newBuilder()
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName("output").build()
                        )
                        .setName("outputx")
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                CountingOutputCommitter.class.getName()))
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
                    .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                    .setEdgeManager(
                        TezEntityDescriptorProto.newBuilder()
                        .setClassName(EdgeManagerForTest.class.getName())
                        .setUserPayload(ByteString.copyFrom(edgePayload))
                        .build())
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

  // Create a plan with 3 vertices: A, B, C
  // A -> B, A -> C, B -> C
  private DAGPlan createSamplerDAGPlan(boolean customEdge) {
    LOG.info("Setting up dag plan");
    VertexPlan.Builder vCBuilder = VertexPlan.newBuilder();
    vCBuilder.setName("C")
      .setType(PlanVertexType.NORMAL)
      .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("C.class"))
      .addTaskLocationHint(
        PlanTaskLocationHint.newBuilder()
          .addHost("host3")
          .addRack("rack3")
          .build()
      )
      .setTaskConfig(
        PlanTaskConfiguration.newBuilder()
          .setNumTasks(customEdge ? -1 : 2)
          .setVirtualCores(4)
          .setMemoryMb(1024)
          .setJavaOpts("foo")
          .setTaskModule("x3.y3")
          .build()
      )
      .setVertexManagerPlugin(
          TezEntityDescriptorProto.newBuilder().setClassName(
              VertexManagerPluginForTest.class.getName()))
      .addInEdgeId("A_C")
      .addInEdgeId("B_C");
    if (customEdge) {
      vCBuilder.setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
          .setClassName(VertexManagerPluginForTest.class.getName()));

    }
    VertexPlan vCPlan = vCBuilder.build();
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("TestSamplerDAG")
        .addVertex(
          VertexPlan.newBuilder()
            .setName("A")
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
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
                .setTaskModule("A.class")
                .build()
            )
            .addOutEdgeId("A_B")
            .addOutEdgeId("A_C")
            .build()
        )
        .addVertex(
          VertexPlan.newBuilder()
            .setName("B")
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("B.class"))
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
                .setTaskModule("")
                .build()
            )
            .addInEdgeId("A_B")
            .addOutEdgeId("B_C")
            .build()
        )
        .addVertex(
          vCPlan
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("A_B.class"))
                .setInputVertexName("A")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("A_B.class"))
                .setOutputVertexName("B")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("A_B")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("A_C"))
                .setInputVertexName("A")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("A_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(
                    customEdge ? PlanEdgeDataMovementType.CUSTOM
                        : PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("A_C")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setInputVertexName("B")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(
                    customEdge ? PlanEdgeDataMovementType.CUSTOM
                        : PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("B_C")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
          )
        .build();

    return dag;
  }
  
  // Create a plan with 3 vertices: A, B, C. Group(A,B)->C
  private DAGPlan createVertexGroupDAGPlan() {
    LOG.info("Setting up group dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("TestGroupDAG")
        .addVertex(
          VertexPlan.newBuilder()
            .setName("A")
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
            .setType(PlanVertexType.NORMAL)
            .setTaskConfig(
              PlanTaskConfiguration.newBuilder()
                .setNumTasks(1)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("A.class")
                .build()
            )
            .addOutEdgeId("A_C")
            .build()
        )
        .addVertex(
          VertexPlan.newBuilder()
            .setName("B")
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("B.class"))
            .setType(PlanVertexType.NORMAL)
            .setTaskConfig(
              PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("")
                .setTaskModule("")
                .build()
            )
            .addOutEdgeId("B_C")
            .build()
        )
        .addVertex(
          VertexPlan.newBuilder()
            .setName("C")
            .setType(PlanVertexType.NORMAL)
            .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("C.class"))
            .setTaskConfig(
              PlanTaskConfiguration.newBuilder()
                .setNumTasks(2)
                .setVirtualCores(4)
                .setMemoryMb(1024)
                .setJavaOpts("foo")
                .setTaskModule("x3.y3")
                .build()
            )
            .addInEdgeId("A_C")
            .addInEdgeId("B_C")
            .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("A_C"))
                .setInputVertexName("A")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("A_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("A_C")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setInputVertexName("B")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("B_C")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
          )
         .addVertexGroups(
              PlanVertexGroupInfo.newBuilder().
                setGroupName("Group").
                addGroupMembers("A").
                addGroupMembers("B").
                addEdgeMergedInputs(
                    PlanGroupInputEdgeInfo.newBuilder().setDestVertexName("C").
                    setMergedInput(
                        TezEntityDescriptorProto.newBuilder().
                          setClassName("Group.class")
                          .build()).build()))
        .build();

    return dag;
  }
  
  private DAGPlan createDAGWithCustomVertexManager() {
    LOG.info("Setting up custom vertex manager dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("TestCustomVMDAG")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("v1")
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("A.class")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(VertexManagerPluginForTest.class.getName()))
                .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("v2")
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("A.class")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(VertexManagerPluginForTest.class.getName()))
                .addOutEdgeId("2_3")
                .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("v3")
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(-1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("A.class")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(VertexManagerPluginForTest.class.getName()))
                .addInEdgeId("2_3")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("2_3"))
                .setInputVertexName("v2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("2_3.class"))
                .setOutputVertexName("v3")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("2_3")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        ).build();
    
    return dag;
  }

  // Create a plan with 3 vertices: A, B, C
  // A -> C, B -> C
  private DAGPlan createSamplerDAGPlan2() {
    LOG.info("Setting up sampler 2 dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("TestSamplerDAG")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("A")
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("A.class"))
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
                        .setTaskModule("A.class")
                        .build()
                )
                .addOutEdgeId("A_C")
                .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("B")
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("B.class"))
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
                        .setTaskModule("")
                        .build()
                )
                .addOutEdgeId("B_C")
                .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("C")
                .setType(PlanVertexType.NORMAL)
                .setProcessorDescriptor(TezEntityDescriptorProto.newBuilder().setClassName("C.class"))
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
                .addInEdgeId("A_C")
                .addInEdgeId("B_C")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("A_C"))
                .setInputVertexName("A")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("A_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("A_C")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setInputVertexName("B")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("B_C.class"))
                .setOutputVertexName("C")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("B_C")
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
      TezVertexID vertexId = TezVertexID.getInstance(dagId, i+1);
      VertexImpl v = null;
      VertexLocationHint locationHint = DagTypeConverters.convertFromDAGPlan(
          vPlan.getTaskLocationHintList());
      if (useCustomInitializer) {
        if (customInitializer == null) {
          v = new VertexImplWithControlledInitializerManager(vertexId, vPlan, vPlan.getName(), conf,
              dispatcher.getEventHandler(), taskAttemptListener,
              clock, thh, appContext, locationHint, dispatcher, updateTracker);
        } else {
          v = new VertexImplWithRunningInputInitializer(vertexId, vPlan, vPlan.getName(), conf,
              dispatcher.getEventHandler(), taskAttemptListener,
              clock, thh, appContext, locationHint, dispatcher, customInitializer, updateTracker);
        }
      } else {
        v = new VertexImpl(vertexId, vPlan, vPlan.getName(), conf,
            dispatcher.getEventHandler(), taskAttemptListener,
            clock, thh, true, appContext, locationHint, vertexGroups, taskSpecificLaunchCmdOption,
            updateTracker);
      }
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

  public void setupPreDagCreation() {
    LOG.info("____________ RESETTING CURRENT DAG ____________");
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
    taskSpecificLaunchCmdOption = mock(TaskSpecificLaunchCmdOption.class);
    doReturn(false).when(taskSpecificLaunchCmdOption).addTaskSpecificLaunchCmdOption(
        any(String.class),
        anyInt());
  }

  public void setupPostDagCreation() {
    String dagName = "dag0";
    dispatcher = new DrainDispatcher();
    appContext = mock(AppContext.class);
    historyEventHandler = mock(HistoryEventHandler.class);
    TaskSchedulerEventHandler taskScheduler = mock(TaskSchedulerEventHandler.class);
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    DAG dag = mock(DAG.class);
    doReturn(ugi).when(dag).getDagUGI();
    doReturn(dagName).when(dag).getName();
    doReturn(appAttemptId).when(appContext).getApplicationAttemptId();
    doReturn(appAttemptId.getApplicationId()).when(appContext).getApplicationID();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn(conf).when(appContext).getAMConf();
    doReturn(new Credentials()).when(dag).getCredentials();
    doReturn(DAGPlan.getDefaultInstance()).when(dag).getJobPlan();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(dagId).when(dag).getID();
    doReturn(taskScheduler).when(appContext).getTaskScheduler();
    doReturn(Resource.newInstance(102400, 60)).when(taskScheduler).getTotalResources();
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(dispatcher.getEventHandler()).when(appContext).getEventHandler();

    vertexGroups = Maps.newHashMap();
    for (PlanVertexGroupInfo groupInfo : dagPlan.getVertexGroupsList()) {
      vertexGroups.put(groupInfo.getGroupName(), new VertexGroupInfo(groupInfo));
    }
    updateTracker = new StateChangeNotifier(dag);
    setupVertices();
    when(dag.getVertex(any(TezVertexID.class))).thenAnswer(new Answer<Vertex>() {
      @Override
      public Vertex answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        if (args.length != 1) {
          return null;
        }
        TezVertexID vId = (TezVertexID) args[0];
        return vertexIdMap.get(vId);
      }
    });
    when(dag.getVertex(any(String.class))).thenAnswer(new Answer<Vertex>() {
      @Override
      public Vertex answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        if (args.length != 1) {
          return null;
        }
        String vId = (String) args[0];
        return vertices.get(vId);
      }
    });

    // TODO - this test logic is tightly linked to impl DAGImpl code.
    edges = new HashMap<String, Edge>();
    for (EdgePlan edgePlan : dagPlan.getEdgeList()) {
      EdgeProperty edgeProperty = DagTypeConverters
          .createEdgePropertyMapFromDAGPlan(edgePlan);
      edges.put(edgePlan.getId(), new Edge(edgeProperty, dispatcher.getEventHandler()));
    }

    parseVertexEdges();
    
    for (Edge edge : edges.values()) {
      edge.initialize();
    }

    taskAttemptEventDispatcher = new TaskAttemptEventDispatcher();
    dispatcher.register(TaskAttemptEventType.class, taskAttemptEventDispatcher);
    taskEventDispatcher = new TaskEventDispatcher();
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    vertexEventDispatcher = new VertexEventDispatcher();
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dagEventDispatcher = new DagEventDispatcher();
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.init(conf);
    dispatcher.start();
  }
  
  @Before
  public void setup() {
    useCustomInitializer = false;
    customInitializer = null;
    setupPreDagCreation();
    dagPlan = createTestDAGPlan();
    invalidDagPlan = createInvalidDAGPlan();
    setupPostDagCreation();
  }

  @After
  public void teardown() {
    if (dispatcher.isInState(STATE.STARTED)) {
      dispatcher.await();
      dispatcher.stop();
    }
    dispatcher = null;
    vertexEventDispatcher = null;
    dagEventDispatcher = null;
    dagPlan = null;
    invalidDagPlan = null;
    this.vertices = null;
    this.edges = null;
    this.vertexIdMap = null;
  }

  private void initAllVertices(VertexState expectedState) {
    for (int i = 1; i <= vertices.size(); ++i) {
      VertexImpl v = vertices.get("vertex" + i);
      if (v.sourceVertices == null || v.sourceVertices.isEmpty()) {
        initVertex(v);
      }
    }
    for (int i = 1; i <= vertices.size(); ++i) {
      VertexImpl v = vertices.get("vertex" + i);
      Assert.assertEquals(expectedState, v.getState());
    }
  }


  @SuppressWarnings("unchecked")
  private void initVertex(VertexImpl v) {
    Assert.assertEquals(VertexState.NEW, v.getState());
    dispatcher.getEventHandler().handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_INIT));
    dispatcher.await();
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
    initAllVertices(VertexState.INITED);

    VertexImpl v3 = vertices.get("vertex3");

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
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);
  }

  @Test(timeout = 5000)
  public void testVertexSetParallelism() {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());
    TezTaskID firstTask = tasks.keySet().iterator().next();

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());

    Map<String, EdgeManagerPluginDescriptor> edgeManagerDescriptors =
        Collections.singletonMap(
       v1.getName(), mockEdgeManagerDescriptor);
    Assert.assertTrue(v3.setParallelism(1, null, edgeManagerDescriptors, null));
    Assert.assertTrue(v3.sourceVertices.get(v1).getEdgeManager() instanceof
        EdgeManagerForTest);
    Assert.assertEquals(1, v3.getTotalTasks());
    Assert.assertEquals(1, tasks.size());
    // the last one is removed
    Assert.assertTrue(tasks.keySet().iterator().next().equals(firstTask));

  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexPendingTaskEvents() {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");
    VertexImpl v2 = vertices.get("vertex2");
    VertexImpl v1 = vertices.get("vertex1");
    
    startVertex(v1);
    
    TezTaskID t0_v2 = TezTaskID.getInstance(v2.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v2 = TezTaskAttemptID.getInstance(t0_v2, 0);

    List<TezEvent> taskEvents = Lists.newLinkedList();
    TezEvent tezEvent1 = new TezEvent(
        CompositeDataMovementEvent.create(0, 1, ByteBuffer.wrap(new byte[0])),
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex3", ta0_t0_v2));
    TezEvent tezEvent2 = new TezEvent(
        DataMovementEvent.create(0, ByteBuffer.wrap(new byte[0])),
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex3", ta0_t0_v2));
    taskEvents.add(tezEvent1);
    taskEvents.add(tezEvent2);
    // send events and test that they are buffered until some task is scheduled
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v3.getVertexId(), taskEvents));
    dispatcher.await();
    Assert.assertEquals(2, v3.pendingTaskEvents.size());
    v3.scheduleTasks(Collections.singletonList(new TaskWithLocationHint(new Integer(0), null)));
    dispatcher.await();
    Assert.assertEquals(0, v3.pendingTaskEvents.size());
    // send events and test that they are not buffered anymore
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v3.getVertexId(), taskEvents));
    dispatcher.await();
    Assert.assertEquals(0, v3.pendingTaskEvents.size());
  }

  @Test(timeout = 5000)
  public void testSetCustomEdgeManager() throws UnsupportedEncodingException {
    initAllVertices(VertexState.INITED);
    Edge edge = edges.get("e4");
    EdgeManagerPlugin em = edge.getEdgeManager();
    EdgeManagerForTest originalEm = (EdgeManagerForTest) em;
    Assert.assertTrue(Arrays.equals(edgePayload, originalEm.getEdgeManagerContext()
        .getUserPayload().deepCopyAsArray()));

    UserPayload userPayload = UserPayload.create(ByteBuffer.wrap(new String("foo").getBytes()));
    EdgeManagerPluginDescriptor edgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    edgeManagerDescriptor.setUserPayload(userPayload);

    Vertex v3 = vertices.get("vertex3");
    Vertex v5 = vertices.get("vertex5"); // Vertex5 linked to v3 (v3 src, v5
                                         // dest)

    Map<String, EdgeManagerPluginDescriptor> edgeManagerDescriptors =
        Collections.singletonMap(v3.getName(), edgeManagerDescriptor);
    Assert.assertTrue(v5.setParallelism(v5.getTotalTasks() - 1, null,
        edgeManagerDescriptors, null)); // Must decrease.

    VertexImpl v5Impl = (VertexImpl) v5;

    EdgeManagerPlugin modifiedEdgeManager = v5Impl.sourceVertices.get(v3)
        .getEdgeManager();
    Assert.assertNotNull(modifiedEdgeManager);
    Assert.assertTrue(modifiedEdgeManager instanceof EdgeManagerForTest);

    // Ensure initialize() is called with the correct payload
    Assert.assertTrue(Arrays.equals(userPayload.deepCopyAsArray(),
        ((EdgeManagerForTest) modifiedEdgeManager).getUserPayload().deepCopyAsArray()));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testBasicVertexCompletion() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.OWN_TASK_FAILURE, v.getTerminationCause());
    String diagnostics =
        StringUtils.join(v.getDiagnostics(), ",").toLowerCase();
    Assert.assertTrue(diagnostics.contains("task failed"
        + ", taskid=" + t1.toString()));
  }

  @Test(timeout = 5000)
  public void testVertexKillDiagnosticsInInit() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex4");
    killVertex(v2);
    String diagnostics =
        StringUtils.join(v2.getDiagnostics(), ",").toLowerCase();
    LOG.info("diagnostics v2: " + diagnostics);
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in inited state"));
  }

  @Test(timeout = 5000)
  public void testVertexKillDiagnosticsInRunning() {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    killVertex(v3);
    String diagnostics =
        StringUtils.join(v3.getDiagnostics(), ",").toLowerCase();
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill while in running state"));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexKillPending() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            TezTaskID.getInstance(v.getVertexId(), 1), TaskState.KILLED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexKill() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            TezTaskID.getInstance(v.getVertexId(), 0), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(
            TezTaskID.getInstance(v.getVertexId(), 1), TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testKilledTasksHandling() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(VertexTerminationCause.OWN_TASK_FAILURE, v.getTerminationCause());
    Assert.assertEquals(TaskState.KILLED, v.getTask(t2).getState());
  }

  @Test(timeout = 5000)
  public void testVertexCommitterInit() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex2");
    Assert.assertNull(v2.getOutputCommitter("output"));

    VertexImpl v6 = vertices.get("vertex6");
    Assert.assertTrue(v6.getOutputCommitter("outputx")
        instanceof CountingOutputCommitter);
  }

  @Test(timeout = 5000)
  public void testVertexManagerInit() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex2");
    Assert.assertTrue(v2.getVertexManager().getPlugin()
        instanceof ImmediateStartVertexManager);

    VertexImpl v6 = vertices.get("vertex6");
    Assert.assertTrue(v6.getVertexManager().getPlugin()
        instanceof ShuffleVertexManager);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexTaskFailure() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex6");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("outputx");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    initAllVertices(VertexState.INITED);

    VertexImpl v6 = vertices.get("vertex6");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    dispatcher.await();
    LOG.info("Verifying v6 state " + v6.getState());
    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(3, v6.getDistanceFromRoot());
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
    initAllVertices(VertexState.INITED);

    VertexImpl v4 = vertices.get("vertex4");
    VertexImpl v5 = vertices.get("vertex5");
    VertexImpl v6 = vertices.get("vertex6");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    dispatcher.await();
    LOG.info("Verifying v6 state " + v6.getState());
    Assert.assertEquals(VertexState.RUNNING, v6.getState());

    TezTaskID t1_v4 = TezTaskID.getInstance(v4.getVertexId(), 0);
    TezTaskID t2_v4 = TezTaskID.getInstance(v4.getVertexId(), 1);
    TezTaskID t1_v5 = TezTaskID.getInstance(v5.getVertexId(), 0);
    TezTaskID t2_v5 = TezTaskID.getInstance(v5.getVertexId(), 1);

    TezTaskAttemptID ta1_t1_v4 = TezTaskAttemptID.getInstance(t1_v4, 0);
    TezTaskAttemptID ta2_t1_v4 = TezTaskAttemptID.getInstance(t1_v4, 0);
    TezTaskAttemptID ta1_t2_v4 = TezTaskAttemptID.getInstance(t2_v4, 0);
    TezTaskAttemptID ta1_t1_v5 = TezTaskAttemptID.getInstance(t1_v5, 0);
    TezTaskAttemptID ta1_t2_v5 = TezTaskAttemptID.getInstance(t2_v5, 0);
    TezTaskAttemptID ta2_t2_v5 = TezTaskAttemptID.getInstance(t2_v5, 0);

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
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");

    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());

  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexSuccessToRunningAfterTaskScheduler() {
    // For downstream failures
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");

    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_COMPLETED).intValue());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskReschedule(t1));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());

    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_RERUNNING).intValue());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(2,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_COMPLETED).intValue());
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexSuccessToFailedAfterTaskScheduler() throws Exception {
    // For downstream failures
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName()).setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig()
                    .toUserPayload())).build())
        .setName("output_v2")
        .setIODescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName("output.class"))
        .build());
    v.setAdditionalOutputs(outputs);

    initAllVertices(VertexState.INITED);
    startVertex(v);

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_COMPLETED).intValue());

    dispatcher.getEventHandler().handle(
        new VertexEventTaskReschedule(t1));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());

    Assert.assertEquals(2,
        dagEventDispatcher.eventCount.get(
            DAGEventType.DAG_VERTEX_COMPLETED).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexCommit() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex6");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("outputx");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testTaskFailedAfterVertexSuccess() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex6");

    startVertex(vertices.get("vertex1"));
    startVertex(vertices.get("vertex2"));
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("outputx");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t1, TaskState.SUCCEEDED));
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);
    Assert.assertEquals(0, committer.abortCounter);
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
    
    dispatcher.getEventHandler().handle(
        new VertexEventTaskCompleted(t2, TaskState.FAILED));
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v.getState());
    Assert.assertEquals(1, committer.commitCounter);
    Assert.assertEquals(1, committer.abortCounter);
    
  }

  @Test(timeout = 5000)
  public void testCommitterInitAndSetup() {
    // FIXME need to add a test for this
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testBadCommitter() throws Exception {
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName()).setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, false).toUserPayload())).build())
        .setName("output_v2")
        .setIODescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName("output.class"))
        .build());
    v.setAdditionalOutputs(outputs);

    initAllVertices(VertexState.INITED);
    startVertex(v);

    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("output_v2");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testBadCommitter2() throws Exception {
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName()).setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, true).toUserPayload())).build())
        .setName("output_v2")
        .setIODescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName("output.class"))
        .build());
    v.setAdditionalOutputs(outputs);

    initAllVertices(VertexState.INITED);
    startVertex(v);

    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("output_v2");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexInitWithCustomVertexManager() {
    setupPreDagCreation();
    dagPlan = createDAGWithCustomVertexManager();
    setupPostDagCreation();
    
    int numTasks = 3;
    VertexImpl v1 = vertices.get("v1");
    VertexImpl v2 = vertices.get("v2");
    VertexImpl v3 = vertices.get("v3");
    initVertex(v1);
    initVertex(v2);
    dispatcher.await();
    // vertex should be in initializing state since parallelism is not set
    Assert.assertEquals(-1, v1.getTotalTasks());
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(-1, v2.getTotalTasks());
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    Assert.assertEquals(-1, v3.getTotalTasks());
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());
    // vertex should not start since parallelism is not set
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(), VertexEventType.V_START));
    dispatcher.await();
    Assert.assertEquals(-1, v1.getTotalTasks());
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    // set the parallelism
    v1.setParallelism(numTasks, null, null, null);
    v2.setParallelism(numTasks, null, null, null);
    dispatcher.await();
    // parallelism set and vertex starts with pending start event
    Assert.assertEquals(numTasks, v1.getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    // parallelism set and vertex inited
    Assert.assertEquals(numTasks, v2.getTotalTasks());
    Assert.assertEquals(VertexState.INITED, v2.getState());
    // send start and vertex should run
    dispatcher.getEventHandler().handle(new VertexEvent(v2.getVertexId(), VertexEventType.V_START));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v2.getState());
    // v3 still initializing with source vertex started. So should start running
    // once num tasks is defined
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());
    v3.setParallelism(numTasks, null, null, null);
    dispatcher.await();
    Assert.assertEquals(numTasks, v3.getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, v3.getState());
  }

  @Test(timeout = 5000)
  public void testVertexManagerHeuristic() {
    setupPreDagCreation();
    dagPlan = createDAGPlanWithMixedEdges();
    setupPostDagCreation();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(ImmediateStartVertexManager.class, 
        vertices.get("vertex1").getVertexManager().getPlugin().getClass());
    Assert.assertEquals(ShuffleVertexManager.class, 
        vertices.get("vertex2").getVertexManager().getPlugin().getClass());
    Assert.assertEquals(InputReadyVertexManager.class, 
        vertices.get("vertex3").getVertexManager().getPlugin().getClass());
    Assert.assertEquals(ImmediateStartVertexManager.class, 
        vertices.get("vertex4").getVertexManager().getPlugin().getClass());
    Assert.assertEquals(ImmediateStartVertexManager.class, 
        vertices.get("vertex5").getVertexManager().getPlugin().getClass());
    Assert.assertEquals(InputReadyVertexManager.class, 
        vertices.get("vertex6").getVertexManager().getPlugin().getClass());
  }


  @Test(timeout = 5000)
  public void testVertexWithOneToOneSplit() {
    // create a diamond shaped dag with 1-1 edges. 
    // split the source and remaining vertices should split equally
    // vertex with 2 incoming splits from the same source should split once
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit("TestInputInitializer", -1, true);
    setupPostDagCreation();
    
    int numTasks = 5;
    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    VertexImpl v5 = vertices.get("vertex5");
    initVertex(v1);
    
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    
    // setting the edge manager should vertex1 should not INIT/START it since
    // input initialization is not complete. v5 should be inited
    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    Edge e = v5.sourceVertices.get(v1);
    Assert.assertNull(e.getEdgeManager());
    e.setCustomEdgeManager(mockEdgeManagerDescriptor);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex5").getState());
    
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(numTasks);
    initializerManager1.completeInputInitialization(0, numTasks, v1Hints);

    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(numTasks, v1.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v1Hints.size(); ++i) {
      Assert.assertEquals(v1Hints.get(i), v1.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex2").getState());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex3").getState());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex5").getState());
    // v5, v6 still initializing since edge is null
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    
    startVertex(v1);
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex5").getState());
    // v5, v6 still initializing since edge is null
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    
    mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    e = vertices.get("vertex6").sourceVertices.get(vertices.get("vertex4"));
    Assert.assertNull(e.getEdgeManager());
    e.setCustomEdgeManager(mockEdgeManagerDescriptor);
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
  }
  
  @Test(timeout = 5000)
  public void testVertexWithOneToOneSplitWhileRunning() {
    int numTasks = 5;
    // create a diamond shaped dag with 1-1 edges. 
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit(null, numTasks, false);
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    initAllVertices(VertexState.INITED);
    
    // fudge vertex manager so that tasks dont start running
    v1.vertexManager = new VertexManager(
        VertexManagerPluginDescriptor.create(VertexManagerPluginForTest.class.getName()),
        v1, appContext);
    v1.vertexManager.initialize();
    startVertex(v1);
    
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
    // change parallelism
    int newNumTasks = 3;
    v1.setParallelism(newNumTasks, null, null, null);
    dispatcher.await();
    Assert.assertEquals(newNumTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(newNumTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(newNumTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
  }
  
  @Test(timeout = 5000)
  public void testVertexWithOneToOneSplitWhileInited() {
    int numTasks = 5;
    // create a diamond shaped dag with 1-1 edges. 
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit(null, numTasks, false);
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    initAllVertices(VertexState.INITED);
    
    // fudge vertex manager so that tasks dont start running
    v1.vertexManager = new VertexManager(
        VertexManagerPluginDescriptor.create(VertexManagerPluginForTest.class.getName()),
        v1, appContext);
    v1.vertexManager.initialize();
    
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    // change parallelism
    int newNumTasks = 3;
    v1.setParallelism(newNumTasks, null, null, null);
    dispatcher.await();
    Assert.assertEquals(newNumTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(newNumTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(newNumTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex4").getState());

    startVertex(v1);
    dispatcher.await();

    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
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

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexWithInitializerFailure() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputInitializer("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.failInputInitialization();

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    RootInputInitializerManagerControlled initializerManager2 = v2.getRootInputInitializerManager();
    initializerManager2.failInputInitialization();
    
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager2.hasShutDown);
  }

  @Test(timeout = 10000)
  public void testVertexWithInitializerParallelismSetTo0() throws InterruptedException {
    useCustomInitializer = true;
    customInitializer = new RootInitializerSettingParallelismTo0(null);
    RootInitializerSettingParallelismTo0 initializer =
        (RootInitializerSettingParallelismTo0) customInitializer;
    setupPreDagCreation();
    dagPlan =
        createDAGPlanWithInitializer0Tasks(RootInitializerSettingParallelismTo0.class.getName());
    setupPostDagCreation();

    VertexImpl v1 = vertices.get("vertex1");
    VertexImpl v2 = vertices.get("vertex2");

    initVertex(v2);

    TezTaskID v2t1 = TezTaskID.getInstance(v2.getVertexId(), 0);

    TezTaskAttemptID ta1V2T1 = TezTaskAttemptID.getInstance(v2t1, 0);

    TezEvent tezEvent = new TezEvent(DataMovementEvent.create(null),
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex1", ta1V2T1));
    List<TezEvent> events = new LinkedList<TezEvent>();
    events.add(tezEvent);
    v1.handle(new VertexEventRouteEvent(v1.getVertexId(), events));

    startVertex(v2);
    dispatcher.await();
    v2.handle(new VertexEventTaskAttemptCompleted(ta1V2T1, TaskAttemptStateInternal.SUCCEEDED));
    v2.handle(new VertexEventTaskCompleted(v2t1, TaskState.SUCCEEDED));
    dispatcher.await();
    Assert.assertEquals(VertexState.SUCCEEDED, v2.getState());

    initializer.go();
    while (v1.getState() == VertexState.INITIALIZING || v1.getState() == VertexState.INITED) {
      Thread.sleep(10);
    }

    while (v1.getState() != VertexState.SUCCEEDED) {
      Thread.sleep(10);
    }
    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());
  }

  @Test(timeout = 10000)
  public void testInputInitializerVertexStateUpdates() throws Exception {
    // v2 running an Input initializer, which is subscribed to events on v1.
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    // Using the EventHandlingRootInputInitializer since it keeps the initializer alive till signalled,
    // which is required to track events that it receives.
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");

    initVertex(v1);
    startVertex(v1);
    Assert.assertEquals(VertexState.RUNNING, v1.getState());

    // Make v1 succeed
    for (TezTaskID taskId : v1.getTasks().keySet()) {
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
    }
    dispatcher.await();

    Assert.assertEquals(VertexState.SUCCEEDED, v1.getState());

    // At this point, 2 events should have been received - since the dispatcher is complete.
    Assert.assertEquals(2, initializer.stateUpdateEvents.size());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.RUNNING,
        initializer.stateUpdateEvents.get(0).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.SUCCEEDED,
        initializer.stateUpdateEvents.get(1).getVertexState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 10000)
  public void testRootInputInitializerEvent() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    VertexImplWithRunningInputInitializer v2 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex2");

    initVertex(v1);
    startVertex(v1);
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    dispatcher.await();

    // Wait for the initializer to be invoked - which may be a separate thread.
    while (!initializer.initStarted.get()) {
      Thread.sleep(10);
    }
    Assert.assertFalse(initializer.eventReceived.get());
    Assert.assertFalse(initializer.initComplete.get());

    // Signal the initializer by sending an event - via vertex1
    InputInitializerEvent event = InputInitializerEvent.create("vertex2", "input1", null);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", null, null));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Both happening in separate threads
    while (!initializer.eventReceived.get()) {
      Thread.sleep(10);
    }
    while (!initializer.initComplete.get()) {
      Thread.sleep(10);
    }

    // Will eventually go into RUNNING state, via INITED
    while (v2.getState()  != VertexState.RUNNING) {
      Thread.sleep(10);
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexWithInitializerSuccess() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputInitializer("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(5);
    initializerManager1.completeInputInitialization(0, 5, v1Hints);

    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(5, v1.getTotalTasks());
    // task events get buffered
    Assert.assertEquals(5, v1.pendingTaskEvents.size());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v1Hints.size(); ++i) {
      Assert.assertEquals(v1Hints.get(i), v1.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    for (int i = 0; i < 5; i++) {
      List<InputSpec> inputSpecs = v1.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(1, inputSpecs.get(0).getPhysicalEdgeCount());
    }
    
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    
    // non-task events dont get buffered
    List<TezEvent> events = Lists.newLinkedList();
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    events.add(new TezEvent(
        VertexManagerEvent.create("vertex2", ByteBuffer.wrap(new byte[0])), new EventMetaData(
            EventProducerConsumerType.PROCESSOR, "vertex1", "vertex2",
            ta0_t0_v1)));
    events.add(new TezEvent(InputDataInformationEvent.createWithSerializedPayload(0,
        ByteBuffer.wrap(new byte[0])),
        new EventMetaData(EventProducerConsumerType.INPUT, "vertex2",
            "NULL_VERTEX", null)));
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v2.getVertexId(), events));
    dispatcher.await();
    Assert.assertEquals(1, v2.pendingTaskEvents.size());
    
    RootInputInitializerManagerControlled initializerManager2 = v2.getRootInputInitializerManager();
    List<TaskLocationHint> v2Hints = createTaskLocationHints(10);
    initializerManager2.completeInputInitialization(0, 10, v2Hints);
    
    Assert.assertEquals(VertexState.INITED, v2.getState());
    Assert.assertEquals(10, v2.getTotalTasks());
    // task events get buffered
    Assert.assertEquals(11, v2.pendingTaskEvents.size());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v2Hints.size(); ++i) {
      Assert.assertEquals(v2Hints.get(i), v2.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager2.hasShutDown);
    for (int i = 0; i < 10; i++) {
      List<InputSpec> inputSpecs = v1.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(1, inputSpecs.get(0).getPhysicalEdgeCount());
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexWithInputDistributor() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputDistributor("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    byte[] payload = new byte[0];
    initializerManager1.completeInputDistribution(payload);
    // edge is still null so its initializing
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    Assert.assertEquals(2, v1.getTotalTasks());
    Assert.assertArrayEquals(payload,
        v1.getInputSpecList(0).get(0).getInputDescriptor().getUserPayload().deepCopyAsArray());
    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    Edge e = v2.sourceVertices.get(v1);
    Assert.assertNull(e.getEdgeManager());
    e.setCustomEdgeManager(mockEdgeManagerDescriptor);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(VertexState.INITED, v2.getState());
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexRootInputSpecUpdateAll() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputInitializer("TestInputInitializer");
    setupPostDagCreation();

    int expectedNumTasks = RootInputSpecUpdaterVertexManager.NUM_TASKS;
    VertexImplWithControlledInitializerManager v3 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex3");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v3.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());
    RootInputInitializerManagerControlled initializerManager1 = v3.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();

    Assert.assertEquals(VertexState.INITED, v3.getState());
    Assert.assertEquals(expectedNumTasks, v3.getTotalTasks());
    Assert.assertEquals(RootInputSpecUpdaterVertexManager.class.getName(), v3.getVertexManager()
        .getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    
    for (int i = 0; i < expectedNumTasks; i++) {
      List<InputSpec> inputSpecs = v3.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(4, inputSpecs.get(0).getPhysicalEdgeCount());
    }
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexRootInputSpecUpdatePerTask() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputInitializer("TestInputInitializer");
    setupPostDagCreation();

    int expectedNumTasks = RootInputSpecUpdaterVertexManager.NUM_TASKS;
    VertexImplWithControlledInitializerManager v4 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex4");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v4.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v4.getState());
    RootInputInitializerManagerControlled initializerManager1 = v4.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();

    Assert.assertEquals(VertexState.INITED, v4.getState());
    Assert.assertEquals(expectedNumTasks, v4.getTotalTasks());
    Assert.assertEquals(RootInputSpecUpdaterVertexManager.class.getName(), v4.getVertexManager()
        .getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    
    for (int i = 0; i < expectedNumTasks; i++) {
      List<InputSpec> inputSpecs = v4.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(i + 1, inputSpecs.get(0).getPhysicalEdgeCount());
    }
  }

  private List<TaskLocationHint> createTaskLocationHints(int numTasks) {
    List<TaskLocationHint> locationHints = Lists
        .newArrayListWithCapacity(numTasks);
    for (int i = 0; i < numTasks; i++) {
      TaskLocationHint taskLocationHint = TaskLocationHint.createTaskLocationHint(
          Sets.newSet("host" + i), null);
      locationHints.add(taskLocationHint);
    }
    return locationHints;
  }

  @Test(timeout = 5000)
  public void testVertexWithNoTasks() {
    TezVertexID vId = null;
    try {
      TezDAGID invalidDagId = TezDAGID.getInstance(
          dagId.getApplicationId(), 1000);
      vId = TezVertexID.getInstance(invalidDagId, 1);
      VertexPlan vPlan = invalidDagPlan.getVertex(0);
      VertexImpl v = new VertexImpl(vId, vPlan, vPlan.getName(), conf,
          dispatcher.getEventHandler(), taskAttemptListener,
          clock, thh, true, appContext, vertexLocationHint, null, taskSpecificLaunchCmdOption,
          updateTracker);
      vertexIdMap.put(vId, v);
      vertices.put(v.getName(), v);
      v.handle(new VertexEvent(vId, VertexEventType.V_INIT));
      dispatcher.await();
      Assert.assertEquals(VertexState.INITED, v.getState());
      v.handle(new VertexEvent(vId, VertexEventType.V_START));
      dispatcher.await();
      Assert.assertEquals(VertexState.SUCCEEDED, v.getState());
    } finally {
      if (vId != null) {
        vertexIdMap.remove(vId);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static class VertexImplWithRunningInputInitializer extends VertexImpl {

    private RootInputInitializerManagerWithRunningInitializer rootInputInitializerManager;
    private final InputInitializer presetInitializer;

    public VertexImplWithRunningInputInitializer(TezVertexID vertexId,
                                                 VertexPlan vertexPlan, String vertexName,
                                                 Configuration conf,
                                                 EventHandler eventHandler,
                                                 TaskAttemptListener taskAttemptListener,
                                                 Clock clock, TaskHeartbeatHandler thh,
                                                 AppContext appContext,
                                                 VertexLocationHint vertexLocationHint,
                                                 DrainDispatcher dispatcher,
                                                 InputInitializer presetInitializer,
                                                 StateChangeNotifier updateTracker) {
      super(vertexId, vertexPlan, vertexName, conf, eventHandler,
          taskAttemptListener, clock, thh, true,
          appContext, vertexLocationHint, null, taskSpecificLaunchCmdOption,
          updateTracker);
      this.presetInitializer = presetInitializer;
    }

    @Override
    protected RootInputInitializerManager createRootInputInitializerManager(
        String dagName, String vertexName, TezVertexID vertexID,
        EventHandler eventHandler, int numTasks, int numNodes,
        Resource taskResource, Resource totalResource) {
      try {
        rootInputInitializerManager =
            new RootInputInitializerManagerWithRunningInitializer(this, this.getAppContext(),
                presetInitializer, stateChangeNotifier);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return rootInputInitializerManager;
    }
  }

  @SuppressWarnings("rawtypes")
  private static class VertexImplWithControlledInitializerManager extends VertexImpl {
    
    private final DrainDispatcher dispatcher;
    private RootInputInitializerManagerControlled rootInputInitializerManager;
    
    public VertexImplWithControlledInitializerManager(TezVertexID vertexId,
                                                      VertexPlan vertexPlan, String vertexName,
                                                      Configuration conf,
                                                      EventHandler eventHandler,
                                                      TaskAttemptListener taskAttemptListener,
                                                      Clock clock, TaskHeartbeatHandler thh,
                                                      AppContext appContext,
                                                      VertexLocationHint vertexLocationHint,
                                                      DrainDispatcher dispatcher,
                                                      StateChangeNotifier updateTracker) {
      super(vertexId, vertexPlan, vertexName, conf, eventHandler,
          taskAttemptListener, clock, thh, true,
          appContext, vertexLocationHint, null, taskSpecificLaunchCmdOption,
          updateTracker);
      this.dispatcher = dispatcher;
    }

    @Override
    protected RootInputInitializerManager createRootInputInitializerManager(
        String dagName, String vertexName, TezVertexID vertexID,
        EventHandler eventHandler, int numTasks, int numNodes,
        Resource taskResource, Resource totalResource) {
      try {
        rootInputInitializerManager =
            new RootInputInitializerManagerControlled(this, this.getAppContext(), eventHandler,
                dispatcher, stateChangeNotifier);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return rootInputInitializerManager;
    }
    
    RootInputInitializerManagerControlled getRootInputInitializerManager() {
      return rootInputInitializerManager;
    }
  }


  private static class RootInputInitializerManagerWithRunningInitializer
      extends RootInputInitializerManager {

    private final InputInitializer presetInitializer;

    public RootInputInitializerManagerWithRunningInitializer(Vertex vertex, AppContext appContext,
                                                             InputInitializer presetInitializer,
                                                             StateChangeNotifier tracker) throws
        IOException {
      super(vertex, appContext, UserGroupInformation.getCurrentUser(), tracker);
      this.presetInitializer = presetInitializer;
    }


    @Override
    protected InputInitializer createInitializer(
        RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input,
        InputInitializerContext context) {
      if (presetInitializer instanceof ContextSettableInputInitialzier) {
        ((ContextSettableInputInitialzier)presetInitializer).setContext(context);
      }
      return presetInitializer;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static class RootInputInitializerManagerControlled extends
      RootInputInitializerManager {

    private List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs;
    private final EventHandler eventHandler;
    private final DrainDispatcher dispatcher;
    private final TezVertexID vertexID;
    private volatile boolean hasShutDown = false;

    public RootInputInitializerManagerControlled(Vertex vertex, AppContext appContext,
                                                 EventHandler eventHandler,
                                                 DrainDispatcher dispatcher,
                                                 StateChangeNotifier tracker
    ) throws IOException {
      super(vertex, appContext, UserGroupInformation.getCurrentUser(), tracker);
      this.eventHandler = eventHandler;
      this.dispatcher = dispatcher;
      this.vertexID = vertex.getVertexId();
    }

    @Override
    public void runInputInitializers(
        List<RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> inputs) {
      this.inputs = inputs;
    }

    @Override
    protected InputInitializer createInitializer(
        RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input,
        InputInitializerContext context) {

      return new InputInitializer(context) {
        @Override
        public List<Event> initialize() throws
            Exception {
          return null;
        }

        @Override
        public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws
            Exception {
        }
      };
    }

    @Override
    public void shutdown() {
      hasShutDown = true;
    }

    public void failInputInitialization() {
      super.runInputInitializers(inputs);
      eventHandler.handle(new VertexEventRootInputFailed(vertexID, inputs
          .get(0).getName(),
          new RuntimeException("MockInitializerFailed")));
      dispatcher.await();
    }

    public void completeInputInitialization() {
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID, inputs.get(0)
          .getName(), null));
      dispatcher.await();
    }

    public void completeInputDistribution(byte[] payload) {
      List<Event> events = Lists.newArrayListWithCapacity(1);
      InputUpdatePayloadEvent event = InputUpdatePayloadEvent.create(ByteBuffer.wrap(payload));
      events.add(event);
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID, inputs
          .get(0).getName(), events));
      dispatcher.await();
    }

    public void completeInputInitialization(int initializerIndex, int targetTasks,
        List<TaskLocationHint> locationHints) {
      List<Event> events = Lists.newArrayListWithCapacity(targetTasks + 1);

      InputConfigureVertexTasksEvent configEvent = InputConfigureVertexTasksEvent.create(
          targetTasks, VertexLocationHint.create(locationHints), null);
      events.add(configEvent);
      for (int i = 0; i < targetTasks; i++) {
        InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(i,
            null);
        events.add(diEvent);
      }
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID, inputs
          .get(initializerIndex).getName(), events));
      dispatcher.await();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout=5000)
  public void testVertexGroupInput() {
    setupPreDagCreation();
    dagPlan = createVertexGroupDAGPlan();
    setupPostDagCreation();

    VertexImpl vA = vertices.get("A");
    VertexImpl vB = vertices.get("B");
    VertexImpl vC = vertices.get("C");

    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
      VertexEventType.V_INIT));
    dispatcher.getEventHandler().handle(new VertexEvent(vB.getVertexId(),
        VertexEventType.V_INIT));
    dispatcher.await();
    
    Assert.assertNull(vA.getGroupInputSpecList(0));
    Assert.assertNull(vB.getGroupInputSpecList(0));
    
    List<GroupInputSpec> groupInSpec = vC.getGroupInputSpecList(0);
    Assert.assertEquals(1, groupInSpec.size());
    Assert.assertEquals("Group", groupInSpec.get(0).getGroupName());
    Assert.assertTrue(groupInSpec.get(0).getGroupVertices().contains("A"));
    Assert.assertTrue(groupInSpec.get(0).getGroupVertices().contains("B"));
    groupInSpec.get(0).getMergedInputDescriptor().getClassName().equals("Group.class");
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testStartWithUninitializedCustomEdge() {
    // Race when a source vertex manages to start before the target vertex has
    // been initialized
    setupPreDagCreation();
    dagPlan = createSamplerDAGPlan(true);
    setupPostDagCreation();

    VertexImpl vA = vertices.get("A");
    VertexImpl vB = vertices.get("B");
    VertexImpl vC = vertices.get("C");

    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
      VertexEventType.V_INIT));
    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
      VertexEventType.V_START));

    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, vA.getState());
    Assert.assertEquals(VertexState.INITIALIZING, vB.getState());
    Assert.assertEquals(VertexState.INITIALIZING, vC.getState());
    
    // setting the edge manager should vA to start
    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    Edge e = vC.sourceVertices.get(vA);
    Assert.assertNull(e.getEdgeManager());
    e.setCustomEdgeManager(mockEdgeManagerDescriptor);
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.INITIALIZING, vB.getState());
    Assert.assertEquals(VertexState.INITIALIZING, vC.getState());
    
    Map<String, EdgeManagerPluginDescriptor> edges = Maps.newHashMap();
    edges.put("B", mockEdgeManagerDescriptor);
    vC.setParallelism(2, vertexLocationHint, edges, null);

    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.RUNNING, vB.getState());
    Assert.assertEquals(VertexState.RUNNING, vC.getState());
    Assert.assertNotNull(vA.getTask(0));
    Assert.assertNotNull(vB.getTask(0));
    Assert.assertNotNull(vC.getTask(0));
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testInitStartRace() {
    // Race when a source vertex manages to start before the target vertex has
    // been initialized
    setupPreDagCreation();
    dagPlan = createSamplerDAGPlan(false);
    setupPostDagCreation();

    VertexImpl vA = vertices.get("A");
    VertexImpl vB = vertices.get("B");
    VertexImpl vC = vertices.get("C");

    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
      VertexEventType.V_INIT));
    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
      VertexEventType.V_START));

    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.RUNNING, vB.getState());
    Assert.assertEquals(VertexState.RUNNING, vC.getState());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testInitStartRace2() {
    // Race when a source vertex manages to start before the target vertex has
    // been initialized
    setupPreDagCreation();
    dagPlan = createSamplerDAGPlan2();
    setupPostDagCreation();

    VertexImpl vA = vertices.get("A");
    VertexImpl vB = vertices.get("B");
    VertexImpl vC = vertices.get("C");

    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
        VertexEventType.V_INIT));
    dispatcher.getEventHandler().handle(new VertexEvent(vA.getVertexId(),
        VertexEventType.V_START));
    dispatcher.getEventHandler().handle(new VertexEvent(vB.getVertexId(),
        VertexEventType.V_INIT));
    dispatcher.getEventHandler().handle(new VertexEvent(vB.getVertexId(),
        VertexEventType.V_START));

    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.RUNNING, vB.getState());
    Assert.assertEquals(VertexState.RUNNING, vC.getState());
  }

  @InterfaceAudience.Private
  public static class RootInputSpecUpdaterVertexManager extends VertexManagerPlugin {

    private static final int NUM_TASKS = 5;

    public RootInputSpecUpdaterVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    }

    @Override
    public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
        List<Event> events) {
      Map<String, InputSpecUpdate> map = new HashMap<String, InputSpecUpdate>();
      if (getContext().getUserPayload().deepCopyAsArray()[0] == 0) {
        map.put("input3", InputSpecUpdate.createAllTaskInputSpecUpdate(4));
      } else {
        List<Integer> pInputList = new LinkedList<Integer>();
        for (int i = 1; i <= NUM_TASKS; i++) {
          pInputList.add(i);
        }
        map.put("input4", InputSpecUpdate.createPerTaskInputSpecUpdate(pInputList));
      }
      getContext().setVertexParallelism(NUM_TASKS, null, null, map);
    }
  }

  @InterfaceAudience.Private
  public static class RootInitializerSettingParallelismTo0 extends InputInitializer {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public RootInitializerSettingParallelismTo0(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      InputConfigureVertexTasksEvent event = InputConfigureVertexTasksEvent.create(0, null,
          InputSpecUpdate.getDefaultSinglePhysicalInputSpecUpdate());
      List<Event> events = new LinkedList<Event>();
      events.add(event);
      lock.lock();
      try {
        condition.await();
      } finally {
        lock.unlock();
      }
      LOG.info("Received signal to proceed. Returning event to set parallelism to 0");
      return events;
    }

    public void go() {
      lock.lock();
      try {
        LOG.info("Signallying initializer to proceed");
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    }
  }

  @InterfaceAudience.Private
  public static class EventHandlingRootInputInitializer extends InputInitializer
      implements ContextSettableInputInitialzier {

    final AtomicBoolean initStarted = new AtomicBoolean(false);
    final AtomicBoolean eventReceived = new AtomicBoolean(false);
    final AtomicBoolean initComplete = new AtomicBoolean(false);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition eventCondition = lock.newCondition();

    private final List<VertexStateUpdate> stateUpdateEvents = new LinkedList<VertexStateUpdate>();
    private volatile InputInitializerContext context;

    public EventHandlingRootInputInitializer(
        InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      context.registerForVertexStateUpdates("vertex1", null);
      initStarted.set(true);
      lock.lock();
      try {
        eventCondition.await();
      } finally {
        lock.unlock();
      }
      initComplete.set(true);
      InputDataInformationEvent diEvent = InputDataInformationEvent.createWithSerializedPayload(0,
          ByteBuffer.wrap(new byte[]{0}));
      List<Event> eventList = new LinkedList<Event>();
      eventList.add(diEvent);
      return eventList;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws
        Exception {
      eventReceived.set(true);
      lock.lock();
      try {
        eventCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void setContext(InputInitializerContext context) {
      this.context = context;
    }

    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
      stateUpdateEvents.add(stateUpdate);
    }
  }

  private interface ContextSettableInputInitialzier {
    void setContext(InputInitializerContext context);
  }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.dag.api.client.VertexStatus;
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
import org.apache.tez.dag.app.dag.RootInputInitializerRunner;
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
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.test.EdgeManagerForTest;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
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

  public static class CountingOutputCommitter extends
      OutputCommitter {

    public int initCounter = 0;
    public int setupCounter = 0;
    public int commitCounter = 0;
    public int abortCounter = 0;
    private boolean throwError;
    private boolean throwErrorOnAbort;
    private boolean throwRuntimeException;

    public CountingOutputCommitter(boolean throwError,
        boolean throwOnAbort,
        boolean throwRuntimeException) {
      this.throwError = throwError;
      this.throwErrorOnAbort = throwOnAbort;
      this.throwRuntimeException = throwRuntimeException;
    }

    public CountingOutputCommitter() {
      this(false, false, false);
    }

    @Override
    public void initialize(OutputCommitterContext context) throws IOException {
      if (context.getUserPayload() != null) {
        CountingOutputCommitterConfig conf =
            new CountingOutputCommitterConfig(context.getUserPayload());
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

      public CountingOutputCommitterConfig(byte[] payload) throws IOException {
        DataInput in = new DataInputStream(new ByteArrayInputStream(payload));
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

  private DAGPlan createDAGPlanWithInputInitializer(String initializerClassName) {
    LOG.info("Setting up invalid dag plan with input initializer");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testVertexWithInitializer")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                    .setInitializerClassName(initializerClassName)
                    .setName("input1")
                    .setEntityDescriptor(
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
                        .setInitializerClassName(initializerClassName)
                        .setName("input2")
                        .setEntityDescriptor(
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
  
  private DAGPlan createDAGPlanForOneToOneSplit(String initializerClassName) {
    LOG.info("Setting up one to one dag plan");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testVertexOneToOneSplit")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addInputs(
                    RootInputLeafOutputProto.newBuilder()
                    .setInitializerClassName(initializerClassName)
                    .setName("input1")
                    .setEntityDescriptor(
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
                .addOutEdgeId("e2")
            .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex2")
                .setType(PlanVertexType.NORMAL)
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
                .addOutEdgeId("e3")
            .build()
        )
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex3")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(-1)
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
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex4")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                    .setNumTasks(-1)
                    .setVirtualCores(4)
                    .setMemoryMb(1024)
                    .setJavaOpts("")
                    .setTaskModule("x4.y4")
                    .build()
                )
                .addInEdgeId("e3")
                .addInEdgeId("e4")
            .build()
        )
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
                .addOutputs(
                    DAGProtos.RootInputLeafOutputProto.newBuilder()
                        .setEntityDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName("output").build()
                        )
                        .setName("outputx")
                        .setInitializerClassName(CountingOutputCommitter.class.getName())
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
                    .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                    .setEdgeManager(
                        TezEntityDescriptorProto.newBuilder()
                        .setClassName(EdgeManagerForTest.class.getName())
                        .setUserPayload(ByteString.copyFrom(edgePayload))
                        .build())
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

  // Create a plan with 3 vertices: A, B, C
  // A -> B, A -> C, B -> C
  private DAGPlan createSamplerDAGPlan() {
    LOG.info("Setting up dag plan");
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
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("A_B.class"))
                .setInputVertexName("A")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("A_B.class"))
                .setOutputVertexName("B")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
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

  // Create a plan with 3 vertices: A, B, C
  // A -> C, B -> C
  private DAGPlan createSamplerDAGPlan2() {
    LOG.info("Setting up dag plan");
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
        v = new VertexImplWithCustomInitializer(vertexId, vPlan, vPlan.getName(), conf,
          dispatcher.getEventHandler(), taskAttemptListener,
          clock, thh, appContext, locationHint, dispatcher);
      } else {
        v = new VertexImpl(vertexId, vPlan, vPlan.getName(), conf,
            dispatcher.getEventHandler(), taskAttemptListener,
            clock, thh, true, appContext, locationHint, vertexGroups);
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
    conf = new Configuration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, false);
    appAttemptId = ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(100, 1), 1);
    dagId = TezDAGID.getInstance(appAttemptId.getApplicationId(), 1);
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
    setupPreDagCreation();
    dagPlan = createTestDAGPlan();
    invalidDagPlan = createInvalidDAGPlan();
    setupPostDagCreation();
  }

  @After
  public void teardown() {
    dispatcher.await();
    dispatcher.stop();
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

    startVertex(v3);

    Vertex v1 = vertices.get("vertex1");
    EdgeManagerDescriptor mockEdgeManagerDescriptor =
        new EdgeManagerDescriptor(EdgeManagerForTest.class.getName());

    Map<String, EdgeManagerDescriptor> edgeManagerDescriptors =
        Collections.singletonMap(
       v1.getName(), mockEdgeManagerDescriptor);
    Assert.assertTrue(v3.setParallelism(1, null, edgeManagerDescriptors));
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
    
    startVertex(v2);
    startVertex(v3);
    
    TezTaskID t0_v2 = TezTaskID.getInstance(v2.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v2 = TezTaskAttemptID.getInstance(t0_v2, 0);

    List<TezEvent> taskEvents = Lists.newLinkedList();
    TezEvent tezEvent1 = new TezEvent(
        new CompositeDataMovementEvent(0, 1, new byte[0]), 
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex3", ta0_t0_v2));
    TezEvent tezEvent2 = new TezEvent(
        new DataMovementEvent(0, new byte[0]), 
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex3", ta0_t0_v2));
    taskEvents.add(tezEvent1);
    taskEvents.add(tezEvent2);
    // send events and test that they are buffered until some task is scheduled
    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v3.getVertexId(), taskEvents));
    dispatcher.await();
    Assert.assertEquals(2, v3.pendingTaskEvents.size());
    v3.scheduleTasks(Collections.singletonList(new Integer(0)));
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
    Edge edge = edges.get("e1");
    EdgeManager em = edge.getEdgeManager();
    EdgeManagerForTest originalEm = (EdgeManagerForTest) em;
    Assert.assertTrue(Arrays.equals(edgePayload, originalEm.getEdgeManagerContext()
        .getUserPayload()));

    byte[] userPayload = new String("foo").getBytes();
    EdgeManagerDescriptor edgeManagerDescriptor =
        new EdgeManagerDescriptor(EdgeManagerForTest.class.getName());
    edgeManagerDescriptor.setUserPayload(userPayload);

    Vertex v1 = vertices.get("vertex1");
    Vertex v3 = vertices.get("vertex3"); // Vertex3 linked to v1 (v1 src, v3
                                         // dest)

    Map<String, EdgeManagerDescriptor> edgeManagerDescriptors =
        Collections.singletonMap(v1.getName(), edgeManagerDescriptor);
    Assert.assertTrue(v3.setParallelism(v3.getTotalTasks() - 1, null,
        edgeManagerDescriptors)); // Must decrease.

    VertexImpl v3Impl = (VertexImpl) v3;

    EdgeManager modifiedEdgeManager = v3Impl.sourceVertices.get(v1)
        .getEdgeManager();
    Assert.assertNotNull(modifiedEdgeManager);
    Assert.assertTrue(modifiedEdgeManager instanceof EdgeManagerForTest);

    // Ensure initialize() is called with the correct payload
    Assert.assertTrue(Arrays.equals(userPayload,
        ((EdgeManagerForTest) modifiedEdgeManager).getUserPayload()));
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
        StringUtils.join(",", v.getDiagnostics()).toLowerCase();
    Assert.assertTrue(diagnostics.contains("task failed"
        + ", taskid=" + t1.toString()));
  }

  @Test(timeout = 5000)
  public void testVertexKillDiagnostics() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex2");
    killVertex(v2);
    String diagnostics =
        StringUtils.join(",", v2.getDiagnostics()).toLowerCase();
    LOG.info("diagnostics v2: " + diagnostics);
    Assert.assertTrue(diagnostics.contains(
        "vertex received kill in inited state"));

    VertexImpl v3 = vertices.get("vertex3");

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

    startVertex(v);
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
    initAllVertices(VertexState.INITED);

    VertexImpl v4 = vertices.get("vertex4");
    VertexImpl v5 = vertices.get("vertex5");
    VertexImpl v6 = vertices.get("vertex6");

    startVertex(v4);
    startVertex(v5);
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
        .setInitializerClassName(CountingOutputCommitter.class.getName())
        .setName("output_v2")
        .setEntityDescriptor(
            TezEntityDescriptorProto.newBuilder()
                .setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig()
                        .toUserPayload())).build())
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

    Assert.assertEquals(1,
        dagEventDispatcher.eventCount.get(
            DAGEventType.INTERNAL_ERROR).intValue());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexCommit() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex6");

    startVertex(v);
    CountingOutputCommitter committer =
        (CountingOutputCommitter) v.getOutputCommitter("outputx");

    TezTaskID t1 = TezTaskID.getInstance(v.getVertexId(), 0);
    TezTaskID t2 = TezTaskID.getInstance(v.getVertexId(), 1);

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
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
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
        .setInitializerClassName(CountingOutputCommitter.class.getName())
        .setName("output_v2")
        .setEntityDescriptor(
            TezEntityDescriptorProto.newBuilder()
                .setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, false).toUserPayload())).build())
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
        .setInitializerClassName(CountingOutputCommitter.class.getName())
        .setName("output_v2")
        .setEntityDescriptor(
            TezEntityDescriptorProto.newBuilder()
                .setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, true).toUserPayload())).build())
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


  @Test(timeout = 5000)
  public void testVertexWithOneToOneSplit() {
    // create a diamond shaped dag with 1-1 edges. 
    // split the source and remaining vertices should split equally
    // vertex with 2 incoming splits from the same source should split once
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit("TestInputInitializer");
    setupPostDagCreation();
    initAllVertices(VertexState.INITIALIZING);
    
    int numTasks = 5;
    VertexImplWithCustomInitializer v1 = (VertexImplWithCustomInitializer) vertices
        .get("vertex1");
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    RootInputInitializerRunnerControlled runner1 = v1.getRootInputInitializerRunner();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(numTasks);
    runner1.completeInputInitialization(numTasks, v1Hints);

    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(numTasks, v1.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(v1Hints, v1.getVertexLocationHint().getTaskLocationHints());
    Assert.assertEquals(true, runner1.hasShutDown);
    
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex2").getState());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex3").getState());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.INITED, vertices.get("vertex4").getState());
    
    startVertex(v1);
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

    VertexImplWithCustomInitializer v1 = (VertexImplWithCustomInitializer) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    RootInputInitializerRunnerControlled runner1 = v1.getRootInputInitializerRunner();
    runner1.failInputInitialization();

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, runner1.hasShutDown);
    
    VertexImplWithCustomInitializer v2 = (VertexImplWithCustomInitializer) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    RootInputInitializerRunnerControlled runner2 = v2.getRootInputInitializerRunner();
    runner2.failInputInitialization();
    
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, runner2.hasShutDown);
  }
  
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testVertexWithInitializerSuccess() {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithInputInitializer("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithCustomInitializer v1 = (VertexImplWithCustomInitializer) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    RootInputInitializerRunnerControlled runner1 = v1.getRootInputInitializerRunner();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(5);
    runner1.completeInputInitialization(5, v1Hints);

    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(5, v1.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(v1Hints, v1.getVertexLocationHint().getTaskLocationHints());
    Assert.assertEquals(true, runner1.hasShutDown);
    
    VertexImplWithCustomInitializer v2 = (VertexImplWithCustomInitializer) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    RootInputInitializerRunnerControlled runner2 = v2.getRootInputInitializerRunner();
    List<TaskLocationHint> v2Hints = createTaskLocationHints(10);
    runner2.completeInputInitialization(10, v2Hints);
    
    Assert.assertEquals(VertexState.INITED, v2.getState());
    Assert.assertEquals(10, v2.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(v2Hints, v2.getVertexLocationHint().getTaskLocationHints());
    Assert.assertEquals(true, runner2.hasShutDown);
  }
  
  private List<TaskLocationHint> createTaskLocationHints(int numTasks) {
    List<TaskLocationHint> locationHints = Lists
        .newArrayListWithCapacity(numTasks);
    for (int i = 0; i < numTasks; i++) {
      TaskLocationHint taskLocationHint = new TaskLocationHint(
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
          clock, thh, true, appContext, vertexLocationHint, null);
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
  private static class VertexImplWithCustomInitializer extends VertexImpl {
    
    private final DrainDispatcher dispatcher;
    private RootInputInitializerRunnerControlled rootInputInitializerRunner;
    
    public VertexImplWithCustomInitializer(TezVertexID vertexId,
        VertexPlan vertexPlan, String vertexName, Configuration conf,
        EventHandler eventHandler, TaskAttemptListener taskAttemptListener,
        Clock clock, TaskHeartbeatHandler thh,
        AppContext appContext, VertexLocationHint vertexLocationHint, DrainDispatcher dispatcher) {
      super(vertexId, vertexPlan, vertexName, conf, eventHandler,
          taskAttemptListener, clock, thh, true,
          appContext, vertexLocationHint, null);
      this.dispatcher = dispatcher;
    }

    @Override
    protected RootInputInitializerRunner createRootInputInitializerRunner(
        String dagName, String vertexName, TezVertexID vertexID,
        EventHandler eventHandler, int numTasks, int numNodes, 
        Resource taskResource, Resource totalResource) {
      try {
        rootInputInitializerRunner = new RootInputInitializerRunnerControlled(dagName, vertexName, vertexID,
            eventHandler, numTasks, dispatcher, taskResource, totalResource);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return rootInputInitializerRunner;
    }
    
    RootInputInitializerRunnerControlled getRootInputInitializerRunner() {
      return rootInputInitializerRunner;
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static class RootInputInitializerRunnerControlled extends
      RootInputInitializerRunner {

    private List<RootInputLeafOutputDescriptor<InputDescriptor>> inputs;
    private final EventHandler eventHandler;
    private final DrainDispatcher dispatcher;
    private final TezVertexID vertexID;
    private volatile boolean hasShutDown = false;

    public RootInputInitializerRunnerControlled(String dagName,
        String vertexName, TezVertexID vertexID, EventHandler eventHandler,
        int numTasks, DrainDispatcher dispatcher,
        Resource taskResource, Resource totalResource) throws IOException {
      super(dagName, vertexName, vertexID, eventHandler, 
          UserGroupInformation.getCurrentUser(), 
          taskResource, totalResource, numTasks, 1, 1);
      this.eventHandler = eventHandler;
      this.dispatcher = dispatcher;
      this.vertexID = vertexID;
    }

    @Override
    public void runInputInitializers(
        List<RootInputLeafOutputDescriptor<InputDescriptor>> inputs) {
      this.inputs = inputs;
    }
    
    @Override
    public void shutdown() {
      hasShutDown = true;
    }

    public void failInputInitialization() {
      super.runInputInitializers(inputs);
      eventHandler.handle(new VertexEventRootInputFailed(vertexID, inputs
          .get(0).getEntityName(),
          new RuntimeException("MockInitializerFailed")));
      dispatcher.await();
    }

    public void completeInputInitialization(int targetTasks, List<TaskLocationHint> locationHints) {
      List<Event> events = Lists.newArrayListWithCapacity(targetTasks + 1);

      RootInputConfigureVertexTasksEvent configEvent = new RootInputConfigureVertexTasksEvent(
          targetTasks, locationHints);
      events.add(configEvent);
      for (int i = 0; i < targetTasks; i++) {
        RootInputDataInformationEvent diEvent = new RootInputDataInformationEvent(
            i, null);
        events.add(diEvent);
      }
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID, inputs
          .get(0).getEntityName(), events));
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
  public void testInitStartRace() {
    // Race when a source vertex manages to start before the target vertex has
    // been initialized
    setupPreDagCreation();
    dagPlan = createSamplerDAGPlan();
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
}

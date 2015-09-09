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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.MockDNSToSwitchMapping;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
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
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.MockClock;
import org.apache.tez.dag.app.TaskAttemptEventInfo;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.RootInputInitializerManager;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.TestStateChangeNotifier.StateChangeNotifierForTest;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.VertexStateUpdateListener;
import org.apache.tez.dag.app.dag.VertexTerminationCause;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.CallableEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventScheduleTask;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
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
import org.apache.tez.dag.app.dag.impl.AMUserCodeException.Source;
import org.apache.tez.dag.app.dag.impl.DAGImpl.VertexGroupInfo;
import org.apache.tez.dag.app.dag.impl.TestVertexImpl.VertexManagerWithException.VMExceptionLocation;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.container.ContainerContextMatcher;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
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
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.test.EdgeManagerForTest;
import org.apache.tez.test.VertexManagerPluginForTest;
import org.apache.tez.test.VertexManagerPluginForTest.VertexManagerPluginForTestConfig;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings("unchecked")
public class TestVertexImpl {

  private static final Logger LOG = LoggerFactory.getLogger(TestVertexImpl.class);
  private ListeningExecutorService execService;

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
  private StateChangeNotifierForTest updateTracker;
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
      if (getContext().getUserPayload() != null && getContext().getUserPayload().hasPayload()) {
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
    List<TaskEvent> events = Lists.newArrayList();
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      events.add(event);
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
  
  /**
   * v1 -> v2
   */
  private DAGPlan createDAGPlanWithVMException(String initializerClassName, VMExceptionLocation exLocation) {
    LOG.info("Setting up dag plan with VertexManager which would throw exception");
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
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
                        .build()
                )
                .addOutEdgeId("e1")
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(VertexManagerWithException.class.getName())
                    .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                        .setUserPayload(ByteString.copyFrom(exLocation.name().getBytes()))))
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
                .addInEdgeId("e1")
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName(VertexManagerWithException.class.getName())
                    .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                        .setUserPayload(ByteString.copyFrom(exLocation.name().getBytes()))))
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

  /**
   * v1 -> v2
   */
  private DAGPlan createDAGPlanWithIIException() {
    LOG.info("Setting up dag plan with VertexManager which would throw exception");
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
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
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

  private DAGPlan createDAGPlanWithNonExistInputInitializer() {
    LOG.info("Setting up dag plan with non exist inputinitializer");
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
                                "non-exist-input-initializer"))
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
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
                        .build()
                )
                .build()
        ).build();
    return dag;
  }

  private DAGPlan createDAGPlanWithNonExistOutputCommitter() {
    LOG.info("Setting up dag plan with non exist output committer");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("initializerWith0Tasks")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .addOutputs(
                    RootInputLeafOutputProto.newBuilder()
                        .setControllerDescriptor(
                            TezEntityDescriptorProto.newBuilder().setClassName(
                                "non-exist-output-committer"))
                        .setName("output1")
                        .setIODescriptor(
                            TezEntityDescriptorProto.newBuilder()
                                .setClassName("OutputClazz")
                                .build()
                        )
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
                .build()
        ).build();
    return dag;
  }

  private DAGPlan createDAGPlanWithNonExistVertexManager() {
    LOG.info("Setting up dag plan with non-exist VertexManager");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("initializerWith0Tasks")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
                        .build()
                )
                .setVertexManagerPlugin(TezEntityDescriptorProto.newBuilder()
                    .setClassName("non-exist-vertexmanager"))
                .build()
        ).build();
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
   
    return dag.createDag(conf, null, null, null, true);
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

  private DAGPlan createDAGPlanWithMultipleInitializers(String initializerClassName) {
    LOG.info("Setting up dag plan with multiple input initializer");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("testVertexWithMultipleInitializers")
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
                .build()
        ).build();
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
                    .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                        .setUserPayload(ByteString.copyFrom(new byte[]{0}))))
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
                            .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                                .setUserPayload(ByteString.copyFrom(new byte[]{1}))))
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

  private DAGPlan createDAGPlanWithRunningInitializer3() {
    // v2    v1 (send event to v3)
    //  \    /
    //   \  /
    //   v3 -----(In)
    //  (Receive events from v1)
    LOG.info("Setting up dag plan with running input initializer3");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("DagWithInputInitializer3")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(1)
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
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(1)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
                        .build()
                )
                .addOutEdgeId("e2")
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
                .addInEdgeId("e2")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v3"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v2_v3"))
                .setInputVertexName("vertex2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e2")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .build();
    return dag;
  }

  private DAGPlan createDAGPlanWithRunningInitializer4() {
    //   v1 (send event to v3)
    //    |
    //    |
    //   v2   (In)    (v2 can optioanlly send events to v2. Is setup via the initializer)
    //    |   /
    //    |  /
    //    v3 (Receive events from v1)
    // Events are not generated by a directly connected vertex
    LOG.info("Setting up dag plan with running input initializer4");
    DAGPlan dag = DAGPlan.newBuilder()
        .setName("DagWithInputInitializer4")
        .addVertex(
            VertexPlan.newBuilder()
                .setName("vertex1")
                .setType(PlanVertexType.NORMAL)
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(1)
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
                .setTaskConfig(
                    PlanTaskConfiguration.newBuilder()
                        .setNumTasks(2)
                        .setVirtualCores(1)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("x1.y1")
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
                .addInEdgeId("e2")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v1_v3"))
                .setInputVertexName("vertex1")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex2")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e1")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("v2_v3"))
                .setInputVertexName("vertex2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("o2"))
                .setOutputVertexName("vertex3")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("e2")
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
                            .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                                .setUserPayload((ByteString.copyFrom(edgePayload))))
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

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void setupPostDagCreation() throws TezException {
    String dagName = "dag0";
    // dispatcher may be created multiple times (setupPostDagCreation may be called multiples)
    if (dispatcher != null) {
      dispatcher.stop();
    }
    dispatcher = new DrainDispatcher();
    appContext = mock(AppContext.class);
    thh = mock(TaskHeartbeatHandler.class);
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
    execService = mock(ListeningExecutorService.class);
    final ListenableFuture<Void> mockFuture = mock(ListenableFuture.class);
    
    Mockito.doAnswer(new Answer() {
      public ListenableFuture<Void> answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          CallableEvent e = (CallableEvent) args[0];
          dispatcher.getEventHandler().handle(e);
          return mockFuture;
      }})
    .when(execService).submit((Callable<Void>) any());
    
    MockClock clock = new MockClock();
    
    doReturn(execService).when(appContext).getExecService();
    doReturn(conf).when(appContext).getAMConf();
    doReturn(new Credentials()).when(dag).getCredentials();
    doReturn(DAGPlan.getDefaultInstance()).when(dag).getJobPlan();
    doReturn(dagId).when(appContext).getCurrentDAGID();
    doReturn(dagId).when(dag).getID();
    doReturn(taskScheduler).when(appContext).getTaskScheduler();
    doReturn(Resource.newInstance(102400, 60)).when(taskScheduler).getTotalResources();
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(dispatcher.getEventHandler()).when(appContext).getEventHandler();
    doReturn(clock).when(appContext).getClock();
    
    vertexGroups = Maps.newHashMap();
    for (PlanVertexGroupInfo groupInfo : dagPlan.getVertexGroupsList()) {
      vertexGroups.put(groupInfo.getGroupName(), new VertexGroupInfo(groupInfo));
    }
    // updateTracker may be created multiple times (setupPostDagCreation may be called multiples)
    if (updateTracker != null) {
      updateTracker.stop();
    }
    updateTracker = new StateChangeNotifierForTest(appContext.getCurrentDAG());
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
      edges.put(edgePlan.getId(), new Edge(edgeProperty, dispatcher.getEventHandler(), conf));
    }

    parseVertexEdges();
    
    for (Edge edge : edges.values()) {
      edge.initialize();
    }

    dispatcher.register(CallableEventType.class, new CallableEventDispatcher());
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

  @BeforeClass
  public static void beforeClass() {
    MockDNSToSwitchMapping.initializeMockRackResolver();
  }

  @Before
  public void setup() throws TezException {
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
    updateTracker.stop();
    execService.shutdownNow();
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

  private void initVertex(VertexImpl v) {
    Assert.assertEquals(VertexState.NEW, v.getState());
    dispatcher.getEventHandler().handle(new VertexEvent(v.getVertexId(),
          VertexEventType.V_INIT));
    dispatcher.await();
  }

  private void startVertex(VertexImpl v) {
    startVertex(v, true);
  }

  private void killVertex(VertexImpl v) {
    dispatcher.getEventHandler().handle(
        new VertexEventTermination(v.getVertexId(), VertexTerminationCause.DAG_KILL));
    dispatcher.await();
    Assert.assertEquals(VertexState.KILLED, v.getState());
    Assert.assertEquals(v.getTerminationCause(), VertexTerminationCause.DAG_KILL);
  }

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
  public void testVertexInit() throws AMUserCodeException {
    initAllVertices(VertexState.INITED);

    VertexImpl v3 = vertices.get("vertex3");

    Assert.assertEquals("x3.y3", v3.getProcessorName());
    Assert.assertTrue(v3.getJavaOpts().contains("foo"));

    Assert.assertEquals(2, v3.getInputSpecList(0).size());
    Assert.assertEquals(2, v3.getInputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());
    Assert.assertEquals(2, v3.getOutputVerticesCount());

    assertTrue("vertex1".equals(v3.getInputSpecList(0).get(0)
        .getSourceVertexName())
        || "vertex2".equals(v3.getInputSpecList(0).get(0)
        .getSourceVertexName()));
    assertTrue("vertex1".equals(v3.getInputSpecList(0).get(1)
        .getSourceVertexName())
        || "vertex2".equals(v3.getInputSpecList(0).get(1)
        .getSourceVertexName()));
    assertTrue("i3_v1".equals(v3.getInputSpecList(0).get(0)
        .getInputDescriptor().getClassName())
        || "i3_v2".equals(v3.getInputSpecList(0).get(0)
        .getInputDescriptor().getClassName()));
    assertTrue("i3_v1".equals(v3.getInputSpecList(0).get(1)
        .getInputDescriptor().getClassName())
        || "i3_v2".equals(v3.getInputSpecList(0).get(1)
        .getInputDescriptor().getClassName()));

    assertTrue("vertex4".equals(v3.getOutputSpecList(0).get(0)
        .getDestinationVertexName())
        || "vertex5".equals(v3.getOutputSpecList(0).get(0)
        .getDestinationVertexName()));
    assertTrue("vertex4".equals(v3.getOutputSpecList(0).get(1)
        .getDestinationVertexName())
        || "vertex5".equals(v3.getOutputSpecList(0).get(1)
        .getDestinationVertexName()));
    assertTrue("o3_v4".equals(v3.getOutputSpecList(0).get(0)
        .getOutputDescriptor().getClassName())
        || "o3_v5".equals(v3.getOutputSpecList(0).get(0)
        .getOutputDescriptor().getClassName()));
    assertTrue("o3_v4".equals(v3.getOutputSpecList(0).get(1)
        .getOutputDescriptor().getClassName())
        || "o3_v5".equals(v3.getOutputSpecList(0).get(1)
        .getOutputDescriptor().getClassName()));
  }

  @Test(timeout=5000)
  public void testNonExistVertexManager() throws TezException {
    setupPreDagCreation();
    dagPlan = createDAGPlanWithNonExistVertexManager();
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    v1.handle(new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.INIT_FAILURE, v1.getTerminationCause());
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(),"")
        .contains("java.lang.ClassNotFoundException: non-exist-vertexmanager"));
  }

  @Test(timeout=5000)
  public void testNonExistInputInitializer() throws TezException {
    setupPreDagCreation();
    dagPlan = createDAGPlanWithNonExistInputInitializer();
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    v1.handle(new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.INIT_FAILURE, v1.getTerminationCause());
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(),"")
        .contains("java.lang.ClassNotFoundException: non-exist-input-initializer"));
  }

  @Test(timeout=5000)
  public void testNonExistOutputCommitter() throws TezException {
    setupPreDagCreation();
    dagPlan = createDAGPlanWithNonExistOutputCommitter();
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    v1.handle(new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.INIT_FAILURE, v1.getTerminationCause());
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(),"")
        .contains("java.lang.ClassNotFoundException: non-exist-output-committer"));
  }

  class TestUpdateListener implements VertexStateUpdateListener {
    List<VertexStateUpdate> events = Lists.newLinkedList();
    @Override
    public void onStateUpdated(VertexStateUpdate event) {
      events.add(event);
    }
  }
  
  @Test (timeout=5000)
  public void testVertexConfigureEvent() throws Exception {
    initAllVertices(VertexState.INITED);
    TestUpdateListener listener = new TestUpdateListener();
    updateTracker
        .registerForVertexUpdates("vertex3",
            EnumSet.of(org.apache.tez.dag.api.event.VertexState.CONFIGURED),
            listener);
    // completely configured event received for vertex when it inits
    Assert.assertEquals(1, listener.events.size());
    Assert.assertEquals("vertex3", listener.events.get(0).getVertexName());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        listener.events.get(0).getVertexState());
    updateTracker.unregisterForVertexUpdates("vertex3", listener);
  }
  
  @Test (timeout=5000)
  public void testVertexConfigureEventWithReconfigure() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    // initialize() will make VM call planned() and started() will make VM call done()
    dagPlan = createDAGPlanWithVMException("TestVMStateUpdate", VMExceptionLocation.NoExceptionDoReconfigure);
    setupPostDagCreation();

    TestUpdateListener listener = new TestUpdateListener();
    updateTracker
      .registerForVertexUpdates("vertex2",
          EnumSet.of(org.apache.tez.dag.api.event.VertexState.CONFIGURED),
          listener);

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    VertexImpl v2 = vertices.get("vertex2");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    // wait to ensure init is completed, so that rootInitManager is not null
    dispatcher.await();
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v2.getState());
    Assert.assertEquals(0, listener.events.size()); // configured event not sent
    startVertex(v1, true);
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v2.getState());
    Assert.assertEquals(1, listener.events.size()); // configured event sent after VM
    Assert.assertEquals("vertex2", listener.events.get(0).getVertexName());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        listener.events.get(0).getVertexState());
    updateTracker.unregisterForVertexUpdates("vertex2", listener);    
  }

  @Test (timeout=5000)
  public void testVertexConfigureEventBadReconfigure() {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");
    VertexImpl v2 = vertices.get("vertex2");
    VertexImpl v1 = vertices.get("vertex1");
    startVertex(v2);
    startVertex(v1);
    try {
      // cannot call doneReconfiguringVertex() without calling vertexReconfigurationPlanned()
      v3.doneReconfiguringVertex();
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("invoked only after vertexReconfigurationPlanned"));
    }
  }
  
  @Test (timeout=5000)
  public void testVertexConfigureEventBadSetParallelism() throws Exception {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");
    VertexImpl v2 = vertices.get("vertex2");
    VertexImpl v1 = vertices.get("vertex1");
    startVertex(v2);
    startVertex(v1);
    try {
      // cannot reconfigure a fully configured vertex without first notifying
      v3.reconfigureVertex(1, null, null);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("context.vertexReconfigurationPlanned() before re-configuring"));
    }
  }

  @Test(timeout = 5000)
  public void testVertexStart() {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex2");
    startVertex(v);
  }
  
  @Test (timeout = 5000)
  public void testVertexGetTAAttempts() throws Exception {
    initAllVertices(VertexState.INITED);
    VertexImpl v1 = vertices.get("vertex1");
    startVertex(v1);
    VertexImpl v2 = vertices.get("vertex2");
    startVertex(v2);
    VertexImpl v3 = vertices.get("vertex3");
    VertexImpl v4 = vertices.get("vertex4");
    
    Assert.assertEquals(VertexState.RUNNING, v4.getState());
    Assert.assertEquals(1, v4.sourceVertices.size());
    Edge e = v4.sourceVertices.get(v3);
    TezTaskAttemptID v3TaId = TezTaskAttemptID.getInstance(
        TezTaskID.getInstance(v3.getVertexId(), 0), 0);
    TezTaskAttemptID v4TaId = TezTaskAttemptID.getInstance(
        TezTaskID.getInstance(v4.getVertexId(), 0), 0);
    
    for (int i=0; i<5; ++i) {
      v4.handle(new VertexEventRouteEvent(v4.getVertexId(), Collections.singletonList(
          new TezEvent(DataMovementEvent.create(0, null), 
              new EventMetaData(EventProducerConsumerType.OUTPUT, v3.getName(), v3.getName(), v3TaId)))));
    }
    dispatcher.await();
    // verify all events have been put in pending.
    // this is not necessary after legacy routing has been removed
    Assert.assertEquals(5, v4.pendingTaskEvents.size());
    List<TaskWithLocationHint> taskList = new LinkedList<VertexManagerPluginContext.TaskWithLocationHint>();
    // scheduling start to trigger edge routing to begin
    for (int i=0; i<v4.getTotalTasks(); ++i) {
      taskList.add(new TaskWithLocationHint(i, null));
    }
    v4.scheduleTasks(taskList);
    dispatcher.await();
    // verify all events have been moved to taskEvents
    Assert.assertEquals(5, v4.getOnDemandRouteEvents().size());
    for (int i=5; i<11; ++i) {
      v4.handle(new VertexEventRouteEvent(v4.getVertexId(), Collections.singletonList(
          new TezEvent(DataMovementEvent.create(0, null), 
              new EventMetaData(EventProducerConsumerType.OUTPUT, v3.getName(), v3.getName(), v3TaId)))));
    }
    dispatcher.await();
    // verify all events have been are in taskEvents
    Assert.assertEquals(11, v4.getOnDemandRouteEvents().size());
    
    TaskAttemptEventInfo eventInfo;
    EdgeManagerPluginOnDemand mockPlugin = mock(EdgeManagerPluginOnDemand.class);
    EventRouteMetadata mockRoute = EventRouteMetadata.create(1, new int[]{0});
    e.edgeManager = mockPlugin;
    // source task id will not match. all events will return null
    when(mockPlugin.routeDataMovementEventToDestination(1, 0, 0)).thenReturn(mockRoute);
    eventInfo = v4.getTaskAttemptTezEvents(v4TaId, 0, 0, 1);
    Assert.assertEquals(11, eventInfo.getNextFromEventId()); // all events traversed
    Assert.assertEquals(0, eventInfo.getEvents().size()); // no events
    
    int fromEventId = 0;
    // source task id will match. all events will be returned
    // max events is respected.
    when(
        mockPlugin.routeDataMovementEventToDestination(anyInt(),
            anyInt(), anyInt())).thenReturn(mockRoute);
    for (int i=0; i<11; ++i) {
      eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 1);
      fromEventId = eventInfo.getNextFromEventId();
      Assert.assertEquals((i+1), fromEventId);
      Assert.assertEquals(1, eventInfo.getEvents().size());
    }
    eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 1);
    Assert.assertEquals(11, eventInfo.getNextFromEventId()); // all events traversed
    Assert.assertEquals(0, eventInfo.getEvents().size()); // no events
    
    // change max events to larger value. max events does not evenly divide total events
    fromEventId = 0;
    for (int i=1; i<=2; ++i) {
      eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 5);
      fromEventId = eventInfo.getNextFromEventId();
      Assert.assertEquals((i*5), fromEventId);
      Assert.assertEquals(5, eventInfo.getEvents().size());
    }
    eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 5);
    Assert.assertEquals(11, eventInfo.getNextFromEventId()); // all events traversed
    Assert.assertEquals(1, eventInfo.getEvents().size()); // remainder events
    
    // return more events that dont evenly fit in max size
    mockRoute = EventRouteMetadata.create(2, new int[]{0, 0});    
    when(
        mockPlugin.routeDataMovementEventToDestination(anyInt(),
            anyInt(), anyInt())).thenReturn(mockRoute);
    fromEventId = 0;
    int lastFromEventId = 0;
    for (int i=1; i<=4; ++i) {
      eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 5);
      fromEventId = eventInfo.getNextFromEventId();
      Assert.assertEquals((i%2 > 0 ? (lastFromEventId+=2) : (lastFromEventId+=3)), fromEventId);     
      Assert.assertEquals(5, eventInfo.getEvents().size());
    }
    eventInfo = v4.getTaskAttemptTezEvents(v4TaId, fromEventId, 0, 5);
    Assert.assertEquals(11, eventInfo.getNextFromEventId()); // all events traversed
    Assert.assertEquals(2, eventInfo.getEvents().size()); // remainder events
  }

  @Test(timeout = 5000)
  public void testVertexReconfigurePlannedAfterInit() throws Exception {
    initAllVertices(VertexState.INITED);
    VertexImpl v3 = vertices.get("vertex3");

    try {
      v3.vertexReconfigurationPlanned();
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("context.vertexReconfigurationPlanned() cannot be called after initialize()"));
    }
  }

  private void checkTasks(Vertex v, int numTasks) {
    Assert.assertEquals(numTasks, v.getTotalTasks());
    Map<TezTaskID, Task> tasks = v.getTasks();
    Assert.assertEquals(numTasks, tasks.size());
    // check all indices
    int i = 0;
    // iteration maintains order due to linked hash map
    for(Task task : tasks.values()) {
      Assert.assertEquals(i, task.getTaskId().getId());
      i++;
    }
  }
  
  @Test(timeout = 5000)
  public void testVertexSetParallelismDecrease() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Assert.assertEquals(2, v3.getTasks().size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());

    EdgeProperty edgeProp = EdgeProperty.create(mockEdgeManagerDescriptor, 
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, OutputDescriptor.create("Out"), 
        InputDescriptor.create("In"));
    Map<String, EdgeProperty> edgeManagerDescriptors =
        Collections.singletonMap(
       v1.getName(), edgeProp);
    v3.reconfigureVertex(1, null, edgeManagerDescriptors);
    v3.doneReconfiguringVertex();
    assertTrue(v3.sourceVertices.get(v1).getEdgeManager() instanceof
        EdgeManagerForTest);
    checkTasks(v3, 1);
  }

  @Test(timeout = 5000)
  public void testVertexSetParallelismIncrease() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Assert.assertEquals(2, v3.getTasks().size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);

    EdgeManagerPluginDescriptor mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    EdgeProperty edgeProp = EdgeProperty.create(mockEdgeManagerDescriptor, 
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, OutputDescriptor.create("Out"), 
        InputDescriptor.create("In"));
    Map<String, EdgeProperty> edgeManagerDescriptors =
        Collections.singletonMap(
       v1.getName(), edgeProp);
    v3.reconfigureVertex(10, null, edgeManagerDescriptors);
    v3.doneReconfiguringVertex();
    assertTrue(v3.sourceVertices.get(v1).getEdgeManager() instanceof
        EdgeManagerForTest);
    checkTasks(v3, 10);
  }
  
  @Test(timeout = 5000)
  public void testVertexSetParallelismMultiple() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    v3.reconfigureVertex(10, null, null);
    checkTasks(v3, 10);
    
    v3.reconfigureVertex(5, null, null);
    checkTasks(v3, 5);
    v3.doneReconfiguringVertex();
  }

  @Test(timeout = 5000)
  public void testVertexSetParallelismMultipleFailAfterDone() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    v3.reconfigureVertex(10, null, null);
    checkTasks(v3, 10);
    v3.doneReconfiguringVertex();

    try {
      v3.reconfigureVertex(5, null, null);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Vertex is fully configured but still"));
    }
  }

  @Test(timeout = 5000)
  public void testVertexSetParallelismMultipleFailAfterSchedule() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    v3.reconfigureVertex(10, null, null);
    checkTasks(v3, 10);
    v3.scheduleTasks(Collections.singletonList(new TaskWithLocationHint(new Integer(0), null)));
    try {
      v3.reconfigureVertex(5, null, null);
      Assert.fail();
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("setParallelism cannot be called after scheduling"));
    }
  }
  
  @Test(timeout = 5000)
  public void testVertexScheduleSendEvent() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    v3.reconfigureVertex(10, null, null);
    checkTasks(v3, 10);
    taskEventDispatcher.events.clear();
    TaskLocationHint mockLocation = mock(TaskLocationHint.class);
    v3.scheduleTasks(Collections.singletonList(new TaskWithLocationHint(new Integer(0), mockLocation)));
    dispatcher.await();
    Assert.assertEquals(1, taskEventDispatcher.events.size());
    TaskEventScheduleTask event = (TaskEventScheduleTask) taskEventDispatcher.events.get(0);
    Assert.assertEquals(mockLocation, event.getTaskLocationHint());
    Assert.assertNotNull(event.getBaseTaskSpec());
  }
  
  @Test(timeout = 5000)
  public void testVertexSetParallelismFailAfterSchedule() throws Exception {
    VertexImpl v3 = vertices.get("vertex3");
    v3.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Assert.assertEquals(2, v3.getTotalTasks());
    Map<TezTaskID, Task> tasks = v3.getTasks();
    Assert.assertEquals(2, tasks.size());

    VertexImpl v1 = vertices.get("vertex1");
    startVertex(vertices.get("vertex2"));
    startVertex(v1);
    v3.scheduleTasks(Collections.singletonList(new TaskWithLocationHint(new Integer(0), null)));
    try {
      v3.reconfigureVertex(5, null, null);
      Assert.fail();
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("setParallelism cannot be called after scheduling"));
    }
  }
  
  @Test(timeout = 5000)
  public void testVertexPendingTaskEvents() {
    // Remove after bulk routing API is removed
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
  public void testSetCustomEdgeManager() throws Exception {
    VertexImpl v5 = vertices.get("vertex5"); // Vertex5 linked to v3 (v3 src, v5 dest)
    v5.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    Edge edge = edges.get("e4");
    EdgeManagerPlugin em = edge.getEdgeManager();
    EdgeManagerForTest originalEm = (EdgeManagerForTest) em;
    assertTrue(Arrays.equals(edgePayload, originalEm.getEdgeManagerContext()
        .getUserPayload().deepCopyAsArray()));

    UserPayload userPayload = UserPayload.create(ByteBuffer.wrap(new String("foo").getBytes()));
    EdgeManagerPluginDescriptor edgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    edgeManagerDescriptor.setUserPayload(userPayload);
    EdgeProperty edgeProp = EdgeProperty.create(edgeManagerDescriptor, 
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, OutputDescriptor.create("Out"), 
        InputDescriptor.create("In"));

    Vertex v3 = vertices.get("vertex3");

    Map<String, EdgeProperty> edgeManagerDescriptors =
        Collections.singletonMap(v3.getName(), edgeProp);
    v5.reconfigureVertex(v5.getTotalTasks() - 1, null, edgeManagerDescriptors);
    v5.doneReconfiguringVertex();

    VertexImpl v5Impl = (VertexImpl) v5;

    EdgeManagerPlugin modifiedEdgeManager = v5Impl.sourceVertices.get(v3)
        .getEdgeManager();
    Assert.assertNotNull(modifiedEdgeManager);
    assertTrue(modifiedEdgeManager instanceof EdgeManagerForTest);

    // Ensure initialize() is called with the correct payload
    assertTrue(Arrays.equals(userPayload.deepCopyAsArray(),
        ((EdgeManagerForTest) modifiedEdgeManager).getUserPayload().deepCopyAsArray()));
  }

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
    Assert.assertTrue(v.initTimeRequested > 0);
    Assert.assertTrue(v.initedTime > 0);
    Assert.assertTrue(v.startTimeRequested > 0);
    Assert.assertTrue(v.startedTime > 0);
    Assert.assertTrue(v.finishTime > 0);
  }

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
        StringUtils.join(v.getDiagnostics(), ",").toLowerCase(Locale.ENGLISH);
    assertTrue(diagnostics.contains("task failed"
        + ", taskid=" + t1.toString()));
  }

  @Test(timeout = 5000)
  public void testVertexKillDiagnosticsInInit() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex4");
    killVertex(v2);
    String diagnostics =
        StringUtils.join(v2.getDiagnostics(), ",").toLowerCase(Locale.ENGLISH);
    LOG.info("diagnostics v2: " + diagnostics);
    assertTrue(diagnostics.contains(
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
        StringUtils.join(v3.getDiagnostics(), ",").toLowerCase(Locale.ENGLISH);
    assertTrue(diagnostics.contains(
        "vertex received kill while in running state"));
    Assert.assertEquals(VertexTerminationCause.DAG_KILL, v3.getTerminationCause());
    assertTrue(diagnostics.contains(v3.getTerminationCause().name().toLowerCase(Locale.ENGLISH)));
  }

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
    assertTrue(v6.getOutputCommitter("outputx")
        instanceof CountingOutputCommitter);
  }

  @Test(timeout = 5000)
  public void testVertexManagerInit() {
    initAllVertices(VertexState.INITED);
    VertexImpl v2 = vertices.get("vertex2");
    assertTrue(v2.getVertexManager().getPlugin()
        instanceof ImmediateStartVertexManager);

    VertexImpl v6 = vertices.get("vertex6");
    assertTrue(v6.getVertexManager().getPlugin()
        instanceof ShuffleVertexManager);
  }

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
  public void testVertexTaskAttemptProcessorFailure() throws Exception {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex1");

    startVertex(v);
    dispatcher.await();
    TaskAttemptImpl ta = (TaskAttemptImpl) v.getTask(0).getAttempts().values().iterator().next();
    ta.handle(new TaskAttemptEventSchedule(ta.getID(), 2, 2));
    
    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appContext);
    containers.addContainerIfNew(container);
    doReturn(containers).when(appContext).getAllContainers();

    ta.handle(new TaskAttemptEventStartedRemotely(ta.getID(), contId, null));
    Assert.assertEquals(TaskAttemptStateInternal.RUNNING, ta.getInternalState());

    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v.getVertexId(), Collections.singletonList(new TezEvent(
            new TaskAttemptFailedEvent("Failed"), new EventMetaData(
                EventProducerConsumerType.PROCESSOR, v.getName(), null, ta.getID())))));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(TaskAttemptTerminationCause.APPLICATION_ERROR, ta.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testVertexTaskAttemptInputFailure() throws Exception {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex1");

    startVertex(v);
    dispatcher.await();
    TaskAttemptImpl ta = (TaskAttemptImpl) v.getTask(0).getAttempts().values().iterator().next();
    ta.handle(new TaskAttemptEventSchedule(ta.getID(), 2, 2));
    
    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appContext);
    containers.addContainerIfNew(container);
    doReturn(containers).when(appContext).getAllContainers();

    ta.handle(new TaskAttemptEventStartedRemotely(ta.getID(), contId, null));
    Assert.assertEquals(TaskAttemptStateInternal.RUNNING, ta.getInternalState());

    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v.getVertexId(), Collections.singletonList(new TezEvent(
            new TaskAttemptFailedEvent("Failed"), new EventMetaData(
                EventProducerConsumerType.INPUT, v.getName(), null, ta.getID())))));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(TaskAttemptTerminationCause.INPUT_READ_ERROR, ta.getTerminationCause());
  }


  @Test(timeout = 5000)
  public void testVertexTaskAttemptOutputFailure() throws Exception {
    initAllVertices(VertexState.INITED);

    VertexImpl v = vertices.get("vertex1");

    startVertex(v);
    dispatcher.await();
    TaskAttemptImpl ta = (TaskAttemptImpl) v.getTask(0).getAttempts().values().iterator().next();
    ta.handle(new TaskAttemptEventSchedule(ta.getID(), 2, 2));
    
    NodeId nid = NodeId.newInstance("127.0.0.1", 0);
    ContainerId contId = ContainerId.newInstance(appAttemptId, 3);
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(contId);
    when(container.getNodeId()).thenReturn(nid);
    when(container.getNodeHttpAddress()).thenReturn("localhost:0");
    AMContainerMap containers = new AMContainerMap(
        mock(ContainerHeartbeatHandler.class), mock(TaskAttemptListener.class),
        new ContainerContextMatcher(), appContext);
    containers.addContainerIfNew(container);
    doReturn(containers).when(appContext).getAllContainers();

    ta.handle(new TaskAttemptEventStartedRemotely(ta.getID(), contId, null));
    Assert.assertEquals(TaskAttemptStateInternal.RUNNING, ta.getInternalState());

    dispatcher.getEventHandler().handle(
        new VertexEventRouteEvent(v.getVertexId(), Collections.singletonList(new TezEvent(
            new TaskAttemptFailedEvent("Failed"), new EventMetaData(
                EventProducerConsumerType.OUTPUT, v.getName(), null, ta.getID())))));
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v.getState());
    Assert.assertEquals(TaskAttemptTerminationCause.OUTPUT_WRITE_ERROR, ta.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testSourceVertexStartHandling() {
    LOG.info("Testing testSourceVertexStartHandling");
    initAllVertices(VertexState.INITED);

    VertexImpl v6 = vertices.get("vertex6");
    VertexImpl v3 = vertices.get("vertex3");

    startVertex(vertices.get("vertex1"));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v3.getState());
    long v3StartTimeRequested = v3.startTimeRequested;
    Assert.assertEquals(1, v3.numStartedSourceVertices);
    Assert.assertTrue(v3StartTimeRequested > 0);
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    startVertex(vertices.get("vertex2"));
    dispatcher.await();
    // start request from second source vertex overrides the value from the first source vertex
    Assert.assertEquals(VertexState.RUNNING, v3.getState());
    Assert.assertEquals(2, v3.numStartedSourceVertices);
    Assert.assertTrue(v3.startTimeRequested > v3StartTimeRequested);
    LOG.info("Verifying v6 state " + v6.getState());
    Assert.assertEquals(VertexState.RUNNING, v6.getState());
    Assert.assertEquals(3, v6.getDistanceFromRoot());
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

  @Test(timeout = 5000)
  public void testVertexSuccessToFailedAfterTaskScheduler() throws Exception {
    // For downstream failures
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName())
                .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                    .setUserPayload(ByteString.copyFrom(
                        new CountingOutputCommitter.CountingOutputCommitterConfig()
                            .toUserPayload())).build()))
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
  public void testBadCommitter() throws Exception {
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName())
                .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                .setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, false).toUserPayload())).build()))
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

    Assert.assertEquals(1, committer.abortCounter);
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
  }

  @Test(timeout = 5000)
  public void testBadCommitter2() throws Exception {
    VertexImpl v = vertices.get("vertex2");

    List<RootInputLeafOutputProto> outputs =
        new ArrayList<RootInputLeafOutputProto>();
    outputs.add(RootInputLeafOutputProto.newBuilder()
        .setControllerDescriptor(
            TezEntityDescriptorProto.newBuilder().setClassName(
                CountingOutputCommitter.class.getName())
                .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                .setUserPayload(ByteString.copyFrom(
                    new CountingOutputCommitter.CountingOutputCommitterConfig(
                        true, true, true).toUserPayload())).build()))
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

    Assert.assertEquals(1, committer.abortCounter);
    Assert.assertEquals(1, committer.initCounter);
    Assert.assertEquals(1, committer.setupCounter);
  }

  @Test(timeout = 5000)
  public void testVertexInitWithCustomVertexManager() throws Exception {
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
    v1.reconfigureVertex(numTasks, null, null);
    v2.reconfigureVertex(numTasks, null, null);
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
    Assert.assertTrue(v3.numStartedSourceVertices > 0);
    long v3StartTimeRequested = v3.startTimeRequested;
    Assert.assertTrue(v3StartTimeRequested > 0);
    v3.reconfigureVertex(numTasks, null, null);
    dispatcher.await();
    Assert.assertEquals(numTasks, v3.getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, v3.getState());
    // the start time requested should remain at its original value
    Assert.assertEquals(v3StartTimeRequested, v3.startTimeRequested);
  }

  @Test(timeout = 5000)
  public void testVertexManagerHeuristic() throws TezException {
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
  public void testVertexWithOneToOneSplit() throws Exception {
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
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(numTasks, v1.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v1Hints.size(); ++i) {
      Assert.assertEquals(v1Hints.get(i), v1.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager1.hasShutDown);

    startVertex(v1);

    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    // v4, v6 still initializing since edge is null
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex6").getState());
    
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex5").getState());
    // v4, v6 still initializing since edge is null
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.INITIALIZING, vertices.get("vertex6").getState());
    
    mockEdgeManagerDescriptor =
        EdgeManagerPluginDescriptor.create(EdgeManagerForTest.class.getName());
    e = vertices.get("vertex6").sourceVertices.get(vertices.get("vertex4"));
    Assert.assertNull(e.getEdgeManager());
    e.setCustomEdgeManager(mockEdgeManagerDescriptor);
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex6").getState());
  }
  
  @Test(timeout = 5000)
  public void testVertexWithOneToOneSplitWhileRunning() throws Exception {
    int numTasks = 5;
    // create a diamond shaped dag with 1-1 edges. 
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit(null, numTasks, false);
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    v1.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    
    // fudge vertex manager so that tasks dont start running
    v1.vertexManager = new VertexManager(
        VertexManagerPluginDescriptor.create(VertexManagerPluginForTest.class.getName()),
        UserGroupInformation.getCurrentUser(), v1, appContext, mock(StateChangeNotifier.class));
    v1.vertexManager.initialize();
    startVertex(v1);
    dispatcher.await();
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex2").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex3").getState());
    Assert.assertEquals(VertexState.RUNNING, vertices.get("vertex4").getState());
    // change parallelism
    int newNumTasks = 3;
    v1.reconfigureVertex(newNumTasks, null, null);
    v1.doneReconfiguringVertex();
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
  public void testVertexWithOneToOneSplitWhileInited() throws Exception {
    int numTasks = 5;
    // create a diamond shaped dag with 1-1 edges. 
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit(null, numTasks, false);
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    v1.vertexReconfigurationPlanned();
    initAllVertices(VertexState.INITED);
    
    // fudge vertex manager so that tasks dont start running
    v1.vertexManager = new VertexManager(
        VertexManagerPluginDescriptor.create(VertexManagerPluginForTest.class.getName()),
        UserGroupInformation.getCurrentUser(), v1, appContext, mock(StateChangeNotifier.class));
    v1.vertexManager.initialize();
    
    Assert.assertEquals(numTasks, vertices.get("vertex2").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex3").getTotalTasks());
    Assert.assertEquals(numTasks, vertices.get("vertex4").getTotalTasks());
    // change parallelism
    int newNumTasks = 3;
    v1.reconfigureVertex(newNumTasks, null, null);
    v1.doneReconfiguringVertex();
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
  public void testVertexVMErrorReport() throws Exception {
    int numTasks = 5;
    // create a diamond shaped dag with 1-1 edges. 
    setupPreDagCreation();
    dagPlan = createDAGPlanForOneToOneSplit(null, numTasks, false);
    setupPostDagCreation();
    VertexImpl v1 = vertices.get("vertex1");
    initAllVertices(VertexState.INITED);
    
    // fudge vertex manager so that tasks dont start running
    // it is not calling reconfigurtionPlanned() but will call reconfigureVertex().
    // the vertex is already fully configured. this causes exception and verify that
    // its caught and reported.
    VertexManagerPluginForTestConfig config = new VertexManagerPluginForTestConfig();
    config.setReconfigureOnStart(true);
    v1.vertexManager = new VertexManager(VertexManagerPluginDescriptor.create(
        VertexManagerPluginForTest.class.getName()).setUserPayload(
        UserPayload.create(config.getPayload())), UserGroupInformation.getCurrentUser(), v1,
        appContext, mock(StateChangeNotifier.class));
    v1.vertexManager.initialize();

    startVertex(v1, false);
    dispatcher.await();

    // failed due to exception
    Assert.assertEquals(VertexState.FAILED, vertices.get("vertex1").getState());
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, vertices.get("vertex1")
        .getTerminationCause());
    Assert.assertTrue(Joiner.on(":").join(vertices.get("vertex1").getDiagnostics()).contains(
        "context.vertexReconfigurationPlanned() before re-configuring"));
  }

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

  @Test(timeout = 5000)
  public void testVertexWithInitializerFailure() throws Exception {
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
    dispatcher.await();

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v1
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager1.hasShutDown);
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(), ",")
        .contains(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE.name()));
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(), ",")
        .contains("MockInitializerFailed"));
    
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    RootInputInitializerManagerControlled initializerManager2 = v2.getRootInputInitializerManager();
    initializerManager2.failInputInitialization();
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    Assert.assertEquals(true, initializerManager2.hasShutDown);
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(), ",")
        .contains(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE.name()));
    Assert.assertTrue(StringUtils.join(v1.getDiagnostics(), ",")
        .contains("MockInitializerFailed"));
  }

  @Test(timeout = 10000)
  public void testVertexWithInitializerParallelismSetTo0() throws InterruptedException, TezException {
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

    while (v1.getState() == VertexState.INITIALIZING || v1.getState() == VertexState.INITED) {
      // initializer thread may not have started, so call initializer.go() in the loop all the time
      initializer.go();
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
    initializer.setNumVertexStateUpdateEvents(3);
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

    // wait for state update events are received, this is done in the state notifier thread
    initializer.waitForVertexStateUpdate();

    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        initializer.stateUpdates.get(0).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.RUNNING,
        initializer.stateUpdates.get(1).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.SUCCEEDED,
        initializer.stateUpdates.get(2).getVertexState());
  }

  @Test(timeout = 10000)
  public void testInputInitializerEventMultipleAttempts() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer4();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    VertexImplWithRunningInputInitializer v2 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex2");
    VertexImplWithRunningInputInitializer v3 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex3");

    initVertex(v1);
    startVertex(v1);
    dispatcher.await();

    // Vertex1 start should trigger downstream vertices
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.RUNNING, v2.getState());
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());

    ByteBuffer expected;

    // Genrate events from v1 to v3's InputInitializer
    ByteBuffer payload = ByteBuffer.allocate(12).putInt(0, 1).putInt(4, 0).putInt(8, 0);
    InputInitializerEvent event = InputInitializerEvent.create("vertex3", "input1", payload);
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", ta0_t0_v1));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    payload = ByteBuffer.allocate(12).putInt(0, 1).putInt(4, 0).putInt(8, 1);
    expected = payload;
    event = InputInitializerEvent.create("vertex3", "input1", payload);
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskAttemptID ta1_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 1);
    tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", ta1_t0_v1));
    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Events should not be cached in the vertex, since the initializer is running
    Assert.assertEquals(0, v3.pendingInitializerEvents.size());

    // Events should be cached since the tasks have not succeeded.
    // Verify that events are cached
    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        v3.rootInputInitializerManager.getInitializerWrapper("input1");
    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(2, initializerWrapper.getPendingEvents().get(v1.getName()).size());


    // Get all tasks of vertex1 to succeed.
    for (TezTaskID taskId : v1.getTasks().keySet()) {
      // Make attempt 1 of every task succeed
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 1);
      v1.handle( new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v1.stateChangeNotifier.taskSucceeded(v1.getName(), taskId, taskAttemptId.getId());
    }
    dispatcher.await();

    // v3 would have processed an INIT event and moved into INITIALIZING state.
    // Since source tasks were complete - the events should have been consumed.
    // Initializer would have run, and processed events.
    while (v3.getState()  != VertexState.RUNNING) {
      Thread.sleep(10);
    }
    Assert.assertEquals(VertexState.RUNNING, v3.getState());

    Assert.assertEquals(1, initializer.initializerEvents.size());
    Assert.assertEquals(expected, initializer.initializerEvents.get(0).getUserPayload());

  }

  @Test(timeout = 10000)
  public void testInputInitializerEventsMultipleSources() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    initializer.setNumExpectedEvents(4);
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer4();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    VertexImplWithRunningInputInitializer v2 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex2");
    VertexImplWithRunningInputInitializer v3 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex3");

    initVertex(v1);
    startVertex(v1);
    dispatcher.await();

    // Vertex1 start should trigger downstream vertices
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.RUNNING, v2.getState());
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());

    List<ByteBuffer> expectedPayloads = new LinkedList<ByteBuffer>();

    // Genrate events from v1 to v3's InputInitializer
    ByteBuffer payload = ByteBuffer.allocate(12).putInt(0, 1).putInt(4, 0).putInt(8, 0);
    expectedPayloads.add(payload);
    InputInitializerEvent event = InputInitializerEvent.create("vertex3", "input1", payload);
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", ta0_t0_v1));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Events should not be cached in the vertex, since the initializer is running
    Assert.assertEquals(0, v3.pendingInitializerEvents.size());

    // Events should be cached since the tasks have not succeeded.
    // Verify that events are cached
    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        v3.rootInputInitializerManager.getInitializerWrapper("input1");
    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(1, initializerWrapper.getPendingEvents().get(v1.getName()).size());


    // Get all tasks of vertex1 to succeed.
    for (TezTaskID taskId : v1.getTasks().keySet()) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      v1.handle( new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v1.stateChangeNotifier.taskSucceeded(v1.getName(), taskId, taskAttemptId.getId());
    }
    dispatcher.await();

    Assert.assertEquals(1, initializer.initializerEvents.size());


    // Test written based on this
    Assert.assertEquals(2, v2.getTotalTasks());
    // Generate events from v2 to v3's initializer. 1 from task 0, 2 from task 1
    for (Task task : v2.getTasks().values()) {
      TezTaskID taskId = task.getTaskId();
      TezTaskAttemptID attemptId = TezTaskAttemptID.getInstance(taskId, 0);
      int numEventsFromTask = taskId.getId() + 1;
      for (int i = 0; i < numEventsFromTask; i++) {
        payload = ByteBuffer.allocate(12).putInt(0, 2).putInt(4, taskId.getId()).putInt(8, i);
        expectedPayloads.add(payload);
        InputInitializerEvent event2 = InputInitializerEvent.create("vertex3", "input1", payload);
        TezEvent tezEvent2 = new TezEvent(event2,
            new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex2", "vertex3", attemptId));
        dispatcher.getEventHandler()
            .handle(
                new VertexEventRouteEvent(v2.getVertexId(), Collections.singletonList(tezEvent2)));
        dispatcher.await();
      }
    }

    // Validate queueing of these events
    // Only v2 events pending
    Assert.assertEquals(1, initializerWrapper.getPendingEvents().keySet().size());
    // 3 events pending
    Assert.assertEquals(3, initializerWrapper.getPendingEvents().get(v2.getName()).size());

    // Get all tasks of vertex1 to succeed.
    for (TezTaskID taskId : v2.getTasks().keySet()) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      v2.handle( new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v2.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v2.stateChangeNotifier.taskSucceeded(v2.getName(), taskId, taskAttemptId.getId());
    }
    dispatcher.await();

    // v3 would have processed an INIT event and moved into INITIALIZING state.
    // Since source tasks were complete - the events should have been consumed.
    // Initializer would have run, and processed events.
    while (v3.getState()  != VertexState.RUNNING) {
      Thread.sleep(10);
    }
    Assert.assertEquals(VertexState.RUNNING, v3.getState());

    Assert.assertEquals(4, initializer.initializerEvents.size());
    Assert.assertTrue(initializer.initComplete.get());

    Assert.assertEquals(2, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(0, initializerWrapper.getPendingEvents().get(v1.getName()).size());

    for (InputInitializerEvent initializerEvent : initializer.initializerEvents) {
      expectedPayloads.remove(initializerEvent.getUserPayload());
    }
    Assert.assertEquals(0, expectedPayloads.size());

  }

  @Test(timeout = 10000)
  public void testInputInitializerEventNoDirectConnection() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer4();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    VertexImplWithRunningInputInitializer v2 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex2");
    VertexImplWithRunningInputInitializer v3 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex3");

    initVertex(v1);
    startVertex(v1);
    dispatcher.await();

    // Vertex1 start should trigger downstream vertices
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.RUNNING, v2.getState());
    Assert.assertEquals(VertexState.INITIALIZING, v3.getState());

    // Genrate events from v1 to v3's InputInitializer
    InputInitializerEvent event = InputInitializerEvent.create("vertex3", "input1", null);
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", ta0_t0_v1));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Events should not be cached in the vertex, since the initializer is running
    Assert.assertEquals(0, v3.pendingInitializerEvents.size());

    // Events should be cached since the tasks have not succeeded.
    // Verify that events are cached
    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        v3.rootInputInitializerManager.getInitializerWrapper("input1");
    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(1, initializerWrapper.getPendingEvents().get(v1.getName()).size());

    // Get all tasks of vertex1 to succeed.
    for (TezTaskID taskId : v1.getTasks().keySet()) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      v1.handle( new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v1.stateChangeNotifier.taskSucceeded(v1.getName(), taskId, taskAttemptId.getId());
    }
    dispatcher.await();

    // v3 would have processed an INIT event and moved into INITIALIZING state.
    // Since source tasks were complete - the events should have been consumed.
    // Initializer would have run, and processed events.
    while (v3.getState()  != VertexState.RUNNING) {
      Thread.sleep(10);
    }

    Assert.assertEquals(VertexState.RUNNING, v3.getState());

    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(0, initializerWrapper.getPendingEvents().get(v1.getName()).size());

    Assert.assertTrue(initializer.eventReceived.get());
    Assert.assertEquals(3, initializer.stateUpdates.size());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        initializer.stateUpdates.get(0).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.RUNNING,
        initializer.stateUpdates.get(1).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.SUCCEEDED,
        initializer.stateUpdates.get(2).getVertexState());
  }

  @Test(timeout = 10000)
  public void testInputInitializerEventsAtNew() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithRunningInitializer3();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    VertexImplWithRunningInputInitializer v2 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex2");
    VertexImplWithRunningInputInitializer v3 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex3");

    initVertex(v1);
    startVertex(v1);
    dispatcher.await();

    // Vertex2 has not been INITED, so the rest of the vertices should be in state NEW.
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.NEW, v2.getState());
    Assert.assertEquals(VertexState.NEW, v3.getState());

    // Genrate events from v1 to v3's InputInitializer
    InputInitializerEvent event = InputInitializerEvent.create("vertex3", "input1", null);
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", ta0_t0_v1));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Events should be cached in the vertex, since the Initializer has not started
    Assert.assertEquals(1, v3.pendingInitializerEvents.size());

    // Get Vertex1 to succeed before Vertex2 is INITED. Contrived case ? This is likely a tiny race.
    for (TezTaskID taskId : v1.getTasks().keySet()) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      TaskImpl task = (TaskImpl)v1.getTask(taskId);
      task.handle(new TaskEvent(taskId, TaskEventType.T_ATTEMPT_LAUNCHED));
      task.handle(new TaskEventTAUpdate(taskAttemptId, TaskEventType.T_ATTEMPT_SUCCEEDED));
      v1.handle(new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v1.stateChangeNotifier.taskSucceeded(v1.getName(), taskId, taskAttemptId.getId());
    }
    dispatcher.await();

    // Events should still be cached in the vertex
    Assert.assertEquals(1, v3.pendingInitializerEvents.size());
    Assert.assertEquals(VertexState.NEW, v3.getState());

    // Move processing along. INIT the remaining root level vertex.
    initVertex(v2);
    startVertex(v2);
    dispatcher.await();


    // v3 would have processed an INIT event and moved into INITIALIZING state.
    // Since source tasks were complete - the events should have been consumed.
    // Initializer would have run, and processed events.
    while (v3.getState()  != VertexState.RUNNING) {
      Thread.sleep(10);
    }
    Assert.assertEquals(VertexState.RUNNING, v3.getState());
    // Events should have been cleared from the vertex.
    Assert.assertEquals(0, v3.pendingInitializerEvents.size());

    // KK Add checks to validate thte RootInputManager doesn't remember the events either

    Assert.assertTrue(initializer.eventReceived.get());
    Assert.assertEquals(3, initializer.stateUpdates.size());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        initializer.stateUpdates.get(0).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.RUNNING,
        initializer.stateUpdates.get(1).getVertexState());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.SUCCEEDED,
        initializer.stateUpdates.get(2).getVertexState());
  }

  @Test(timeout = 10000)
  public void testInputInitializerEvents() throws Exception {
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
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex2", ta0_t0_v1));

    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();

    // Events should not be cached in the vertex, since the initializer is running
    Assert.assertEquals(0, v2.pendingInitializerEvents.size());

    // Verify that events are cached
    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        v2.rootInputInitializerManager.getInitializerWrapper("input1");
    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(1, initializerWrapper.getPendingEvents().get(v1.getName()).size());

    for (TezTaskID taskId : v1.getTasks().keySet()) {
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 0);
      v1.handle(new VertexEventTaskAttemptCompleted(taskAttemptId, TaskAttemptStateInternal.SUCCEEDED));
      v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
      dispatcher.await();
      v1.stateChangeNotifier.taskSucceeded(v1.getName(), taskId, taskAttemptId.getId());
    }

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

    // Verify the events are no longer cached, but attempts are remembered
    Assert.assertEquals(1, initializerWrapper.getFirstSuccessfulAttemptMap().size());
    Assert.assertEquals(0, initializerWrapper.getPendingEvents().get(v1.getName()).size());
  }

  @Test(timeout = 5000)
  /**
   * Ref: TEZ-1494
   * If broadcast, one-to-one or custom edges are present in source, tasks should not start until
   * 1 task from each source vertex is complete.
   */
  public void testTaskSchedulingWithCustomEdges() throws TezException {
    setupPreDagCreation();
    dagPlan = createCustomDAGWithCustomEdges();
    setupPostDagCreation();

    /**
     *
     *   M2 --(SG)--> R3 --(B)--\
     *                           \
     *   M7 --(B)---------------->M5 ---(SG)--> R6
     *                            /
     *   M8---(C)--------------->/
     *                          /
     *   M9---(B)--------------> (zero task vertex)
     */

    //init M2
    VertexImpl m2 = vertices.get("M2");
    VertexImpl m7 = vertices.get("M7");
    VertexImpl r3 = vertices.get("R3");
    VertexImpl m5 = vertices.get("M5");
    VertexImpl m8 = vertices.get("M8");
    VertexImpl m9 = vertices.get("M9");

    initVertex(m2);
    initVertex(m7);
    initVertex(m8);
    initVertex(m9);
    assertTrue(m7.getState().equals(VertexState.INITED));
    assertTrue(m9.getState().equals(VertexState.INITED));
    assertTrue(m5.getState().equals(VertexState.INITED));
    assertTrue(m8.getState().equals(VertexState.INITED));
    assertTrue(m5.getVertexManager().getPlugin() instanceof ImmediateStartVertexManager);

    //Start M9
    dispatcher.getEventHandler().handle(new VertexEvent(m9.getVertexId(),
        VertexEventType.V_START));
    dispatcher.await();

    //Start M2; Let tasks complete in M2; Also let 1 task complete in R3
    dispatcher.getEventHandler().handle(new VertexEvent(m2.getVertexId(), VertexEventType.V_START));
    dispatcher.await();

    //R3 should be in running state
    assertTrue(r3.getState().equals(VertexState.RUNNING));

    //Let us start M7; M5 should start not start as it is dependent on M8 as well
    dispatcher.getEventHandler().handle(new VertexEvent(m7.getVertexId(),VertexEventType.V_START));
    dispatcher.await();

    //M5 should be in INITED state, as it depends on M8
    assertTrue(m5.getState().equals(VertexState.INITED));
    for(Task task : m5.getTasks().values()) {
      assertTrue(task.getState().equals(TaskState.NEW));
    }

    //Let us start M8; M5 should start now
    dispatcher.getEventHandler().handle(new VertexEvent(m8.getVertexId(),VertexEventType.V_START));
    dispatcher.await();

    assertTrue(m9.getState().equals(VertexState.SUCCEEDED));

    //M5 in running state. All source vertices have started and are configured
    assertTrue(m5.getState().equals(VertexState.RUNNING));
    for(Task task : m5.getTasks().values()) {
      assertTrue(task.getState().equals(TaskState.SCHEDULED));
    }
  }

  //For TEZ-1494
  private DAGPlan createCustomDAGWithCustomEdges() {
    /**
     *
     *   M2 --(SG)--> R3 --(B)--\
     *                           \
     *   M7 --(B)---------------->M5 ---(SG)--> R6
     *                            /
     *   M8---(C)--------------->/
     *                          /
     *   M9---(B)--------------> (zero task vertex)
     */
    DAGPlan dag = DAGPlan.newBuilder().setName("TestSamplerDAG")
        .addVertex(VertexPlan.newBuilder()
                .setName("M2")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("M2.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host1").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("M2.class")
                        .build()
                )
                .addOutEdgeId("M2_R3")
                .build()
        )
        .addVertex(VertexPlan.newBuilder()
                .setName("M8")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("M8.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host1").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("M8.class")
                        .build()
                )
                .addOutEdgeId("M8_M5")
                .build()
        )
        .addVertex(VertexPlan.newBuilder()
                .setName("M9")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("M9.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host1").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(0) //Zero task vertex
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("M9.class")
                        .build()
                )
                .addOutEdgeId("M9_M5")
                .build()
        )
         .addVertex(VertexPlan.newBuilder()
                 .setName("R3")
                 .setProcessorDescriptor(
                     TezEntityDescriptorProto.newBuilder().setClassName("M2.class"))
                 .setType(PlanVertexType.NORMAL)
                 .addTaskLocationHint(
                     PlanTaskLocationHint.newBuilder().addHost("host2").addRack("rack1").build())
                 .setTaskConfig(PlanTaskConfiguration.newBuilder()
                         .setNumTasks(10)
                         .setVirtualCores(4)
                         .setMemoryMb(1024)
                         .setJavaOpts("")
                         .setTaskModule("R3.class")
                         .build()
                 )
                 .addInEdgeId("M2_R3")
                 .addOutEdgeId("R3_M5")
                 .build()
         )
        .addVertex(VertexPlan.newBuilder()
                .setName("M5")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("M5.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host3").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(10)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("M5.class")
                        .build()
                )
                .addInEdgeId("R3_M5")
                .addInEdgeId("M7_M5")
                .addInEdgeId("M8_M5")
                .addInEdgeId("M9_M5")
                .addOutEdgeId("M5_R6")
                .build()
        )
        .addVertex(VertexPlan.newBuilder()
                .setName("M7")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("M7.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host4").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(10)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("M7.class")
                        .build()
                )
                .addOutEdgeId("M7_M5")
                .build()
        )
        .addVertex(VertexPlan.newBuilder()
                .setName("R6")
                .setProcessorDescriptor(
                    TezEntityDescriptorProto.newBuilder().setClassName("R6.class"))
                .setType(PlanVertexType.NORMAL)
                .addTaskLocationHint(
                    PlanTaskLocationHint.newBuilder().addHost("host3").addRack("rack1").build())
                .setTaskConfig(PlanTaskConfiguration.newBuilder()
                        .setNumTasks(1)
                        .setVirtualCores(4)
                        .setMemoryMb(1024)
                        .setJavaOpts("")
                        .setTaskModule("R6.class")
                        .build()
                )
                .addInEdgeId("M5_R6")
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("M2_R3"))
                .setInputVertexName("M2")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("M2_R3.class"))
                .setOutputVertexName("R3")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("M2_R3")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("R3_M5"))
                .setInputVertexName("R3")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("R3_M5.class"))
                .setOutputVertexName("M5")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("R3_M5")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("M7_M5"))
                .setInputVertexName("M7")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("M7_M5.class"))
                .setOutputVertexName("M5")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("M7_M5")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("M5_R6"))
                .setInputVertexName("M5")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("M5_R6.class"))
                .setOutputVertexName("R6")
                .setDataMovementType(PlanEdgeDataMovementType.SCATTER_GATHER)
                .setId("M5_R6")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("M9_M5"))
                .setInputVertexName("M9")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("M9_M5.class"))
                .setOutputVertexName("M5")
                .setDataMovementType(PlanEdgeDataMovementType.BROADCAST)
                .setId("M9_M5")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .addEdge(
            EdgePlan.newBuilder()
                .setEdgeDestination(TezEntityDescriptorProto.newBuilder().setClassName("M8_M5"))
                .setInputVertexName("M8")
                .setEdgeSource(TezEntityDescriptorProto.newBuilder().setClassName("M8_M5.class"))
                .setEdgeManager(
                    TezEntityDescriptorProto.newBuilder()
                        .setClassName(EdgeManagerForTest.class.getName())
                        .setTezUserPayload(DAGProtos.TezUserPayloadProto.newBuilder()
                            .setUserPayload(ByteString.copyFrom(edgePayload)))
                        .build())
                .setOutputVertexName("M5")
                .setDataMovementType(PlanEdgeDataMovementType.CUSTOM)
                .setId("M8_M5")
                .setDataSourceType(PlanEdgeDataSourceType.PERSISTED)
                .setSchedulingType(PlanEdgeSchedulingType.SEQUENTIAL)
                .build()
        )
        .build();

    return dag;
  }

  @Test(timeout = 5000)
  public void testVertexWithMultipleInitializers1() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithMultipleInitializers("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");

    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());

    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(5);

    // Complete initializer which sets parallelism first
    initializerManager1.completeInputInitialization(0, 5, v1Hints);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(1, v1.numInitializerCompletionsHandled);
    // Complete second initializer
    initializerManager1.completeInputInitialization(1);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(2, v1.numInitializerCompletionsHandled);
  }

  @Test(timeout = 5000)
  public void testVertexWithMultipleInitializers2() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithMultipleInitializers("TestInputInitializer");
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");

    dispatcher.getEventHandler().handle(
        new VertexEvent(v1.getVertexId(), VertexEventType.V_INIT));
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());

    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    List<TaskLocationHint> v1Hints = createTaskLocationHints(5);

    // Complete initializer which does not set parallelism
    initializerManager1.completeInputInitialization(1);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITIALIZING, v1.getState());
    Assert.assertEquals(1, v1.numInitializerCompletionsHandled);
    // Complete second initializer which sets parallelism
    initializerManager1.completeInputInitialization(0, 5, v1Hints);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(2, v1.numInitializerCompletionsHandled);
  }

  @Test(timeout = 500000)
  public void testVertexWithInitializerSuccess() throws Exception {
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
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(5, v1.getTotalTasks());
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
    
    List<TaskWithLocationHint> taskList = new LinkedList<VertexManagerPluginContext.TaskWithLocationHint>();
    // scheduling start to trigger edge routing to begin
    for (int i=0; i<v1.getTotalTasks(); ++i) {
      taskList.add(new TaskWithLocationHint(i, null));
    }
    v1.scheduleTasks(taskList);
    dispatcher.await();
    // check all tasks get their events
    for (int i=0; i<v1.getTotalTasks(); ++i) {
      Assert.assertEquals(
          1,
          v1.getTaskAttemptTezEvents(TezTaskAttemptID.getInstance(v1.getTask(i).getTaskId(), 0),
              0, 0, 100).getEvents().size());
    }
    
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());
    
    // non-task events don't get buffered
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
    
    RootInputInitializerManagerControlled initializerManager2 = v2.getRootInputInitializerManager();
    List<TaskLocationHint> v2Hints = createTaskLocationHints(10);
    initializerManager2.completeInputInitialization(0, 10, v2Hints);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v2.getState());
    Assert.assertEquals(10, v2.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v2Hints.size(); ++i) {
      Assert.assertEquals(v2Hints.get(i), v2.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager2.hasShutDown);
    
    // scheduling start to trigger edge routing to begin
    taskList = new LinkedList<VertexManagerPluginContext.TaskWithLocationHint>();
    // scheduling start to trigger edge routing to begin
    for (int i=0; i<v2.getTotalTasks(); ++i) {
      taskList.add(new TaskWithLocationHint(i, null));
    }
    v2.scheduleTasks(taskList);
    dispatcher.await();
    // check all tasks get their events
    for (int i=0; i<v2.getTotalTasks(); ++i) {
      Assert.assertEquals(
          ((i==0) ? 2 : 1),
          v2.getTaskAttemptTezEvents(TezTaskAttemptID.getInstance(v2.getTask(i).getTaskId(), 0),
              0, 0, 100).getEvents().size());
    }
    for (int i = 0; i < 10; i++) {
      List<InputSpec> inputSpecs = v1.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(1, inputSpecs.get(0).getPhysicalEdgeCount());
    }
  }
  
  @Test(timeout = 5000)
  public void testVertexWithInitializerSuccessLegacyRouting() throws Exception {
    // Remove after legacy routing is removed
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
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    Assert.assertEquals(5, v1.getTotalTasks());
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
    // task events get buffered
    Assert.assertEquals(5, v1.pendingTaskEvents.size());
    
    VertexImplWithControlledInitializerManager v2 = (VertexImplWithControlledInitializerManager) vertices.get("vertex2");
    Assert.assertEquals(VertexState.INITIALIZING, v2.getState());

    // non-task events don't get buffered
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
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v2.getState());
    Assert.assertEquals(10, v2.getTotalTasks());
    Assert.assertEquals(RootInputVertexManager.class.getName(), v2
        .getVertexManager().getPlugin().getClass().getName());
    for (int i=0; i < v2Hints.size(); ++i) {
      Assert.assertEquals(v2Hints.get(i), v2.getTaskLocationHints()[i]);
    }
    Assert.assertEquals(true, initializerManager2.hasShutDown);
    // task events get buffered
    Assert.assertEquals(11, v2.pendingTaskEvents.size());
    for (int i = 0; i < 10; i++) {
      List<InputSpec> inputSpecs = v1.getInputSpecList(i);
      Assert.assertEquals(1, inputSpecs.size());
      Assert.assertEquals(1, inputSpecs.get(0).getPhysicalEdgeCount());
    }
  }

  
  @Test(timeout = 5000)
  public void testVertexWithInputDistributor() throws Exception {
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
    dispatcher.await();
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
  
  @Test(timeout = 5000)
  public void testVertexRootInputSpecUpdateAll() throws Exception {
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
    dispatcher.await();
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

  @Test(timeout = 5000)
  public void testVertexRootInputSpecUpdatePerTask() throws Exception {
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
    dispatcher.await();
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
      v.setInputVertices(new HashMap());
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

    public void failInputInitialization() throws TezException {
      super.runInputInitializers(inputs);
      eventHandler.handle(new VertexEventRootInputFailed(vertexID, inputs
          .get(0).getName(),
          new AMUserCodeException(Source.InputInitializer,
              new RuntimeException("MockInitializerFailed"))));
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

    public void completeInputInitialization(int initializerIndex) {
      eventHandler.handle(new VertexEventRootInputInitialized(vertexID, inputs
          .get(initializerIndex).getName(), null));
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

  @Test(timeout=5000)
  public void testVertexGroupInput() throws TezException {
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
    assertTrue(groupInSpec.get(0).getGroupVertices().contains("A"));
    assertTrue(groupInSpec.get(0).getGroupVertices().contains("B"));
    groupInSpec.get(0).getMergedInputDescriptor().getClassName().equals("Group.class");
  }

  @Test(timeout = 5000)
  public void testStartWithUninitializedCustomEdge() throws Exception {
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
    
    EdgeProperty edgeProp = EdgeProperty.create(mockEdgeManagerDescriptor, 
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, OutputDescriptor.create("Out"), 
        InputDescriptor.create("In"));
    Map<String, EdgeProperty> edges = Maps.newHashMap();
    edges.put("B", edgeProp);
    vC.reconfigureVertex(2, vertexLocationHint, edges);

    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.RUNNING, vB.getState());
    Assert.assertEquals(VertexState.RUNNING, vC.getState());
    Assert.assertNotNull(vA.getTask(0));
    Assert.assertNotNull(vB.getTask(0));
    Assert.assertNotNull(vC.getTask(0));
  }

  @Test(timeout = 5000)
  public void testVertexConfiguredDoneByVMBeforeEdgeDefined() throws Exception {
    // Race when a source vertex manages to start before the target vertex has
    // been initialized
    setupPreDagCreation();
    dagPlan = createSamplerDAGPlan(true);
    setupPostDagCreation();
    
    VertexImpl vA = vertices.get("A");
    VertexImpl vB = vertices.get("B");
    VertexImpl vC = vertices.get("C");

    TestUpdateListener listener = new TestUpdateListener();
    updateTracker
        .registerForVertexUpdates(vB.getName(),
            EnumSet.of(org.apache.tez.dag.api.event.VertexState.CONFIGURED),
            listener);

    // fudge the vm so we can do custom stuff
    vB.vertexManager = new VertexManager(
        VertexManagerPluginDescriptor.create(VertexManagerPluginForTest.class.getName()),
        UserGroupInformation.getCurrentUser(), vB, appContext, mock(StateChangeNotifier.class));
    
    vB.vertexReconfigurationPlanned();
    
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
    
    // vB is not configured yet. Edge to C is not configured. So it should not send configured event
    // even thought VM says its doneConfiguring vertex
    vB.doneReconfiguringVertex();
    Assert.assertEquals(0, listener.events.size());
    
    // complete configuration and verify getting configured signal from vB
    EdgeProperty edgeProp = EdgeProperty.create(mockEdgeManagerDescriptor, 
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, OutputDescriptor.create("Out"), 
        InputDescriptor.create("In"));
    Map<String, EdgeProperty> edges = Maps.newHashMap();
    edges.put("B", edgeProp);
    vC.reconfigureVertex(2, vertexLocationHint, edges);

    dispatcher.await();
    Assert.assertEquals(1, listener.events.size());
    Assert.assertEquals(vB.getName(), listener.events.get(0).getVertexName());
    Assert.assertEquals(org.apache.tez.dag.api.event.VertexState.CONFIGURED,
        listener.events.get(0).getVertexState());
    updateTracker.unregisterForVertexUpdates(vB.getName(), listener);
    
    Assert.assertEquals(VertexState.RUNNING, vA.getState());
    Assert.assertEquals(VertexState.RUNNING, vB.getState());
    Assert.assertEquals(VertexState.RUNNING, vC.getState());
    Assert.assertNotNull(vA.getTask(0));
    Assert.assertNotNull(vB.getTask(0));
    Assert.assertNotNull(vC.getTask(0));
  }

  @Test(timeout = 5000)
  public void testInitStartRace() throws TezException {
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

  @Test(timeout = 5000)
  public void testInitStartRace2() throws TezException {
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

  @Test(timeout = 5000)
  public void testExceptionFromVM_Initialize() throws TezException {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestInputInitializer", VMExceptionLocation.Initialize);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    dispatcher.await();

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    Assert.assertTrue(diagnostics.contains(VMExceptionLocation.Initialize.name()));
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, v1.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromVM_OnRootVertexInitialized() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestInputInitializer", VMExceptionLocation.OnRootVertexInitialized);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    dispatcher.await();

    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();
    dispatcher.await();
    Assert.assertEquals(VertexManagerWithException.class, v1.vertexManager.getPlugin().getClass());
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertTrue(initializerManager1.hasShutDown);
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    Assert.assertTrue(diagnostics.contains(VMExceptionLocation.OnRootVertexInitialized.name()));
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, v1.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromVM_OnVertexStarted() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestInputInitializer", VMExceptionLocation.OnVertexStarted);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    // wait to ensure init is completed, so that rootInitManager is not null
    dispatcher.await();

    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_START));
    dispatcher.await();
    
    Assert.assertEquals(VertexManagerWithException.class, v1.vertexManager.getPlugin().getClass());
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    Assert.assertTrue(diagnostics.contains(VMExceptionLocation.OnVertexStarted.name()));
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, v1.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromVM_OnSourceTaskCompleted() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestInputInitializer", VMExceptionLocation.OnSourceTaskCompleted);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    VertexImpl v2 = vertices.get("vertex2");

    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    // wait to ensure V_INIT is handled, so that rootInitManager is not null
    dispatcher.await();
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_START));

    // ensure v2 go to RUNNING and start one task in v2
    dispatcher.await();

    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(TezTaskID.getInstance(v1.getVertexId(), 0), 0);
    v1.getEventHandler().handle(new VertexEventTaskAttemptCompleted(taId,
        TaskAttemptStateInternal.SUCCEEDED));
    dispatcher.await();

    Assert.assertEquals(VertexManagerWithException.class, v1.vertexManager.getPlugin().getClass());
    Assert.assertEquals(VertexManagerWithException.class, v2.vertexManager.getPlugin().getClass());
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    String diagnostics = StringUtils.join(v2.getDiagnostics(), ",");
    Assert.assertTrue(diagnostics.contains(VMExceptionLocation.OnSourceTaskCompleted.name()));
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, v2.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromVM_OnVertexManagerEventReceived() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestInputInitializer", VMExceptionLocation.OnVertexManagerEventReceived);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    // wait to ensure init is completed, so that rootInitManager is not null
    dispatcher.await();
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();

    Event vmEvent = VertexManagerEvent.create(v1.getName(), ByteBuffer.wrap(new byte[0]));
    TezEvent tezEvent = new TezEvent(vmEvent, null);
    dispatcher.getEventHandler().handle(new VertexEventRouteEvent(v1.getVertexId(), Lists.newArrayList(tezEvent)));
    dispatcher.await();

    Assert.assertEquals(VertexState.FAILED, v1.getState());
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    Assert.assertTrue(diagnostics.contains(VMExceptionLocation.OnVertexManagerEventReceived.name()));
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromVM_OnVertexManagerVertexStateUpdated() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithVMException("TestVMStateUpdate", VMExceptionLocation.OnVertexManagerVertexStateUpdated);
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 = (VertexImplWithControlledInitializerManager) vertices
        .get("vertex1");
    VertexImpl v2 = vertices.get("vertex2");
    dispatcher.getEventHandler().handle(new VertexEvent(v1.getVertexId(),
        VertexEventType.V_INIT));
    // wait to ensure init is completed, so that rootInitManager is not null
    dispatcher.await();
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization();
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v2.getState());
    startVertex(v1, false);
    dispatcher.await();
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    String diagnostics = StringUtils.join(v2.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(VMExceptionLocation.OnVertexManagerVertexStateUpdated.name()));
    Assert.assertEquals(VertexTerminationCause.AM_USERCODE_FAILURE, v2.getTerminationCause());
  }
  
  @Test(timeout = 5000)
  public void testExceptionFromII_Initialize() throws InterruptedException, TezException {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null, IIExceptionLocation.Initialize);
    EventHandlingRootInputInitializer initializer =
        (EventHandlingRootInputInitializer) customInitializer;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithIIException();
    setupPostDagCreation();

    VertexImplWithRunningInputInitializer v1 =
        (VertexImplWithRunningInputInitializer) vertices.get("vertex1");
    initVertex(v1);
    // Wait for the initializer to been invoked, and fail the vertex finally.
    while (v1.getState() != VertexState.FAILED) {
      Thread.sleep(10);
    }

    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(IIExceptionLocation.Initialize.name()));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v1.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testExceptionFromII_InitFailedAfterInitialized() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithIIException();
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 =
        (VertexImplWithControlledInitializerManager)vertices.get("vertex1");
    initVertex(v1);
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization(0);
    dispatcher.await();
    Assert.assertEquals(VertexState.INITED, v1.getState());
    String errorMsg = "ErrorWhenInitFailureAtInited";
    dispatcher.getEventHandler().handle(new VertexEventRootInputFailed(v1.getVertexId(), "input1",
        new AMUserCodeException(Source.InputInitializer, new Exception(errorMsg))));
    dispatcher.await();
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(errorMsg));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v1.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testExceptionFromII_InitFailedAfterRunning() throws Exception {
    useCustomInitializer = true;
    setupPreDagCreation();
    dagPlan = createDAGPlanWithIIException();
    setupPostDagCreation();

    VertexImplWithControlledInitializerManager v1 =
        (VertexImplWithControlledInitializerManager)vertices.get("vertex1");
    initVertex(v1);
    RootInputInitializerManagerControlled initializerManager1 = v1.getRootInputInitializerManager();
    initializerManager1.completeInputInitialization(0);
    dispatcher.await();
    startVertex(v1);
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    String errorMsg = "ErrorWhenInitFailureAtRunning";
    dispatcher.getEventHandler().handle(new VertexEventRootInputFailed(v1.getVertexId(), "input1",
        new AMUserCodeException(Source.InputInitializer, new Exception(errorMsg))));
    dispatcher.await();
    String diagnostics = StringUtils.join(v1.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(errorMsg));
    Assert.assertEquals(VertexState.FAILED, v1.getState());
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v1.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testExceptionFromII_HandleInputInitializerEvent() throws Exception {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null, IIExceptionLocation.HandleInputInitializerEvent);
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
    // Create taskId and taskAttemptId for the single task that exists in vertex1
    TezTaskID t0_v1 = TezTaskID.getInstance(v1.getVertexId(), 0);
    TezTaskAttemptID ta0_t0_v1 = TezTaskAttemptID.getInstance(t0_v1, 0);
    TezEvent tezEvent = new TezEvent(event,
        new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex2", ta0_t0_v1));

    // at least one task attempt is succeed, otherwise input initialize events won't been handled.
    dispatcher.getEventHandler().handle(new TaskEvent(t0_v1, TaskEventType.T_ATTEMPT_LAUNCHED));
    dispatcher.getEventHandler().handle(new TaskEventTAUpdate(ta0_t0_v1, TaskEventType.T_ATTEMPT_SUCCEEDED));
    dispatcher.getEventHandler()
        .handle(new VertexEventRouteEvent(v1.getVertexId(), Collections.singletonList(tezEvent)));
    dispatcher.await();
    
    // it would cause v2 fail as its II throw exception in handleInputInitializerEvent
    String diagnostics = StringUtils.join(v2.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(IIExceptionLocation.HandleInputInitializerEvent.name()));
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v2.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testExceptionFromII_OnVertexStateUpdated() throws InterruptedException, TezException {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null, IIExceptionLocation.OnVertexStateUpdated);
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
    // Wait for the initializer to be invoked - which may be a separate thread.
    while (!initializer.initStarted.get()) {
      Thread.sleep(10);
    }
    startVertex(v1);  // v2 would get the state update from v1
    dispatcher.await();
    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    String diagnostics = StringUtils.join(v2.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(IIExceptionLocation.OnVertexStateUpdated.name()));
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v2.getTerminationCause());
  }

  @Test(timeout = 5000)
  public void testExceptionFromII_InitSucceededAfterInitFailure() throws InterruptedException, TezException {
    useCustomInitializer = true;
    customInitializer = new EventHandlingRootInputInitializer(null, IIExceptionLocation.OnVertexStateUpdated);
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
    // Wait for the initializer to be invoked - which may be a separate thread.
    while (!initializer.initStarted.get()) {
      Thread.sleep(10);
    }
    startVertex(v1);  // v2 would get the state update from v1
    // it should be OK receive INIT_SUCCEEDED event after INIT_FAILED event
    dispatcher.getEventHandler().handle(new VertexEventRootInputInitialized(
        v2.getVertexId(), "input1", null));

    Assert.assertEquals(VertexState.RUNNING, v1.getState());
    Assert.assertEquals(VertexState.FAILED, v2.getState());
    String diagnostics = StringUtils.join(v2.getDiagnostics(), ",");
    assertTrue(diagnostics.contains(IIExceptionLocation.OnVertexStateUpdated.name()));
    Assert.assertEquals(VertexTerminationCause.ROOT_INPUT_INIT_FAILURE, v2.getTerminationCause());
  }

  @Test (timeout = 5000)
  public void testRouteEvent_RecoveredEvent() throws IOException {
    doReturn(historyEventHandler).when(appContext).getHistoryHandler();
    doReturn(true).when(appContext).isRecoveryEnabled();

    initAllVertices(VertexState.INITED);
    VertexImpl v1 = (VertexImpl)vertices.get("vertex1");
    VertexImpl v2 = (VertexImpl)vertices.get("vertex2");
    VertexImpl v3 = (VertexImpl)vertices.get("vertex3");
    startVertex(v1);
    startVertex(v2);
    TezTaskID taskId = TezTaskID.getInstance(v1.getVertexId(), 0);
    v1.handle(new VertexEventTaskCompleted(taskId, TaskState.SUCCEEDED));
    DataMovementEvent dmEvent = DataMovementEvent.create(0, ByteBuffer.wrap(new byte[0]));
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(taskId, 0);
    TezEvent tezEvent1 = new TezEvent(dmEvent, new EventMetaData(EventProducerConsumerType.OUTPUT, "vertex1", "vertex3", taId));
    v1.handle(new VertexEventRouteEvent(v1.getVertexId(), Lists.newArrayList(tezEvent1)));
    dispatcher.await();
    assertTrue(v3.pendingTaskEvents.size() != 0);
    ArgumentCaptor<DAGHistoryEvent> argCaptor = ArgumentCaptor.forClass(DAGHistoryEvent.class);
    verify(historyEventHandler, atLeast(1)).handle(argCaptor.capture());
    verifyHistoryEvents(argCaptor.getAllValues(), HistoryEventType.VERTEX_DATA_MOVEMENT_EVENTS_GENERATED, 1);

    v3.scheduleTasks(Lists.newArrayList(new TaskWithLocationHint(0, null)));
    dispatcher.await();
    assertTrue(v3.pendingTaskEvents.size() == 0);
    // recovery events is not only handled one time
    argCaptor = ArgumentCaptor.forClass(DAGHistoryEvent.class);
    verify(historyEventHandler, atLeast(1)).handle(argCaptor.capture());
    verifyHistoryEvents(argCaptor.getAllValues(), HistoryEventType.VERTEX_DATA_MOVEMENT_EVENTS_GENERATED, 1);
  }

  private void verifyHistoryEvents(List<DAGHistoryEvent> events, HistoryEventType eventType, int expectedTimes) {
    int actualTimes = 0;
    LOG.info("");
    for (DAGHistoryEvent event : events) {
      LOG.info(event.getHistoryEvent().getEventType() + "");
      if (event.getHistoryEvent().getEventType() == eventType) {
        actualTimes ++;
      }
    }
    Assert.assertEquals(actualTimes, expectedTimes);
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
      getContext().reconfigureVertex(map, null, NUM_TASKS);
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
  public static class VertexManagerWithException extends RootInputVertexManager{

    public static enum VMExceptionLocation {
      NoExceptionDoReconfigure,
      OnRootVertexInitialized,
      OnSourceTaskCompleted,
      OnVertexStarted,
      OnVertexManagerEventReceived,
      OnVertexManagerVertexStateUpdated,
      Initialize,
    }
    
    private VMExceptionLocation exLocation;
    
    public VertexManagerWithException(VertexManagerPluginContext context) {
      super(context);
    }
    
    @Override
    public void initialize() {
      super.initialize();
      this.exLocation = VMExceptionLocation.valueOf(
          new String(getContext().getUserPayload().deepCopyAsArray()));
      if (this.exLocation == VMExceptionLocation.Initialize) {
        throw new RuntimeException(this.exLocation.name());
      }
      if (this.exLocation == VMExceptionLocation.NoExceptionDoReconfigure) {
        getContext().vertexReconfigurationPlanned();
      }
    }
    
    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) {
      if (this.exLocation == VMExceptionLocation.OnRootVertexInitialized) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onRootVertexInitialized(inputName, inputDescriptor, events);
    }
    
    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
      if (this.exLocation == VMExceptionLocation.OnSourceTaskCompleted) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onSourceTaskCompleted(srcVertexName, attemptId);
    }
    
    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
      if (this.exLocation == VMExceptionLocation.OnVertexStarted) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onVertexStarted(completions);
      if (this.exLocation == VMExceptionLocation.NoExceptionDoReconfigure) {
        getContext().doneReconfiguringVertex();
      }
    }
    
    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
      super.onVertexManagerEventReceived(vmEvent);
      if (this.exLocation == VMExceptionLocation.OnVertexManagerEventReceived) {
        throw new RuntimeException(this.exLocation.name());
      }
    }
    
    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
      super.onVertexStateUpdated(stateUpdate);
      // depends on ImmediateStartVertexManager to register for state update
      if (this.exLocation == VMExceptionLocation.OnVertexManagerVertexStateUpdated) {
        throw new RuntimeException(this.exLocation.name());
      }
    }
  }
  
  public static enum IIExceptionLocation {
    Initialize,
    HandleInputInitializerEvent,
    OnVertexStateUpdated
  }

  @InterfaceAudience.Private
  public static class EventHandlingRootInputInitializer extends InputInitializer
      implements ContextSettableInputInitialzier {

    final AtomicBoolean initStarted = new AtomicBoolean(false);
    final AtomicBoolean eventReceived = new AtomicBoolean(false);
    final AtomicBoolean initComplete = new AtomicBoolean(false);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition eventCondition = lock.newCondition();

    private final List<VertexStateUpdate> stateUpdates = new LinkedList<VertexStateUpdate>();
    private int numExpectedVertexStateUpdate = 1;
    private Object waitForVertexStateUpdate = new Object();
    private final List<InputInitializerEvent> initializerEvents = new LinkedList<InputInitializerEvent>();
    private volatile InputInitializerContext context;
    private volatile int numExpectedEvents = 1;
    private IIExceptionLocation exLocation = null;

    public EventHandlingRootInputInitializer(
        InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    public EventHandlingRootInputInitializer(
        InputInitializerContext initializerContext, IIExceptionLocation exLocation) {
      super(initializerContext);
      this.exLocation = exLocation;
    }

    @Override
    public List<Event> initialize() throws Exception {
      context.registerForVertexStateUpdates("vertex1", null);
      initStarted.set(true);
      if (exLocation == IIExceptionLocation.Initialize) {
        throw new Exception(exLocation.name());
      }
      lock.lock();
      try {
        if (!eventReceived.get()) {
          eventCondition.await();
        }
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
      if (exLocation == IIExceptionLocation.HandleInputInitializerEvent) {
        throw new Exception(exLocation.name());
      }
      initializerEvents.addAll(events);
      if (initializerEvents.size() == numExpectedEvents) {
        eventReceived.set(true);
        lock.lock();
        try {
          eventCondition.signal();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void setContext(InputInitializerContext context) {
      this.context = context;
    }

    public void setNumExpectedEvents(int numEvents) {
      this.numExpectedEvents = numEvents;
    }

    public void setNumVertexStateUpdateEvents(int numEvents) {
      this.numExpectedVertexStateUpdate = numEvents;
    }

    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
      if (exLocation == IIExceptionLocation.OnVertexStateUpdated) {
        throw new RuntimeException(exLocation.name());
      }
      stateUpdates.add(stateUpdate);
      if (stateUpdates.size() == numExpectedVertexStateUpdate) {
        synchronized (waitForVertexStateUpdate) {
          waitForVertexStateUpdate.notify();
        }
      }
    }

    public void waitForVertexStateUpdate() throws InterruptedException {
      if (stateUpdates.size() < numExpectedVertexStateUpdate) {
        synchronized (waitForVertexStateUpdate) {
          waitForVertexStateUpdate.wait();
        }
      }
    }
  }

  private interface ContextSettableInputInitialzier {
    void setContext(InputInitializerContext context);
  }
}

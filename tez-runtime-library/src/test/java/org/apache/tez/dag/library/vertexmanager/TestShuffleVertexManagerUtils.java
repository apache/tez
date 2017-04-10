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

package org.apache.tez.dag.library.vertexmanager;


import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.library.vertexmanager.FairShuffleVertexManager.FairRoutingType;
import org.apache.tez.dag.library.vertexmanager.FairShuffleVertexManager.FairShuffleVertexManagerConfigBuilder;
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.VertexIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestShuffleVertexManagerUtils {
  static long MB = 1024l * 1024l;
  TezVertexID vertexId = TezVertexID.fromString("vertex_1436907267600_195589_1_00");
  int taskId = 0;

  VertexManagerPluginContext createVertexManagerContext(
      String mockSrcVertexId1, int numTasksSrcVertexId1,
      String mockSrcVertexId2, int numTasksSrcVertexId2,
      String mockSrcVertexId3, int numTasksSrcVertexId3,
      String mockManagedVertexId, int numTasksmockManagedVertexId,
      List<Integer> scheduledTasks,
      Map<String, EdgeManagerPlugin> newEdgeManagers) {
    HashMap<String, EdgeProperty> mockInputVertices =
        new HashMap<String, EdgeProperty>();
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);
    mockInputVertices.put(mockSrcVertexId3, eProp3);

    final VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(numTasksSrcVertexId1);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(numTasksSrcVertexId2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(numTasksSrcVertexId3);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(numTasksmockManagedVertexId);
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());
    doAnswer(new reconfigVertexAnswer(mockContext, mockManagedVertexId,
        newEdgeManagers)).when(mockContext).reconfigureVertex(
        anyInt(), any(VertexLocationHint.class), anyMap());
    return mockContext;
  }

  VertexManagerEvent getVertexManagerEvent(long[] sizes,
      long totalSize, String vertexName) throws IOException {
    return getVertexManagerEvent(sizes, totalSize, vertexName, false);
  }

  VertexManagerEvent getVertexManagerEvent(long[] sizes,
      long totalSize, String vertexName, boolean reportDetailedStats)
      throws IOException {
    ByteBuffer payload;
    if (sizes != null) {
      RoaringBitmap partitionStats = ShuffleUtils.getPartitionStatsForPhysicalOutput(sizes);
      DataOutputBuffer dout = new DataOutputBuffer();
      partitionStats.serialize(dout);
      ByteString
          partitionStatsBytes = TezCommonUtils.compressByteArrayToByteString(dout.getData());
      if (reportDetailedStats) {
        payload = VertexManagerEventPayloadProto.newBuilder()
            .setOutputSize(totalSize)
            .setDetailedPartitionStats(ShuffleUtils.getDetailedPartitionStatsForPhysicalOutput(sizes))
            .build().toByteString()
            .asReadOnlyByteBuffer();
      } else {
        payload = VertexManagerEventPayloadProto.newBuilder()
            .setOutputSize(totalSize)
            .setPartitionStats(partitionStatsBytes)
            .build().toByteString()
            .asReadOnlyByteBuffer();
      }

    } else {
      payload = VertexManagerEventPayloadProto.newBuilder()
          .setOutputSize(totalSize)
          .build().toByteString()
          .asReadOnlyByteBuffer();
    }
    TaskAttemptIdentifierImpl taId = new TaskAttemptIdentifierImpl("dag", vertexName,
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, taskId++), 0));
    VertexManagerEvent vmEvent = VertexManagerEvent.create(vertexName, payload);
    vmEvent.setProducerAttemptIdentifier(taId);
    return vmEvent;
  }

  public static TaskAttemptIdentifier createTaskAttemptIdentifier(String vName, int tId) {
    VertexIdentifier mockVertex = mock(VertexIdentifier.class);
    when(mockVertex.getName()).thenReturn(vName);
    TaskIdentifier mockTask = mock(TaskIdentifier.class);
    when(mockTask.getIdentifier()).thenReturn(tId);
    when(mockTask.getVertexIdentifier()).thenReturn(mockVertex);
    TaskAttemptIdentifier mockAttempt = mock(TaskAttemptIdentifier.class);
    when(mockAttempt.getIdentifier()).thenReturn(0);
    when(mockAttempt.getTaskIdentifier()).thenReturn(mockTask);
    return mockAttempt;
  }

  static public ShuffleVertexManagerBase createManager(
      Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass,
      Configuration conf, VertexManagerPluginContext context,
      Boolean enableAutoParallelism, Long desiredTaskInputSize, Float min,
      Float max) {
    if (shuffleVertexManagerClass.equals(ShuffleVertexManager.class)) {
      return createShuffleVertexManager(conf, context, enableAutoParallelism,
          desiredTaskInputSize, min, max);
    } else if (shuffleVertexManagerClass.equals(FairShuffleVertexManager.class)) {
      FairRoutingType fairRoutingType = null;
      if (enableAutoParallelism != null) {
        fairRoutingType = enableAutoParallelism ?
            FairRoutingType.REDUCE_PARALLELISM : FairRoutingType.NONE;
      }
      return createFairShuffleVertexManager(conf, context,
          fairRoutingType, desiredTaskInputSize, min, max);
    } else {
      return null;
    }
  }

  static ShuffleVertexManager createShuffleVertexManager(
      Configuration conf, VertexManagerPluginContext context,
      Boolean enableAutoParallelism, Long desiredTaskInputSize, Float min,
      Float max) {
    if (min != null) {
      conf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
              min);
    } else {
      conf.unset(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION);
    }
    if (max != null) {
      conf.setFloat(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
              max);
    } else {
      conf.unset(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION);
    }
    if (enableAutoParallelism != null) {
      conf.setBoolean(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
          enableAutoParallelism);
    }
    if (desiredTaskInputSize != null) {
      conf.setLong(
          ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          desiredTaskInputSize);
    }
    UserPayload payload;
    try {
      payload = TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    when(context.getUserPayload()).thenReturn(payload);
    ShuffleVertexManager manager = new ShuffleVertexManager(context);
    manager.initialize();
    return manager;
  }

  static FairShuffleVertexManager createFairShuffleVertexManager(
      Configuration conf, VertexManagerPluginContext context,
      FairRoutingType fairRoutingType, Long desiredTaskInputSize, Float min,
      Float max) {
    FairShuffleVertexManagerConfigBuilder builder =
        FairShuffleVertexManager.createConfigBuilder(conf);
    if (min != null) {
      builder.setSlowStartMinSrcCompletionFraction(min);
    } else if (conf != null) {
      conf.unset(
          FairShuffleVertexManager.TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION);
    }
    if (max != null) {
      builder.setSlowStartMaxSrcCompletionFraction(max);
    } else if (conf != null) {
      conf.unset(
          FairShuffleVertexManager.TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION);
    }
    if (fairRoutingType != null) {
      builder.setAutoParallelism(fairRoutingType);
    }
    if (desiredTaskInputSize != null) {
      builder.setDesiredTaskInputSize(desiredTaskInputSize);
    }
    UserPayload payload = builder.build().getUserPayload();
    when(context.getUserPayload()).thenReturn(payload);
    FairShuffleVertexManager manager = new FairShuffleVertexManager(context);
    manager.initialize();
    return manager;
  }

  protected static class ScheduledTasksAnswer implements Answer<Object> {
    private List<Integer> scheduledTasks;
    public ScheduledTasksAnswer(List<Integer> scheduledTasks) {
      this.scheduledTasks = scheduledTasks;
    }
    @Override
    public Object answer(InvocationOnMock invocation) throws IOException {
      Object[] args = invocation.getArguments();
      scheduledTasks.clear();
      List<VertexManagerPluginContext.ScheduleTaskRequest> tasks =
          (List<VertexManagerPluginContext.ScheduleTaskRequest>)args[0];
      for (VertexManagerPluginContext.ScheduleTaskRequest task : tasks) {
        scheduledTasks.add(task.getTaskIndex());
      }
      return null;
    }
  }

  protected static class reconfigVertexAnswer implements Answer<Object> {

    private VertexManagerPluginContext mockContext;
    private String mockManagedVertexId;
    private Map<String, EdgeManagerPlugin> newEdgeManagers;
    public reconfigVertexAnswer(VertexManagerPluginContext mockContext,
        String mockManagedVertexId,
        Map<String, EdgeManagerPlugin> newEdgeManagers) {
      this.mockContext = mockContext;
      this.mockManagedVertexId = mockManagedVertexId;
      this.newEdgeManagers = newEdgeManagers;

    }
    public Object answer(InvocationOnMock invocation) throws Exception {
      final int numTasks = ((Integer) invocation.getArguments()[0]).intValue();
      when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(numTasks);
      if (newEdgeManagers != null) {
        newEdgeManagers.clear();
      }
      for (Map.Entry<String, EdgeProperty> entry :
          ((Map<String, EdgeProperty>) invocation.getArguments()[2]).entrySet()) {

        EdgeManagerPluginDescriptor pluginDesc = entry.getValue().getEdgeManagerDescriptor();
        final UserPayload userPayload = pluginDesc.getUserPayload();
        EdgeManagerPluginContext emContext = new EdgeManagerPluginContext() {
          @Override
          public UserPayload getUserPayload() {
            return userPayload == null ? null : userPayload;
          }

          @Override
          public String getSourceVertexName() {
            return null;
          }

          @Override
          public String getDestinationVertexName() {
            return null;
          }

          @Override
          public int getSourceVertexNumTasks() {
            return 2;
          }

          @Override
          public int getDestinationVertexNumTasks() {
            return numTasks;
          }

          @Override
          public String getVertexGroupName() {
            return null;
          }
        };
        if (newEdgeManagers != null) {
          EdgeManagerPlugin edgeManager = ReflectionUtils
                  .createClazzInstance(pluginDesc.getClassName(),
                          new Class[]{EdgeManagerPluginContext.class}, new Object[]{emContext});
          edgeManager.initialize();
          newEdgeManagers.put(entry.getKey(), edgeManager);
        }
      }
      return null;
    }
  }
}

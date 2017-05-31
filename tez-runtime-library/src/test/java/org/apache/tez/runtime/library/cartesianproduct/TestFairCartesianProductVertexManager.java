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
package org.apache.tez.runtime.library.cartesianproduct;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.BROADCAST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestFairCartesianProductVertexManager {
  @Captor
  private ArgumentCaptor<Map<String, EdgeProperty>> edgePropertiesCaptor;
  @Captor
  private ArgumentCaptor<List<ScheduleTaskRequest>> scheduleRequestCaptor;
  private FairCartesianProductVertexManager vertexManager;
  private VertexManagerPluginContext ctx;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    ctx = mock(VertexManagerPluginContext.class);
    vertexManager = new FairCartesianProductVertexManager(ctx);
  }

  /**
   * v0 and v1 are two cartesian product sources
   */
  private void setupDAGVertexOnly(int maxParallelism, long minOpsPerWorker, int numPartition,
                                  int srcParallelismMultiplier) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(2));
    setSrcParallelism(ctx, srcParallelismMultiplier, 2, 3);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .setMaxParallelism(maxParallelism).setMinOpsPerWorker(minOpsPerWorker)
      .setNumPartitionsForFairCase(numPartition);
    vertexManager.initialize(builder.build());
  }

  /**
   * v0 and v1 are two cartesian product sources; v2 is broadcast source
   */
  private void setupDAGVertexOnlyWithBroadcast(int maxParallelism, long minWorkloadPerWorker,
                                               int srcParallelismMultiplier) throws Exception {
    Map<String, EdgeProperty> edgePropertyMap = getEdgePropertyMap(2);
    edgePropertyMap.put("v2", EdgeProperty.create(BROADCAST, null, null, null, null));
    when(ctx.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    setSrcParallelism(ctx, srcParallelismMultiplier, 2, 3, 5);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .setMaxParallelism(maxParallelism).setMinOpsPerWorker(minWorkloadPerWorker)
      .setNumPartitionsForFairCase(maxParallelism);
    vertexManager.initialize(builder.build());
  }

  /**
   * v0 and g0 are two sources; g0 is vertex group of v1 and v2
   */
  private void setupDAGVertexGroup(int maxParallelism, long minWorkloadPerWorker,
                                   int srcParallelismMultiplier) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(3));
    setSrcParallelism(ctx, srcParallelismMultiplier, 2, 3, 4);

    Map<String, List<String>> vertexGroupMap = new HashMap<>();
    vertexGroupMap.put("g0", Arrays.asList("v1", "v2"));
    when(ctx.getInputVertexGroups()).thenReturn(vertexGroupMap);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("g0")
      .setNumPartitionsForFairCase(maxParallelism).setMaxParallelism(maxParallelism)
      .setMinOpsPerWorker(minWorkloadPerWorker);
    vertexManager.initialize(builder.build());
  }

  /**
   * g0 and g1 are two sources; g0 is vertex group of v0 and v1; g1 is vertex group of v2 and v3
   */
  private void setupDAGVertexGroupOnly(int maxParallelism, long minWorkloadPerWorker,
                                       int srcParallelismMultiplier) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(4));
    setSrcParallelism(ctx, srcParallelismMultiplier, 2, 3, 4, 5);

    Map<String, List<String>> vertexGroupMap = new HashMap<>();
    vertexGroupMap.put("g0", Arrays.asList("v0", "v1"));
    vertexGroupMap.put("g1", Arrays.asList("v2", "v3"));
    when(ctx.getInputVertexGroups()).thenReturn(vertexGroupMap);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("g0").addSources("g1")
      .setNumPartitionsForFairCase(maxParallelism)
      .setMaxParallelism(maxParallelism).setMinOpsPerWorker(minWorkloadPerWorker);
    vertexManager.initialize(builder.build());
  }

  private Map<String, EdgeProperty> getEdgePropertyMap(int numSrcV) {
    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    for (int i = 0; i < numSrcV; i++) {
      edgePropertyMap.put("v"+i, EdgeProperty.create(EdgeManagerPluginDescriptor.create(
        CartesianProductEdgeManager.class.getName()), null, null, null, null));
    }
    return edgePropertyMap;
  }

  private void setSrcParallelism(VertexManagerPluginContext ctx, int multiplier, int... numTasks) {
    int i = 0;
    for (int numTask : numTasks) {
      when(ctx.getVertexNumTasks(eq("v"+i))).thenReturn(numTask * multiplier);
      i++;
    }
  }

  private TaskAttemptIdentifier getTaId(String vertexName, int taskId) {
    return new TaskAttemptIdentifierImpl("dag", vertexName,
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 0), taskId), 0));
  }

  private VertexManagerEvent getVMEvent(long numRecord, String vName, int taskId) {

    VertexManagerEventPayloadProto.Builder builder = VertexManagerEventPayloadProto.newBuilder();
    builder.setNumRecord(numRecord);
    VertexManagerEvent vmEvent =
      VertexManagerEvent.create("cp vertex", builder.build().toByteString().asReadOnlyByteBuffer());
    vmEvent.setProducerAttemptIdentifier(getTaId(vName, taskId));
    return vmEvent;
  }

  private void verifyEdgeProperties(EdgeProperty edgeProperty, String[] sources,
                                    int[] numChunksPerSrc, int maxParallelism)
    throws InvalidProtocolBufferException {
    CartesianProductConfigProto config = CartesianProductConfigProto.parseFrom(ByteString.copyFrom(
      edgeProperty.getEdgeManagerDescriptor().getUserPayload().getPayload()));
    assertArrayEquals(sources, config.getSourcesList().toArray());
    assertArrayEquals(numChunksPerSrc, Ints.toArray(config.getNumChunksList()));
    assertEquals(maxParallelism, config.getMaxParallelism());
  }

  private void verifyVertexGroupInfo(EdgeProperty edgeProperty, int positionInGroup,
                                     int... numTaskPerVertexInGroup)
    throws InvalidProtocolBufferException {
    CartesianProductConfigProto config = CartesianProductConfigProto.parseFrom(ByteString.copyFrom(
      edgeProperty.getEdgeManagerDescriptor().getUserPayload().getPayload()));
    assertEquals(positionInGroup, config.getPositionInGroup());
    int i = 0;
    for (int numTask : numTaskPerVertexInGroup) {
      assertEquals(numTask, config.getNumTaskPerVertexInGroup(i));
      i++;
    }
  }

  private void verifyScheduleRequest(int expectedTimes, int... expectedTid) {
    verify(ctx, times(expectedTimes)).scheduleTasks(scheduleRequestCaptor.capture());
    if (expectedTimes > 0) {
      List<ScheduleTaskRequest> requests = scheduleRequestCaptor.getValue();
      int i = 0;
      for (int tid : expectedTid) {
        assertEquals(tid, requests.get(i).getTaskIndex());
        i++;
      }
    }
  }

  @Test(timeout = 5000)
  public void testDAGVertexOnlyGroupByMaxParallelism() throws Exception {
    setupDAGVertexOnly(30, 1, 30, 1);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEvent(250, "v0", 0));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexManagerEventReceived(getVMEvent(200, "v1", 0));
    verify(ctx, times(1)).reconfigureVertex(
      eq(30), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1"}, new int[]{5, 6}, 30);
    verifyVertexGroupInfo(edgeProperties.get("v0"), 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "v1"}, new int[]{5, 6}, 30);
    verifyVertexGroupInfo(edgeProperties.get("v1"), 0);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 0));
    verifyScheduleRequest(1, 0, 6, 1, 7);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
    verifyScheduleRequest(2, 12, 13, 18, 19, 24, 25);
  }

  @Test(timeout = 5000)
  public void testDAGVertexOnlyGroupByMinOpsPerWorker() throws Exception {
    setupDAGVertexOnly(100, 10000, 10, 10);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    for (int i = 0; i < 20; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEvent(20, "v0", i));
    }

    for (int i = 0; i < 30; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEvent(10, "v1", i));
    }

    verify(ctx, times(1)).reconfigureVertex(
      eq(12), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1"}, new int[]{4, 3}, 100);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "v1"}, new int[]{4, 3}, 100);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);

    for (int i = 0; i < 5; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v0", i));
    }
    for (int i = 0; i < 10; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", 10 + i));
    }
    verifyScheduleRequest(1, 1);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroup() throws Exception {
    setupDAGVertexGroup(100, 1, 1);

    for (int i = 0; i < 3; i++) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v" + i, VertexState.CONFIGURED));
    }

    vertexManager.onVertexManagerEventReceived(getVMEvent(100, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(10, "v1", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(5, "v2", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(5, "v2", 1));
    verify(ctx, times(1)).reconfigureVertex(
      eq(100), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    for (int i = 0; i < 3; i++) {
      verifyEdgeProperties(edgeProperties.get("v" + i), new String[]{"v0", "g0"},
        new int[]{20, 5}, 100);
    }

    vertexManager.onVertexStarted(null);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 0));
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v2", 0));
    verifyScheduleRequest(1, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45);
    vertexManager.onSourceTaskCompleted(getTaId("v1", 1));
    verifyScheduleRequest(1);
    vertexManager.onSourceTaskCompleted(getTaId("v2", 1));
    verifyScheduleRequest(2, 1, 6, 11, 16, 21, 26, 31, 36, 41, 46);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroupOnly() throws Exception {
    setupDAGVertexGroupOnly(100, 1, 1);

    for (int i = 0; i < 4; i++) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v" + i, VertexState.CONFIGURED));
    }

    vertexManager.onVertexManagerEventReceived(getVMEvent(20, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(20, "v1", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(5, "v2", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(5, "v2", 1));
    vertexManager.onVertexManagerEventReceived(getVMEvent(16, "v3", 0));

    verify(ctx, times(1)).reconfigureVertex(
      eq(100), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    for (int i = 0; i < 4; i++) {
      verifyEdgeProperties(edgeProperties.get("v" + i), new String[]{"g0", "g1"},
        new int[]{10, 10}, 100);
    }
    verifyVertexGroupInfo(edgeProperties.get("v0"), 0);
    verifyVertexGroupInfo(edgeProperties.get("v1"), 1, 2);
    verifyVertexGroupInfo(edgeProperties.get("v2"), 0);
    verifyVertexGroupInfo(edgeProperties.get("v3"), 1, 4);

    vertexManager.onVertexStarted(null);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v2", 1));
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v3", 1));
    verifyScheduleRequest(1, 3, 13, 23);
  }

  @Test(timeout = 5000)
  public void testSchedulingVertexOnlyWithBroadcast() throws Exception {
    setupDAGVertexOnlyWithBroadcast(30, 1, 1);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEvent(250, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(200, "v1", 0));
    verify(ctx, times(1)).reconfigureVertex(
      eq(30), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertFalse(edgePropertiesCaptor.getValue().containsKey("v2"));

    vertexManager.onVertexStarted(null);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 0));
    verifyScheduleRequest(0);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    verifyScheduleRequest(1, 0, 1, 6, 7);
  }


  @Test(timeout = 5000)
  public void testOnVertexStart() throws Exception {
    setupDAGVertexOnly(6, 1, 6, 1);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexManagerEventReceived(getVMEvent(100, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(100, "v1", 0));

    verifyScheduleRequest(0);

    vertexManager.onVertexStarted(Arrays.asList(getTaId("v0", 0), getTaId("v1", 0)));
    verifyScheduleRequest(1, 0);
  }

  @Test(timeout = 5000)
  public void testZeroSrcTask() throws Exception {
    ctx = mock(VertexManagerPluginContext.class);
    vertexManager = new FairCartesianProductVertexManager(ctx);
    when(ctx.getVertexNumTasks("v0")).thenReturn(2);
    when(ctx.getVertexNumTasks("v1")).thenReturn(0);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .addNumChunks(2).addNumChunks(3).setMaxParallelism(6);
    CartesianProductConfigProto config = builder.build();

    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v1", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    when(ctx.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);

    vertexManager.initialize(config);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStarted(new ArrayList<TaskAttemptIdentifier>());
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
  }

  private void setupGroupingFractionTest() throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(2));
    setSrcParallelism(ctx, 10, 2, 3);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .setMaxParallelism(30).setMinOpsPerWorker(1)
      .setNumPartitionsForFairCase(30).setGroupingFraction(0.5f);
    vertexManager.initialize(builder.build());

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
  }
  @Test(timeout = 5000)
  public void testGroupingFraction() throws Exception {
    setupGroupingFractionTest();
    vertexManager.onVertexManagerEventReceived(getVMEvent(10000, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(10000, "v1", 0));
    for (int i = 0; i < 10; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v0", i));
    }
    for (int i = 0; i < 14; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", i));
    }
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onSourceTaskCompleted(getTaId("v1", 14));
    verify(ctx, times(1)).reconfigureVertex(
      eq(24), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testGroupFractionWithZeroStats() throws Exception {
    setupGroupingFractionTest();

    for (int i = 0; i < 10; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v0", i));
    }
    for (int i = 0; i < 15; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", i));
    }
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));
  }

  @Test(timeout = 5000)
  public void testGroupingFractionWithZeroOutput() throws Exception {
    setupGroupingFractionTest();

    for (int i = 0; i < 20; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v0", i));
    }
    for (int i = 0; i < 30; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", i));
    }
    verify(ctx, times(1)).reconfigureVertex(
      eq(0), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testZeroSrcOutput() throws Exception {
    setupDAGVertexOnly(10, 1, 10, 1);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexManagerEventReceived(getVMEvent(0, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(0, "v0", 1));
    vertexManager.onVertexManagerEventReceived(getVMEvent(0, "v1", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(0, "v1", 1));
    vertexManager.onVertexManagerEventReceived(getVMEvent(0, "v1", 2));
    verify(ctx, times(1)).reconfigureVertex(
      eq(0), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testDisableGrouping() throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(2));
    setSrcParallelism(ctx, 1, 2, 3);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .setMaxParallelism(30).setMinOpsPerWorker(1).setEnableGrouping(false);
    vertexManager.initialize(builder.build());

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEvent(250, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(200, "v1", 0));
    verify(ctx, times(1)).reconfigureVertex(
      eq(6), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testParallelismTwoSkewedSource() throws Exception {
    setupDAGVertexOnly(100, 10000, 10, 10);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEvent(15000, "v0", 0));

    for (int i = 0; i < 30; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEvent(1, "v1", i));
    }

    verify(ctx, times(1)).reconfigureVertex(
      eq(99), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1"},
      new int[]{99, 1}, 100);
  }

  @Test(timeout = 5000)
  public void testParallelismThreeSkewedSource() throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(3));
    setSrcParallelism(ctx, 10, 2, 3, 4);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1").addSources("v2")
      .setMaxParallelism(100).setMinOpsPerWorker(10000)
      .setNumPartitionsForFairCase(10);
    vertexManager.initialize(builder.build());

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEvent(60000, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEvent(4000, "v1", 0));
    for (int i = 0; i < 40; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEvent(3, "v2", i));
    }

    verify(ctx, times(1)).reconfigureVertex(
      eq(93), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1", "v2"},
      new int[]{31, 3, 1}, 100);
  }
}
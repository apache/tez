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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCartesianProductVertexManagerUnpartitioned {
  private static long desiredBytesPerGroup = 1000;
  @Captor
  private ArgumentCaptor<Map<String, EdgeProperty>> edgePropertiesCaptor;
  @Captor
  private ArgumentCaptor<List<ScheduleTaskRequest>> scheduleRequestCaptor;
  @Captor
  private ArgumentCaptor<Integer> parallelismCaptor;
  private CartesianProductVertexManagerUnpartitioned vertexManager;
  private VertexManagerPluginContext ctx;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    ctx = mock(VertexManagerPluginContext.class);
    vertexManager = new CartesianProductVertexManagerUnpartitioned(ctx);
  }

  /**
   * v0 and v1 are two cartesian product sources
   */
  private void setupDAGVertexOnly(boolean doGrouping) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(2));
    setSrcParallelism(ctx, doGrouping ? 10 : 1, 2, 3);

    CartesianProductVertexManagerConfig config = new CartesianProductVertexManagerConfig(
      false, new String[]{"v0","v1"}, null, 0, 0, doGrouping, desiredBytesPerGroup, null);
    vertexManager.initialize(config);
  }

  /**
   * v0 and v1 are two cartesian product sources; v2 is broadcast source; without auto grouping
   */
  private void setupDAGVertexOnlyWithBroadcast() throws Exception {
    Map<String, EdgeProperty> edgePropertyMap = getEdgePropertyMap(2);
    edgePropertyMap.put("v2", EdgeProperty.create(BROADCAST, null, null, null, null));
    when(ctx.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    setSrcParallelism(ctx, 2, 3, 5);

    CartesianProductVertexManagerConfig config =
      new CartesianProductVertexManagerConfig(
        false, new String[]{"v0","v1"}, null, 0, 0, false, 0, null);
    vertexManager.initialize(config);
  }

  /**
   * v0 and g0 are two sources; g0 is vertex group of v1 and v2
   */
  private void setupDAGVertexGroup(boolean doGrouping) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(3));
    setSrcParallelism(ctx, doGrouping ? 10: 1, 2, 3, 4);

    Map<String, List<String>> vertexGroupMap = new HashMap<>();
    vertexGroupMap.put("g0", Arrays.asList("v1", "v2"));
    when(ctx.getInputVertexGroups()).thenReturn(vertexGroupMap);

    CartesianProductVertexManagerConfig config = new CartesianProductVertexManagerConfig(
      false, new String[]{"v0","g0"}, null, 0, 0, doGrouping, desiredBytesPerGroup, null);
    vertexManager.initialize(config);
  }

  /**
   * g0 and g1 are two sources; g0 is vertex group of v0 and v1; g1 is vertex group of v2 and v3
   */
  private void setupDAGVertexGroupOnly(boolean doGrouping) throws Exception {
    when(ctx.getInputVertexEdgeProperties()).thenReturn(getEdgePropertyMap(4));
    setSrcParallelism(ctx, doGrouping ? 10 : 1, 2, 3, 4, 5);

    Map<String, List<String>> vertexGroupMap = new HashMap<>();
    vertexGroupMap.put("g0", Arrays.asList("v0", "v1"));
    vertexGroupMap.put("g1", Arrays.asList("v2", "v3"));
    when(ctx.getInputVertexGroups()).thenReturn(vertexGroupMap);

    CartesianProductVertexManagerConfig config = new CartesianProductVertexManagerConfig(
      false, new String[]{"g0","g1"}, null, 0, 0, doGrouping, desiredBytesPerGroup, null);
    vertexManager.initialize(config);
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

  private VertexManagerEvent getVMEevnt(long outputSize, String vName, int taskId) {

    VertexManagerEventPayloadProto.Builder builder = VertexManagerEventPayloadProto.newBuilder();
    builder.setOutputSize(outputSize);
    VertexManagerEvent vmEvent =
      VertexManagerEvent.create("cp vertex", builder.build().toByteString().asReadOnlyByteBuffer());
    vmEvent.setProducerAttemptIdentifier(getTaId(vName, taskId));
    return vmEvent;
  }

  private void verifyEdgeProperties(EdgeProperty edgeProperty, String[] sources,
                                    int[] numChunksPerSrc, int numChunk, int chunkIdOffset)
    throws InvalidProtocolBufferException {
    CartesianProductEdgeManagerConfig conf = CartesianProductEdgeManagerConfig.fromUserPayload(
      edgeProperty.getEdgeManagerDescriptor().getUserPayload());
    assertArrayEquals(sources, conf.getSourceVertices().toArray());
    assertArrayEquals(numChunksPerSrc, conf.numChunksPerSrc);
    assertEquals(numChunk, conf.numChunk);
    assertEquals(chunkIdOffset, conf.chunkIdOffset);
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
  public void testDAGVertexOnly() throws Exception {
    setupDAGVertexOnly(false);

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    verify(ctx, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals(6, (int) parallelismCaptor.getValue());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1"}, new int[]{2, 3}, 2, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "v1"}, new int[]{2, 3}, 3, 0);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    verify(ctx, never()).scheduleTasks(scheduleRequestCaptor.capture());
    vertexManager.onSourceTaskCompleted(getTaId("v1", 0));
    verify(ctx, times(1)).scheduleTasks(scheduleRequestCaptor.capture());
    verifyScheduleRequest(1, 0);

    vertexManager.onSourceTaskCompleted(getTaId("v1", 1));
    verify(ctx, times(2)).scheduleTasks(scheduleRequestCaptor.capture());
    verifyScheduleRequest(2, 1);
  }

  @Test(timeout = 5000)
  public void testDAGVertexOnlyWithGrouping() throws Exception {
    setupDAGVertexOnly(true);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup, "v0", 0));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexManagerEventReceived(getVMEevnt(1, "v1", 0));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexManagerEventReceived(getVMEevnt(0, "v0", 1));
    for (int i = 1; i < 30; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEevnt(1, "v1", i));
    }
    verify(ctx, times(1)).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "v1"}, new int[]{10, 1}, 10, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "v1"}, new int[]{10, 1}, 1, 0);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
    for (int i = 0; i < 29; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", i));
    }
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v1", 29));
    verifyScheduleRequest(1, 0);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroup() throws Exception {
    setupDAGVertexGroup(false);

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.CONFIGURED));
    verify(ctx, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals(14, (int) parallelismCaptor.getValue());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();

    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "g0"}, new int[]{2, 7}, 2, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "g0"}, new int[]{2, 7}, 3, 0);
    verifyEdgeProperties(edgeProperties.get("v2"), new String[]{"v0", "g0"}, new int[]{2, 7}, 4, 3);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0",1));
    vertexManager.onSourceTaskCompleted(getTaId("v1",2));
    verifyScheduleRequest(1, 9);
    vertexManager.onSourceTaskCompleted(getTaId("v2", 0));
    verifyScheduleRequest(2, 10);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroupWithGrouping() throws Exception {
    setupDAGVertexGroup(true);

    for (int i = 0; i < 3; i++) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v" + i, VertexState.CONFIGURED));
    }

    vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup, "v1", 0));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexManagerEventReceived(getVMEevnt(0, "v0", 1));
    for (int i = 0; i < 40; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEevnt(1, "v2", i));
    }

    verify(ctx, times(1)).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"v0", "g0"}, new int[]{10, 31}, 10, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"v0", "g0"}, new int[]{10, 31}, 30, 0);
    verifyEdgeProperties(edgeProperties.get("v2"), new String[]{"v0", "g0"}, new int[]{10, 31}, 1, 30);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 10));
    vertexManager.onSourceTaskCompleted(getTaId("v2", 0));
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
    verifyScheduleRequest(1, 10);
    for (int i = 1; i < 40; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v2", i));
    }
    verifyScheduleRequest(2, 30);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroupOnly() throws Exception {
    setupDAGVertexGroupOnly(false);

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.CONFIGURED));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v3", VertexState.CONFIGURED));
    verify(ctx, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals(45, (int) parallelismCaptor.getValue());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();

    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"g0", "g1"}, new int[]{5, 9}, 2, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"g0", "g1"}, new int[]{5, 9}, 3, 2);
    verifyEdgeProperties(edgeProperties.get("v2"), new String[]{"g0", "g1"}, new int[]{5, 9}, 4, 0);
    verifyEdgeProperties(edgeProperties.get("v3"), new String[]{"g0", "g1"}, new int[]{5, 9}, 5, 4);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
    vertexManager.onSourceTaskCompleted(getTaId("v2", 3));
    verifyScheduleRequest(1, 12);
    vertexManager.onSourceTaskCompleted(getTaId("v1", 2));
    verifyScheduleRequest(2, 39);
    vertexManager.onSourceTaskCompleted(getTaId("v3", 0));
    verifyScheduleRequest(3, 13, 40);
  }

  @Test(timeout = 5000)
  public void testDAGVertexGroupOnlyWithGrouping() throws Exception {
    setupDAGVertexGroupOnly(true);

    for (int i = 0; i < 4; i++) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v" + i, VertexState.CONFIGURED));
    }

    vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup, "v0", 0));
    vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup, "v2", 0));
    verify(ctx, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));

    vertexManager.onVertexManagerEventReceived(getVMEevnt(0, "v0", 1));
    for (int i = 0; i < 5; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEevnt(desiredBytesPerGroup/5, "v1", i));
    }
    for (int i = 0; i < 50; i++) {
      vertexManager.onVertexManagerEventReceived(getVMEevnt(1, "v3", i));
    }

    verify(ctx, times(1)).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    verifyEdgeProperties(edgeProperties.get("v0"), new String[]{"g0", "g1"}, new int[]{16, 41}, 10, 0);
    verifyEdgeProperties(edgeProperties.get("v1"), new String[]{"g0", "g1"}, new int[]{16, 41}, 6, 10);
    verifyEdgeProperties(edgeProperties.get("v2"), new String[]{"g0", "g1"}, new int[]{16, 41}, 40, 0);
    verifyEdgeProperties(edgeProperties.get("v3"), new String[]{"g0", "g1"}, new int[]{16, 41}, 1, 40);

    vertexManager.onVertexStarted(null);
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 1));
    vertexManager.onSourceTaskCompleted(getTaId("v2", 20));
    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    verifyScheduleRequest(1, 20);
    vertexManager.onSourceTaskCompleted(getTaId("v3", 0));
    verifyScheduleRequest(1);
    for (int i = 1; i < 50; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v3", i));
    }
    verifyScheduleRequest(2, 40);
    vertexManager.onSourceTaskCompleted(getTaId("v1", 5));
    verifyScheduleRequest(2);
    for (int i = 6; i < 10; i++) {
      vertexManager.onSourceTaskCompleted(getTaId("v1", i));
    }
    verifyScheduleRequest(3, 471, 491);
  }

  @Test(timeout = 5000)
  public void testSchedulingVertexOnlyWithBroadcast() throws Exception {
    setupDAGVertexOnlyWithBroadcast();
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStarted(null);

    verifyScheduleRequest(0);
    vertexManager.onSourceTaskCompleted(getTaId("v0", 0));
    vertexManager.onSourceTaskCompleted(getTaId("v1", 1));
    verifyScheduleRequest(0);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    verifyScheduleRequest(1);
    verify(ctx, times(1)).scheduleTasks(scheduleRequestCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testOnVertexStart() throws Exception {
    setupDAGVertexOnly(false);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    vertexManager.onVertexStarted(Arrays.asList(getTaId("v0", 0), getTaId("v1", 0)));
    verifyScheduleRequest(1, 0);
  }

  @Test(timeout = 5000)
  public void testZeroSrcTask() throws Exception {
    ctx = mock(VertexManagerPluginContext.class);
    vertexManager = new CartesianProductVertexManagerUnpartitioned(ctx);
    when(ctx.getVertexNumTasks(eq("v0"))).thenReturn(2);
    when(ctx.getVertexNumTasks(eq("v1"))).thenReturn(0);

    CartesianProductVertexManagerConfig config =
      new CartesianProductVertexManagerConfig(
        false, new String[]{"v0","v1"}, null, 0, 0, false, 0, null);
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
}
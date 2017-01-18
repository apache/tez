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

import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.UserPayload;
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
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.BROADCAST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
  @Captor
  private ArgumentCaptor<Map<String, EdgeProperty>> edgePropertiesCaptor;
  @Captor
  private ArgumentCaptor<List<ScheduleTaskRequest>> scheduleTaskRequestCaptor;
  private CartesianProductVertexManagerUnpartitioned vertexManager;
  private VertexManagerPluginContext context;
  private List<TaskAttemptIdentifier> allCompletions;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    context = mock(VertexManagerPluginContext.class);
    vertexManager = new CartesianProductVertexManagerUnpartitioned(context);

    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
        CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v1", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v2", EdgeProperty.create(BROADCAST, null, null, null, null));
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    when(context.getVertexNumTasks(eq("v0"))).thenReturn(2);
    when(context.getVertexNumTasks(eq("v1"))).thenReturn(3);
    when(context.getVertexNumTasks(eq("v2"))).thenReturn(5);

    CartesianProductVertexManagerConfig config =
      new CartesianProductVertexManagerConfig(
        false, new String[]{"v0","v1"}, null, 0, 0, false, 0, null);
    vertexManager.initialize(config);
    allCompletions = new ArrayList<>();
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 0), 0), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 0), 1), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v1",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 1), 0), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v1",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 1), 1), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v1",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 1), 2), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v2",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 3), 0), 0)));
  }

  @Test(timeout = 5000)
  public void testReconfigureVertex() throws Exception {
    ArgumentCaptor<Integer> parallelismCaptor = ArgumentCaptor.forClass(Integer.class);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    verify(context, never()).reconfigureVertex(
      anyInt(), any(VertexLocationHint.class), anyMapOf(String.class, EdgeProperty.class));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    verify(context, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals(6, (int)parallelismCaptor.getValue());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    assertFalse(edgeProperties.containsKey("v2"));
    for (EdgeProperty edgeProperty : edgeProperties.values()) {
      UserPayload payload = edgeProperty.getEdgeManagerDescriptor().getUserPayload();
      CartesianProductEdgeManagerConfig newConfig =
        CartesianProductEdgeManagerConfig.fromUserPayload(payload);
      assertArrayEquals(new int[]{2,3}, newConfig.getNumTasks());
      assertArrayEquals(new int[]{2,3}, newConfig.getNumGroups());
    }
  }

  @Test(timeout = 5000)
  public void testOnSourceTaskComplete() throws Exception {
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStarted(null);
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    vertexManager.onSourceTaskCompleted(allCompletions.get(0));
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    vertexManager.onSourceTaskCompleted(allCompletions.get(2));
    // cannot start schedule because broadcast vertex isn't in RUNNING state
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    List<ScheduleTaskRequest> requests = scheduleTaskRequestCaptor.getValue();
    assertNotNull(requests);
    assertEquals(1, requests.size());
    assertEquals(0, requests.get(0).getTaskIndex());

    // v2 completion shouldn't matter
    vertexManager.onSourceTaskCompleted(allCompletions.get(5));
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());

    vertexManager.onSourceTaskCompleted(allCompletions.get(3));
    verify(context, times(2)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    requests = scheduleTaskRequestCaptor.getValue();
    assertNotNull(requests);
    assertEquals(1, requests.size());
    assertEquals(1, requests.get(0).getTaskIndex());
  }

  private void testOnVertexStartHelper(boolean broadcastRunning) throws Exception {
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    if (broadcastRunning) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    }

    List<TaskAttemptIdentifier> completions = new ArrayList<>();
    completions.add(allCompletions.get(0));
    completions.add(allCompletions.get(2));
    completions.add(allCompletions.get(5));
    vertexManager.onVertexStarted(completions);

    if (!broadcastRunning) {
      verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    }

    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    List<ScheduleTaskRequest> requests = scheduleTaskRequestCaptor.getValue();
    assertNotNull(requests);
    assertEquals(1, requests.size());
    assertEquals(0, requests.get(0).getTaskIndex());
  }

  @Test(timeout = 5000)
  public void testOnVertexStartWithBroadcastRunning() throws Exception {
    testOnVertexStartHelper(true);
  }

  @Test(timeout = 5000)
  public void testOnVertexStartWithoutBroadcastRunning() throws Exception {
    testOnVertexStartHelper(false);

  }

  @Test(timeout = 5000)
  public void testZeroSrcTask() throws Exception {
    context = mock(VertexManagerPluginContext.class);
    vertexManager = new CartesianProductVertexManagerUnpartitioned(context);
    when(context.getVertexNumTasks(eq("v0"))).thenReturn(2);
    when(context.getVertexNumTasks(eq("v1"))).thenReturn(0);

    CartesianProductVertexManagerConfig config =
      new CartesianProductVertexManagerConfig(
        false, new String[]{"v0","v1"}, null, 0, 0, false, 0, null);
    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v1", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);

    vertexManager.initialize(config);
    allCompletions = new ArrayList<>();
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 0), 0), 0)));
    allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
        TezDAGID.getInstance("0", 0, 0), 0), 1), 0)));

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStarted(new ArrayList<TaskAttemptIdentifier>());
    vertexManager.onSourceTaskCompleted(allCompletions.get(0));
    vertexManager.onSourceTaskCompleted(allCompletions.get(1));
  }

  @Test(timeout = 5000)
  public void testAutoGrouping() throws Exception {
    testAutoGroupingHelper(false);
    testAutoGroupingHelper(true);
  }

  private void testAutoGroupingHelper(boolean enableAutoGrouping) throws Exception {
    int numTaskV0 = 20;
    int numTaskV1 = 10;
    long desiredBytesPerGroup = 1000;
    long outputBytesPerTaskV0 = 500;
    long outputBytesPerTaskV1 = 10;
    int expectedNumGroupV0 = 10;
    int expectedNumGroupV1 = 1;
    ArgumentCaptor<Integer> parallelismCaptor = ArgumentCaptor.forClass(Integer.class);
    CartesianProductVertexManagerConfig config = new CartesianProductVertexManagerConfig(
      false, new String[]{"v0","v1"}, null, 0, 0, enableAutoGrouping, desiredBytesPerGroup, null);
    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    EdgeProperty edgeProperty = EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null);
    edgePropertyMap.put("v0", edgeProperty);
    edgePropertyMap.put("v1", edgeProperty);
    edgePropertyMap.put("v2", EdgeProperty.create(BROADCAST, null, null, null, null));
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    when(context.getVertexNumTasks(eq("v0"))).thenReturn(2);
    when(context.getVertexNumTasks(eq("v1"))).thenReturn(3);

    context = mock(VertexManagerPluginContext.class);
    vertexManager = new CartesianProductVertexManagerUnpartitioned(context);
    when(context.getVertexNumTasks(eq("v0"))).thenReturn(numTaskV0);
    when(context.getVertexNumTasks(eq("v1"))).thenReturn(numTaskV1);
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);

    vertexManager.initialize(config);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    if (!enableAutoGrouping) {
      // auto grouping disabled, shouldn't auto group
      verify(context, times(1)).reconfigureVertex(parallelismCaptor.capture(),
        isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
      assertEquals(numTaskV0 * numTaskV1, parallelismCaptor.getValue().intValue());
      return;
    }

    // not enough input size, shouldn't auto group
    verify(context, never()).reconfigureVertex(anyInt(), any(VertexLocationHint.class),
      anyMapOf(String.class, EdgeProperty.class));

    // only v0 reach threshold or finish all task, shouldn't auto group
    VertexManagerEventPayloadProto.Builder builder = VertexManagerEventPayloadProto.newBuilder();
    builder.setOutputSize(outputBytesPerTaskV0);
    VertexManagerEventPayloadProto proto = builder.build();
    VertexManagerEvent vmEvent =
      VertexManagerEvent.create("cp vertex", proto.toByteString().asReadOnlyByteBuffer());

    Formatter formatter = new Formatter();
    for (int i = 0; i < desiredBytesPerGroup/outputBytesPerTaskV0; i++) {
      vmEvent.setProducerAttemptIdentifier(
        new TaskAttemptIdentifierImpl("dag", "v0", TezTaskAttemptID.fromString(
          formatter.format("attempt_1441301219877_0109_1_00_%06d_0", i).toString())));
      vertexManager.onVertexManagerEventReceived(vmEvent);
    }
    verify(context, never()).reconfigureVertex(anyInt(), any(VertexLocationHint.class),
      anyMapOf(String.class, EdgeProperty.class));

    // vmEvent from broadcast vertex shouldn't matter
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", "v2",
        TezTaskAttemptID.fromString("attempt_1441301219877_0109_1_00_000000_0")));
    vertexManager.onVertexManagerEventReceived(vmEvent);

    // v1 finish all tasks but still doesn't reach threshold, auto group anyway
    proto = builder.setOutputSize(outputBytesPerTaskV1).build();
    vmEvent = VertexManagerEvent.create("cp vertex", proto.toByteString().asReadOnlyByteBuffer());
    for (int i = 0; i < numTaskV1; i++) {
      verify(context, never()).reconfigureVertex(anyInt(), any(VertexLocationHint.class),
        anyMapOf(String.class, EdgeProperty.class));
      vmEvent.setProducerAttemptIdentifier(
        new TaskAttemptIdentifierImpl("dag", "v1", TezTaskAttemptID.fromString(
          formatter.format("attempt_1441301219877_0109_1_01_%06d_0", i).toString())));
      vertexManager.onVertexManagerEventReceived(vmEvent);
    }
    formatter.close();
    verify(context, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    Map<String, EdgeProperty> edgeProperties = edgePropertiesCaptor.getValue();
    for (EdgeProperty property : edgeProperties.values()) {
      UserPayload payload = property.getEdgeManagerDescriptor().getUserPayload();
      CartesianProductEdgeManagerConfig newConfig =
        CartesianProductEdgeManagerConfig.fromUserPayload(payload);
      assertArrayEquals(new int[]{numTaskV0, numTaskV1}, newConfig.getNumTasks());
      assertArrayEquals(new int[]{expectedNumGroupV0,expectedNumGroupV1}, newConfig.getNumGroups());
    }

    assertEquals(expectedNumGroupV0 * expectedNumGroupV1, parallelismCaptor.getValue().intValue());
    for (EdgeProperty property : edgePropertiesCaptor.getValue().values()) {
      CartesianProductEdgeManagerConfig emConfig =
        CartesianProductEdgeManagerConfig.fromUserPayload(
          property.getEdgeManagerDescriptor().getUserPayload());
      assertArrayEquals(new int[] {numTaskV0, numTaskV1}, emConfig.getNumTasks());
      assertArrayEquals(new int[] {expectedNumGroupV0, expectedNumGroupV1}, emConfig.getNumGroups());
    }

    vertexManager.onVertexStarted(null);
    // v0 t0 finish, shouldn't schedule
    vertexManager.onSourceTaskCompleted(allCompletions.get(0));
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());

    // v1 all task finish, shouldn't schedule
    for (int i = 0; i < numTaskV1; i++) {
      vertexManager.onSourceTaskCompleted(new TaskAttemptIdentifierImpl("dag", "v1",
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
          TezDAGID.getInstance("0", 0, 0), 1), i), 0)));
      verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    }

    // v0 t1 finish, should schedule
    vertexManager.onSourceTaskCompleted(allCompletions.get(1));
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    List<ScheduleTaskRequest> requests = scheduleTaskRequestCaptor.getValue();
    assertNotNull(requests);
    assertEquals(1, requests.size());
    assertEquals(0, requests.get(0).getTaskIndex());
  }
}
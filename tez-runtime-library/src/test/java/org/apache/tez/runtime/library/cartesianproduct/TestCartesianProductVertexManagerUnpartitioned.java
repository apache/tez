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
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
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
      new CartesianProductVertexManagerConfig(false, new String[]{"v0","v1"}, null, 0, 0, null);
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
      new CartesianProductVertexManagerConfig(false, new String[]{"v0","v1"}, null, 0, 0, null);
    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v1", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);

    vertexManager.initialize(config);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    vertexManager.onVertexStarted(null);
    vertexManager.onSourceTaskCompleted(allCompletions.get(0));
    vertexManager.onSourceTaskCompleted(allCompletions.get(1));
  }
}
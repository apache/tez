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
import org.apache.tez.dag.api.TezReflectionException;
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
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCartesianProductVertexManagerPartitioned {
  @Captor
  private ArgumentCaptor<Map<String, EdgeProperty>> edgePropertiesCaptor;
  @Captor
  private ArgumentCaptor<List<ScheduleTaskRequest>> scheduleTaskRequestCaptor;
  private CartesianProductVertexManagerPartitioned vertexManager;
  private VertexManagerPluginContext context;
  private List<TaskAttemptIdentifier> allCompletions;

  @Before
  public void setup() throws TezReflectionException {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(true).addSources("v0").addSources("v1")
      .addNumPartitions(2).addNumPartitions(2);
    setupWithConfig(builder.build());
  }

  private void setupWithConfig(CartesianProductConfigProto config)
    throws TezReflectionException {
    MockitoAnnotations.initMocks(this);
    context = mock(VertexManagerPluginContext.class);
    when(context.getVertexName()).thenReturn("cp");
    when(context.getVertexNumTasks("cp")).thenReturn(-1);
    vertexManager = new CartesianProductVertexManagerPartitioned(context);
    Map<String, EdgeProperty> edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v1", EdgeProperty.create(EdgeManagerPluginDescriptor.create(
      CartesianProductEdgeManager.class.getName()), null, null, null, null));
    edgePropertyMap.put("v2", EdgeProperty.create(BROADCAST, null, null, null, null));
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    when(context.getVertexNumTasks(eq("v0"))).thenReturn(4);
    when(context.getVertexNumTasks(eq("v1"))).thenReturn(4);
    when(context.getVertexNumTasks(eq("v2"))).thenReturn(4);
    vertexManager.initialize(config);

    allCompletions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 4; j++) {
        allCompletions.add(new TaskAttemptIdentifierImpl("dag", "v" + i,
          TezTaskAttemptID.getInstance(TezTaskID.getInstance(TezVertexID.getInstance(
            TezDAGID.getInstance("0", 0, 0), i), j), 0)));
      }
    }
  }

  private void testReconfigureVertexHelper(CartesianProductConfigProto config,
                                           int parallelism)
    throws Exception {
    setupWithConfig(config);
    ArgumentCaptor<Integer> parallelismCaptor = ArgumentCaptor.forClass(Integer.class);

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    verify(context, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals((int)parallelismCaptor.getValue(), parallelism);
    assertNull(edgePropertiesCaptor.getValue());
  }

  @Test(timeout = 5000)
  public void testReconfigureVertex() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(true).addSources("v0").addSources("v1")
      .addNumPartitions(5).addNumPartitions(5).setFilterClassName(TestFilter.class.getName());
    testReconfigureVertexHelper(builder.build(), 10);
    builder.clearFilterClassName();
    testReconfigureVertexHelper(builder.build(), 25);
  }

  @Test(timeout = 5000)
  public void testScheduling() throws Exception {
    vertexManager.onVertexStarted(null);
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));


    vertexManager.onSourceTaskCompleted(allCompletions.get(0));
    vertexManager.onSourceTaskCompleted(allCompletions.get(1));
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());

    List<ScheduleTaskRequest> scheduleTaskRequests;
    vertexManager.onSourceTaskCompleted(allCompletions.get(2));
    // shouldn't start schedule because broadcast src is not in RUNNING state
    verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(0, scheduleTaskRequests.get(0).getTaskIndex());

    // completion from broadcast src shouldn't matter
    vertexManager.onSourceTaskCompleted(allCompletions.get(8));
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());

    for (int i = 3; i < 6; i++) {
      vertexManager.onSourceTaskCompleted(allCompletions.get(i));
      verify(context, times(i-1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
      scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
      assertEquals(1, scheduleTaskRequests.size());
      assertEquals(i-2, scheduleTaskRequests.get(0).getTaskIndex());
    }

    for (int i = 6; i < 8; i++) {
      vertexManager.onSourceTaskCompleted(allCompletions.get(i));
      verify(context, times(4)).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    }
  }

  @Test(timeout = 5000)
  public void testOnVertexStartWithBroadcastRunning() throws Exception {
    testOnVertexStartHelper(true);
  }

  @Test(timeout = 5000)
  public void testOnVertexStartWithoutBroadcastRunning() throws Exception {
    testOnVertexStartHelper(false);
  }

  private void testOnVertexStartHelper(boolean broadcastRunning) throws Exception {
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));
    if (broadcastRunning) {
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    }

    List<TaskAttemptIdentifier> completions = new ArrayList<>();
    completions.add(allCompletions.get(0));
    completions.add(allCompletions.get(1));
    completions.add(allCompletions.get(4));
    completions.add(allCompletions.get(8));

    vertexManager.onVertexStarted(completions);

    if (!broadcastRunning) {
      verify(context, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
      vertexManager.onVertexStateUpdated(new VertexStateUpdate("v2", VertexState.RUNNING));
    }

    List<ScheduleTaskRequest> scheduleTaskRequests;
    verify(context, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(0, scheduleTaskRequests.get(0).getTaskIndex());
  }

  public static class TestFilter extends CartesianProductFilter {
    public TestFilter(UserPayload payload) {
      super(payload);
    }

    @Override
    public boolean isValidCombination(Map<String, Integer> vertexPartitionMap) {
      return vertexPartitionMap.get("v0") > vertexPartitionMap.get("v1");
    }
  }
}

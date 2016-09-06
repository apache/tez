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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;
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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
  private TezConfiguration conf = new TezConfiguration();

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
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

  private void testReconfigureVertexHelper(CartesianProductConfig config, int parallelism)
    throws Exception {
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getUserPayload()).thenReturn(config.toUserPayload(conf));

    EdgeProperty edgeProperty =
      EdgeProperty.create(EdgeManagerPluginDescriptor.create(
        CartesianProductEdgeManager.class.getName()), null, null, null, null);
    Map<String, EdgeProperty> inputEdgeProperties = new HashMap<>();
    for (String vertex : config.getSourceVertices()) {
      inputEdgeProperties.put(vertex, edgeProperty);
    }
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(inputEdgeProperties);
    CartesianProductVertexManager vertexManager = new CartesianProductVertexManager(mockContext);
    vertexManager.initialize();
    ArgumentCaptor<Integer> parallelismCaptor = ArgumentCaptor.forClass(Integer.class);

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    verify(mockContext, times(1)).reconfigureVertex(parallelismCaptor.capture(),
      isNull(VertexLocationHint.class), edgePropertiesCaptor.capture());
    assertEquals((int)parallelismCaptor.getValue(), parallelism);
    assertNull(edgePropertiesCaptor.getValue());
  }

  @Test(timeout = 5000)
  public void testReconfigureVertex() throws Exception {
    testReconfigureVertexHelper(
      new CartesianProductConfig(new int[]{5,5}, new String[]{"v0", "v1"},
        new CartesianProductFilterDescriptor(TestFilter.class.getName())), 10);
    testReconfigureVertexHelper(
      new CartesianProductConfig(new int[]{5,5}, new String[]{"v0", "v1"}, null), 25);
  }

  @Test(timeout = 5000)
  public void testScheduling() throws Exception {
    CartesianProductConfig config = new CartesianProductConfig(new int[]{2,2},
      new String[]{"v0", "v1"}, null);
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getUserPayload()).thenReturn(config.toUserPayload(conf));
    Set<String> inputVertices = new HashSet<String>();
    inputVertices.add("v0");
    inputVertices.add("v1");
    when(mockContext.getVertexInputNames()).thenReturn(inputVertices);
    when(mockContext.getVertexNumTasks("v0")).thenReturn(4);
    when(mockContext.getVertexNumTasks("v1")).thenReturn(4);
    EdgeProperty edgeProperty =
      EdgeProperty.create(EdgeManagerPluginDescriptor.create(
        CartesianProductEdgeManager.class.getName()), null, null, null, null);
    Map<String, EdgeProperty> inputEdgeProperties = new HashMap<String, EdgeProperty>();
    inputEdgeProperties.put("v0", edgeProperty);
    inputEdgeProperties.put("v1", edgeProperty);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(inputEdgeProperties);
    CartesianProductVertexManager vertexManager = new CartesianProductVertexManager(mockContext);
    vertexManager.initialize();

    vertexManager.onVertexStarted(new ArrayList<TaskAttemptIdentifier>());
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));


    TaskAttemptIdentifier taId = mock(TaskAttemptIdentifier.class, Mockito.RETURNS_DEEP_STUBS);
    when(taId.getTaskIdentifier().getVertexIdentifier().getName()).thenReturn("v0", "v0", "v1",
      "v1", "v0", "v0", "v1", "v1");
    when(taId.getTaskIdentifier().getIdentifier()).thenReturn(0, 1, 0, 1, 2, 3, 2, 3);

    for (int i = 0; i < 2; i++) {
      vertexManager.onSourceTaskCompleted(taId);
      verify(mockContext, never()).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    }

    List<ScheduleTaskRequest> scheduleTaskRequests;

    vertexManager.onSourceTaskCompleted(taId);
    verify(mockContext, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(0, scheduleTaskRequests.get(0).getTaskIndex());

    vertexManager.onSourceTaskCompleted(taId);
    verify(mockContext, times(2)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(1, scheduleTaskRequests.get(0).getTaskIndex());

    vertexManager.onSourceTaskCompleted(taId);
    verify(mockContext, times(3)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(2, scheduleTaskRequests.get(0).getTaskIndex());

    vertexManager.onSourceTaskCompleted(taId);
    verify(mockContext, times(4)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(3, scheduleTaskRequests.get(0).getTaskIndex());

    for (int i = 0; i < 2; i++) {
      vertexManager.onSourceTaskCompleted(taId);
      verify(mockContext, times(4)).scheduleTasks(Matchers.<List<ScheduleTaskRequest>>any());
    }
  }

  @Test(timeout = 5000)
  public void testVertexStartWithCompletion() throws Exception {
    CartesianProductConfig config = new CartesianProductConfig(new int[]{2,2},
      new String[]{"v0", "v1"}, null);
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getUserPayload()).thenReturn(config.toUserPayload(conf));
    Set<String> inputVertices = new HashSet<String>();
    inputVertices.add("v0");
    inputVertices.add("v1");
    when(mockContext.getVertexInputNames()).thenReturn(inputVertices);
    when(mockContext.getVertexNumTasks("v0")).thenReturn(4);
    when(mockContext.getVertexNumTasks("v1")).thenReturn(4);
    EdgeProperty edgeProperty =
      EdgeProperty.create(EdgeManagerPluginDescriptor.create(
        CartesianProductEdgeManager.class.getName()), null, null, null, null);
    Map<String, EdgeProperty> inputEdgeProperties = new HashMap<String, EdgeProperty>();
    inputEdgeProperties.put("v0", edgeProperty);
    inputEdgeProperties.put("v1", edgeProperty);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(inputEdgeProperties);
    CartesianProductVertexManager vertexManager = new CartesianProductVertexManager(mockContext);
    vertexManager.initialize();

    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v0", VertexState.CONFIGURED));
    vertexManager.onVertexStateUpdated(new VertexStateUpdate("v1", VertexState.CONFIGURED));

    List<TaskAttemptIdentifier> completions = new ArrayList<>();
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(0, 0), 0);
    TezVertexID v0Id = TezVertexID.getInstance(dagId, 0);
    TezVertexID v1Id = TezVertexID.getInstance(dagId, 1);

    completions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(v0Id, 0), 0)));
    completions.add(new TaskAttemptIdentifierImpl("dag", "v0",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(v0Id, 1), 0)));
    completions.add(new TaskAttemptIdentifierImpl("dag", "v1",
      TezTaskAttemptID.getInstance(TezTaskID.getInstance(v1Id, 0), 0)));

    vertexManager.onVertexStarted(completions);

    List<ScheduleTaskRequest> scheduleTaskRequests;
    verify(mockContext, times(1)).scheduleTasks(scheduleTaskRequestCaptor.capture());
    scheduleTaskRequests = scheduleTaskRequestCaptor.getValue();
    assertEquals(1, scheduleTaskRequests.size());
    assertEquals(0, scheduleTaskRequests.get(0).getTaskIndex());
  }
}

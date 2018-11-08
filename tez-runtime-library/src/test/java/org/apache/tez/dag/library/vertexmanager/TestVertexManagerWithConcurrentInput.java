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

import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.library.edgemanager.SilentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestVertexManagerWithConcurrentInput {

  @Captor
  ArgumentCaptor<List<VertexManagerPluginContext.ScheduleTaskRequest>> requestCaptor;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test(timeout = 5000)
  public void testBasicVertexWithConcurrentInput() throws Exception {
    HashMap<String, EdgeProperty> mockInputVertices =
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    int srcVertex1Parallelism = 2;
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeManagerPluginDescriptor.create(SilentEdgeManager.class.getName()),
        EdgeProperty.DataSourceType.EPHEMERAL,
        EdgeProperty.SchedulingType.CONCURRENT,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    String mockSrcVertexId2 = "Vertex2";
    int srcVertex2Parallelism = 3;
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeManagerPluginDescriptor.create(SilentEdgeManager.class.getName()),
        EdgeProperty.DataSourceType.EPHEMERAL,
        EdgeProperty.SchedulingType.CONCURRENT,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    String mockManagedVertexId = "Vertex";
    int vertexParallelism = 2;

    VertexManagerWithConcurrentInput.ConcurrentInputVertexManagerConfigBuilder configurer =
        VertexManagerWithConcurrentInput.createConfigBuilder(null);
    VertexManagerPluginDescriptor pluginDesc = configurer.build();

    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(vertexParallelism);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(srcVertex1Parallelism);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(srcVertex2Parallelism);
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);

    VertexManagerWithConcurrentInput manager = new VertexManagerWithConcurrentInput(mockContext);
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());
    manager.initialize();
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());

    // source vertex 1 configured
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    verify(mockContext, times(0)).scheduleTasks(requestCaptor.capture());

    // source vertex 2 configured
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    verify(mockContext, times(0)).scheduleTasks(requestCaptor.capture());

    // then own vertex started
    manager.onVertexStarted(Collections.singletonList(
        TestShuffleVertexManager.createTaskAttemptIdentifier(mockSrcVertexId1, 0)));
    verify(mockContext, times(1)).scheduleTasks(requestCaptor.capture());
    Assert.assertEquals(0, manager.completedUpstreamTasks);
  }
}

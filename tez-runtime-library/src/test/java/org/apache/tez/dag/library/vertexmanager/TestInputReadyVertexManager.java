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

import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class TestInputReadyVertexManager {
  
  @Captor
  ArgumentCaptor<List<TaskWithLocationHint>> requestCaptor;
  
  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test (timeout=5000)
  public void testBasicScatterGather() {
    HashMap<String, EdgeProperty> mockInputVertices = 
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    
    String mockManagedVertexId = "Vertex";
    
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(3);
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    
    Map<String, List<Integer>> initialCompletions = Maps.newHashMap();
    initialCompletions.put(mockSrcVertexId1, Collections.singletonList(0));
    
    InputReadyVertexManager manager = new InputReadyVertexManager(mockContext);
    manager.initialize();
    manager.onVertexStarted(initialCompletions);
    manager.onSourceTaskCompleted(mockSrcVertexId1, 1);
    verify(mockContext, times(0)).scheduleVertexTasks(anyList());
    manager.onSourceTaskCompleted(mockSrcVertexId1, 2);
    verify(mockContext, times(1)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(2, requestCaptor.getValue().size());
  }
  
  @Test (timeout=5000)
  public void testBasicOneToOne() {
    HashMap<String, EdgeProperty> mockInputVertices = 
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.ONE_TO_ONE,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    
    String mockManagedVertexId = "Vertex";
    Container mockContainer1 = mock(Container.class);
    ContainerId mockCId1 = mock(ContainerId.class);
    when(mockContainer1.getId()).thenReturn(mockCId1);
    Container mockContainer2 = mock(Container.class);
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(3);
    when(mockContext.getTaskContainer(mockSrcVertexId1, 0)).thenReturn(mockContainer1);
    when(mockContext.getTaskContainer(mockSrcVertexId1, 1)).thenReturn(mockContainer2);
    when(mockContext.getTaskContainer(mockSrcVertexId1, 2)).thenReturn(mockContainer1);
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    
    Map<String, List<Integer>> initialCompletions = Maps.newHashMap();
    initialCompletions.put(mockSrcVertexId1, Collections.singletonList(0));
    
    InputReadyVertexManager manager = new InputReadyVertexManager(mockContext);
    manager.initialize();
    manager.onVertexStarted(initialCompletions);
    verify(mockContext, times(1)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(0, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId1, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer());
    manager.onSourceTaskCompleted(mockSrcVertexId1, 1);
    verify(mockContext, times(2)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(1, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId2, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer());
    manager.onSourceTaskCompleted(mockSrcVertexId1, 2);
    verify(mockContext, times(3)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(2, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId1, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer());
  }

  @Test (timeout=5000)
  public void testComplex() {
    HashMap<String, EdgeProperty> mockInputVertices = 
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String mockSrcVertexId2 = "Vertex2";
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.ONE_TO_ONE,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String mockSrcVertexId3 = "Vertex3";
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.ONE_TO_ONE,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    
    String mockManagedVertexId = "Vertex";
    Container mockContainer2 = mock(Container.class);
    ContainerId mockCId2 = mock(ContainerId.class);
    when(mockContainer2.getId()).thenReturn(mockCId2);
    Container mockContainer3 = mock(Container.class);
    ContainerId mockCId3 = mock(ContainerId.class);
    when(mockContainer3.getId()).thenReturn(mockCId3);
    
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(3);
    when(mockContext.getTaskContainer(mockSrcVertexId2, 0)).thenReturn(mockContainer2);
    when(mockContext.getTaskContainer(mockSrcVertexId2, 1)).thenReturn(mockContainer2);
    when(mockContext.getTaskContainer(mockSrcVertexId2, 2)).thenReturn(mockContainer2);
    when(mockContext.getTaskContainer(mockSrcVertexId3, 0)).thenReturn(mockContainer3);
    when(mockContext.getTaskContainer(mockSrcVertexId3, 1)).thenReturn(mockContainer3);
    when(mockContext.getTaskContainer(mockSrcVertexId3, 2)).thenReturn(mockContainer3);
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);
    mockInputVertices.put(mockSrcVertexId3, eProp3);
    
    Map<String, List<Integer>> initialCompletions = Maps.newHashMap();
    
    // 1-1 sources do not match managed tasks
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);
    InputReadyVertexManager manager = new InputReadyVertexManager(mockContext);
    manager.initialize();
    try {
      manager.onVertexStarted(initialCompletions);
      Assert.assertTrue("Should have exception", false);
    } catch (TezUncheckedException e) {
      e.getMessage().contains("Managed task number must equal 1-1 source");
    }
    
    // 1-1 sources do not match
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(4);
    manager = new InputReadyVertexManager(mockContext);
    manager.initialize();
    try {
      manager.onVertexStarted(initialCompletions);
      Assert.assertTrue("Should have exception", false);
    } catch (TezUncheckedException e) {
      e.getMessage().contains("1-1 source vertices must have identical concurrency");
    }
    
    initialCompletions.put(mockSrcVertexId1, Collections.singletonList(0));
    initialCompletions.put(mockSrcVertexId2, Collections.singletonList(0));
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(3);
    manager = new InputReadyVertexManager(mockContext);
    manager.initialize();
    manager.onVertexStarted(initialCompletions);
    // all 1-1 0's done but not scheduled because v1 is not done
    manager.onSourceTaskCompleted(mockSrcVertexId3, 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, 1);
    manager.onSourceTaskCompleted(mockSrcVertexId1, 1); // duplicate
    manager.onSourceTaskCompleted(mockSrcVertexId2, 1);
    verify(mockContext, times(0)).scheduleVertexTasks(anyList());
    manager.onSourceTaskCompleted(mockSrcVertexId1, 2); // v1 done
    verify(mockContext, times(1)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(0, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId3, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer()); // affinity to last completion
    // 1-1 completion triggers since other 1-1 is done
    manager.onSourceTaskCompleted(mockSrcVertexId3, 1);
    verify(mockContext, times(2)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(1, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId3, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer()); // affinity to last completion
    // 1-1 completion does not trigger since other 1-1 is not done
    manager.onSourceTaskCompleted(mockSrcVertexId3, 2);
    verify(mockContext, times(2)).scheduleVertexTasks(anyList());
    // 1-1 completion trigger start
    manager.onSourceTaskCompleted(mockSrcVertexId2, 2);
    verify(mockContext, times(3)).scheduleVertexTasks(requestCaptor.capture());
    Assert.assertEquals(1, requestCaptor.getValue().size());
    Assert.assertEquals(2, requestCaptor.getValue().get(0).getTaskIndex().intValue());
    Assert.assertEquals(mockCId2, requestCaptor.getValue().get(0)
        .getTaskLocationHint().getAffinitizedContainer()); // affinity to last completion
    
    // no more starts
    manager.onSourceTaskCompleted(mockSrcVertexId3, 2);
    verify(mockContext, times(3)).scheduleVertexTasks(anyList());
    
  }
}

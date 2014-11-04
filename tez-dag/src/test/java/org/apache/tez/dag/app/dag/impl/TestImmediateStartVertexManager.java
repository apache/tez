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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestImmediateStartVertexManager {
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test (timeout=5000)
  public void testBasic() {
    HashMap<String, EdgeProperty> mockInputVertices = 
        new HashMap<String, EdgeProperty>();
    final String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    final String mockSrcVertexId2 = "Vertex2";
    EdgeProperty eProp2 = EdgeProperty.create(mock(EdgeManagerPluginDescriptor.class),
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    final String mockSrcVertexId3 = "Vertex3";
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    
    final String mockManagedVertexId = "Vertex4";
    
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);
    mockInputVertices.put(mockSrcVertexId3, eProp3);

    final VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(2);
    
    final HashSet<Integer> scheduledTasks = new HashSet<Integer>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          scheduledTasks.clear();
          List<TaskWithLocationHint> tasks = (List<TaskWithLocationHint>)args[0];
          for (TaskWithLocationHint task : tasks) {
            scheduledTasks.add(task.getTaskIndex());
          }
          return null;
      }}).when(mockContext).scheduleVertexTasks(anyList());
    
    ImmediateStartVertexManager manager = new ImmediateStartVertexManager(mockContext);
    manager.initialize();
    manager.onVertexStarted(null);
    verify(mockContext, times(0)).scheduleVertexTasks(anyList());
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3,
        VertexState.CONFIGURED));
    verify(mockContext, times(1)).scheduleVertexTasks(anyList());
    Assert.assertEquals(4, scheduledTasks.size());

    // simulate race between onVertexStarted and notifications
    scheduledTasks.clear();
    final ImmediateStartVertexManager raceManager = new ImmediateStartVertexManager(mockContext);
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) throws Exception {
        raceManager.onVertexStateUpdated(new VertexStateUpdate((String)invocation.getArguments()[0],
            VertexState.CONFIGURED));
        scheduledTasks.clear();
        return null;
    }}).when(mockContext).registerForVertexStateUpdates(anyString(), anySet());
    raceManager.initialize();
    raceManager.onVertexStarted(null);
    verify(mockContext, times(2)).scheduleVertexTasks(anyList());
    Assert.assertEquals(4, scheduledTasks.size());
  }
  
}

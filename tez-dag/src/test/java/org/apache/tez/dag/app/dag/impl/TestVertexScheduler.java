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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class TestVertexScheduler {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerAutoParallelism() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        true);
    conf.setLong(TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, 1000L);
    ShuffleVertexManager scheduler = null;
    EventHandler mockEventHandler = mock(EventHandler.class);
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    HashMap<Vertex, Edge> mockInputVertices = 
        new HashMap<Vertex, Edge>();
    Vertex mockSrcVertex1 = mock(Vertex.class);
    TezVertexID mockSrcVertexId1 = TezVertexID.getInstance(dagId, 1);
    EdgeProperty eProp1 = new EdgeProperty(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex1.getVertexId()).thenReturn(mockSrcVertexId1);
    Vertex mockSrcVertex2 = mock(Vertex.class);
    TezVertexID mockSrcVertexId2 = TezVertexID.getInstance(dagId, 2);
    EdgeProperty eProp2 = new EdgeProperty(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex2.getVertexId()).thenReturn(mockSrcVertexId2);
    Vertex mockSrcVertex3 = mock(Vertex.class);
    TezVertexID mockSrcVertexId3 = TezVertexID.getInstance(dagId, 3);
    EdgeProperty eProp3 = new EdgeProperty(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED, 
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex3.getVertexId()).thenReturn(mockSrcVertexId3);
    
    Vertex mockManagedVertex = mock(Vertex.class);
    TezVertexID mockManagedVertexId = TezVertexID.getInstance(dagId, 4);
    when(mockManagedVertex.getVertexId()).thenReturn(mockManagedVertexId);
    when(mockManagedVertex.getInputVertices()).thenReturn(mockInputVertices);
    
    mockInputVertices.put(mockSrcVertex1, new Edge(eProp1, mockEventHandler));
    mockInputVertices.put(mockSrcVertex2, new Edge(eProp2, mockEventHandler));
    mockInputVertices.put(mockSrcVertex3, new Edge(eProp3, mockEventHandler));

    // check initialization
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
    Assert.assertTrue(scheduler.bipartiteSources.size() == 2);
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId2));
    
    final HashMap<TezTaskID, Task> managedTasks = new HashMap<TezTaskID, Task>();
    final TezTaskID mockTaskId1 = TezTaskID.getInstance(mockManagedVertexId, 0);
    managedTasks.put(mockTaskId1, null);
    final TezTaskID mockTaskId2 = TezTaskID.getInstance(mockManagedVertexId, 1);
    managedTasks.put(mockTaskId2, null);
    final TezTaskID mockTaskId3 = TezTaskID.getInstance(mockManagedVertexId, 2);
    managedTasks.put(mockTaskId3, null);
    final TezTaskID mockTaskId4 = TezTaskID.getInstance(mockManagedVertexId, 3);
    managedTasks.put(mockTaskId4, null);
    
    when(mockManagedVertex.getTotalTasks()).thenReturn(managedTasks.size());
    when(mockManagedVertex.getTasks()).thenReturn(managedTasks);
    
    final HashSet<TezTaskID> scheduledTasks = new HashSet<TezTaskID>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          scheduledTasks.clear();
          scheduledTasks.addAll((Collection<TezTaskID>)args[0]); 
          return null;
      }}).when(mockManagedVertex).scheduleTasks(anyCollection());
    
    final Map<Vertex, EdgeManager> newEdgeManagers = new HashMap<Vertex, EdgeManager>();
    
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          managedTasks.remove(mockTaskId3);
          managedTasks.remove(mockTaskId4);
          newEdgeManagers.clear();
          newEdgeManagers.putAll((Map<Vertex, EdgeManager>)invocation.getArguments()[1]);
          return null;
      }}).when(mockManagedVertex).setParallelism(eq(2), anyMap());
    
    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockSrcVertex1.getTotalTasks()).thenReturn(0);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(0);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 4); // all tasks scheduled
    scheduledTasks.clear();
    
    when(mockSrcVertex1.getTotalTasks()).thenReturn(2);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(2);

    TezTaskAttemptID mockSrcAttemptId11 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId1, 0), 0);
    TezTaskAttemptID mockSrcAttemptId12 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId1, 1), 0);
    TezTaskAttemptID mockSrcAttemptId21 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId2, 0), 0);
    TezTaskAttemptID mockSrcAttemptId31 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId3, 0), 0);

    byte[] payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(5000L).build().toByteArray();
    VertexManagerEvent vmEvent = new VertexManagerEvent("Vertex", payload);
    // parallelism not change due to large data size
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 4); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onVertexManagerEventReceived(vmEvent);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    // managedVertex tasks reduced
    verify(mockManagedVertex, times(0)).setParallelism(anyInt(), anyMap());
    Assert.assertEquals(0, scheduler.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted); // TODO
    Assert.assertEquals(5000L, scheduler.completedSourceTasksOutputSize);
    
    
    // parallelism changed due to small data size
    scheduledTasks.clear();
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(500L).build().toByteArray();
    vmEvent = new VertexManagerEvent("Vertex", payload);
    
    scheduler = createScheduler(conf, mockManagedVertex, 0.5f, 0.5f);
    scheduler.onVertexStarted(null);
    Assert.assertEquals(4, scheduler.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, scheduler.numSourceTasks);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31);
    Assert.assertEquals(4, scheduler.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, scheduler.numSourceTasks);
    Assert.assertEquals(0, scheduler.numSourceTasksCompleted);
    scheduler.onVertexManagerEventReceived(vmEvent);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    Assert.assertEquals(4, scheduler.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(1, scheduler.numVertexManagerEventsReceived);
    Assert.assertEquals(500L, scheduler.completedSourceTasksOutputSize);
    // ignore duplicate completion
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    Assert.assertEquals(4, scheduler.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(500L, scheduler.completedSourceTasksOutputSize);
    
    scheduler.onVertexManagerEventReceived(vmEvent);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    // managedVertex tasks reduced
    verify(mockManagedVertex).setParallelism(eq(2), anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    // TODO improve tests for parallelism
    Assert.assertEquals(0, scheduler.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(mockTaskId1));
    Assert.assertTrue(scheduledTasks.contains(mockTaskId2));
    Assert.assertEquals(2, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(2, scheduler.numVertexManagerEventsReceived);
    Assert.assertEquals(1000L, scheduler.completedSourceTasksOutputSize);
    
    // more completions dont cause recalculation of parallelism
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21);
    verify(mockManagedVertex).setParallelism(eq(2), anyMap());
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerSlowStart() {
    Configuration conf = new Configuration();
    ShuffleVertexManager scheduler = null;
    EventHandler mockEventHandler = mock(EventHandler.class);
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    HashMap<Vertex, Edge> mockInputVertices = 
        new HashMap<Vertex, Edge>();
    Vertex mockSrcVertex1 = mock(Vertex.class);
    TezVertexID mockSrcVertexId1 = TezVertexID.getInstance(dagId, 1);
    EdgeProperty eProp1 = new EdgeProperty(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED, 
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex1.getVertexId()).thenReturn(mockSrcVertexId1);
    Vertex mockSrcVertex2 = mock(Vertex.class);
    TezVertexID mockSrcVertexId2 = TezVertexID.getInstance(dagId, 2);
    EdgeProperty eProp2 = new EdgeProperty(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex2.getVertexId()).thenReturn(mockSrcVertexId2);
    Vertex mockSrcVertex3 = mock(Vertex.class);
    TezVertexID mockSrcVertexId3 = TezVertexID.getInstance(dagId, 3);
    EdgeProperty eProp3 = new EdgeProperty(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED, 
        SchedulingType.SEQUENTIAL, 
        new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex3.getVertexId()).thenReturn(mockSrcVertexId3);
    
    Vertex mockManagedVertex = mock(Vertex.class);
    TezVertexID mockManagedVertexId = TezVertexID.getInstance(dagId, 3);
    when(mockManagedVertex.getVertexId()).thenReturn(mockManagedVertexId);
    when(mockManagedVertex.getInputVertices()).thenReturn(mockInputVertices);
    
    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertex3, new Edge(eProp3, mockEventHandler));
    try {
      scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
     Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Atleast 1 bipartite source should exist"));
    }
    
    mockInputVertices.put(mockSrcVertex1, new Edge(eProp1, mockEventHandler));
    mockInputVertices.put(mockSrcVertex2, new Edge(eProp2, mockEventHandler));
    
    // check initialization
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
    Assert.assertTrue(scheduler.bipartiteSources.size() == 2);
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId2));
    
    HashMap<TezTaskID, Task> managedTasks = new HashMap<TezTaskID, Task>();
    TezTaskID mockTaskId1 = TezTaskID.getInstance(mockManagedVertexId, 0);
    managedTasks.put(mockTaskId1, null);
    TezTaskID mockTaskId2 = TezTaskID.getInstance(mockManagedVertexId, 1);
    managedTasks.put(mockTaskId2, null);
    TezTaskID mockTaskId3 = TezTaskID.getInstance(mockManagedVertexId, 2);
    managedTasks.put(mockTaskId3, null);
    
    when(mockManagedVertex.getTotalTasks()).thenReturn(3);
    when(mockManagedVertex.getTasks()).thenReturn(managedTasks);
    
    final HashSet<TezTaskID> scheduledTasks = new HashSet<TezTaskID>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          scheduledTasks.clear();
          scheduledTasks.addAll((Collection<TezTaskID>)args[0]); 
          return null;
      }}).when(mockManagedVertex).scheduleTasks(anyCollection());
    
    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockSrcVertex1.getTotalTasks()).thenReturn(0);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(0);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    when(mockSrcVertex1.getTotalTasks()).thenReturn(2);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(2);

    try {
      // source vertex have some tasks. min < 0.
      scheduler = createScheduler(conf, mockManagedVertex, -0.1f, 0);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinSrcCompletionFraction"));
    }
    
    try {
      // source vertex have some tasks. min > max
      scheduler = createScheduler(conf, mockManagedVertex, 0.5f, 0.3f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinSrcCompletionFraction"));
    }
    
    // source vertex have some tasks. min, max == 0
    scheduler = createScheduler(conf, mockManagedVertex, 0, 0);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.totalTasksToSchedule == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled

    TezTaskAttemptID mockSrcAttemptId11 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId1, 0), 0);
    TezTaskAttemptID mockSrcAttemptId12 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId1, 1), 0);
    TezTaskAttemptID mockSrcAttemptId21 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId2, 0), 0);
    TezTaskAttemptID mockSrcAttemptId22 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId2, 1), 0);
    TezTaskAttemptID mockSrcAttemptId31 = 
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(mockSrcVertexId3, 0), 0);
    
    // min, max > 0 and min == max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 0.25f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    
    // min, max > 0 and min == max == absolute max 1.0
    scheduler = createScheduler(conf, mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);
    
    // min, max > 0 and min == max
    scheduler = createScheduler(conf, mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);
    
    // min, max > and min < max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 0.75f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    // completion of same task again should not get counted
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21);
    Assert.assertTrue(scheduler.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduledTasks.clear();
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22); // we are done. no action
    Assert.assertTrue(scheduler.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);

    // min, max > and min < max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 1.0f);
    scheduler.onVertexStarted(null);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21);
    Assert.assertTrue(scheduler.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22);
    Assert.assertTrue(scheduler.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // no task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);

  }
  
  private ShuffleVertexManager createScheduler(Configuration conf, 
      Vertex vertex, float min, float max) {
    ShuffleVertexManager scheduler = new ShuffleVertexManager(vertex);
    conf.setFloat(TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, min);
    conf.setFloat(TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, max);
    scheduler.initialize(conf);
    return scheduler;
  }
}

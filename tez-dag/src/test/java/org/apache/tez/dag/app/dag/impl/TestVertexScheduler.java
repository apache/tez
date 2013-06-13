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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class TestVertexScheduler {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testBipartiteSlowStartVertexScheduler() {
    BipartiteSlowStartVertexScheduler scheduler = null;
    TezDAGID dagId = new TezDAGID("1", 1, 1);
    HashMap<Vertex, EdgeProperty> mockInputVertices = 
        new HashMap<Vertex, EdgeProperty>();
    Vertex mockSrcVertex1 = mock(Vertex.class);
    TezVertexID mockSrcVertexId1 = new TezVertexID(dagId, 1);
    EdgeProperty eProp1 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out", null),
        new InputDescriptor("in", null));
    when(mockSrcVertex1.getVertexId()).thenReturn(mockSrcVertexId1);
    Vertex mockSrcVertex2 = mock(Vertex.class);
    TezVertexID mockSrcVertexId2 = new TezVertexID(dagId, 2);
    EdgeProperty eProp2 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out", null),
        new InputDescriptor("in", null));
    when(mockSrcVertex2.getVertexId()).thenReturn(mockSrcVertexId2);
    Vertex mockSrcVertex3 = mock(Vertex.class);
    TezVertexID mockSrcVertexId3 = new TezVertexID(dagId, 3);
    EdgeProperty eProp3 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.ONE_TO_ALL,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out", null),
        new InputDescriptor("in", null));
    when(mockSrcVertex3.getVertexId()).thenReturn(mockSrcVertexId3);
    
    Vertex mockManagedVertex = mock(Vertex.class);
    TezVertexID mockManagedVertexId = new TezVertexID(dagId, 3);
    when(mockManagedVertex.getVertexId()).thenReturn(mockManagedVertexId);
    when(mockManagedVertex.getInputVertices()).thenReturn(mockInputVertices);

    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertex3, eProp3);
    try {
     scheduler = new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0, 0);
     Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Atleast 1 bipartite source should exist"));
    }
    
    mockInputVertices.put(mockSrcVertex1, eProp1);
    mockInputVertices.put(mockSrcVertex2, eProp2);
    
    // check initialization
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0.1f, 0.1f);
    Assert.assertTrue(scheduler.bipartiteSources.size() == 2);
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId2));
    
    HashMap<TezTaskID, Task> managedTasks = new HashMap<TezTaskID, Task>();
    TezTaskID mockTaskId1 = new TezTaskID(mockManagedVertexId, 0);
    managedTasks.put(mockTaskId1, null);
    TezTaskID mockTaskId2 = new TezTaskID(mockManagedVertexId, 1);
    managedTasks.put(mockTaskId2, null);
    TezTaskID mockTaskId3 = new TezTaskID(mockManagedVertexId, 2);
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
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    when(mockSrcVertex1.getTotalTasks()).thenReturn(2);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(2);
    
    // source vertex have some tasks. min, max == 0
    scheduler = new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0, 0);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.totalTasksToSchedule == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    TezTaskAttemptID mockSrcAttemptId11 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 0), 0);
    TezTaskAttemptID mockSrcAttemptId12 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 0), 1);
    TezTaskAttemptID mockSrcAttemptId21 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId2, 0), 0);
    TezTaskAttemptID mockSrcAttemptId22 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId2, 0), 1);
    TezTaskAttemptID mockSrcAttemptId31 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId3, 0), 0);
    
    // min, max > 0 and min == max
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0.25f, 0.25f);
    scheduler.onVertexStarted();
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
    
    // min, max > 0 and min == max
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted();
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
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted();
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
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0.25f, 0.75f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11);
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
    scheduler = 
        new BipartiteSlowStartVertexScheduler(mockManagedVertex, 0.25f, 1.0f);
    scheduler.onVertexStarted();
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
}

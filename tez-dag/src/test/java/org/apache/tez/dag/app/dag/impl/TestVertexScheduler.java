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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.engine.records.TezDependentTaskCompletionEvent;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
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
    TezDAGID dagId = new TezDAGID("1", 1, 1);
    HashMap<Vertex, EdgeProperty> mockInputVertices = 
        new HashMap<Vertex, EdgeProperty>();
    Vertex mockSrcVertex1 = mock(Vertex.class);
    TezVertexID mockSrcVertexId1 = new TezVertexID(dagId, 1);
    EdgeProperty eProp1 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex1.getVertexId()).thenReturn(mockSrcVertexId1);
    Vertex mockSrcVertex2 = mock(Vertex.class);
    TezVertexID mockSrcVertexId2 = new TezVertexID(dagId, 2);
    EdgeProperty eProp2 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex2.getVertexId()).thenReturn(mockSrcVertexId2);
    Vertex mockSrcVertex3 = mock(Vertex.class);
    TezVertexID mockSrcVertexId3 = new TezVertexID(dagId, 3);
    EdgeProperty eProp3 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.ONE_TO_ALL,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex3.getVertexId()).thenReturn(mockSrcVertexId3);
    
    Vertex mockManagedVertex = mock(Vertex.class);
    TezVertexID mockManagedVertexId = new TezVertexID(dagId, 4);
    when(mockManagedVertex.getVertexId()).thenReturn(mockManagedVertexId);
    when(mockManagedVertex.getInputVertices()).thenReturn(mockInputVertices);
    
    TezDependentTaskCompletionEvent mockEvent = 
        mock(TezDependentTaskCompletionEvent.class);
    
    mockInputVertices.put(mockSrcVertex1, eProp1);
    mockInputVertices.put(mockSrcVertex2, eProp2);
    mockInputVertices.put(mockSrcVertex3, eProp3);

    // check initialization
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
    Assert.assertTrue(scheduler.bipartiteSources.size() == 2);
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(scheduler.bipartiteSources.containsKey(mockSrcVertexId2));
    
    final HashMap<TezTaskID, Task> managedTasks = new HashMap<TezTaskID, Task>();
    final TezTaskID mockTaskId1 = new TezTaskID(mockManagedVertexId, 0);
    managedTasks.put(mockTaskId1, null);
    final TezTaskID mockTaskId2 = new TezTaskID(mockManagedVertexId, 1);
    managedTasks.put(mockTaskId2, null);
    final TezTaskID mockTaskId3 = new TezTaskID(mockManagedVertexId, 2);
    managedTasks.put(mockTaskId3, null);
    final TezTaskID mockTaskId4 = new TezTaskID(mockManagedVertexId, 3);
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
    
    final List<byte[]> taskPayloads = new ArrayList<byte[]>();
    
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          managedTasks.remove(mockTaskId3);
          managedTasks.remove(mockTaskId4);
          taskPayloads.clear();
          taskPayloads.addAll((List<byte[]>)invocation.getArguments()[1]);
          return null;
      }}).when(mockManagedVertex).setParallelism(eq(2), anyList());
    
    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockSrcVertex1.getTotalTasks()).thenReturn(0);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(0);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 4); // all tasks scheduled
    scheduledTasks.clear();
    
    when(mockSrcVertex1.getTotalTasks()).thenReturn(2);
    when(mockSrcVertex2.getTotalTasks()).thenReturn(2);

    TezTaskAttemptID mockSrcAttemptId11 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 0), 0);
    TezTaskAttemptID mockSrcAttemptId12 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 1), 0);
    TezTaskAttemptID mockSrcAttemptId21 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId2, 0), 0);
    TezTaskAttemptID mockSrcAttemptId31 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId3, 0), 0);

    // parallelism not change due to large data size
    when(mockEvent.getDataSize()).thenReturn(5000L);
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 4); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    // managedVertex tasks reduced
    verify(mockManagedVertex, times(0)).setParallelism(anyInt(), anyList());
    Assert.assertEquals(0, scheduler.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(5000L, scheduler.completedSourceTasksOutputSize);
    
    // parallelism changed due to small data size
    when(mockEvent.getDataSize()).thenReturn(500L);
    scheduledTasks.clear();
    Configuration procConf = new Configuration();
    ProcessorDescriptor procDesc = new ProcessorDescriptor("REDUCE");
    procDesc.setUserPayload(MRHelpers.createUserPayloadFromConf(procConf));
    when(mockManagedVertex.getProcessorDescriptor()).thenReturn(procDesc);
    
    scheduler = createScheduler(conf, mockManagedVertex, 0.5f, 0.5f);
    scheduler.onVertexStarted();
    Assert.assertEquals(4, scheduler.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, scheduler.numSourceTasks);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31, mockEvent);
    Assert.assertEquals(4, scheduler.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, scheduler.numSourceTasks);
    Assert.assertEquals(0, scheduler.numSourceTasksCompleted);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    Assert.assertEquals(4, scheduler.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(500L, scheduler.completedSourceTasksOutputSize);
    // ignore duplicate completion
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    Assert.assertEquals(4, scheduler.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(500L, scheduler.completedSourceTasksOutputSize);
    
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    // managedVertex tasks reduced
    verify(mockManagedVertex).setParallelism(eq(2), anyList());
    Assert.assertEquals(2, taskPayloads.size());
    Assert.assertEquals(0, scheduler.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(mockTaskId1));
    Assert.assertTrue(scheduledTasks.contains(mockTaskId2));
    Assert.assertEquals(2, scheduler.numSourceTasksCompleted);
    Assert.assertEquals(1000L, scheduler.completedSourceTasksOutputSize);
    Configuration taskConf = TezUtils.createConfFromUserPayload(taskPayloads.get(0));
    Assert.assertEquals(2,
        taskConf.getInt(TezJobConfig.TEZ_ENGINE_SHUFFLE_PARTITION_RANGE, 0));
    taskConf = TezUtils.createConfFromUserPayload(taskPayloads.get(1));
    Assert.assertEquals(2,
        taskConf.getInt(TezJobConfig.TEZ_ENGINE_SHUFFLE_PARTITION_RANGE, 0));
    // more completions dont cause recalculation of parallelism
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21, mockEvent);
    verify(mockManagedVertex).setParallelism(eq(2), anyList());
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerSlowStart() {
    Configuration conf = new Configuration();
    ShuffleVertexManager scheduler = null;
    TezDAGID dagId = new TezDAGID("1", 1, 1);
    HashMap<Vertex, EdgeProperty> mockInputVertices = 
        new HashMap<Vertex, EdgeProperty>();
    Vertex mockSrcVertex1 = mock(Vertex.class);
    TezVertexID mockSrcVertexId1 = new TezVertexID(dagId, 1);
    EdgeProperty eProp1 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex1.getVertexId()).thenReturn(mockSrcVertexId1);
    Vertex mockSrcVertex2 = mock(Vertex.class);
    TezVertexID mockSrcVertexId2 = new TezVertexID(dagId, 2);
    EdgeProperty eProp2 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.BIPARTITE,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex2.getVertexId()).thenReturn(mockSrcVertexId2);
    Vertex mockSrcVertex3 = mock(Vertex.class);
    TezVertexID mockSrcVertexId3 = new TezVertexID(dagId, 3);
    EdgeProperty eProp3 = new EdgeProperty(
        EdgeProperty.ConnectionPattern.ONE_TO_ALL,
        EdgeProperty.SourceType.STABLE, new OutputDescriptor("out"),
        new InputDescriptor("in"));
    when(mockSrcVertex3.getVertexId()).thenReturn(mockSrcVertexId3);
    
    Vertex mockManagedVertex = mock(Vertex.class);
    TezVertexID mockManagedVertexId = new TezVertexID(dagId, 3);
    when(mockManagedVertex.getVertexId()).thenReturn(mockManagedVertexId);
    when(mockManagedVertex.getInputVertices()).thenReturn(mockInputVertices);
    
    TezDependentTaskCompletionEvent mockEvent = 
        mock(TezDependentTaskCompletionEvent.class);

    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertex3, eProp3);
    try {
      scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
     Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Atleast 1 bipartite source should exist"));
    }
    
    mockInputVertices.put(mockSrcVertex1, eProp1);
    mockInputVertices.put(mockSrcVertex2, eProp2);
    
    // check initialization
    scheduler = createScheduler(conf, mockManagedVertex, 0.1f, 0.1f);
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
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.totalTasksToSchedule == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled

    TezTaskAttemptID mockSrcAttemptId11 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 0), 0);
    TezTaskAttemptID mockSrcAttemptId12 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId1, 1), 0);
    TezTaskAttemptID mockSrcAttemptId21 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId2, 0), 0);
    TezTaskAttemptID mockSrcAttemptId22 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId2, 1), 0);
    TezTaskAttemptID mockSrcAttemptId31 = 
        new TezTaskAttemptID(new TezTaskID(mockSrcVertexId3, 0), 0);
    
    // min, max > 0 and min == max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 0.25f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    
    // min, max > 0 and min == max == absolute max 1.0
    scheduler = createScheduler(conf, mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);
    
    // min, max > 0 and min == max
    scheduler = createScheduler(conf, mockManagedVertex, 1.0f, 1.0f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    scheduler.onSourceTaskCompleted(mockSrcAttemptId31, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 0);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 1);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 3);
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);
    
    // min, max > and min < max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 0.75f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    // completion of same task again should not get counted
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduledTasks.clear();
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22, mockEvent); // we are done. no action
    Assert.assertTrue(scheduler.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 4);

    // min, max > and min < max
    scheduler = createScheduler(conf, mockManagedVertex, 0.25f, 1.0f);
    scheduler.onVertexStarted();
    Assert.assertTrue(scheduler.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduler.numSourceTasks == 4);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId11, mockEvent);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId12, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 2);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId21, mockEvent);
    Assert.assertTrue(scheduler.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(scheduler.numSourceTasksCompleted == 3);
    scheduler.onSourceTaskCompleted(mockSrcAttemptId22, mockEvent);
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

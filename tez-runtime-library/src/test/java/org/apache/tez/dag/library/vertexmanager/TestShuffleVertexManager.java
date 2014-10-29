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

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestShuffleVertexManager {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerAutoParallelism() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        true);
    conf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, 1000L);
    ShuffleVertexManager manager = null;
    
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
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String mockSrcVertexId3 = "Vertex3";
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

    //Check via setters
    ShuffleVertexManager.ShuffleVertexManagerConfigBuilder configurer = ShuffleVertexManager
        .createConfigBuilder(null);
    VertexManagerPluginDescriptor pluginDesc = configurer.setAutoReduceParallelism(true)
        .setDesiredTaskInputSize(1000l)
        .setMinTaskParallelism(10).setSlowStartMaxSrcCompletionFraction(0.5f).build();
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());


    manager = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
        new Class[]{VertexManagerPluginContext.class}, new Object[]{mockContext});
    manager.initialize();

    Assert.assertTrue(manager.enableAutoParallelism == true);
    Assert.assertTrue(manager.desiredTaskInputDataSize == 1000l);
    Assert.assertTrue(manager.minTaskParallelism == 10);
    Assert.assertTrue(manager.slowStartMinSrcCompletionFraction == 0.25f);
    Assert.assertTrue(manager.slowStartMaxSrcCompletionFraction == 0.5f);

    configurer = ShuffleVertexManager.createConfigBuilder(null);
    pluginDesc = configurer.setAutoReduceParallelism(false).build();
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());

    manager = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
        new Class[]{VertexManagerPluginContext.class}, new Object[]{mockContext});
    manager.initialize();

    Assert.assertTrue(manager.enableAutoParallelism == false);
    Assert.assertTrue(manager.desiredTaskInputDataSize ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    Assert.assertTrue(manager.minTaskParallelism == 1);
    Assert.assertTrue(manager.slowStartMinSrcCompletionFraction ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    Assert.assertTrue(manager.slowStartMaxSrcCompletionFraction ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);


    // check initialization
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    Assert.assertTrue(manager.bipartiteSources == 2);

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
    
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
        new HashMap<String, EdgeManagerPlugin>();

    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) throws Exception {
          when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(2);
          newEdgeManagers.clear();
          for (Entry<String, EdgeManagerPluginDescriptor> entry :
              ((Map<String, EdgeManagerPluginDescriptor>)invocation.getArguments()[2]).entrySet()) {


            final UserPayload userPayload = entry.getValue().getUserPayload();
            EdgeManagerPluginContext emContext = new EdgeManagerPluginContext() {
              @Override
              public UserPayload getUserPayload() {
                return userPayload == null ? null : userPayload;
              }

              @Override
              public String getSourceVertexName() {
                return null;
              }

              @Override
              public String getDestinationVertexName() {
                return null;
              }

              @Override
              public int getSourceVertexNumTasks() {
                return 2;
              }

              @Override
              public int getDestinationVertexNumTasks() {
                return 2;
              }
            };
            EdgeManagerPlugin edgeManager = ReflectionUtils
                .createClazzInstance(entry.getValue().getClassName(),
                    new Class[]{EdgeManagerPluginContext.class}, new Object[]{emContext});
            edgeManager.initialize();
            newEdgeManagers.put(entry.getKey(), edgeManager);
          }
          return null;
      }}).when(mockContext).setVertexParallelism(eq(2), any(VertexLocationHint.class), anyMap(), anyMap());
    
    // source vertices have 0 tasks.
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(1);

    manager.onVertexStarted(null);
    Assert.assertFalse(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 0); // no tasks scheduled
    manager.onSourceTaskCompleted(mockSrcVertexId3, 0);
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 4); // all tasks scheduled
    scheduledTasks.clear();
    
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    ByteBuffer payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(5000L).build().toByteString().asReadOnlyByteBuffer();
    VertexManagerEvent vmEvent = VertexManagerEvent.create("Vertex", payload);
    // parallelism not change due to large data size
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 4); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    manager.onVertexManagerEventReceived(vmEvent);

    //1 task of every source vertex needs to be completed.  Until then, we defer scheduling
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    verify(mockContext, times(0)).setVertexParallelism(anyInt(), any(VertexLocationHint.class), anyMap(), anyMap());
    Assert.assertEquals(4, manager.pendingTasks.size()); // no task scheduled

    //1 task of every source vertex needs to be completed.  Until then, we defer scheduling
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    verify(mockContext, times(0)).setVertexParallelism(anyInt(), any(VertexLocationHint.class), anyMap(), anyMap());
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(5000L, manager.completedSourceTasksOutputSize);

    /**
     * Test for TEZ-978
     * Delay determining parallelism until enough data has been received.
     */
    scheduledTasks.clear();
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(1L).build().toByteString().asReadOnlyByteBuffer();
    vmEvent = VertexManagerEvent.create("Vertex", payload);

    //min/max fraction of 0.01/0.75 would ensure that we hit determineParallelism code path on receiving first event itself.
    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(null);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //First task in src1 completed with small payload
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.determineParallelismAndApply() == false);
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(1L, manager.completedSourceTasksOutputSize);

    //Second task in src1 completed with small payload
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    //Still overall data gathered has not reached threshold; So, ensure parallelism can be determined later
    Assert.assertTrue(manager.determineParallelismAndApply() == false);
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(2, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(2L, manager.completedSourceTasksOutputSize);

    //First task in src2 completed (with larger payload) to trigger determining parallelism
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(1200L).build().toByteString()
            .asReadOnlyByteBuffer();
    vmEvent = VertexManagerEvent.create("Vertex", payload);
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertTrue(manager.determineParallelismAndApply()); //ensure parallelism is determined
    verify(mockContext, times(1)).setVertexParallelism(eq(2), any(VertexLocationHint.class),
        anyMap(),
        anyMap());
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    //Need to have 1 task completed from all sources for this vertex
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertEquals(1, manager.pendingTasks.size());
    Assert.assertEquals(1, scheduledTasks.size());
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(3, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(1202L, manager.completedSourceTasksOutputSize);

    //Test for max fraction. Min fraction is just instruction to framework, but honor max fraction
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(40);
    scheduledTasks.clear();
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(100L).build().toByteString()
            .asReadOnlyByteBuffer();
    vmEvent = VertexManagerEvent.create("Vertex", payload);

    //min/max fraction of 0.0/0.2
    manager = createManager(conf, mockContext, 0.0f, 0.2f);
    manager.onVertexStarted(null);
    Assert.assertEquals(40, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(40, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    //send 7 events with payload size as 100
    for(int i=0;i<7;i++) {
      manager.onVertexManagerEventReceived(vmEvent); //small payload
      manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(i));
      //should not change parallelism
      verify(mockContext, times(0)).setVertexParallelism(eq(4), any(VertexLocationHint.class),
          anyMap(),
          anyMap());
    }
    //send 8th event with payload size as 100
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(8));
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    //Since max threshold (40 * 0.2 = 8) is met, vertex manager should determine parallelism
    verify(mockContext, times(1)).setVertexParallelism(eq(4), any(VertexLocationHint.class),
        anyMap(),
        anyMap());

    //reset context for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);

    // parallelism changed due to small data size
    scheduledTasks.clear();
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(500L).build().toByteString().asReadOnlyByteBuffer();
    vmEvent = VertexManagerEvent.create("Vertex", payload);

    manager = createManager(conf, mockContext, 0.5f, 0.5f);
    manager.onVertexStarted(null);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    // ignore duplicate completion
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    // managedVertex tasks reduced
    verify(mockContext, times(2)).setVertexParallelism(eq(2), any(VertexLocationHint.class),
        anyMap(),
        anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    // TODO improve tests for parallelism
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(new Integer(0)));
    Assert.assertTrue(scheduledTasks.contains(new Integer(1)));
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(2, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(1000L, manager.completedSourceTasksOutputSize);
    
    // more completions dont cause recalculation of parallelism
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    verify(mockContext, times(2)).setVertexParallelism(eq(2), any(VertexLocationHint.class),
        anyMap(),
        anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    
    EdgeManagerPlugin edgeManager = newEdgeManagers.values().iterator().next();
    Map<Integer, List<Integer>> targets = Maps.newHashMap();
    DataMovementEvent dmEvent = DataMovementEvent.create(1, ByteBuffer.wrap(new byte[0]));
    // 4 source task outputs - same as original number of partitions
    Assert.assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(0));
    // 4 destination task inputs - 2 source tasks + 2 merged partitions
    Assert.assertEquals(4, edgeManager.getNumDestinationTaskPhysicalInputs(0));
    edgeManager.routeDataMovementEventToDestination(dmEvent, 1, dmEvent.getSourceIndex(), targets);
    Assert.assertEquals(1, targets.size());
    Map.Entry<Integer, List<Integer>> e = targets.entrySet().iterator().next();
    Assert.assertEquals(0, e.getKey().intValue());
    Assert.assertEquals(1, e.getValue().size());
    Assert.assertEquals(3, e.getValue().get(0).intValue());
    targets.clear();
    dmEvent = DataMovementEvent.create(2, ByteBuffer.wrap(new byte[0]));
    edgeManager.routeDataMovementEventToDestination(dmEvent, 0, dmEvent.getSourceIndex(), targets);
    Assert.assertEquals(1, targets.size());
    e = targets.entrySet().iterator().next();
    Assert.assertEquals(1, e.getKey().intValue());
    Assert.assertEquals(1, e.getValue().size());
    Assert.assertEquals(0, e.getValue().get(0).intValue());
    targets.clear();
    edgeManager.routeInputSourceTaskFailedEventToDestination(2, targets);
    Assert.assertEquals(2, targets.size());
    for (Map.Entry<Integer, List<Integer>> entry : targets.entrySet()) {
      Assert.assertTrue(entry.getKey().intValue() == 0 || entry.getKey().intValue() == 1);
      Assert.assertEquals(2, entry.getValue().size());
      Assert.assertEquals(4, entry.getValue().get(0).intValue());
      Assert.assertEquals(5, entry.getValue().get(1).intValue());
    }
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerSlowStart() {
    Configuration conf = new Configuration();
    ShuffleVertexManager manager = null;
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
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String mockSrcVertexId3 = "Vertex3";
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    
    String mockManagedVertexId = "Vertex4";
    
    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    
    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertexId3, eProp3);
    try {
      manager = createManager(conf, mockContext, 0.1f, 0.1f);
      Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Atleast 1 bipartite source should exist"));
    }
    
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);
    
    // check initialization
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    Assert.assertTrue(manager.bipartiteSources == 2);

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
    
    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    try {
      // source vertex have some tasks. min < 0.
      manager = createManager(conf, mockContext, -0.1f, 0);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinSrcCompletionFraction"));
    }
    
    try {
      // source vertex have some tasks. min > max
      manager = createManager(conf, mockContext, 0.5f, 0.3f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinSrcCompletionFraction"));
    }
    
    // source vertex have some tasks. min, max == 0
    manager = createManager(conf, mockContext, 0, 0);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.totalTasksToSchedule == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    //Atleast 1 task should be complete in all sources
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 0.25f, 0.25f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    //1 task has to be completed in every source vertex. Until then, defer scheduling
    Assert.assertFalse(manager.pendingTasks.isEmpty());
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    
    // min, max > 0 and min == max == absolute max 1.0
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    
    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    
    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    // completion of same task again should not get counted
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1)); // we are done. no action
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);

    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);

  }


  /**
   * Tasks should be scheduled only when at least 1 task from each source vertex is complete
   */
  @Test(timeout = 5000)
  public void test_Tez1649_with_scatter_gather_edges() {
    Configuration conf = new Configuration();
    conf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        true);
    conf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, 1000L);
    ShuffleVertexManager manager = null;

    HashMap<String, EdgeProperty> mockInputVertices_R2 = new HashMap<String, EdgeProperty>();
    String r1 = "R1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m2 = "M2";
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m3 = "M3";
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    final String mockManagedVertexId_R2 = "R2";
    mockInputVertices_R2.put(r1, eProp1);
    mockInputVertices_R2.put(m2, eProp2);
    mockInputVertices_R2.put(m3, eProp3);

    final VertexManagerPluginContext mockContext_R2 = mock(VertexManagerPluginContext.class);
    when(mockContext_R2.getInputVertexEdgeProperties()).thenReturn(mockInputVertices_R2);
    when(mockContext_R2.getVertexName()).thenReturn(mockManagedVertexId_R2);
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(r1)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(m2)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(m3)).thenReturn(3);

    final Map<String, EdgeManagerPlugin> edgeManagerR2 =
        new HashMap<String, EdgeManagerPlugin>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) throws Exception {
        when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(2);
        edgeManagerR2.clear();
        for (Entry<String, EdgeManagerPluginDescriptor> entry :
            ((Map<String, EdgeManagerPluginDescriptor>)invocation.getArguments()[2]).entrySet()) {


          final UserPayload userPayload = entry.getValue().getUserPayload();
          EdgeManagerPluginContext emContext = new EdgeManagerPluginContext() {
            @Override
            public UserPayload getUserPayload() {
              return userPayload == null ? null : userPayload;
            }

            @Override
            public String getSourceVertexName() {
              return null;
            }

            @Override
            public String getDestinationVertexName() {
              return null;
            }

            @Override
            public int getSourceVertexNumTasks() {
              return 2;
            }

            @Override
            public int getDestinationVertexNumTasks() {
              return 2;
            }
          };
          EdgeManagerPlugin edgeManager = ReflectionUtils
              .createClazzInstance(entry.getValue().getClassName(),
                  new Class[]{EdgeManagerPluginContext.class}, new Object[]{emContext});
          edgeManager.initialize();
          edgeManagerR2.put(entry.getKey(), edgeManager);
        }
        return null;
      }}).when(mockContext_R2).setVertexParallelism(eq(2), any(VertexLocationHint.class), anyMap(), anyMap());

    ByteBuffer payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(50L).build().toByteString().asReadOnlyByteBuffer();
    VertexManagerEvent vmEvent = VertexManagerEvent.create("Vertex", payload);

    // check initialization
    manager = createManager(conf, mockContext_R2, 0.001f, 0.001f);
    Assert.assertTrue(manager.bipartiteSources == 3);

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
      }}).when(mockContext_R2).scheduleVertexTasks(anyList());

    manager.onVertexStarted(null);
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(9, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 9);

    //Send events for all tasks of m3.
    manager.onSourceTaskCompleted(m3, new Integer(0));
    manager.onSourceTaskCompleted(m3, new Integer(1));
    manager.onSourceTaskCompleted(m3, new Integer(2));

    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 9);

    //Send an event for m2. But still we need to wait for at least 1 event from r1.
    manager.onSourceTaskCompleted(m2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 9);

    //Ensure that setVertexParallelism is not called for R2.
    verify(mockContext_R2, times(0)).setVertexParallelism(anyInt(), any(VertexLocationHint.class),
        anyMap(),
        anyMap());

    //1 Task completes in R1.  Now things in R2 should be able to proceed.
    manager.onSourceTaskCompleted(r1, new Integer(0));
    verify(mockContext_R2, times(1)).setVertexParallelism(eq(1), any(VertexLocationHint.class),
        anyMap(),
        anyMap());
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //try with zero task vertices
    scheduledTasks.clear();
    when(mockContext_R2.getInputVertexEdgeProperties()).thenReturn(mockInputVertices_R2);
    when(mockContext_R2.getVertexName()).thenReturn(mockManagedVertexId_R2);
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(r1)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m2)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m3)).thenReturn(3);

    manager = createManager(conf, mockContext_R2, 0.001f, 0.001f);
    manager.onVertexStarted(null);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send events for all tasks of m3.
    manager.onSourceTaskCompleted(m3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);
  }

  @Test(timeout = 5000)
  public void test_Tez1649_with_mixed_edges() {
    Configuration conf = new Configuration();
    conf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        true);
    conf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, 1000L);
    ShuffleVertexManager manager = null;

    HashMap<String, EdgeProperty> mockInputVertices =
        new HashMap<String, EdgeProperty>();
    String r1 = "R1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m2 = "M2";
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m3 = "M3";
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    final String mockManagedVertexId = "R2";

    mockInputVertices.put(r1, eProp1);
    mockInputVertices.put(m2, eProp2);
    mockInputVertices.put(m3, eProp3);

    VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m2)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3);

    // check initialization
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    Assert.assertTrue(manager.bipartiteSources == 1);

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

    manager.onVertexStarted(null);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send events for 2 tasks of r1.
    manager.onSourceTaskCompleted(r1, new Integer(0));
    manager.onSourceTaskCompleted(r1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send an event for m2.
    manager.onSourceTaskCompleted(m2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send an event for m2.
    manager.onSourceTaskCompleted(m3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //Scenario when numBipartiteSourceTasksCompleted == totalNumBipartiteSourceTasks.
    //Still, wait for a task to be completed from other edges
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(null);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m2)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    manager.onSourceTaskCompleted(r1, new Integer(0));
    manager.onSourceTaskCompleted(r1, new Integer(1));
    manager.onSourceTaskCompleted(r1, new Integer(2));
    //Tasks from non-scatter edges of m2 and m3 are not complete.
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    manager.onSourceTaskCompleted(m2, new Integer(0));
    manager.onSourceTaskCompleted(m3, new Integer(0));
    //Got an event from other edges. Schedule all
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);


    //try with a zero task vertex (with non-scatter-gather edges)
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(null);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3); //scatter gather
    when(mockContext.getVertexNumTasks(m2)).thenReturn(0); //broadcast
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3); //broadcast

    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(null);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send 2 events for tasks of r1.
    manager.onSourceTaskCompleted(r1, new Integer(0));
    manager.onSourceTaskCompleted(r1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 0);

    manager.onSourceTaskCompleted(m3, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //try with all zero task vertices in non-SG edges
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(null);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3); //scatter gather
    when(mockContext.getVertexNumTasks(m2)).thenReturn(0); //broadcast
    when(mockContext.getVertexNumTasks(m3)).thenReturn(0); //broadcast

    //Send 1 events for tasks of r1.
    manager.onSourceTaskCompleted(r1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);
  }

  private ShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context, float min, float max) {
    conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, min);
    conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, max);
    UserPayload payload;
    try {
      payload = TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    when(context.getUserPayload()).thenReturn(payload);
    ShuffleVertexManager manager = new ShuffleVertexManager(context);
    manager.initialize();
    return manager;
  }
}

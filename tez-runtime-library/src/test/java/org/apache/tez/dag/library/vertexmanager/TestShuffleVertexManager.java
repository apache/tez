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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
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

import com.google.common.collect.Maps;

import static org.mockito.Mockito.*;

public class TestShuffleVertexManager {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 5000)
  public void testShuffleVertexManagerAutoParallelism() throws IOException {
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
    Assert.assertTrue(manager.bipartiteSources.size() == 2);
    Assert.assertTrue(manager.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(manager.bipartiteSources.containsKey(mockSrcVertexId2));
    
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
      public Object answer(InvocationOnMock invocation) {
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
                return 0;
              }

              @Override
              public int getDestinationVertexNumTasks() {
                return 0;
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
    
    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(1);

    manager.onVertexStarted(null);
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
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    // managedVertex tasks reduced
    verify(mockContext, times(0)).setVertexParallelism(anyInt(), any(VertexLocationHint.class), anyMap(), anyMap());
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    Assert.assertEquals(1, manager.numSourceTasksCompleted);
    Assert.assertEquals(5000L, manager.completedSourceTasksOutputSize);
    
    
    // parallelism changed due to small data size
    scheduledTasks.clear();
    payload =
        VertexManagerEventPayloadProto.newBuilder().setOutputSize(500L).build().toByteString().asReadOnlyByteBuffer();
    vmEvent = VertexManagerEvent.create("Vertex", payload);
    
    manager = createManager(conf, mockContext, 0.5f, 0.5f);
    manager.onVertexStarted(null);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumSourceTasks);
    Assert.assertEquals(0, manager.numSourceTasksCompleted);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    // ignore duplicate completion
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numSourceTasksCompleted);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    // managedVertex tasks reduced
    verify(mockContext).setVertexParallelism(eq(2), any(VertexLocationHint.class), anyMap(), anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    // TODO improve tests for parallelism
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(new Integer(0)));
    Assert.assertTrue(scheduledTasks.contains(new Integer(1)));
    Assert.assertEquals(2, manager.numSourceTasksCompleted);
    Assert.assertEquals(2, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(1000L, manager.completedSourceTasksOutputSize);
    
    // more completions dont cause recalculation of parallelism
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    verify(mockContext).setVertexParallelism(eq(2), any(VertexLocationHint.class), anyMap(), anyMap());
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
    Assert.assertTrue(manager.bipartiteSources.size() == 2);
    Assert.assertTrue(manager.bipartiteSources.containsKey(mockSrcVertexId1));
    Assert.assertTrue(manager.bipartiteSources.containsKey(mockSrcVertexId2));
        
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
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    Assert.assertTrue(manager.totalTasksToSchedule == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 0);
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    
    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 0.25f, 0.25f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    Assert.assertTrue(manager.numSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 1);
    
    // min, max > 0 and min == max == absolute max 1.0
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    Assert.assertTrue(manager.numSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 4);
    
    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(mockSrcVertexId3, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    Assert.assertTrue(manager.numSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 4);
    
    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 2);
    // completion of same task again should not get counted
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 tasks scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 3);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1)); // we are done. no action
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 4);

    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 1.0f);
    manager.onVertexStarted(null);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumSourceTasks == 4);
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(0));
    manager.onSourceTaskCompleted(mockSrcVertexId1, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(0));
    Assert.assertTrue(manager.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(mockSrcVertexId2, new Integer(1));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // no task scheduled
    Assert.assertTrue(manager.numSourceTasksCompleted == 4);

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

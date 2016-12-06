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

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestShuffleVertexManager extends TestShuffleVertexManagerUtils {

  List<TaskAttemptIdentifier> emptyCompletions = null;

  @Test(timeout = 5000)
  public void testLargeDataSize() throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
        new HashMap<String, EdgeManagerPlugin>();
    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        mockSrcVertexId1, 2, mockSrcVertexId2, 2, mockSrcVertexId3, 2,
        mockManagedVertexId, 4, scheduledTasks, newEdgeManagers);

    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 5000L, mockSrcVertexId1);
    // parallelism not change due to large data size
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez notified of reconfig
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.pendingTasks.size() == 4); // no tasks scheduled
    manager.onVertexManagerEventReceived(vmEvent);

    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    verify(mockContext, times(0)).reconfigureVertex(anyInt(), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(0)).doneReconfiguringVertex();
    // trigger scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    verify(mockContext, times(0)).reconfigureVertex(anyInt(), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).doneReconfiguringVertex(); // reconfig done
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    // TODO TEZ-1714 locking verify(mockContext, times(2)).vertexManagerDone(); // notified after scheduling all tasks
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(5000L, manager.completedSourceTasksOutputSize);
    scheduledTasks.clear();

    // Ensure long overflow doesn't reduce mistakenly
    // Overflow can occur previously when output size * num tasks for a single vertex would over flow max long
    //
    manager = createManager(conf, mockContext, true, (long)(Long.MAX_VALUE / 1.5), 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    // First source 1 task completes
    vmEvent = getVertexManagerEvent(null, 0L, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(0L, manager.completedSourceTasksOutputSize);
    // Second source 1 task completes
    vmEvent = getVertexManagerEvent(null, 0L, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(0L, manager.completedSourceTasksOutputSize);
    // First source 2 task completes
    vmEvent = getVertexManagerEvent(null, Long.MAX_VALUE >> 1 , mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(Long.MAX_VALUE >> 1, manager.completedSourceTasksOutputSize);
    // Second source 2 task completes
    vmEvent = getVertexManagerEvent(null, Long.MAX_VALUE >> 1 , mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    // Auto-reduce is triggered
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(new Integer(0)));
    Assert.assertTrue(scheduledTasks.contains(new Integer(1)));
    Assert.assertEquals(4, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(4, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(Long.MAX_VALUE >> 1 << 1, manager.completedSourceTasksOutputSize);

    //reset context for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);

    // parallelism changed due to small data size
    scheduledTasks.clear();
  }

  @Test(timeout = 5000)
  public void testAutoParallelismConfig() throws Exception {
    ShuffleVertexManager manager;

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        "Vertex1", 2, "Vertex2", 2, "Vertex3", 2,
        "Vertex4", 4, scheduledTasks, null);

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
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez notified of reconfig

    Assert.assertTrue(manager.config.isAutoParallelismEnabled());
    Assert.assertTrue(manager.config.getDesiredTaskInputDataSize() == 1000l);
    Assert.assertTrue(manager.mgrConfig.getMinTaskParallelism() == 10);
    Assert.assertTrue(manager.config.getMinFraction() == 0.25f);
    Assert.assertTrue(manager.config.getMaxFraction() == 0.5f);

    configurer = ShuffleVertexManager.createConfigBuilder(null);
    pluginDesc = configurer.setAutoReduceParallelism(false).build();
    when(mockContext.getUserPayload()).thenReturn(pluginDesc.getUserPayload());

    manager = ReflectionUtils.createClazzInstance(pluginDesc.getClassName(),
        new Class[]{VertexManagerPluginContext.class}, new Object[]{mockContext});
    manager.initialize();
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez not notified of reconfig

    Assert.assertTrue(!manager.config.isAutoParallelismEnabled());
    Assert.assertTrue(manager.config.getDesiredTaskInputDataSize() ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    Assert.assertTrue(manager.mgrConfig.getMinTaskParallelism() == 1);
    Assert.assertTrue(manager.config.getMinFraction() ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    Assert.assertTrue(manager.config.getMaxFraction() ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);
  }

  @Test(timeout = 5000)
  public void testSchedulingWithPartitionStats() throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    HashMap<String, EdgeProperty> mockInputVertices = new HashMap<String, EdgeProperty>();
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

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    // check initialization
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 1);

    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));

    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send an event for r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Tasks should be scheduled in task 2, 0, 1 order
    long[] sizes = new long[]{(100 * 1000l * 1000l), (0l), (5000 * 1000l * 1000l)};
    VertexManagerEvent vmEvent = getVertexManagerEvent(sizes, 1060000000, r1);
    manager.onVertexManagerEventReceived(vmEvent); //send VM event

    //stats from another vertex (more of empty stats)
    sizes = new long[]{(0l), (0l), (0l)};
    vmEvent = getVertexManagerEvent(sizes, 1060000000, r1);
    manager.onVertexManagerEventReceived(vmEvent); //send VM event

    //Send an event for m2.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send an event for m3.
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //Order of scheduling should be 2,0,1 based on the available partition statistics
    Assert.assertTrue(scheduledTasks.get(0) == 2);
    Assert.assertTrue(scheduledTasks.get(1) == 0);
    Assert.assertTrue(scheduledTasks.get(2) == 1);
  }


  private static ShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context, Float min, Float max) {
    return createManager(conf, context, true, 1000l, min, max);
  }

  private static ShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context,
      Boolean enableAutoParallelism, Long desiredTaskInputSize, Float min,
      Float max) {
    return (ShuffleVertexManager)TestShuffleVertexManagerBase.createManager(
        ShuffleVertexManager.class, conf, context,
        enableAutoParallelism, desiredTaskInputSize, min, max);
  }
}

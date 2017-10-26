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
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.library.vertexmanager.FairShuffleVertexManager.FairRoutingType;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class TestFairShuffleVertexManager
    extends TestShuffleVertexManagerUtils {
  List<TaskAttemptIdentifier> emptyCompletions = null;

  @Test(timeout = 5000)
  public void testAutoParallelismConfig() throws Exception {
    FairShuffleVertexManager manager;

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        "Vertex1", 2, "Vertex2", 2, "Vertex3", 2,
            "Vertex4", 4, scheduledTasks, null);

    manager = createManager(null, mockContext, null, 0.5f);
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez notified of reconfig
    Assert.assertTrue(manager.config.isAutoParallelismEnabled());
    Assert.assertTrue(manager.config.getDesiredTaskInputDataSize() == 1000l * MB);
    Assert.assertTrue(manager.config.getMinFraction() == 0.25f);
    Assert.assertTrue(manager.config.getMaxFraction() == 0.5f);

    manager = createManager(null, mockContext, null, null, null, null);
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez not notified of reconfig

    Assert.assertTrue(!manager.config.isAutoParallelismEnabled());
    Assert.assertTrue(manager.config.getDesiredTaskInputDataSize() ==
        FairShuffleVertexManager.TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    Assert.assertTrue(manager.config.getMinFraction() ==
        FairShuffleVertexManager.TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    Assert.assertTrue(manager.config.getMaxFraction() ==
        FairShuffleVertexManager.TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);
  }

  @Test(timeout = 5000)
  public void testInvalidSetup() {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        "Vertex1", 2, "Vertex2", 2, "Vertex3", 2,
        "Vertex4", 4, scheduledTasks, null);

    // fail if there are more than one bipartite for FAIR_PARALLELISM
    try {
      manager = createFairShuffleVertexManager(conf, mockContext,
          FairRoutingType.FAIR_PARALLELISM, 1000 * MB, 0.001f, 0.001f);
      manager.onVertexStarted(emptyCompletions);
      Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Having more than one destination task process same partition(s) " +
              "only works with one bipartite source."));
    }
  }

  @Test(timeout = 5000)
  public void testReduceSchedulingWithPartitionStats() throws Exception {
    final int numScatherAndGatherSourceTasks = 300;
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
        new HashMap<String, EdgeManagerPlugin>();
    long[] partitionStats = new long[]{(MB), (2 * MB), (5 * MB)};
    testSchedulingWithPartitionStats(
        FairRoutingType.REDUCE_PARALLELISM, numScatherAndGatherSourceTasks,
        partitionStats, 2,2, 2, newEdgeManagers);
    EdgeManagerPluginOnDemand edgeManager =
        (EdgeManagerPluginOnDemand)newEdgeManagers.values().iterator().next();

    // The first destination task fetches two partitions from all source tasks.
    // Thus the # of inputs == # of source tasks * 2 merged partitions
    Assert.assertEquals(numScatherAndGatherSourceTasks * 2,
        edgeManager.getNumDestinationTaskPhysicalInputs(0));
    for (int sourceTaskIndex = 0;
        sourceTaskIndex < numScatherAndGatherSourceTasks; sourceTaskIndex++) {
      for (int j = 0; j < 2; j++) {
        if (j == 0) {
          EdgeManagerPluginOnDemand.CompositeEventRouteMetadata routeMetadata =
              edgeManager.routeCompositeDataMovementEventToDestination(sourceTaskIndex, 0);
          Assert.assertEquals(2, routeMetadata.getCount());
          Assert.assertEquals(0, routeMetadata.getSource());
          Assert.assertEquals(sourceTaskIndex*2, routeMetadata.getTarget());
        } else {
          EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
              edgeManager.routeInputSourceTaskFailedEventToDestination(sourceTaskIndex, 0);
          Assert.assertEquals(2, routeMetadata.getNumEvents());
          Assert.assertArrayEquals(
              new int[]{0 + sourceTaskIndex * 2, 1 + sourceTaskIndex * 2},
              routeMetadata.getTargetIndices());
        }
      }
    }
  }

  @Test(timeout = 5000)
  public void testFairSchedulingWithPartitionStats() throws Exception {
    final int numScatherAndGatherSourceTasks = 300;
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
        new HashMap<String, EdgeManagerPlugin>();
    long[] partitionStats = new long[]{(MB), (2 * MB), (5 * MB)};

    testSchedulingWithPartitionStats(
        FairRoutingType.FAIR_PARALLELISM,
        numScatherAndGatherSourceTasks, partitionStats,
        2, 3, 2, newEdgeManagers);

    // Get the first edgeManager which is SCATTER_GATHER.
    EdgeManagerPluginOnDemand edgeManager =
        (EdgeManagerPluginOnDemand)newEdgeManagers.values().iterator().next();

    // The first destination task fetches two partitions from all source tasks.
    // Thus the # of inputs == # of source tasks * 2 merged partitions
    Assert.assertEquals(numScatherAndGatherSourceTasks * 2,
        edgeManager.getNumDestinationTaskPhysicalInputs(0));
    for (int sourceTaskIndex = 0; sourceTaskIndex < numScatherAndGatherSourceTasks;
        sourceTaskIndex++) {
      for (int j = 0; j < 2; j++) {
        if (j == 0) {
          EdgeManagerPluginOnDemand.CompositeEventRouteMetadata routeMetadata =
              edgeManager.routeCompositeDataMovementEventToDestination(sourceTaskIndex, 0);
          Assert.assertEquals(2, routeMetadata.getCount());
          Assert.assertEquals(0, routeMetadata.getSource());
          Assert.assertEquals(sourceTaskIndex*2, routeMetadata.getTarget());
        } else {
          EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
              edgeManager.routeInputSourceTaskFailedEventToDestination(sourceTaskIndex, 0);
          Assert.assertEquals(2, routeMetadata.getNumEvents());
          Assert.assertArrayEquals(
              new int[]{0 + sourceTaskIndex * 2, 1 + sourceTaskIndex * 2},
              routeMetadata.getTargetIndices());
        }
      }
    }

    // The 2nd destination task fetches one partition from the first half of
    // source tasks.
    Assert.assertEquals(numScatherAndGatherSourceTasks / 2,
        edgeManager.getNumDestinationTaskPhysicalInputs(1));
    for (int j = 0; j < 2; j++) {
      if (j == 0) {
        EdgeManagerPluginOnDemand.CompositeEventRouteMetadata routeMetadata =
            edgeManager.routeCompositeDataMovementEventToDestination(0, 1);
        Assert.assertEquals(1, routeMetadata.getCount());
        Assert.assertEquals(2, routeMetadata.getSource());
        Assert.assertEquals(0, routeMetadata.getTarget());
      } else {
        EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
            edgeManager.routeInputSourceTaskFailedEventToDestination(0, 1);
        Assert.assertEquals(1, routeMetadata.getNumEvents());
        Assert.assertEquals(0, routeMetadata.getTargetIndices()[0]);
      }
    }

    // The 3rd destination task fetches one partition from 2nd half of
    // source tasks.
    Assert.assertEquals(numScatherAndGatherSourceTasks / 2,
        edgeManager.getNumDestinationTaskPhysicalInputs(2));
    for (int sourceTaskIndex = numScatherAndGatherSourceTasks / 2;
        sourceTaskIndex < numScatherAndGatherSourceTasks; sourceTaskIndex++) {
      for (int j = 0; j < 2; j++) {
        if (j == 0) {
          EdgeManagerPluginOnDemand.CompositeEventRouteMetadata routeMetadata =
              edgeManager.routeCompositeDataMovementEventToDestination(sourceTaskIndex, 2);
          Assert.assertEquals(1, routeMetadata.getCount());
          Assert.assertEquals(2, routeMetadata.getSource());
          Assert.assertEquals(
              sourceTaskIndex - numScatherAndGatherSourceTasks / 2,
              routeMetadata.getTarget());
        } else {
          EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
              edgeManager.routeInputSourceTaskFailedEventToDestination(sourceTaskIndex, 2);
          Assert.assertEquals(1, routeMetadata.getNumEvents());
          Assert.assertEquals(sourceTaskIndex - numScatherAndGatherSourceTasks / 2,
              routeMetadata.getTargetIndices()[0]);
        }
      }
    }
  }

  @Test(timeout = 500000)
  public void testOverflow() throws Exception {
    final int numScatherAndGatherSourceTasks = 30000;
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
            new HashMap<String, EdgeManagerPlugin>();
    final int firstPartitionSize = 1;
    final int secondPartitionSize = 2;
    final int thirdPartitionSize = 500;
    long[] partitionStats = new long[]{(firstPartitionSize * MB),
        (secondPartitionSize * MB), (thirdPartitionSize * MB)};
    final int expectedDestinationTasks =
        (firstPartitionSize + secondPartitionSize + thirdPartitionSize)
           * numScatherAndGatherSourceTasks / 1000;

    testSchedulingWithPartitionStats(
        FairRoutingType.FAIR_PARALLELISM,
        numScatherAndGatherSourceTasks, partitionStats, 1000,
        expectedDestinationTasks, 3, newEdgeManagers);
  }

  // Create a DAG with one destination vertexes connected to 3 source vertexes.
  // There are 3 tasks for each vertex. One edge is of type SCATTER_GATHER.
  // The other edges are BROADCAST.
  private void testSchedulingWithPartitionStats(
      FairRoutingType fairRoutingType, int numTasks, long[] partitionStats,
      int numCompletedEvents,
      int expectedScheduledTasks, int expectedNumDestinationConsumerTasks,
      Map<String, EdgeManagerPlugin> newEdgeManagers)
      throws Exception {
    Configuration conf = new Configuration();
    FairShuffleVertexManager manager;

    HashMap<String, EdgeProperty> mockInputVertices = new HashMap<String, EdgeProperty>();
    String r1 = "R1";
    final int numOfTasksInr1 = numTasks;
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m2 = "M2";
    final int numOfTasksInM2 = 3;
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String m3 = "M3";
    final int numOfTasksInM3 = 3;
    EdgeProperty eProp3 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    final String mockManagedVertexId = "R2";
    final int numOfTasksInDestination = 3;

    mockInputVertices.put(r1, eProp1);
    mockInputVertices.put(m2, eProp2);
    mockInputVertices.put(m3, eProp3);

    final VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(numOfTasksInDestination);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(numOfTasksInr1);
    when(mockContext.getVertexNumTasks(m2)).thenReturn(numOfTasksInM2);
    when(mockContext.getVertexNumTasks(m3)).thenReturn(numOfTasksInM3);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    doAnswer(new reconfigVertexAnswer(mockContext, mockManagedVertexId,
        newEdgeManagers)).when(mockContext).reconfigureVertex(
        anyInt(), any(VertexLocationHint.class), anyMap());

    // check initialization
    manager = createFairShuffleVertexManager(conf, mockContext,
        fairRoutingType, 1000 * MB, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 1);

    manager.onVertexStateUpdated(new VertexStateUpdate(r1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2,
        VertexState.CONFIGURED));

    Assert.assertEquals(numOfTasksInDestination,
        manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(numOfTasksInr1,
        manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    Assert.assertTrue(manager.pendingTasks.size() == numOfTasksInDestination); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == numOfTasksInr1);


    for (int i = 0; i < numCompletedEvents; i++) {
      VertexManagerEvent vmEvent = getVertexManagerEvent(partitionStats, 0,
          r1, true);
      manager.onSourceTaskCompleted(vmEvent.getProducerAttemptIdentifier());
      manager.onVertexManagerEventReceived(vmEvent); //send VM event
    }

    //Send an event for m2.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == numOfTasksInDestination); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == numOfTasksInr1);

    //Send an event for m3.
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == expectedScheduledTasks);

    Assert.assertEquals(1, newEdgeManagers.size());
    EdgeManagerPluginOnDemand edgeManager =
        (EdgeManagerPluginOnDemand)newEdgeManagers.values().iterator().next();
    // For each source task, there are 3 outputs,
    // the same as original number of partitions.
    for (int i = 0; i < numOfTasksInr1; i++) {
      Assert.assertEquals(numOfTasksInDestination,
          edgeManager.getNumSourceTaskPhysicalOutputs(0));
    }

    for (int sourceTaskIndex = 0; sourceTaskIndex < numOfTasksInr1;
        sourceTaskIndex++) {
      Assert.assertEquals(expectedNumDestinationConsumerTasks,
          edgeManager.getNumDestinationConsumerTasks(sourceTaskIndex));
    }
  }

  private static FairShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context, Float min, Float max) {
    return createManager(conf, context, true, 1000l * MB, min, max);
  }

  private static FairShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context,
      Boolean enableAutoParallelism, Long desiredTaskInputSize, Float min,
      Float max) {
    return (FairShuffleVertexManager)TestShuffleVertexManagerBase.createManager(
        FairShuffleVertexManager.class, conf, context, enableAutoParallelism,
            desiredTaskInputSize, min, max);
  }
}

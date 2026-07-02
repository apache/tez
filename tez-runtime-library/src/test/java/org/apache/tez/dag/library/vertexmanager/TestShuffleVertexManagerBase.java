/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.dag.library.vertexmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestShuffleVertexManagerBase extends TestShuffleVertexManagerUtils {

  List<TaskAttemptIdentifier> emptyCompletions = null;

  @SuppressWarnings("deprecation")
  public static Stream<Arguments> data() {
    return Stream.of(Arguments.of(ShuffleVertexManager.class), Arguments.of(FairShuffleVertexManager.class));
  }

  // Test zero source tasks and onVertexStarted is called
  // before onVertexStateUpdated.
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testZeroSourceTasksWithVertexStartedFirst(
      Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";
    final List<Integer> scheduledTasks = Lists.newLinkedList();

    // source vertices have 0 tasks.
    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        mockSrcVertexId1, 0, mockSrcVertexId2, 0, mockSrcVertexId3, 1,
        mockManagedVertexId, 4, scheduledTasks, null);
    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig

    manager.onVertexStarted(emptyCompletions);
    verify(mockContext, times(1)).vertexReconfigurationPlanned();
    // The edge between destination and source vertex mockSrcVertexId3 is
    // broadcast type. Thus mockSrcVertexId3 isn't counted as bipartiteSource.
    assertEquals(2, manager.bipartiteSources);

    // check waiting for notification before scheduling
    assertFalse(manager.pendingTasks.isEmpty());
    // source vertices have 0 tasks. triggers scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertTrue(manager.pendingTasks.isEmpty());
    verify(mockContext, times(1)).reconfigureVertex(eq(1), any(), anyMap());
    verify(mockContext, times(1)).doneReconfiguringVertex(); // reconfig done
    assertEquals(1, scheduledTasks.size()); // all tasks scheduled and parallelism changed
    scheduledTasks.clear();
    // TODO TEZ-1714 locking verify(mockContext, times(1)).vertexManagerDone(); // notified after scheduling all tasks
  }

  // Test zero source tasks and onVertexStateUpdated is called
  // before onVertexStarted.
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testZeroSourceTasksWithVertexStateUpdatedFirst(
      Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";
    final List<Integer> scheduledTasks = Lists.newLinkedList();

    // source vertices have 0 tasks.
    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        mockSrcVertexId1, 0, mockSrcVertexId2, 0, mockSrcVertexId3, 1,
        mockManagedVertexId, 4, scheduledTasks, null);
    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig

    verify(mockContext, times(1)).vertexReconfigurationPlanned();
    // source vertices have 0 tasks. so only 1 notification needed. does not trigger scheduling
    // normally this event will not come before onVertexStarted() is called
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    verify(mockContext, times(0)).doneReconfiguringVertex(); // no change. will trigger after start
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    // trigger start and processing of pending notification events
    manager.onVertexStarted(emptyCompletions);
    assertEquals(2, manager.bipartiteSources);
    verify(mockContext, times(1)).reconfigureVertex(eq(1), any(), anyMap());
    verify(mockContext, times(1)).doneReconfiguringVertex(); // reconfig done
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(1, scheduledTasks.size()); // all tasks scheduled and parallelism changed
  }

  // Test vmEvent and vertexStatusUpdate before started.
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testVMEventFirst(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        mockSrcVertexId1, 2, mockSrcVertexId2, 2, mockSrcVertexId3, 2,
        mockManagedVertexId, 4, scheduledTasks, null);
    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 1L, "Vertex");

    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.01f, 0.75f);
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    TezTaskAttemptID taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(0, manager.numVertexManagerEventsReceived); // nothing happens
    manager.onVertexStarted(emptyCompletions); // now the processing happens
    assertEquals(1, manager.numVertexManagerEventsReceived);
  }

  // Test partition stats.
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPartitionStats(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass)
      throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
        mockSrcVertexId1, 2, mockSrcVertexId2, 2, mockSrcVertexId3, 2,
        mockManagedVertexId, 4, scheduledTasks, null);
    //{5,9,12,18} in bitmap
    final long MB = 1024L * 1024L;
    long[] sizes = new long[]{(0L), (1 * MB), (964 * MB), (48 * MB)};
    VertexManagerEvent vmEvent = getVertexManagerEvent(sizes, 0, "Vertex", false);

    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    TezTaskAttemptID taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(1, manager.numVertexManagerEventsReceived);

    assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0)); //0 MB bucket
    assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1)); //1 MB bucket
    assertEquals(100, manager.getCurrentlyKnownStatsAtIndex(2)); //100 MB bucket
    assertEquals(10, manager.getCurrentlyKnownStatsAtIndex(3)); //10 MB bucket

    // sending again from a different version of the same task has not impact
    TezTaskAttemptID taId2 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_1");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId2));
    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(1, manager.numVertexManagerEventsReceived);

    assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0)); //0 MB bucket
    assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1)); //1 MB bucket
    assertEquals(100, manager.getCurrentlyKnownStatsAtIndex(2)); //100 MB bucket
    assertEquals(10, manager.getCurrentlyKnownStatsAtIndex(3)); //10 MB bucket

    // Testing for detailed partition stats
    vmEvent = getVertexManagerEvent(sizes, 0, "Vertex", true);

    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(1, manager.numVertexManagerEventsReceived);

    assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0));
    assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1));
    assertEquals(964, manager.getCurrentlyKnownStatsAtIndex(2));
    assertEquals(48, manager.getCurrentlyKnownStatsAtIndex(3));

    // sending again from a different version of the same task has not impact
    taId2 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_1");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId2));
    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(1, manager.numVertexManagerEventsReceived);

    assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0));
    assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1));
    assertEquals(964, manager.getCurrentlyKnownStatsAtIndex(2));
    assertEquals(48, manager.getCurrentlyKnownStatsAtIndex(3));
  }

  // Delay determining parallelism until enough data has been received.
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testTez978(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
            mockSrcVertexId1, 2, mockSrcVertexId2, 2, mockSrcVertexId3, 2,
            mockManagedVertexId, 4, scheduledTasks, null);

    //min/max fraction of 0.01/0.75 would ensure that we hit determineParallelism code path on receiving first event itself.
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //First task in src1 completed with small payload
    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 1L, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertFalse(manager.determineParallelismAndApply(0f));
    assertEquals(4, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    assertEquals(1, manager.numVertexManagerEventsReceived);
    assertEquals(1L, manager.completedSourceTasksOutputSize);

    //First task in src2 completed with small payload
    vmEvent = getVertexManagerEvent(null, 1L, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    //Still overall data gathered has not reached threshold; So, ensure parallelism can be determined later
    assertFalse(manager.determineParallelismAndApply(0.25f));
    assertEquals(4, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    assertEquals(2, manager.numVertexManagerEventsReceived);
    assertEquals(2L, manager.completedSourceTasksOutputSize);

    //First task in src2 completed (with larger payload) to trigger determining parallelism
    vmEvent = getVertexManagerEvent(null, 160 * MB, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    assertTrue(manager.determineParallelismAndApply(0.25f)); //ensure parallelism is determined
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(), anyMap());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(2, scheduledTasks.size());
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    assertEquals(3, manager.numVertexManagerEventsReceived);
    assertEquals(160 * MB + 2, manager.completedSourceTasksOutputSize);

    //Test for max fraction. Min fraction is just instruction to framework, but honor max fraction
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(40);
    scheduledTasks.clear();

    //min/max fraction of 0.0/0.2
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.0f, 0.2f);
    // initial invocation count == 3
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(40, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(40, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    //send 8 events with payload size as 10MB
    for(int i=0;i<8;i++) {
      //small payload - create new event each time or it will be ignored (from same task)
      manager.onVertexManagerEventReceived(getVertexManagerEvent(null, 10 * MB, mockSrcVertexId1));
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, i));
      //should not change parallelism
      verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    }
    for(int i=0;i<3;i++) {
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, i));
      verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    }
    //Since max threshold (40 * 0.2 = 8) is met, vertex manager should determine parallelism
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 8));
    // parallelism updated
    verify(mockContext, times(2)).reconfigureVertex(anyInt(), any(), anyMap());
    // check exact update value - 8 events with 100 each => 20 -> 2000 => 2 tasks (with 1000 per task)
    verify(mockContext, times(2)).reconfigureVertex(eq(2), any(), anyMap());
  }

  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testAutoParallelism(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass)
      throws Exception {
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

    // parallelism changed due to small data size
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.5f, 0.5f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 50 * MB, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertEquals(4, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    assertEquals(1, manager.numVertexManagerEventsReceived);
    assertEquals(50 * MB, manager.completedSourceTasksOutputSize);
    // ignore duplicate completion
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertEquals(4, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    assertEquals(50 * MB, manager.completedSourceTasksOutputSize);
    vmEvent = getVertexManagerEvent(null, 50 * MB, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);

    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    // managedVertex tasks reduced
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(), anyMap());
    assertEquals(2, newEdgeManagers.size());
    // TODO improve tests for parallelism
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(2, scheduledTasks.size());
    assertTrue(scheduledTasks.contains(0));
    assertTrue(scheduledTasks.contains(1));
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    assertEquals(2, manager.numVertexManagerEventsReceived);
    assertEquals(100 * MB, manager.completedSourceTasksOutputSize);

    // more completions dont cause recalculation of parallelism
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(), anyMap());
    assertEquals(2, newEdgeManagers.size());

    EdgeManagerPluginOnDemand edgeManager =
        (EdgeManagerPluginOnDemand)newEdgeManagers.values().iterator().next();

    // 4 source task outputs - same as original number of partitions
    assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(0));
    // 4 destination task inputs - 2 source tasks * 2 merged partitions
    assertEquals(4, edgeManager.getNumDestinationTaskPhysicalInputs(0));
    EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
        edgeManager.routeDataMovementEventToDestination(1, 1, 0);
    assertEquals(1, routeMetadata.getNumEvents());
    assertEquals(3, routeMetadata.getTargetIndices()[0]);

    routeMetadata = edgeManager.routeDataMovementEventToDestination(0, 2, 1);
    assertEquals(1, routeMetadata.getNumEvents());
    assertEquals(0, routeMetadata.getTargetIndices()[0]);

    routeMetadata = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 0);
    assertEquals(2, routeMetadata.getNumEvents());
    assertEquals(2, routeMetadata.getTargetIndices()[0]);
    assertEquals(3, routeMetadata.getTargetIndices()[1]);

    routeMetadata = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertEquals(2, routeMetadata.getNumEvents());
    assertEquals(2, routeMetadata.getTargetIndices()[0]);
    assertEquals(3, routeMetadata.getTargetIndices()[1]);
  }

  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testShuffleVertexManagerSlowStart(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager = null;
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
    when(mockContext.getVertexStatistics(any())).thenReturn(mock(VertexStatistics.class));
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(1);

    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertexId3, eProp3);
    try {
      manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.1f, 0.1f);
      manager.onVertexStarted(emptyCompletions);
      fail();
    } catch (TezUncheckedException e) {
      assertTrue(e.getMessage().contains(
          "At least 1 bipartite source should exist"));
    }

    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);

    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.1f, 0.1f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(2, manager.bipartiteSources);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    manager.onVertexStarted(emptyCompletions);
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(3, scheduledTasks.size()); // all tasks scheduled

    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    try {
      // source vertex have some tasks. min < 0.
      manager = createManager(shuffleVertexManagerClass, conf, mockContext, -0.1f, 0.0f);
      fail(); // should not come here
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. max > 1.
      manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.0f, 95.0f);
      fail(); // should not come here
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. min > max
      manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.5f, 0.3f);
      fail(); // should not come here
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    // source vertex have some tasks. min > default and max undefined
    int numTasks = 20;
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(numTasks);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(numTasks);
    scheduledTasks.clear();

    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.8f, null);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));

    assertEquals(3, manager.pendingTasks.size());
    assertEquals(numTasks*2, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    float completedTasksThreshold = 0.8f * numTasks;
    // Finish all tasks before exceeding the threshold
    for (String mockSrcVertex : new String[] { mockSrcVertexId1, mockSrcVertexId2 }) {
      for (int i = 0; i < mockContext.getVertexNumTasks(mockSrcVertex); ++i) {
        // complete 0th tasks outside the loop
        manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertex, i+1));
        if ((i + 2) >= completedTasksThreshold) {
          // stop before completing more than min/max source tasks
          break;
        }
      }
    }
    // Since we haven't exceeded the threshold, all tasks are still pending
    assertEquals(manager.totalTasksToSchedule, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no tasks scheduled

    // Cross the threshold min/max threshold to schedule all tasks
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(manager.totalTasksToSchedule, scheduledTasks.size()); // all tasks scheduled

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    // source vertex have some tasks. min, max == 0
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.0f, 0.0f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(3, manager.totalTasksToSchedule);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    // all source vertices need to be configured
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(3, scheduledTasks.size()); // all tasks scheduled

    // min, max > 0 and min == max
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.25f, 0.25f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    // task completion on only 1 SG edge does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(3, scheduledTasks.size()); // all tasks scheduled
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);

    // min, max > 0 and min == max == absolute max 1.0
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(3, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(3, scheduledTasks.size()); // all tasks scheduled
    assertEquals(4, manager.numBipartiteSourceTasksCompleted);

    // min, max > 0 and min == max
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(4, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(3, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    assertTrue(manager.pendingTasks.isEmpty());
    assertEquals(3, scheduledTasks.size()); // all tasks scheduled
    assertEquals(4, manager.numBipartiteSourceTasksCompleted);

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(4);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(4);

    // min, max > and min < max
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(8, manager.totalNumBipartiteSourceTasks);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    // completion of same task again should not get counted
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    assertEquals(3, manager.pendingTasks.size());
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(1, manager.pendingTasks.size());
    assertEquals(2, scheduledTasks.size()); // 2 task scheduled
    assertEquals(4, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(1, scheduledTasks.size()); // 1 tasks scheduled
    assertEquals(6, manager.numBipartiteSourceTasksCompleted);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3)); // we are done. no action
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no task scheduled
    assertEquals(7, manager.numBipartiteSourceTasksCompleted);

    // min, max > and min < max
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.25f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(8, manager.totalNumBipartiteSourceTasks);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(2, manager.pendingTasks.size());
    assertEquals(1, scheduledTasks.size()); // 1 task scheduled
    assertEquals(4, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    assertEquals(1, manager.pendingTasks.size());
    assertEquals(1, scheduledTasks.size()); // 1 task scheduled
    assertEquals(6, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 3));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(1, scheduledTasks.size()); // no task scheduled
    assertEquals(8, manager.numBipartiteSourceTasksCompleted);

    // if there is single task to schedule, it should be schedule when src completed
    // fraction is more than min slow start fraction
    scheduledTasks.clear();
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(1);
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    assertEquals(1, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(8, manager.totalNumBipartiteSourceTasks);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    assertEquals(1, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no task scheduled
    assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(1, scheduledTasks.size()); // 1 task scheduled
    assertEquals(4, manager.numBipartiteSourceTasksCompleted);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no task scheduled
    assertEquals(6, manager.numBipartiteSourceTasksCompleted);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3)); // we are done. no action
    assertEquals(0, manager.pendingTasks.size());
    assertEquals(0, scheduledTasks.size()); // no task scheduled
    assertEquals(7, manager.numBipartiteSourceTasksCompleted);
  }

  /**
   * Tasks should be scheduled only when all source vertices are configured completely
   * @throws IOException
   */
  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void test_Tez1649_with_scatter_gather_edges(
      Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) throws IOException {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager = null;

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

    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 50L, r1);
    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext_R2, 0.001f, 0.001f);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext_R2).scheduleTasks(anyList());

    manager.onVertexStarted(emptyCompletions);
    assertEquals(3, manager.bipartiteSources);
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));

    manager.onVertexManagerEventReceived(vmEvent);
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(6, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(6, manager.totalNumBipartiteSourceTasks);

    //Send events for all tasks of m3.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 2));

    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(6, manager.totalNumBipartiteSourceTasks);

    //Send events for m2. But still we need to wait for at least 1 event from r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 1));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(6, manager.totalNumBipartiteSourceTasks);

    // we need to wait for at least 1 event from r1 to make sure all vertices cross min threshold
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(6, manager.totalNumBipartiteSourceTasks);

    //Ensure that setVertexParallelism is not called for R2.
    verify(mockContext_R2, times(0)).reconfigureVertex(anyInt(), any(), anyMap());

    //ShuffleVertexManager's updatePendingTasks relies on getVertexNumTasks. Setting this for test
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(1);

    // complete configuration of r1 triggers the scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    assertEquals(9, manager.totalNumBipartiteSourceTasks);
    verify(mockContext_R2, times(1)).reconfigureVertex(eq(1), any(), anyMap());

    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(1, scheduledTasks.size());

    //try with zero task vertices
    scheduledTasks.clear();
    when(mockContext_R2.getInputVertexEdgeProperties()).thenReturn(mockInputVertices_R2);
    when(mockContext_R2.getVertexName()).thenReturn(mockManagedVertexId_R2);
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(r1)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m2)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m3)).thenReturn(3);

    manager = createManager(shuffleVertexManagerClass, conf, mockContext_R2, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    // Only need completed configuration notification from m3
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    assertEquals(3, manager.totalNumBipartiteSourceTasks);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(3, scheduledTasks.size());
  }

  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void test_Tez1649_with_mixed_edges(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager = null;

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

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());
    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    assertEquals(1, manager.bipartiteSources);

    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));

    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(3, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send events for 2 tasks of r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(3, manager.totalNumBipartiteSourceTasks);

    //Send an event for m2.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(3, manager.totalNumBipartiteSourceTasks);

    //Send an event for m3.
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(3, scheduledTasks.size());

    //Scenario when numBipartiteSourceTasksCompleted == totalNumBipartiteSourceTasks.
    //Still, wait for a configuration to be completed from other edges
    scheduledTasks.clear();
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));

    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m2)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3);
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(3, manager.totalNumBipartiteSourceTasks);

    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 2));
    //Tasks from non-scatter edges of m2 and m3 are not complete.
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    //Got an event from other edges. Schedule all
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(3, scheduledTasks.size());


    //try with a zero task vertex (with non-scatter-gather edges)
    scheduledTasks.clear();
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3); //scatter gather
    when(mockContext.getVertexNumTasks(m2)).thenReturn(0); //broadcast
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3); //broadcast

    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));

    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(3, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send 2 events for tasks of r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, scheduledTasks.size());

    // event from m3 triggers scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(3, scheduledTasks.size());

    //try with all zero task vertices in non-SG edges
    scheduledTasks.clear();
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3); //scatter gather
    when(mockContext.getVertexNumTasks(m2)).thenReturn(0); //broadcast
    when(mockContext.getVertexNumTasks(m3)).thenReturn(0); //broadcast

    //Send 1 events for tasks of r1.
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    assertEquals(3, scheduledTasks.size());
  }

  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  public void testZeroTasksSendsConfigured(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager = null;

    HashMap<String, EdgeProperty> mockInputVertices = new HashMap<String, EdgeProperty>();
    String r1 = "R1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.SCATTER_GATHER,
        EdgeProperty.DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    final String mockManagedVertexId = "R2";
    mockInputVertices.put(r1, eProp1);

    final VertexManagerPluginContext mockContext = mock(VertexManagerPluginContext.class);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(0);

    // check initialization
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.001f, 0.001f);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    assertEquals(1, manager.bipartiteSources);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    assertEquals(0, manager.totalNumBipartiteSourceTasks);
    assertEquals(0, manager.pendingTasks.size()); // no tasks scheduled
    assertEquals(0, scheduledTasks.size());
    verify(mockContext).doneReconfiguringVertex();
  }


  @ParameterizedTest(name = "test[{0}]")
  @MethodSource("data")
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testTezDrainCompletionsOnVertexStart(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    Configuration conf = new Configuration();
    ShuffleVertexManagerBase manager;

    final String mockSrcVertexId1 = "Vertex1";
    final String mockSrcVertexId2 = "Vertex2";
    final String mockSrcVertexId3 = "Vertex3";
    final String mockManagedVertexId = "Vertex4";

    final List<Integer> scheduledTasks = Lists.newLinkedList();

    final VertexManagerPluginContext mockContext = createVertexManagerContext(
      mockSrcVertexId1, 2, mockSrcVertexId2, 2, mockSrcVertexId3, 2,
      mockManagedVertexId, 4, scheduledTasks, null);

    //min/max fraction of 0.01/0.75 would ensure that we hit determineParallelism code path on receiving first event itself.
    manager = createManager(shuffleVertexManagerClass, conf, mockContext, 0.01f, 0.75f);
    assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    manager.onVertexStarted(Collections.singletonList(
      TestShuffleVertexManager.createTaskAttemptIdentifier(mockSrcVertexId1, 0)));
    assertEquals(1, manager.numBipartiteSourceTasksCompleted);

  }

  private ShuffleVertexManagerBase createManager(Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass,
                                                 Configuration conf, VertexManagerPluginContext context, Float min,
                                                 Float max) {
    return createManager(shuffleVertexManagerClass, conf, context, true, null, min, max);
  }
}

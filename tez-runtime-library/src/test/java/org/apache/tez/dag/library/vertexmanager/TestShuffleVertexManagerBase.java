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
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

@SuppressWarnings({ "unchecked", "rawtypes" })
@RunWith(Parameterized.class)
public class TestShuffleVertexManagerBase extends TestShuffleVertexManagerUtils {

  List<TaskAttemptIdentifier> emptyCompletions = null;
  Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass;

  @SuppressWarnings("deprecation")
  @Parameterized.Parameters(name = "test[{0}]")
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][]{
        {ShuffleVertexManager.class},
        {FairShuffleVertexManager.class}};
    return Arrays.asList(data);
  }

  public TestShuffleVertexManagerBase(
      Class<? extends ShuffleVertexManagerBase> shuffleVertexManagerClass) {
    this.shuffleVertexManagerClass = shuffleVertexManagerClass;
  }

  // Test zero source tasks and onVertexStarted is called
  // before onVertexStateUpdated.
  @Test(timeout = 5000)
  public void testZeroSourceTasksWithVertexStartedFirst() {
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
    manager = createManager(conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig

    manager.onVertexStarted(emptyCompletions);
    verify(mockContext, times(1)).vertexReconfigurationPlanned();
    // The edge between destination and source vertex mockSrcVertexId3 is
    // broadcast type. Thus mockSrcVertexId3 isn't counted as bipartiteSource.
    Assert.assertTrue(manager.bipartiteSources == 2);

    // check waiting for notification before scheduling
    Assert.assertFalse(manager.pendingTasks.isEmpty());
    // source vertices have 0 tasks. triggers scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    verify(mockContext, times(1)).reconfigureVertex(eq(1), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).doneReconfiguringVertex(); // reconfig done
    Assert.assertTrue(scheduledTasks.size() == 1); // all tasks scheduled and parallelism changed
    scheduledTasks.clear();
    // TODO TEZ-1714 locking verify(mockContext, times(1)).vertexManagerDone(); // notified after scheduling all tasks
  }

  // Test zero source tasks and onVertexStateUpdated is called
  // before onVertexStarted.
  @Test(timeout = 5000)
  public void testZeroSourceTasksWithVertexStateUpdatedFirst() {
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
    manager = createManager(conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig

    verify(mockContext, times(1)).vertexReconfigurationPlanned();
    // source vertices have 0 tasks. so only 1 notification needed. does not trigger scheduling
    // normally this event will not come before onVertexStarted() is called
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    verify(mockContext, times(0)).doneReconfiguringVertex(); // no change. will trigger after start
    Assert.assertTrue(scheduledTasks.size() == 0); // no tasks scheduled
    // trigger start and processing of pending notification events
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 2);
    verify(mockContext, times(1)).reconfigureVertex(eq(1), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).doneReconfiguringVertex(); // reconfig done
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 1); // all tasks scheduled and parallelism changed
  }

  // Test vmEvent and vertexStatusUpdate before started.
  @Test(timeout = 5000)
  public void testVMEventFirst() throws IOException {
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

    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    TezTaskAttemptID taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(0, manager.numVertexManagerEventsReceived); // nothing happens
    manager.onVertexStarted(emptyCompletions); // now the processing happens
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
  }

  // Test partition stats.
  @Test(timeout = 5000)
  public void testPartitionStats() throws IOException {
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
    final long MB = 1024l * 1024l;
    long[] sizes = new long[]{(0l), (1 * MB), (964 * MB), (48 * MB)};
    VertexManagerEvent vmEvent = getVertexManagerEvent(sizes, 1L, "Vertex", false);

    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    TezTaskAttemptID taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0)); //0 MB bucket
    Assert.assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1)); //1 MB bucket
    Assert.assertEquals(100, manager.getCurrentlyKnownStatsAtIndex(2)); //100 MB bucket
    Assert.assertEquals(10, manager.getCurrentlyKnownStatsAtIndex(3)); //10 MB bucket

    // sending again from a different version of the same task has not impact
    TezTaskAttemptID taId2 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_1");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId2));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0)); //0 MB bucket
    Assert.assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1)); //1 MB bucket
    Assert.assertEquals(100, manager.getCurrentlyKnownStatsAtIndex(2)); //100 MB bucket
    Assert.assertEquals(10, manager.getCurrentlyKnownStatsAtIndex(3)); //10 MB bucket

    // Testing for detailed partition stats
    vmEvent = getVertexManagerEvent(sizes, 1L, "Vertex", true);

    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0));
    Assert.assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1));
    Assert.assertEquals(964, manager.getCurrentlyKnownStatsAtIndex(2));
    Assert.assertEquals(48, manager.getCurrentlyKnownStatsAtIndex(3));

    // sending again from a different version of the same task has not impact
    taId2 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_1");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId2));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(0, manager.getCurrentlyKnownStatsAtIndex(0));
    Assert.assertEquals(1, manager.getCurrentlyKnownStatsAtIndex(1));
    Assert.assertEquals(964, manager.getCurrentlyKnownStatsAtIndex(2));
    Assert.assertEquals(48, manager.getCurrentlyKnownStatsAtIndex(3));
  }

  // Delay determining parallelism until enough data has been received.
  @Test(timeout = 5000)
  public void testTez978() throws IOException {
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
    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(4, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //First task in src1 completed with small payload
    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 1L, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertTrue(manager.determineParallelismAndApply(0f) == false);
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(1L, manager.completedSourceTasksOutputSize);

    //First task in src2 completed with small payload
    vmEvent = getVertexManagerEvent(null, 1L, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent); //small payload
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    //Still overall data gathered has not reached threshold; So, ensure parallelism can be determined later
    Assert.assertTrue(manager.determineParallelismAndApply(0.25f) == false);
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(2, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(2L, manager.completedSourceTasksOutputSize);

    //First task in src2 completed (with larger payload) to trigger determining parallelism
    vmEvent = getVertexManagerEvent(null, 160 * MB, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertTrue(manager.determineParallelismAndApply(0.25f)); //ensure parallelism is determined
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertEquals(0, manager.pendingTasks.size());
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(3, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(160 * MB + 2, manager.completedSourceTasksOutputSize);

    //Test for max fraction. Min fraction is just instruction to framework, but honor max fraction
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(20);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(40);
    scheduledTasks.clear();

    //min/max fraction of 0.0/0.2
    manager = createManager(conf, mockContext, 0.0f, 0.2f);
    // initial invocation count == 3
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertEquals(40, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(40, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    //send 8 events with payload size as 10MB
    for(int i=0;i<8;i++) {
      //small payload - create new event each time or it will be ignored (from same task)
      manager.onVertexManagerEventReceived(getVertexManagerEvent(null, 10 * MB, mockSrcVertexId1));
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, i));
      //should not change parallelism
      verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    }
    for(int i=0;i<3;i++) {
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, i));
      verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    }
    //Since max threshold (40 * 0.2 = 8) is met, vertex manager should determine parallelism
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 8));
    // parallelism updated
    verify(mockContext, times(2)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    // check exact update value - 8 events with 100 each => 20 -> 2000 => 2 tasks (with 1000 per task)
    verify(mockContext, times(2)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
  }

  @Test(timeout = 5000)
  public void testAutoParallelism() throws Exception {
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
    manager = createManager(conf, mockContext, 0.5f, 0.5f);
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
    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 50 * MB, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(50 * MB, manager.completedSourceTasksOutputSize);
    // ignore duplicate completion
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(50 * MB, manager.completedSourceTasksOutputSize);
    vmEvent = getVertexManagerEvent(null, 50 * MB, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);

    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    // managedVertex tasks reduced
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());
    // TODO improve tests for parallelism
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(2, scheduledTasks.size());
    Assert.assertTrue(scheduledTasks.contains(new Integer(0)));
    Assert.assertTrue(scheduledTasks.contains(new Integer(1)));
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(2, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(100 * MB, manager.completedSourceTasksOutputSize);

    // more completions dont cause recalculation of parallelism
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    verify(mockContext, times(1)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    Assert.assertEquals(2, newEdgeManagers.size());

    EdgeManagerPluginOnDemand edgeManager =
        (EdgeManagerPluginOnDemand)newEdgeManagers.values().iterator().next();

    // 4 source task outputs - same as original number of partitions
    Assert.assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(0));
    // 4 destination task inputs - 2 source tasks * 2 merged partitions
    Assert.assertEquals(4, edgeManager.getNumDestinationTaskPhysicalInputs(0));
    EdgeManagerPluginOnDemand.EventRouteMetadata routeMetadata =
        edgeManager.routeDataMovementEventToDestination(1, 1, 0);
    Assert.assertEquals(1, routeMetadata.getNumEvents());
    Assert.assertEquals(3, routeMetadata.getTargetIndices()[0]);

    routeMetadata = edgeManager.routeDataMovementEventToDestination(0, 2, 1);
    Assert.assertEquals(1, routeMetadata.getNumEvents());
    Assert.assertEquals(0, routeMetadata.getTargetIndices()[0]);

    routeMetadata = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 0);
    Assert.assertEquals(2, routeMetadata.getNumEvents());
    Assert.assertEquals(2, routeMetadata.getTargetIndices()[0]);
    Assert.assertEquals(3, routeMetadata.getTargetIndices()[1]);

    routeMetadata = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    Assert.assertEquals(2, routeMetadata.getNumEvents());
    Assert.assertEquals(2, routeMetadata.getTargetIndices()[0]);
    Assert.assertEquals(3, routeMetadata.getTargetIndices()[1]);
  }

  @Test(timeout = 5000)
  public void testShuffleVertexManagerSlowStart() {
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
    when(mockContext.getVertexStatistics(any(String.class))).thenReturn(mock(VertexStatistics.class));
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(1);

    // fail if there is no bipartite src vertex
    mockInputVertices.put(mockSrcVertexId3, eProp3);
    try {
      manager = createManager(conf, mockContext, 0.1f, 0.1f);
      manager.onVertexStarted(emptyCompletions);
      Assert.assertFalse(true);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "At least 1 bipartite source should exist"));
    }

    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);

    // check initialization
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 2);

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
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled

    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    try {
      // source vertex have some tasks. min < 0.
      manager = createManager(conf, mockContext, -0.1f, 0.0f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. max > 1.
      manager = createManager(conf, mockContext, 0.0f, 95.0f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. min > max
      manager = createManager(conf, mockContext, 0.5f, 0.3f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    // source vertex have some tasks. min > default and max undefined
    int numTasks = 20;
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(numTasks);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(numTasks);
    scheduledTasks.clear();

    manager = createManager(conf, mockContext, 0.8f, null);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));

    Assert.assertEquals(3, manager.pendingTasks.size());
    Assert.assertEquals(numTasks*2, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
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
    Assert.assertEquals(manager.totalTasksToSchedule, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled

    // Cross the threshold min/max threshold to schedule all tasks
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(3, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertEquals(0, manager.pendingTasks.size());
    Assert.assertEquals(manager.totalTasksToSchedule, scheduledTasks.size()); // all tasks scheduled

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    // source vertex have some tasks. min, max == 0
    manager = createManager(conf, mockContext, 0.0f, 0.0f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.totalTasksToSchedule == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    // all source vertices need to be configured
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled

    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 0.25f, 0.25f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    // task completion on only 1 SG edge does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);

    // min, max > 0 and min == max == absolute max 1.0
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);

    // min, max > 0 and min == max
    manager = createManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    // task completion from non-bipartite stage does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 0);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 3);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 3); // all tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(4);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(4);

    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    // completion of same task again should not get counted
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3);
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 tasks scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 6);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3)); // we are done. no action
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 7);

    // min, max > and min < max
    manager = createManager(conf, mockContext, 0.25f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    Assert.assertTrue(manager.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 6);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 3));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 8);

    // if there is single task to schedule, it should be schedule when src completed
    // fraction is more than min slow start fraction
    scheduledTasks.clear();
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(1);
    manager = createManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 1); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 1);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 6);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 3)); // we are done. no action
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 0); // no task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 7);
  }

  /**
   * Tasks should be scheduled only when all source vertices are configured completely
   * @throws IOException
   */
  @Test(timeout = 5000)
  public void test_Tez1649_with_scatter_gather_edges() throws IOException {
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
    manager = createManager(conf, mockContext_R2, 0.001f, 0.001f);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext_R2).scheduleTasks(anyList());

    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 3);
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));

    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(6, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 6);

    //Send events for all tasks of m3.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 2));

    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 6);

    //Send events for m2. But still we need to wait for at least 1 event from r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 6);

    // we need to wait for at least 1 event from r1 to make sure all vertices cross min threshold
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 6);

    //Ensure that setVertexParallelism is not called for R2.
    verify(mockContext_R2, times(0)).reconfigureVertex(anyInt(), any(VertexLocationHint.class),
        anyMap());

    //ShuffleVertexManager's updatePendingTasks relies on getVertexNumTasks. Setting this for test
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(1);

    // complete configuration of r1 triggers the scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 9);
    verify(mockContext_R2, times(1)).reconfigureVertex(eq(1), any(VertexLocationHint.class),
        anyMap());
  
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 1);

    //try with zero task vertices
    scheduledTasks.clear();
    when(mockContext_R2.getInputVertexEdgeProperties()).thenReturn(mockInputVertices_R2);
    when(mockContext_R2.getVertexName()).thenReturn(mockManagedVertexId_R2);
    when(mockContext_R2.getVertexNumTasks(mockManagedVertexId_R2)).thenReturn(3);
    when(mockContext_R2.getVertexNumTasks(r1)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m2)).thenReturn(0);
    when(mockContext_R2.getVertexNumTasks(m3)).thenReturn(3);

    manager = createManager(conf, mockContext_R2, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    // Only need completed configuration notification from m3
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m3, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);
  }

  @Test(timeout = 5000)
  public void test_Tez1649_with_mixed_edges() {
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
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 1);

    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));

    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send events for 2 tasks of r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send an event for m2.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(m2, 0));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    //Send an event for m3.
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //Scenario when numBipartiteSourceTasksCompleted == totalNumBipartiteSourceTasks.
    //Still, wait for a configuration to be completed from other edges
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));

    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m2)).thenReturn(3);
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3);
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 3);

    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 2));
    //Tasks from non-scatter edges of m2 and m3 are not complete.
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    //Got an event from other edges. Schedule all
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);


    //try with a zero task vertex (with non-scatter-gather edges)
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    when(mockContext.getInputVertexEdgeProperties()).thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);
    when(mockContext.getVertexNumTasks(r1)).thenReturn(3); //scatter gather
    when(mockContext.getVertexNumTasks(m2)).thenReturn(0); //broadcast
    when(mockContext.getVertexNumTasks(m3)).thenReturn(3); //broadcast

    manager = createManager(conf, mockContext, 0.001f, 0.001f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));

    Assert.assertEquals(3, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(3, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    //Send 2 events for tasks of r1.
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(r1, 1));
    Assert.assertTrue(manager.pendingTasks.size() == 3); // no tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 0);

    // event from m3 triggers scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(m3, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(m2, VertexState.CONFIGURED));
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);

    //try with all zero task vertices in non-SG edges
    scheduledTasks.clear();
    manager = createManager(conf, mockContext, 0.001f, 0.001f);
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
    Assert.assertTrue(manager.pendingTasks.size() == 0); // all tasks scheduled
    Assert.assertTrue(scheduledTasks.size() == 3);
  }

  @Test
  public void testZeroTasksSendsConfigured() throws IOException {
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
    manager = createManager(conf, mockContext, 0.001f, 0.001f);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(r1, VertexState.CONFIGURED));
    Assert.assertEquals(1, manager.bipartiteSources);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(0, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, scheduledTasks.size());
    verify(mockContext).doneReconfiguringVertex();
  }


  @Test(timeout=5000)
  public void testTezDrainCompletionsOnVertexStart() throws IOException {
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
    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    manager.onVertexStarted(Collections.singletonList(
      TestShuffleVertexManager.createTaskAttemptIdentifier(mockSrcVertexId1, 0)));
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);

  }

  private ShuffleVertexManagerBase createManager(Configuration conf,
      VertexManagerPluginContext context, Float min, Float max) {
    return createManager(this.shuffleVertexManagerClass, conf, context, true,
        null, min, max);
  }
}

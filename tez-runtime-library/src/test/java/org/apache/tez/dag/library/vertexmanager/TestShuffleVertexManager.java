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
import com.google.common.collect.Maps;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezCommonUtils;
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
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TaskAttemptIdentifierImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.VertexIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.RoaringBitmap;

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

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestShuffleVertexManager {

  TezVertexID vertexId = TezVertexID.fromString("vertex_1436907267600_195589_1_00");
  int taskId = 0;
  List<TaskAttemptIdentifier> emptyCompletions = null;

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
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez notified of reconfig

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
    verify(mockContext, times(1)).vertexReconfigurationPlanned(); // Tez not notified of reconfig

    Assert.assertTrue(manager.enableAutoParallelism == false);
    Assert.assertTrue(manager.desiredTaskInputDataSize ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    Assert.assertTrue(manager.minTaskParallelism == 1);
    Assert.assertTrue(manager.slowStartMinSrcCompletionFraction ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    Assert.assertTrue(manager.slowStartMaxSrcCompletionFraction ==
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);


    final HashSet<Integer> scheduledTasks = new HashSet<Integer>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          scheduledTasks.clear();
          List<ScheduleTaskRequest> tasks = (List<ScheduleTaskRequest>)args[0];
          for (ScheduleTaskRequest task : tasks) {
            scheduledTasks.add(task.getTaskIndex());
          }
          return null;
      }}).when(mockContext).scheduleTasks(anyList());
    
    final Map<String, EdgeManagerPlugin> newEdgeManagers =
        new HashMap<String, EdgeManagerPlugin>();

    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) throws Exception {
          final int numTasks = ((Integer)invocation.getArguments()[0]).intValue();
          when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(numTasks);
          newEdgeManagers.clear();
          for (Entry<String, EdgeProperty> entry :
              ((Map<String, EdgeProperty>)invocation.getArguments()[2]).entrySet()) {

            EdgeManagerPluginDescriptor pluginDesc = entry.getValue().getEdgeManagerDescriptor();
            final UserPayload userPayload = pluginDesc.getUserPayload();
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
                return numTasks;
              }
            };
            EdgeManagerPlugin edgeManager = ReflectionUtils
                .createClazzInstance(pluginDesc.getClassName(),
                    new Class[]{EdgeManagerPluginContext.class}, new Object[]{emContext});
            edgeManager.initialize();
            newEdgeManagers.put(entry.getKey(), edgeManager);
          }
          return null;
      }}).when(mockContext).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    
    // check initialization
    manager = createManager(conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig
    // source vertices have 0 tasks.
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId3)).thenReturn(1);

    manager.onVertexStarted(emptyCompletions);
    verify(mockContext, times(2)).vertexReconfigurationPlanned();
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

    // check scheduling only after onVertexStarted
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);
    manager = createManager(conf, mockContext, 0.1f, 0.1f); // Tez notified of reconfig
    verify(mockContext, times(3)).vertexReconfigurationPlanned();
    // source vertices have 0 tasks. so only 1 notification needed. does not trigger scheduling
    // normally this event will not come before onVertexStarted() is called
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    verify(mockContext, times(1)).doneReconfiguringVertex(); // no change. will trigger after start
    Assert.assertTrue(scheduledTasks.size() == 0); // no tasks scheduled
    // trigger start and processing of pending notification events
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 2);
    verify(mockContext, times(2)).reconfigureVertex(eq(1), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(2)).doneReconfiguringVertex(); // reconfig done
    Assert.assertTrue(manager.pendingTasks.isEmpty());
    Assert.assertTrue(scheduledTasks.size() == 1); // all tasks scheduled and parallelism changed

    
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);

    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 5000L, mockSrcVertexId1);
    // parallelism not change due to large data size
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    verify(mockContext, times(4)).vertexReconfigurationPlanned(); // Tez notified of reconfig
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.pendingTasks.size() == 4); // no tasks scheduled
    manager.onVertexManagerEventReceived(vmEvent);

    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    verify(mockContext, times(2)).reconfigureVertex(anyInt(), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(2)).doneReconfiguringVertex();
    // trigger scheduling
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertTrue(manager.totalNumBipartiteSourceTasks == 4);
    verify(mockContext, times(2)).reconfigureVertex(anyInt(), any
        (VertexLocationHint.class), anyMap());
    verify(mockContext, times(3)).doneReconfiguringVertex(); // reconfig done
    Assert.assertEquals(0, manager.pendingTasks.size()); // all tasks scheduled
    Assert.assertEquals(4, scheduledTasks.size());
    // TODO TEZ-1714 locking verify(mockContext, times(2)).vertexManagerDone(); // notified after scheduling all tasks
    Assert.assertEquals(2, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(5000L, manager.completedSourceTasksOutputSize);

    /**
     * Test vmEvent and vertexStatusUpdate before started
     */
    scheduledTasks.clear();
    //{5,9,12,18} in bitmap
    vmEvent = getVertexManagerEvent(null, 1L, "Vertex");

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

    /**
     * Test partition stats
     */
    scheduledTasks.clear();
    //{5,9,12,18} in bitmap
    long[] sizes = new long[]{(0l), (1000l * 1000l),
                              (1010 * 1000l * 1000l), (50 * 1000l * 1000l)};
    vmEvent = getVertexManagerEvent(sizes, 1L, "Vertex");

    manager = createManager(conf, mockContext, 0.01f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertEquals(4, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);

    taId1 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_0");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId1));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(4, manager.stats.length);
    Assert.assertEquals(0, manager.stats[0]); //0 MB bucket
    Assert.assertEquals(1, manager.stats[1]); //1 MB bucket
    Assert.assertEquals(100, manager.stats[2]); //100 MB bucket
    Assert.assertEquals(10, manager.stats[3]); //10 MB bucket

    // sending again from a different version of the same task has not impact
    TezTaskAttemptID taId2 = TezTaskAttemptID.fromString("attempt_1436907267600_195589_1_00_000000_1");
    vmEvent.setProducerAttemptIdentifier(new TaskAttemptIdentifierImpl("dag", mockSrcVertexId1, taId2));
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);

    Assert.assertEquals(4, manager.stats.length);
    Assert.assertEquals(0, manager.stats[0]); //0 MB bucket
    Assert.assertEquals(1, manager.stats[1]); //1 MB bucket
    Assert.assertEquals(100, manager.stats[2]); //100 MB bucket
    Assert.assertEquals(10, manager.stats[3]); //10 MB bucket

    /**
     * Test for TEZ-978
     * Delay determining parallelism until enough data has been received.
     */
    scheduledTasks.clear();

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
    vmEvent = getVertexManagerEvent(null, 1L, mockSrcVertexId1);
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
    vmEvent = getVertexManagerEvent(null, 1200L, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    Assert.assertTrue(manager.determineParallelismAndApply(0.25f)); //ensure parallelism is determined
    verify(mockContext, times(3)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    verify(mockContext, times(1)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
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

    //min/max fraction of 0.0/0.2
    manager = createManager(conf, mockContext, 0.0f, 0.2f);
    // initial invocation count == 3
    verify(mockContext, times(3)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2, VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId3, VertexState.CONFIGURED));
    Assert.assertEquals(40, manager.pendingTasks.size()); // no tasks scheduled
    Assert.assertEquals(40, manager.totalNumBipartiteSourceTasks);
    Assert.assertEquals(0, manager.numBipartiteSourceTasksCompleted);
    //send 7 events with payload size as 100
    for(int i=0;i<8;i++) {
      //small payload - create new event each time or it will be ignored (from same task)
      manager.onVertexManagerEventReceived(getVertexManagerEvent(null, 100L, mockSrcVertexId1));
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, i));
      //should not change parallelism
      verify(mockContext, times(3)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    }
    for(int i=0;i<3;i++) {
      manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, i));
      verify(mockContext, times(3)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    }
    //ShuffleVertexManager's updatePendingTasks relies on getVertexNumTasks. Setting this for test
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);
    //Since max threshold (40 * 0.2 = 8) is met, vertex manager should determine parallelism
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 8));
    // parallelism updated
    verify(mockContext, times(4)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    // check exact update value - 8 events with 100 each => 20 -> 2000 => 2 tasks (with 1000 per task)
    verify(mockContext, times(2)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());

    //reset context for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(4);

    // parallelism changed due to small data size
    scheduledTasks.clear();

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
    vmEvent = getVertexManagerEvent(null, 500L, mockSrcVertexId1);
    manager.onVertexManagerEventReceived(vmEvent);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(1, manager.numVertexManagerEventsReceived);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    // ignore duplicate completion
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 0));
    Assert.assertEquals(4, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled
    Assert.assertEquals(1, manager.numBipartiteSourceTasksCompleted);
    Assert.assertEquals(500L, manager.completedSourceTasksOutputSize);
    vmEvent = getVertexManagerEvent(null, 500L, mockSrcVertexId2);
    manager.onVertexManagerEventReceived(vmEvent);
    //ShuffleVertexManager's updatePendingTasks relies on getVertexNumTasks. Setting this for test
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(2);

    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 1));
    // managedVertex tasks reduced
    verify(mockContext, times(5)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
    verify(mockContext, times(3)).reconfigureVertex(eq(2), any(VertexLocationHint.class), anyMap());
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
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 0));
    verify(mockContext, times(5)).reconfigureVertex(anyInt(), any(VertexLocationHint.class), anyMap());
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
          "Atleast 1 bipartite source should exist"));
    }
    
    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);
    
    // check initialization
    manager = createManager(conf, mockContext, 0.1f, 0.1f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertTrue(manager.bipartiteSources == 2);

    final HashSet<Integer> scheduledTasks = new HashSet<Integer>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          scheduledTasks.clear();
          List<ScheduleTaskRequest> tasks = (List<ScheduleTaskRequest>)args[0];
          for (ScheduleTaskRequest task : tasks) {
            scheduledTasks.add(task.getTaskIndex());
          }
          return null;
      }}).when(mockContext).scheduleTasks(anyList());
    
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
          "Invalid values for slowStartMinSrcCompletionFraction"));
    }

    try {
      // source vertex have some tasks. max > 1.
      manager = createManager(conf, mockContext, 0.0f, 95.0f);
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
    Assert.assertTrue(manager.pendingTasks.size() == 2);
    Assert.assertTrue(scheduledTasks.size() == 1); // 1 task scheduled
    Assert.assertTrue(manager.numBipartiteSourceTasksCompleted == 4);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(mockSrcVertexId2, 2));
    Assert.assertTrue(manager.pendingTasks.size() == 0);
    Assert.assertTrue(scheduledTasks.size() == 2); // 2 tasks scheduled
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

  }


  /**
   * Tasks should be scheduled only when all source vertices are configured completely
   * @throws IOException 
   */
  @Test(timeout = 5000)
  public void test_Tez1649_with_scatter_gather_edges() throws IOException {
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

    VertexManagerEvent vmEvent = getVertexManagerEvent(null, 50L, r1);
    // check initialization
    manager = createManager(conf, mockContext_R2, 0.001f, 0.001f);

    final HashSet<Integer> scheduledTasks = new HashSet<Integer>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        scheduledTasks.clear();
        List<ScheduleTaskRequest> tasks = (List<ScheduleTaskRequest>)args[0];
        for (ScheduleTaskRequest task : tasks) {
          scheduledTasks.add(task.getTaskIndex());
        }
        return null;
      }}).when(mockContext_R2).scheduleTasks(anyList());

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

  VertexManagerEvent getVertexManagerEvent(long[] sizes, long totalSize, String vertexName)
      throws IOException {
    ByteBuffer payload = null;
    if (sizes != null) {
      RoaringBitmap partitionStats = ShuffleUtils.getPartitionStatsForPhysicalOutput(sizes);
      DataOutputBuffer dout = new DataOutputBuffer();
      partitionStats.serialize(dout);
      ByteString
          partitionStatsBytes = TezCommonUtils.compressByteArrayToByteString(dout.getData());
      payload =
          VertexManagerEventPayloadProto.newBuilder()
              .setOutputSize(totalSize)
              .setPartitionStats(partitionStatsBytes)
              .build().toByteString()
              .asReadOnlyByteBuffer();
    } else {
      payload =
          VertexManagerEventPayloadProto.newBuilder()
              .setOutputSize(totalSize)
              .build().toByteString()
              .asReadOnlyByteBuffer();
    }
    TaskAttemptIdentifierImpl taId = new TaskAttemptIdentifierImpl("dag", vertexName,
        TezTaskAttemptID.getInstance(TezTaskID.getInstance(vertexId, taskId++), 0));
    VertexManagerEvent vmEvent = VertexManagerEvent.create(vertexName, payload);
    vmEvent.setProducerAttemptIdentifier(taId);
    return vmEvent;
  }

  @Test(timeout = 5000)
  public void testSchedulingWithPartitionStats() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        true);
    conf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, 1000L);
    ShuffleVertexManager manager = null;

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
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        scheduledTasks.clear();
        List<ScheduleTaskRequest> tasks = (List<ScheduleTaskRequest>)args[0];
        for (ScheduleTaskRequest task : tasks) {
          scheduledTasks.add(task.getTaskIndex());
        }
        return null;
      }}).when(mockContext).scheduleTasks(anyList());

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

    final HashSet<Integer> scheduledTasks = new HashSet<Integer>();
    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        scheduledTasks.clear();
        List<ScheduleTaskRequest> tasks = (List<ScheduleTaskRequest>)args[0];
        for (ScheduleTaskRequest task : tasks) {
          scheduledTasks.add(task.getTaskIndex());
        }
        return null;
      }}).when(mockContext).scheduleTasks(anyList());

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
  
  public static TaskAttemptIdentifier createTaskAttemptIdentifier(String vName, int tId) {
    VertexIdentifier mockVertex = mock(VertexIdentifier.class);
    when(mockVertex.getName()).thenReturn(vName);
    TaskIdentifier mockTask = mock(TaskIdentifier.class);
    when(mockTask.getIdentifier()).thenReturn(tId);
    when(mockTask.getVertexIdentifier()).thenReturn(mockVertex);
    TaskAttemptIdentifier mockAttempt = mock(TaskAttemptIdentifier.class);
    when(mockAttempt.getIdentifier()).thenReturn(0);
    when(mockAttempt.getTaskIdentifier()).thenReturn(mockTask);
    return mockAttempt;
  }

  private ShuffleVertexManager createManager(Configuration conf,
      VertexManagerPluginContext context, Float min, Float max) {
    if (min != null) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, min);
    } else {
      conf.unset(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION);
    }
    if (max != null) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, max);
    } else {
      conf.unset(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION);
    }
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

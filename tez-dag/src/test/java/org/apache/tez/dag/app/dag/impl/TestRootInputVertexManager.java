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

import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START;
import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION;
import static org.apache.tez.dag.app.dag.impl.RootInputVertexManager.TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.VertexIdentifier;
import org.apache.tez.runtime.api.VertexStatistics;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestRootInputVertexManager {

  List<TaskAttemptIdentifier> emptyCompletions = null;

  @Test(timeout = 5000)
  public void testEventsFromMultipleInputs() throws IOException {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    TezConfiguration conf = new TezConfiguration();
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(conf);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(1).when(context).getVertexNumTasks(eq("vertex1"));
    doReturn(vertexPayload).when(context).getUserPayload();

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager(context);
    rootInputVertexManager.initialize();

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputDataInformationEvent diEvent11 = InputDataInformationEvent.createWithSerializedPayload(0,
        null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputDataInformationEvent diEvent21 = InputDataInformationEvent.createWithSerializedPayload(0,
        null);
    events2.add(diEvent21);
    try {
      // Should fail due to second input
      rootInputVertexManager.onRootVertexInitialized("input2", id2, events2);
      fail("Expecting failure in case of multiple inputs attempting to send events");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith(
          "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"));
    }
  }

  @Test(timeout = 5000)
  public void testConfigureFromMultipleInputs() throws IOException {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    TezConfiguration conf = new TezConfiguration();
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(conf);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(-1).when(context).getVertexNumTasks(eq("vertex1"));
    doReturn(vertexPayload).when(context).getUserPayload();

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager(context);
    rootInputVertexManager.initialize();

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputConfigureVertexTasksEvent diEvent11 = InputConfigureVertexTasksEvent.create(1, null,
        null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputConfigureVertexTasksEvent diEvent21 = InputConfigureVertexTasksEvent.create(1, null,
        null);
    events2.add(diEvent21);
    try {
      // Should fail due to second input
      rootInputVertexManager.onRootVertexInitialized("input2", id2, events2);
      fail("Expecting failure in case of multiple inputs attempting to send events");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith(
          "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"));
    }
  }

  @Test(timeout = 5000)
  public void testRootInputVertexManagerSlowStart() {
    Configuration conf = new Configuration();
    RootInputVertexManager manager = null;
    HashMap<String, EdgeProperty> mockInputVertices =
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));
    String mockSrcVertexId2 = "Vertex2";
    EdgeProperty eProp2 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    String mockManagedVertexId = "Vertex3";

    VertexManagerPluginContext mockContext =
        mock(VertexManagerPluginContext.class);
    when(mockContext.getVertexStatistics(any(String.class)))
        .thenReturn(mock(VertexStatistics.class));
    when(mockContext.getInputVertexEdgeProperties())
        .thenReturn(mockInputVertices);
    when(mockContext.getVertexName()).thenReturn(mockManagedVertexId);
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(3);

    mockInputVertices.put(mockSrcVertexId1, eProp1);
    mockInputVertices.put(mockSrcVertexId2, eProp2);

    // check initialization
    manager = createRootInputVertexManager(conf, mockContext, 0.1f, 0.1f);

    final List<Integer> scheduledTasks = Lists.newLinkedList();
    doAnswer(new ScheduledTasksAnswer(scheduledTasks)).when(
        mockContext).scheduleTasks(anyList());

    // source vertices have 0 tasks. immediate start of all managed tasks
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(0);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(0);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 3); // all tasks scheduled

    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    try {
      // source vertex have some tasks. min < 0.
      manager = createRootInputVertexManager(conf, mockContext, -0.1f, 0.0f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. max > 1.
      manager = createRootInputVertexManager(conf, mockContext, 0.0f, 95.0f);
      Assert.assertTrue(false); // should not come here
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Invalid values for slowStartMinFraction"));
    }

    try {
      // source vertex have some tasks. min > max
      manager = createRootInputVertexManager(conf, mockContext, 0.5f, 0.3f);
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

    manager = createRootInputVertexManager(conf, mockContext, 0.975f, null);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));

    Assert.assertEquals(3, manager.pendingTasks.size());
    Assert.assertEquals(numTasks*2, manager.totalNumSourceTasks);
    Assert.assertEquals(0, manager.numSourceTasksCompleted);
    float completedTasksThreshold = 0.975f * numTasks;
    // Finish all tasks before exceeding the threshold
    for (String mockSrcVertex : new String[] { mockSrcVertexId1,
        mockSrcVertexId2 }) {
      for (int i = 0; i < mockContext.getVertexNumTasks(mockSrcVertex); ++i) {
        // complete 0th tasks outside the loop
        manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
            mockSrcVertex, i+1));
        if ((i + 2) >= completedTasksThreshold) {
          // stop before completing more than min/max source tasks
          break;
        }
      }
    }
    // Since we haven't exceeded the threshold, all tasks are still pending
    Assert.assertEquals(manager.totalTasksToSchedule,
        manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size()); // no tasks scheduled

    // Cross the threshold min/max threshold to schedule all tasks
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    Assert.assertEquals(3, manager.pendingTasks.size());
    Assert.assertEquals(0, scheduledTasks.size());
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(0, manager.pendingTasks.size());
    // all tasks scheduled
    Assert.assertEquals(manager.totalTasksToSchedule, scheduledTasks.size());

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(2);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(2);

    // source vertex have some tasks. min, max, 0
    manager = createRootInputVertexManager(conf, mockContext, 0.0f, 0.0f);
    manager.onVertexStarted(emptyCompletions);
    Assert.assertEquals(manager.totalTasksToSchedule, 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 0);
    // all source vertices need to be configured
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 3); // all tasks scheduled

    // min, max > 0 and min, max
    manager = createRootInputVertexManager(conf, mockContext, 0.25f, 0.25f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    // task completion from non-bipartite stage does nothing
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    Assert.assertEquals(manager.numSourceTasksCompleted, 0);
    // task completion on only 1 SG edge does nothing
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    Assert.assertEquals(manager.numSourceTasksCompleted, 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 3); // all tasks scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);

    // min, max > 0 and min, max, absolute max 1.0
    manager = createRootInputVertexManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    // task completion from non-bipartite stage does nothing
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    Assert.assertEquals(manager.numSourceTasksCompleted, 0);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 3);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 3); // all tasks scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 4);

    // min, max > 0 and min, max
    manager = createRootInputVertexManager(conf, mockContext, 1.0f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    // task completion from non-bipartite stage does nothing
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 4);
    Assert.assertEquals(manager.numSourceTasksCompleted, 0);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 1);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 3);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 3); // all tasks scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 4);

    // reset vertices for next test
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(4);
    when(mockContext.getVertexNumTasks(mockSrcVertexId2)).thenReturn(4);

    // min, max > and min < max
    manager = createRootInputVertexManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);
    // completion of same task again should not get counted
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 3);
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 1);
    Assert.assertEquals(scheduledTasks.size(), 2); // 2 task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 4);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 2));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 1); // 1 tasks scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 6);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 3)); // we are done. no action
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 0); // no task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 7);

    // min, max > and min < max
    manager = createRootInputVertexManager(conf, mockContext, 0.25f, 1.0f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 3); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 2);
    Assert.assertEquals(scheduledTasks.size(), 1); // 1 task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 4);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 2));
    Assert.assertEquals(manager.pendingTasks.size(), 1);
    Assert.assertEquals(scheduledTasks.size(), 1); // 1 task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 6);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 3));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 3));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 1); // no task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 8);

    // if there is single task to schedule, it should be schedule when src
    // completed fraction is more than min slow start fraction
    scheduledTasks.clear();
    when(mockContext.getVertexNumTasks(mockManagedVertexId)).thenReturn(1);
    manager = createRootInputVertexManager(conf, mockContext, 0.25f, 0.75f);
    manager.onVertexStarted(emptyCompletions);
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId1,
        VertexState.CONFIGURED));
    manager.onVertexStateUpdated(new VertexStateUpdate(mockSrcVertexId2,
        VertexState.CONFIGURED));
    Assert.assertEquals(manager.pendingTasks.size(), 1); // no tasks scheduled
    Assert.assertEquals(manager.totalNumSourceTasks, 8);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 0));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 1));
    Assert.assertEquals(manager.pendingTasks.size(), 1);
    Assert.assertEquals(scheduledTasks.size(), 0); // no task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 2);
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 1));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 0));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 1); // 1 task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 4);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 2));
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId2, 2));
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 0); // no task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 6);
    scheduledTasks.clear();
    manager.onSourceTaskCompleted(createTaskAttemptIdentifier(
        mockSrcVertexId1, 3)); // we are done. no action
    Assert.assertEquals(manager.pendingTasks.size(), 0);
    Assert.assertEquals(scheduledTasks.size(), 0); // no task scheduled
    Assert.assertEquals(manager.numSourceTasksCompleted, 7);
  }

  @Test
  public void testTezDrainCompletionsOnVertexStart() throws IOException {
    Configuration conf = new Configuration();
    RootInputVertexManager manager = null;
    HashMap<String, EdgeProperty> mockInputVertices =
        new HashMap<String, EdgeProperty>();
    String mockSrcVertexId1 = "Vertex1";
    EdgeProperty eProp1 = EdgeProperty.create(
        EdgeProperty.DataMovementType.BROADCAST,
        EdgeProperty.DataSourceType.PERSISTED,
        EdgeProperty.SchedulingType.SEQUENTIAL,
        OutputDescriptor.create("out"),
        InputDescriptor.create("in"));

    VertexManagerPluginContext mockContext =
        mock(VertexManagerPluginContext.class);
    when(mockContext.getVertexStatistics(any(String.class)))
        .thenReturn(mock(VertexStatistics.class));
    when(mockContext.getInputVertexEdgeProperties())
        .thenReturn(mockInputVertices);
    when(mockContext.getVertexNumTasks(mockSrcVertexId1)).thenReturn(3);

    mockInputVertices.put(mockSrcVertexId1, eProp1);

    // check initialization
    manager = createRootInputVertexManager(conf, mockContext, 0.1f, 0.1f);
    Assert.assertEquals(0, manager.numSourceTasksCompleted);
    manager.onVertexStarted(Collections.singletonList(
      createTaskAttemptIdentifier(mockSrcVertexId1, 0)));
    Assert.assertEquals(1, manager.numSourceTasksCompleted);
  }


  static RootInputVertexManager createRootInputVertexManager(
      Configuration conf, VertexManagerPluginContext context, Float min,
        Float max) {
    if (min != null) {
      conf.setFloat(
          TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION,
              min);
    } else {
      conf.unset(
          TEZ_ROOT_INPUT_VERTEX_MANAGER_MIN_SRC_FRACTION);
    }
    if (max != null) {
      conf.setFloat(
          TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION,
              max);
    } else {
      conf.unset(TEZ_ROOT_INPUT_VERTEX_MANAGER_MAX_SRC_FRACTION);
    }
    if(max != null || min != null) {
      conf.setBoolean(TEZ_ROOT_INPUT_VERTEX_MANAGER_ENABLE_SLOW_START,
          true);
    }
    UserPayload payload;
    try {
      payload = TezUtils.createUserPayloadFromConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    when(context.getUserPayload()).thenReturn(payload);
    RootInputVertexManager manager = new RootInputVertexManager(context);
    manager.initialize();
    return manager;
  }

  protected static class ScheduledTasksAnswer implements Answer<Object> {
    private List<Integer> scheduledTasks;
    public ScheduledTasksAnswer(List<Integer> scheduledTasks) {
      this.scheduledTasks = scheduledTasks;
    }
    @Override
    public Object answer(InvocationOnMock invocation) throws IOException {
      Object[] args = invocation.getArguments();
      scheduledTasks.clear();
      List<VertexManagerPluginContext.ScheduleTaskRequest> tasks =
          (List<VertexManagerPluginContext.ScheduleTaskRequest>)args[0];
      for (VertexManagerPluginContext.ScheduleTaskRequest task : tasks) {
        scheduledTasks.add(task.getTaskIndex());
      }
      return null;
    }
  }

  public static TaskAttemptIdentifier createTaskAttemptIdentifier(String vName,
      int tId) {
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

}

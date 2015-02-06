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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Maps;

public class TestEdge {

  @Test (timeout = 5000)
  public void testOneToOneEdgeManager() {
    EdgeManagerPluginContext mockContext = mock(EdgeManagerPluginContext.class);
    when(mockContext.getSourceVertexName()).thenReturn("Source");
    when(mockContext.getDestinationVertexName()).thenReturn("Destination");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    OneToOneEdgeManager manager = new OneToOneEdgeManager(mockContext);
    manager.initialize();
    Map<Integer, List<Integer>> destinationTaskAndInputIndices = Maps.newHashMap();
    DataMovementEvent event = DataMovementEvent.create(1, null);

    // fail when source and destination are inconsistent
    when(mockContext.getDestinationVertexNumTasks()).thenReturn(4);
    try {
      manager.routeDataMovementEventToDestination(event, 1, 1, destinationTaskAndInputIndices);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("1-1 source and destination task counts must match"));
    }
    
    // now make it consistent
    when(mockContext.getDestinationVertexNumTasks()).thenReturn(3);
    manager.routeDataMovementEventToDestination(event, 1, 1, destinationTaskAndInputIndices);
    Assert.assertEquals(1, destinationTaskAndInputIndices.size());
    Assert.assertEquals(1, destinationTaskAndInputIndices.entrySet().iterator().next().getKey()
        .intValue());
    Assert.assertEquals(0, destinationTaskAndInputIndices.entrySet().iterator().next().getValue()
        .get(0).intValue());
  }

  @SuppressWarnings({ "rawtypes" })
  @Test (timeout = 5000)
  public void testCompositeEventHandling() throws AMUserCodeException {
    EventHandler eventHandler = mock(EventHandler.class);
    EdgeProperty edgeProp = EdgeProperty.create(DataMovementType.SCATTER_GATHER,
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, mock(OutputDescriptor.class),
        mock(InputDescriptor.class));
    Edge edge = new Edge(edgeProp, eventHandler);
    
    TezVertexID srcVertexID = createVertexID(1);
    TezVertexID destVertexID = createVertexID(2);
    LinkedHashMap<TezTaskID, Task> srcTasks = mockTasks(srcVertexID, 1);
    LinkedHashMap<TezTaskID, Task> destTasks = mockTasks(destVertexID, 5);
    
    TezTaskID srcTaskID = srcTasks.keySet().iterator().next();
    
    Vertex srcVertex = mockVertex("src", srcVertexID, srcTasks);
    Vertex destVertex = mockVertex("dest", destVertexID, destTasks);
    
    edge.setSourceVertex(srcVertex);
    edge.setDestinationVertex(destVertex);
    edge.initialize();
    
    TezTaskAttemptID srcTAID = createTAIDForTest(srcTaskID, 2); // Task0, Attempt 0
    
    EventMetaData srcMeta = new EventMetaData(EventProducerConsumerType.OUTPUT, "consumerVertex", "producerVertex", srcTAID);
    
    // Verification via a CompositeEvent
    CompositeDataMovementEvent cdmEvent = CompositeDataMovementEvent.create(0, destTasks.size(),
        ByteBuffer.wrap("bytes".getBytes()));
    cdmEvent.setVersion(2); // AttemptNum
    TezEvent tezEvent = new TezEvent(cdmEvent, srcMeta);
    // Event setup to look like it would after the Vertex is done with it.

    edge.sendTezEventToDestinationTasks(tezEvent);
    verifyEvents(srcTAID, destTasks);

    // Same Verification via regular DataMovementEvents
    // Reset the mock
    resetTaskMocks(destTasks.values());

    for (int i = 0 ; i < destTasks.size() ; i++) {
      DataMovementEvent dmEvent = DataMovementEvent.create(i, ByteBuffer.wrap("bytes".getBytes()));
      dmEvent.setVersion(2);
      tezEvent = new TezEvent(dmEvent, srcMeta);
      edge.sendTezEventToDestinationTasks(tezEvent);
    }
    verifyEvents(srcTAID, destTasks);
  }

  private void verifyEvents(TezTaskAttemptID srcTAID, LinkedHashMap<TezTaskID, Task> destTasks) {
    int count = 0;

    for (Entry<TezTaskID, Task> taskEntry : destTasks.entrySet()) {
      Task mockTask = taskEntry.getValue();
      ArgumentCaptor<TezEvent> args = ArgumentCaptor.forClass(TezEvent.class);
      verify(mockTask, times(1)).registerTezEvent(args.capture());
      TezEvent capturedEvent = args.getValue();

      DataMovementEvent dmEvent = (DataMovementEvent) capturedEvent.getEvent();
      assertEquals(srcTAID.getId(), dmEvent.getVersion());
      assertEquals(count++, dmEvent.getSourceIndex());
      assertEquals(srcTAID.getTaskID().getId(), dmEvent.getTargetIndex());
      byte[] res = new byte[dmEvent.getUserPayload().limit() - dmEvent.getUserPayload().position()];
      dmEvent.getUserPayload().slice().get(res);
      assertTrue(Arrays.equals("bytes".getBytes(), res));
    }
  }

  private void resetTaskMocks(Collection<Task> tasks) {
    for (Task task : tasks) {
      TezTaskID taskID = task.getTaskId();
      reset(task);
      doReturn(taskID).when(task).getTaskId();
    }
  }

  private LinkedHashMap<TezTaskID, Task> mockTasks(TezVertexID vertexID, int numTasks) {
    LinkedHashMap<TezTaskID, Task> tasks = new LinkedHashMap<TezTaskID, Task>();
    for (int i = 0 ; i < numTasks ; i++) {
      Task task = mock(Task.class);
      TezTaskID taskID = TezTaskID.getInstance(vertexID, i);
      doReturn(taskID).when(task).getTaskId();
      tasks.put(taskID, task);
    }
    return tasks;
  }
  
  private Vertex mockVertex(String name, TezVertexID vertexID, LinkedHashMap<TezTaskID, Task> tasks) {
    Vertex vertex = mock(Vertex.class);
    doReturn(vertexID).when(vertex).getVertexId();
    doReturn(name).when(vertex).getName();
    doReturn(tasks).when(vertex).getTasks();
    doReturn(tasks.size()).when(vertex).getTotalTasks();
    for (Entry<TezTaskID, Task> entry : tasks.entrySet()) {
      doReturn(entry.getValue()).when(vertex).getTask(eq(entry.getKey()));
      doReturn(entry.getValue()).when(vertex).getTask(eq(entry.getKey().getId()));
    }
    return vertex;
  }
  
  private TezVertexID createVertexID(int id) {
    TezDAGID dagID = TezDAGID.getInstance("1000", 1, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, id);
    return vertexID;
  }
  
  private TezTaskAttemptID createTAIDForTest(TezTaskID taskID, int taId) {
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, taId);
    return taskAttemptID;
  }
}

/*
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestDAGSchedulerNaturalOrderControlled {

  @Test(timeout = 5000)
  public void testSimpleFlow() {
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = createMockDag();
    DAGSchedulerNaturalOrderControlled dagScheduler =
        new DAGSchedulerNaturalOrderControlled(dag, eventHandler);

    int numVertices = 5;
    Vertex[] vertices = new Vertex[numVertices];
    for (int i = 0; i < numVertices; i++) {
      vertices[i] = dag.getVertex("vertex" + i);
    }

    // Schedule all tasks belonging to v0
    for (int i = 0; i < vertices[0].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[0].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[0].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);

    // Schedule 3 tasks belonging to v2
    for (int i = 0; i < 3; i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    verify(eventHandler, times(3)).handle(any(Event.class));
    reset(eventHandler);

    // Schedule 3 tasks belonging to v3
    for (int i = 0; i < 3; i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[3].getVertexId(), i, 0));
    }
    verify(eventHandler, times(3)).handle(any(Event.class));
    reset(eventHandler);

    // Schedule remaining tasks belonging to v2
    for (int i = 3; i < vertices[2].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[2].getTotalTasks() - 3)).handle(any(Event.class));
    reset(eventHandler);

    // Schedule remaining tasks belonging to v3
    for (int i = 3; i < vertices[3].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[3].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[3].getTotalTasks() - 3)).handle(any(Event.class));
    reset(eventHandler);


    // Schedule all tasks belonging to v4
    for (int i = 0; i < vertices[4].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[4].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[4].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);
  }

  @Test(timeout = 5000)
  public void testSourceRequestDelayed() {
    // ShuffleVertexHandler - slowstart simulation
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = createMockDag();
    DAGSchedulerNaturalOrderControlled dagScheduler =
        new DAGSchedulerNaturalOrderControlled(dag, eventHandler);

    int numVertices = 5;
    Vertex[] vertices = new Vertex[numVertices];
    for (int i = 0; i < numVertices; i++) {
      vertices[i] = dag.getVertex("vertex" + i);
    }

    // Schedule all tasks belonging to v0
    for (int i = 0; i < vertices[0].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[0].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[0].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);

    // v2 behaving as if configured with slow-start.
    // Schedule all tasks belonging to v3.
    for (int i = 0; i < vertices[3].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[3].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[3].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);

    // Scheduling all tasks belonging to v4. None should get scheduled.
    for (int i = 0; i < vertices[4].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[4].getVertexId(), i, 0));
    }
    verify(eventHandler, never()).handle(any(Event.class));
    reset(eventHandler);

    // v2 now starts scheduling ...
    // Schedule 3 tasks for v2 initially.
    for (int i = 0; i < 3; i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    verify(eventHandler, times(3)).handle(any(Event.class));
    reset(eventHandler);

    // Schedule remaining tasks belonging to v2
    for (int i = 3; i < vertices[2].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    ArgumentCaptor<Event> args = ArgumentCaptor.forClass(Event.class);
    // All of v2 and v3 should be sent out.
    verify(eventHandler, times(vertices[2].getTotalTasks() - 3 + vertices[4].getTotalTasks()))
        .handle(
            args.capture());
    int count = 0;
    // Verify the order in which the events were sent out.
    for (Event raw : args.getAllValues()) {
      TaskAttemptEventSchedule event = (TaskAttemptEventSchedule) raw;
      if (count < vertices[2].getTotalTasks() - 3) {
        assertEquals(2, event.getTaskAttemptID().getTaskID().getVertexID().getId());
      } else {
        assertEquals(4, event.getTaskAttemptID().getTaskID().getVertexID().getId());
      }
      count++;
    }
    reset(eventHandler);
  }


  @Test(timeout = 5000)
  public void testParallelismUpdated() {
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = createMockDag();
    DAGSchedulerNaturalOrderControlled dagScheduler =
        new DAGSchedulerNaturalOrderControlled(dag, eventHandler);

    int numVertices = 5;
    Vertex[] vertices = new Vertex[numVertices];
    for (int i = 0; i < numVertices; i++) {
      vertices[i] = dag.getVertex("vertex" + i);
    }

    // Schedule all tasks belonging to v0
    for (int i = 0; i < vertices[0].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[0].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[0].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);

    assertEquals(10, vertices[2].getTotalTasks());

    // v2 will change parallelism
    // Schedule all tasks belonging to v3
    for (int i = 0; i < vertices[3].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[3].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[3].getTotalTasks())).handle(any(Event.class));
    reset(eventHandler);

    // Schedule all tasks belonging to v4
    for (int i = 0; i < vertices[4].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[4].getVertexId(), i, 0));
    }
    verify(eventHandler, never()).handle(any(Event.class));
    reset(eventHandler);

    // Reset the parallelism for v2.
    updateParallelismOnMockVertex(vertices[2], 3);
    assertEquals(3, vertices[2].getTotalTasks());

    // Schedule all tasks belonging to v2
    for (int i = 0; i < vertices[2].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[2].getTotalTasks() + vertices[4].getTotalTasks()))
        .handle(any(Event.class));
    reset(eventHandler);
  }

  @Test(timeout = 5000)
  public void testMultipleRequestsForSameTask() {
    EventHandler eventHandler = mock(EventHandler.class);
    DAG dag = createMockDag();
    DAGSchedulerNaturalOrderControlled dagScheduler =
        new DAGSchedulerNaturalOrderControlled(dag, eventHandler);

    int numVertices = 5;
    Vertex[] vertices = new Vertex[numVertices];
    for (int i = 0; i < numVertices; i++) {
      vertices[i] = dag.getVertex("vertex" + i);
    }

    // Schedule all but 1 task belonging to v0
    for (int i = 0; i < vertices[0].getTotalTasks() - 1; i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[0].getVertexId(), i, 0));
    }
    verify(eventHandler, times(vertices[0].getTotalTasks() - 1)).handle(any(Event.class));
    reset(eventHandler);


    // Schedule all tasks belonging to v2
    for (int i = 0; i < vertices[2].getTotalTasks(); i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[2].getVertexId(), i, 0));
    }
    // Nothing should be scheduled
    verify(eventHandler, never()).handle(any(Event.class));
    reset(eventHandler);

    // Schedule an extra attempt for all but 1 task belonging to v0
    for (int i = 0; i < vertices[0].getTotalTasks() - 1; i++) {
      dagScheduler.scheduleTask(createScheduleRequest(vertices[0].getVertexId(), i, 1));
    }
    // Only v0 requests should have gone out
    verify(eventHandler, times(vertices[0].getTotalTasks() - 1)).handle(any(Event.class));
    reset(eventHandler);

    // Schedule last task of v0, with attempt 1
    dagScheduler.scheduleTask(
        createScheduleRequest(vertices[0].getVertexId(), vertices[0].getTotalTasks() - 1, 1));
    // One v0 request and all of v2 should have gone out
    verify(eventHandler, times(1 + vertices[2].getTotalTasks())).handle(any(Event.class));
  }


  // Test parallelism updated form -1
  // Reduce parallelism
  // Different attempts scheduled for a single task.

  private DAG createMockDag() {
    DAG dag = mock(DAG.class);
    /*
    v0             v1
      \            /
       \          /
        v2       v3
         \      /
          \    /
           \  /
            v4
     v0 - Root
     v1 - Root with 0 tasks.
     v2 - can simulate AutoReduce. Parallelism goes down. Slow schedule.
     v3 - can simulate ImmediateStart
     v4 - Simulate one shuffle input, one broadcast input.
     */

    int numVertices = 5;
    Vertex[] vertices = new Vertex[numVertices];

    vertices[0] = createMockVertex("vertex0", 0, 10, 0);
    vertices[1] = createMockVertex("vertex1", 1, 0, 0);
    vertices[2] = createMockVertex("vertex2", 2, 10, 1);
    vertices[3] = createMockVertex("vertex3", 3, 10, 1);
    vertices[4] = createMockVertex("vertex4", 4, 10, 2);

    for (int i = 0; i < numVertices; i++) {
      String name = vertices[i].getName();
      TezVertexID vertexId = vertices[i].getVertexId();
      doReturn(vertices[i]).when(dag).getVertex(name);
      doReturn(vertices[i]).when(dag).getVertex(vertexId);
    }


    updateMockVertexWithConnections(vertices[0], createConnectionMap(null),
        createConnectionMap(vertices[2]));
    updateMockVertexWithConnections(vertices[1], createConnectionMap(null),
        createConnectionMap(vertices[3]));
    updateMockVertexWithConnections(vertices[2], createConnectionMap(vertices[0]),
        createConnectionMap(vertices[4]));
    updateMockVertexWithConnections(vertices[3], createConnectionMap(vertices[1]),
        createConnectionMap(vertices[4]));
    updateMockVertexWithConnections(vertices[4], createConnectionMap(vertices[2], vertices[3]),
        createConnectionMap(null));

    return dag;
  }


  private void updateParallelismOnMockVertex(Vertex vertex, int newParallelism) {
    doReturn(newParallelism).when(vertex).getTotalTasks();
  }

  private Vertex createMockVertex(String name, int vertexIdInt, int totalTasks,
                                  int distanceFromRoot) {
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, vertexIdInt);

    Vertex vertex = mock(Vertex.class);
    doReturn(name).when(vertex).getName();
    doReturn(totalTasks).when(vertex).getTotalTasks();
    doReturn(vertexId).when(vertex).getVertexId();
    doReturn(distanceFromRoot).when(vertex).getDistanceFromRoot();
    doReturn(vertexId + " [" + name + "]").when(vertex).getLogIdentifier();
    return vertex;
  }

  private Map<Vertex, Edge> createConnectionMap(Vertex... vertices) {
    Map<Vertex, Edge> map = new HashMap<Vertex, Edge>();
    if (vertices != null) {
      for (Vertex vertex : vertices) {
        map.put(vertex, mock(Edge.class));
      }
    }
    return map;
  }

  private void updateMockVertexWithConnections(Vertex mockVertex, Map<Vertex, Edge> sources,
                                               Map<Vertex, Edge> destinations) {
    doReturn(sources).when(mockVertex).getInputVertices();
    doReturn(destinations).when(mockVertex).getOutputVertices();
  }

  private TaskAttempt createTaskAttempt(TezVertexID vertexId, int taskIdInt, int attemptIdInt) {
    TaskAttempt taskAttempt = mock(TaskAttempt.class);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, taskIdInt);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, attemptIdInt);
    doReturn(taskAttemptId).when(taskAttempt).getID();
    doReturn(vertexId).when(taskAttempt).getVertexID();
    return taskAttempt;
  }

  private DAGEventSchedulerUpdate createScheduleRequest(TezVertexID vertexId, int taskIdInt,
                                                        int attemptIdInt) {
    TaskAttempt mockAttempt = createTaskAttempt(vertexId, taskIdInt, attemptIdInt);
    return new DAGEventSchedulerUpdate(DAGEventSchedulerUpdate.UpdateType.TA_SCHEDULE, mockAttempt);
  }

}

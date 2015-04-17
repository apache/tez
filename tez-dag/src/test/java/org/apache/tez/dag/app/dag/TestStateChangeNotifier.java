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

package org.apache.tez.dag.app.dag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.Lists;

import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.api.event.VertexStateUpdateParallelismUpdated;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestStateChangeNotifier {
  
  // uses the thread based notification code path but effectively blocks update
  // events till listeners have been notified
  public static class StateChangeNotifierForTest extends StateChangeNotifier {
    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger totalCount = new AtomicInteger(0);
    
    public StateChangeNotifierForTest(DAG dag) {
      super(dag);
    }

    public void reset() {
      count.set(0);
      totalCount.set(0);
    }
    
    @Override
    protected void processedEventFromQueue() {
      synchronized (count) {
        if (count.decrementAndGet() == 0) {
          count.notifyAll();
        }
      }
    }
    
    @Override
    protected void addedEventToQueue() {
      totalCount.incrementAndGet();
      synchronized (count) {
        // processing may finish by the time we get here
        if (count.incrementAndGet() > 0) {
          try {
            count.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  @Test(timeout = 5000)
  public void testEventsOnRegistration() {
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    Vertex v1 = createMockVertex(dagId, 1);
    Vertex v2 = createMockVertex(dagId, 2);
    Vertex v3 = createMockVertex(dagId, 3);
    DAG dag = createMockDag(dagId, v1, v2, v3);

    StateChangeNotifierForTest tracker = new StateChangeNotifierForTest(dag);

    // Vertex has sent one event
    notifyTracker(tracker, v1, VertexState.RUNNING);
    VertexStateUpdateListener mockListener11 = mock(VertexStateUpdateListener.class);
    VertexStateUpdateListener mockListener12 = mock(VertexStateUpdateListener.class);
    VertexStateUpdateListener mockListener13 = mock(VertexStateUpdateListener.class);
    VertexStateUpdateListener mockListener14 = mock(VertexStateUpdateListener.class);
      // Register for all states
    tracker.registerForVertexUpdates(v1.getName(), null, mockListener11);
      // Register for all states
    tracker.registerForVertexUpdates(v1.getName(), EnumSet.allOf(
        VertexState.class), mockListener12);
      // Register for specific state, event generated
    tracker.registerForVertexUpdates(v1.getName(), EnumSet.of(
        VertexState.RUNNING), mockListener13);
      // Register for specific state, event not generated
    tracker.registerForVertexUpdates(v1.getName(), EnumSet.of(
        VertexState.SUCCEEDED), mockListener14);
    ArgumentCaptor<VertexStateUpdate> argumentCaptor =
        ArgumentCaptor.forClass(VertexStateUpdate.class);
    verify(mockListener11, times(1)).onStateUpdated(argumentCaptor.capture());
    assertEquals(VertexState.RUNNING,
        argumentCaptor.getValue().getVertexState());
    verify(mockListener12, times(1)).onStateUpdated(argumentCaptor.capture());
    assertEquals(VertexState.RUNNING,
        argumentCaptor.getValue().getVertexState());
    verify(mockListener13, times(1)).onStateUpdated(argumentCaptor.capture());
    assertEquals(VertexState.RUNNING,
        argumentCaptor.getValue().getVertexState());
    verify(mockListener14, never()).onStateUpdated(any(VertexStateUpdate.class));

    // Vertex has not notified of state
    tracker.reset();
    VertexStateUpdateListener mockListener2 = mock(VertexStateUpdateListener.class);
    tracker.registerForVertexUpdates(v2.getName(), null, mockListener2);
    Assert.assertEquals(0, tracker.totalCount.get()); // there should no be any event sent out
    verify(mockListener2, never()).onStateUpdated(any(VertexStateUpdate.class));

    // Vertex has notified about parallelism update only
    tracker.stateChanged(v3.getVertexId(), new VertexStateUpdateParallelismUpdated(v3.getName(), 23, -1));
    VertexStateUpdateListener mockListener3 = mock(VertexStateUpdateListener.class);
    tracker.registerForVertexUpdates(v3.getName(), null, mockListener3);
    verify(mockListener3, times(1)).onStateUpdated(argumentCaptor.capture());
    assertEquals(VertexState.PARALLELISM_UPDATED,
        argumentCaptor.getValue().getVertexState());
  }

  @Test(timeout = 5000)
  public void testSimpleStateUpdates() {
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    Vertex v1 = createMockVertex(dagId, 1);
    DAG dag = createMockDag(dagId, v1);

    StateChangeNotifierForTest tracker = new StateChangeNotifierForTest(dag);

    VertexStateUpdateListener mockListener = mock(VertexStateUpdateListener.class);
    tracker.registerForVertexUpdates(v1.getName(), null, mockListener);

    List<VertexState> expectedStates = Lists.newArrayList(
        VertexState.RUNNING,
        VertexState.SUCCEEDED,
        VertexState.FAILED,
        VertexState.KILLED,
        VertexState.RUNNING,
        VertexState.SUCCEEDED);

    for (VertexState state : expectedStates) {
      notifyTracker(tracker, v1, state);
    }

    ArgumentCaptor<VertexStateUpdate> argumentCaptor =
        ArgumentCaptor.forClass(VertexStateUpdate.class);
    verify(mockListener, times(expectedStates.size())).onStateUpdated(argumentCaptor.capture());
    List<VertexStateUpdate> stateUpdatesSent = argumentCaptor.getAllValues();

    Iterator<VertexState> expectedStateIter =
        expectedStates.iterator();
    for (int i = 0; i < expectedStates.size(); i++) {
      assertEquals(expectedStateIter.next(), stateUpdatesSent.get(i).getVertexState());
    }
  }

  @Test(timeout = 5000)
  public void testDuplicateRegistration() {
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    Vertex v1 = createMockVertex(dagId, 1);
    DAG dag = createMockDag(dagId, v1);

    StateChangeNotifierForTest tracker = new StateChangeNotifierForTest(dag);
    VertexStateUpdateListener mockListener = mock(VertexStateUpdateListener.class);

    tracker.registerForVertexUpdates(v1.getName(), null, mockListener);
    try {
      tracker.registerForVertexUpdates(v1.getName(), null, mockListener);
      fail("Expecting an error from duplicate registrations of the same listener");
    } catch (TezUncheckedException e) {
      // Expected, ignore
    }
  }

  @Test(timeout = 5000)
  public void testSpecificStateUpdates() {
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    Vertex v1 = createMockVertex(dagId, 1);
    DAG dag = createMockDag(dagId, v1);

    StateChangeNotifierForTest tracker = new StateChangeNotifierForTest(dag);

    VertexStateUpdateListener mockListener = mock(VertexStateUpdateListener.class);
    tracker.registerForVertexUpdates(v1.getName(), EnumSet.of(
        VertexState.RUNNING,
        VertexState.SUCCEEDED), mockListener);

    List<VertexState> states = Lists.newArrayList(
        VertexState.RUNNING,
        VertexState.SUCCEEDED,
        VertexState.FAILED,
        VertexState.KILLED,
        VertexState.RUNNING,
        VertexState.SUCCEEDED);
    List<VertexState> expectedStates = Lists.newArrayList(
        VertexState.RUNNING,
        VertexState.SUCCEEDED,
        VertexState.RUNNING,
        VertexState.SUCCEEDED);

    for (VertexState state : states) {
      notifyTracker(tracker, v1, state);
    }

    ArgumentCaptor<VertexStateUpdate> argumentCaptor =
        ArgumentCaptor.forClass(VertexStateUpdate.class);
    verify(mockListener, times(expectedStates.size())).onStateUpdated(argumentCaptor.capture());
    List<VertexStateUpdate> stateUpdatesSent = argumentCaptor.getAllValues();

    Iterator<VertexState> expectedStateIter =
        expectedStates.iterator();
    for (int i = 0; i < expectedStates.size(); i++) {
      assertEquals(expectedStateIter.next(), stateUpdatesSent.get(i).getVertexState());
    }
  }

  @Test(timeout = 5000)
  public void testUnregister() {
    TezDAGID dagId = TezDAGID.getInstance("1", 1, 1);
    Vertex v1 = createMockVertex(dagId, 1);
    DAG dag = createMockDag(dagId, v1);

    StateChangeNotifierForTest tracker = new StateChangeNotifierForTest(dag);

    VertexStateUpdateListener mockListener = mock(VertexStateUpdateListener.class);
    tracker.registerForVertexUpdates(v1.getName(), null, mockListener);

    List<VertexState> expectedStates = Lists.newArrayList(
        VertexState.RUNNING,
        VertexState.SUCCEEDED,
        VertexState.FAILED,
        VertexState.KILLED,
        VertexState.RUNNING,
        VertexState.SUCCEEDED);

    int count = 0;
    int numExpectedEvents = 3;
    for (VertexState state : expectedStates) {
      if (count == numExpectedEvents) {
        tracker.unregisterForVertexUpdates(v1.getName(), mockListener);
      }
      notifyTracker(tracker, v1, state);
      count++;
    }

    ArgumentCaptor<VertexStateUpdate> argumentCaptor =
        ArgumentCaptor.forClass(VertexStateUpdate.class);
    verify(mockListener, times(numExpectedEvents)).onStateUpdated(argumentCaptor.capture());
    List<VertexStateUpdate> stateUpdatesSent = argumentCaptor.getAllValues();

    Iterator<VertexState> expectedStateIter =
        expectedStates.iterator();
    for (int i = 0; i < numExpectedEvents; i++) {
      assertEquals(expectedStateIter.next(), stateUpdatesSent.get(i).getVertexState());
    }
  }

  private DAG createMockDag(TezDAGID dagId, Vertex... vertices) {
    DAG dag = mock(DAG.class);
    doReturn(dagId).when(dag).getID();
    for (Vertex v : vertices) {
      String vertexName = v.getName();
      TezVertexID vertexId = v.getVertexId();

      doReturn(v).when(dag).getVertex(vertexName);
      doReturn(v).when(dag).getVertex(vertexId);
    }
    return dag;
  }

  private Vertex createMockVertex(TezDAGID dagId, int id) {
    TezVertexID vertexId = TezVertexID.getInstance(dagId, id);
    String vertexName = "vertex" + id;
    Vertex v = mock(Vertex.class);
    doReturn(vertexId).when(v).getVertexId();
    doReturn(vertexName).when(v).getName();
    return v;
  }

  private void notifyTracker(StateChangeNotifier notifier, Vertex v,
                             VertexState state) {
    notifier.stateChanged(v.getVertexId(), new VertexStateUpdate(v.getName(), state));
  }
}

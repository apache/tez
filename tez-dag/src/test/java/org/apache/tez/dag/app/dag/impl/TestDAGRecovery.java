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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventRecoverEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventRecoverVertex;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TestDAGRecovery {

  private DAGImpl dag;
  private EventHandler mockEventHandler;

  private String user = "root";
  private String dagName = "dag1";

  private AppContext mockAppContext;
  private ApplicationId appId = ApplicationId.newInstance(
      System.currentTimeMillis(), 1);
  private TezDAGID dagId = TezDAGID.getInstance(appId, 1);
  private long initTime = 100L;
  private long startTime = initTime + 200L;
  private long commitStartTime = startTime + 200L;
  private long finishTime = commitStartTime + 200L;
  private TezCounters tezCounters = new TezCounters();

  @Before
  public void setUp() {
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    when(mockAppContext.getCurrentDAG().getDagUGI()).thenReturn(null);
    mockEventHandler = mock(EventHandler.class);
    tezCounters.findCounter("grp_1", "counter_1").increment(1);

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    dag =
        new DAGImpl(dagId, new Configuration(), dagPlan, mockEventHandler,
            mock(TaskAttemptListener.class), new Credentials(),
            new SystemClock(), user, mock(TaskHeartbeatHandler.class),
            mockAppContext);
  }

  private void assertNewState() {
    assertEquals(0, dag.getVertices().size());
    assertEquals(0, dag.edges.size());
    assertNull(dag.dagScheduler);
    assertFalse(dag.recoveryCommitInProgress);
    assertEquals(0, dag.recoveredGroupCommits.size());
  }

  private void restoreFromDAGInitializedEvent() {
    DAGState recoveredState =
        dag.restoreFromEvent(new DAGInitializedEvent(dagId, initTime, user,
            dagName));
    assertEquals(DAGState.INITED, recoveredState);
    assertEquals(initTime, dag.initTime);
    assertEquals(6, dag.getVertices().size());
    assertEquals(6, dag.edges.size());
    assertNotNull(dag.dagScheduler);
  }

  private void restoreFromDAGStartedEvent() {
    DAGState recoveredState =
        dag.restoreFromEvent(new DAGStartedEvent(dagId, startTime, user,
            dagName));
    assertEquals(startTime, dag.startTime);
    assertEquals(DAGState.RUNNING, recoveredState);
  }

  private void restoreFromDAGCommitStartedEvent() {
    DAGState recoveredState =
        dag.restoreFromEvent(new DAGCommitStartedEvent(dagId, commitStartTime));
    assertTrue(dag.recoveryCommitInProgress);
    assertEquals(DAGState.RUNNING, recoveredState);
  }

  private void restoreFromVertexGroupCommitStartedEvent() {
    DAGState recoveredState =
        dag.restoreFromEvent(new VertexGroupCommitStartedEvent(dagId, "g1",
            commitStartTime));
    assertEquals(1, dag.recoveredGroupCommits.size());
    assertFalse(dag.recoveredGroupCommits.get("g1").booleanValue());
    assertEquals(DAGState.RUNNING, recoveredState);
  }

  private void restoreFromVertexGroupCommitFinishedEvent() {
    DAGState recoveredState =
        dag.restoreFromEvent(new VertexGroupCommitFinishedEvent(dagId, "g1",
            commitStartTime + 100L));
    assertEquals(1, dag.recoveredGroupCommits.size());
    assertTrue(dag.recoveredGroupCommits.get("g1").booleanValue());
    assertEquals(DAGState.RUNNING, recoveredState);
  }

  private void restoreFromDAGFinishedEvent(DAGState finalState) {
    DAGState recoveredState =
        dag.restoreFromEvent(new DAGFinishedEvent(dagId, startTime, finishTime,
            finalState, "", tezCounters, user, dagName));
    assertEquals(finishTime, dag.finishTime);
    assertFalse(dag.recoveryCommitInProgress);
    assertEquals(finalState, recoveredState);
    assertEquals(tezCounters, dag.fullCounters);
  }

  /**
   * New -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_FromNew() {
    assertNewState();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));

    ArgumentCaptor<DAGEvent> eventCaptor =
        ArgumentCaptor.forClass(DAGEvent.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<DAGEvent> events = eventCaptor.getAllValues();
    assertEquals(2, events.size());
    assertEquals(DAGEventType.DAG_INIT, events.get(0).getType());
    assertEquals(DAGEventType.DAG_START, events.get(1).getType());
  }

  /**
   * restoreFromDAGInitializedEvent -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_FromInited() {
    assertNewState();
    restoreFromDAGInitializedEvent();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));

    assertEquals(DAGState.RUNNING, dag.getState());
    // send recover event to 2 root vertex
    ArgumentCaptor<VertexEvent> eventCaptor =
        ArgumentCaptor.forClass(VertexEvent.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<VertexEvent> vertexEvents = eventCaptor.getAllValues();
    assertEquals(2, vertexEvents.size());
    for (VertexEvent vEvent : vertexEvents) {
      assertTrue(vEvent instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent = (VertexEventRecoverVertex) vEvent;
      assertEquals(VertexState.RUNNING, recoverEvent.getDesiredState());
    }
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * RecoverTransition
   */
  @Test
  public void testDAGRecovery_FromStarted() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));

    assertEquals(DAGState.RUNNING, dag.getState());
    // send recover event to 2 root vertex
    ArgumentCaptor<VertexEvent> eventCaptor =
        ArgumentCaptor.forClass(VertexEvent.class);
    verify(mockEventHandler, times(2)).handle(eventCaptor.capture());
    List<VertexEvent> vertexEvents = eventCaptor.getAllValues();
    assertEquals(2, vertexEvents.size());
    for (VertexEvent vEvent : vertexEvents) {
      assertTrue(vEvent instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent = (VertexEventRecoverVertex) vEvent;
      assertEquals(VertexState.RUNNING, recoverEvent.getDesiredState());
    }
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromDAGFinishedEvent (SUCCEEDED) -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_Finished_SUCCEEDED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromDAGFinishedEvent(DAGState.SUCCEEDED);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.SUCCEEDED, dag.getState());
    assertEquals(tezCounters, dag.getAllCounters());
    // recover all the vertices to SUCCEED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.SUCCEEDED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.SUCCEEDED, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromDAGFinishedEvent(FAILED) -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_Finished_FAILED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromDAGFinishedEvent(DAGState.FAILED);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.FAILED, dag.getState());
    assertEquals(tezCounters, dag.getAllCounters());
    // recover all the vertices to FAILED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.FAILED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.FAILED, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent -> ->
   * restoreFromDAGFinishedEvent -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_Finished_KILLED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromDAGFinishedEvent(DAGState.KILLED);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.KILLED, dag.getState());
    assertEquals(tezCounters, dag.getAllCounters());
    // recover all the vertices to KILLED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.KILLED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.KILLED, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent -> ->
   * restoreFromDAGFinishedEvent -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_Finished_ERROR() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromDAGFinishedEvent(DAGState.ERROR);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.ERROR, dag.getState());
    assertEquals(tezCounters, dag.getAllCounters());
    // recover all the vertices to KILLED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.FAILED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.ERROR, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromDAG_COMMIT_STARTED -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_COMMIT_STARTED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromDAGCommitStartedEvent();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.FAILED, dag.getState());

    // recover all the vertices to SUCCEEDED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.SUCCEEDED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.FAILED, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromDAG_COMMIT_STARTED -> -> restoreFromDAGFinished (SUCCEEDED)->
   * RecoverTransition
   */
  @Test
  public void testDAGRecovery_COMMIT_STARTED_Finished_SUCCEEDED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();

    restoreFromDAGCommitStartedEvent();
    restoreFromDAGFinishedEvent(DAGState.SUCCEEDED);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.SUCCEEDED, dag.getState());

    // recover all the vertices to SUCCEED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.SUCCEEDED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.SUCCEEDED, dagFinishedEvent.getDAGState());

  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromVERTEX_GROUP_COMMIT_STARTED -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_GROUP_COMMIT_STARTED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();
    restoreFromVertexGroupCommitStartedEvent();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.FAILED, dag.getState());

    // recover all the vertices to SUCCEEDED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.SUCCEEDED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.FAILED, dagFinishedEvent.getDAGState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromVERTEX_GROUP_COMMIT_STARTED -> VERTEX_GROUP_COMMIT_FINISHED ->
   * RecoverTransition
   */
  @Test
  public void testDAGRecovery_GROUP_COMMIT_STARTED_FINISHED() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();

    restoreFromVertexGroupCommitStartedEvent();
    restoreFromVertexGroupCommitFinishedEvent();

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.RUNNING, dag.getState());

    // send recover event to 2 root vertex
    verify(mockEventHandler, times(2)).handle(
        any(VertexEventRecoverVertex.class));
    assertEquals(DAGState.RUNNING, dag.getState());
  }

  /**
   * restoreFromDAGInitializedEvent -> restoreFromDAGStartedEvent ->
   * restoreFromVERTEX_GROUP_COMMIT_STARTED -> VERTEX_GROUP_COMMIT_FINISHED ->
   * restoreFromDAG_Finished -> RecoverTransition
   */
  @Test
  public void testDAGRecovery_GROUP_COMMIT_Finished() {
    assertNewState();
    restoreFromDAGInitializedEvent();
    restoreFromDAGStartedEvent();

    restoreFromVertexGroupCommitStartedEvent();
    restoreFromVertexGroupCommitFinishedEvent();
    restoreFromDAGFinishedEvent(DAGState.SUCCEEDED);

    dag.handle(new DAGEventRecoverEvent(dagId, new ArrayList<URL>()));
    assertEquals(DAGState.SUCCEEDED, dag.getState());
    assertEquals(tezCounters, dag.getAllCounters());
    // recover all the vertices to SUCCEEDED
    ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
    verify(mockEventHandler, times(7)).handle(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    int i = 0;
    for (; i < 6; ++i) {
      assertTrue(events.get(i) instanceof VertexEventRecoverVertex);
      VertexEventRecoverVertex recoverEvent =
          (VertexEventRecoverVertex) events.get(i);
      assertEquals(VertexState.SUCCEEDED, recoverEvent.getDesiredState());
    }

    // send DAGAppMasterEventDAGFinished at last
    assertTrue(events.get(i) instanceof DAGAppMasterEventDAGFinished);
    DAGAppMasterEventDAGFinished dagFinishedEvent =
        (DAGAppMasterEventDAGFinished) events.get(i);
    assertEquals(DAGState.SUCCEEDED, dagFinishedEvent.getDAGState());
  }

}

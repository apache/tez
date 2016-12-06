/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.dag.api.HistoryLogLevel;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.HistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.junit.Test;

public class TestHistoryEventHandler {

  private static ApplicationId appId = ApplicationId.newInstance(1000l, 1);
  private static ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
  private static String user = "TEST_USER";

  @Test
  public void testAll() {
    testLogLevel(null, 6);
    testLogLevel(HistoryLogLevel.NONE, 0);
    testLogLevel(HistoryLogLevel.AM, 1);
    testLogLevel(HistoryLogLevel.DAG, 3);
    testLogLevel(HistoryLogLevel.VERTEX, 4);
    testLogLevel(HistoryLogLevel.TASK, 5);
    testLogLevel(HistoryLogLevel.ALL, 6);
  }

  @Test
  public void testWithDAGRecovery() {
    testLogLevelWithRecovery(null, 6);
    testLogLevelWithRecovery(HistoryLogLevel.AM, 1);
    testLogLevelWithRecovery(HistoryLogLevel.DAG, 3);
    testLogLevelWithRecovery(HistoryLogLevel.VERTEX, 4);
    testLogLevelWithRecovery(HistoryLogLevel.TASK, 5);
    testLogLevelWithRecovery(HistoryLogLevel.ALL, 6);
  }

  @Test
  public void testMultipleDag() {
    testLogLevel(null, HistoryLogLevel.NONE, 7);
    testLogLevel(null, HistoryLogLevel.AM, 7);
    testLogLevel(null, HistoryLogLevel.DAG, 9);
    testLogLevel(null, HistoryLogLevel.VERTEX, 10);
    testLogLevel(null, HistoryLogLevel.TASK, 11);
    testLogLevel(null, HistoryLogLevel.ALL, 12);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.NONE, 5);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.AM, 5);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.DAG, 7);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.VERTEX, 8);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.TASK, 9);
    testLogLevel(HistoryLogLevel.VERTEX, HistoryLogLevel.ALL, 10);
    testLogLevel(HistoryLogLevel.NONE, HistoryLogLevel.NONE, 0);
  }

  private void testLogLevelWithRecovery(HistoryLogLevel level, int expectedCount) {
    HistoryEventHandler handler = createHandler(level);
    InMemoryHistoryLoggingService.events.clear();
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    List<DAGHistoryEvent> events = makeHistoryEvents(dagId, handler.getConfig());
    events.set(1, new DAGHistoryEvent(dagId,
        new DAGRecoveredEvent(attemptId, dagId, "test", user, 0, null)));
    for (DAGHistoryEvent event : events) {
      handler.handle(event);
    }
    assertEquals("Failed for level: " + level,
        expectedCount, InMemoryHistoryLoggingService.events.size());
    handler.stop();
  }

  private void testLogLevel(HistoryLogLevel level, int expectedCount) {
    HistoryEventHandler handler = createHandler(level);
    InMemoryHistoryLoggingService.events.clear();
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId, handler.getConfig())) {
      handler.handle(event);
    }
    assertEquals("Failed for level: " + level,
        expectedCount, InMemoryHistoryLoggingService.events.size());
    handler.stop();
  }

  private void testLogLevel(HistoryLogLevel defaultLogLevel, HistoryLogLevel dagLogLevel,
      int expectedCount) {
    HistoryEventHandler handler = createHandler(defaultLogLevel);
    InMemoryHistoryLoggingService.events.clear();
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 1);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, handler.getConfig())) {
      handler.handle(event);
    }
    TezDAGID dagId2 = TezDAGID.getInstance(appId, 2);
    Configuration conf = new Configuration(handler.getConfig());
    conf.setEnum(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL, dagLogLevel);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId2, conf)) {
      handler.handle(event);
    }

    assertEquals(expectedCount, InMemoryHistoryLoggingService.events.size());
    handler.stop();
  }

  public static class InMemoryHistoryLoggingService extends HistoryLoggingService {
    public InMemoryHistoryLoggingService() {
      super("InMemoryHistoryLoggingService");
    }
    static List<DAGHistoryEvent> events = new ArrayList<>();
    @Override
    public void handle(DAGHistoryEvent event) {
      events.add(event);
    }
  }

  private HistoryEventHandler createHandler(HistoryLogLevel logLevel) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.DAG_RECOVERY_ENABLED, false);
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        InMemoryHistoryLoggingService.class.getName());
    if (logLevel != null) {
      conf.setEnum(TezConfiguration.TEZ_HISTORY_LOGGING_LOGLEVEL, logLevel);
    }

    DAG dag = mock(DAG.class);
    when(dag.getConf()).thenReturn(conf);

    AppContext appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getHadoopShim()).thenReturn(new HadoopShim() {});
    when(appContext.getAMConf()).thenReturn(conf);
    when(appContext.getCurrentDAG()).thenReturn(dag);

    HistoryEventHandler handler =  new HistoryEventHandler(appContext);
    handler.init(conf);

    return handler;
  }

  private List<DAGHistoryEvent> makeHistoryEvents(TezDAGID dagId, Configuration inConf) {
    List<DAGHistoryEvent> historyEvents = new ArrayList<>();

    long time = System.currentTimeMillis();
    Configuration conf = new Configuration(inConf);
    historyEvents.add(new DAGHistoryEvent(null,
        new AMStartedEvent(attemptId, time, user)));
    historyEvents.add(new DAGHistoryEvent(dagId,
        new DAGSubmittedEvent(dagId, time, DAGPlan.getDefaultInstance(), attemptId, null, user,
            conf, null)));
    TezVertexID vertexID = TezVertexID.getInstance(dagId, 1);
    historyEvents.add(new DAGHistoryEvent(dagId,
        new VertexStartedEvent(vertexID, time, time)));
    TezTaskID tezTaskID = TezTaskID.getInstance(vertexID, 1);
    historyEvents.add(new DAGHistoryEvent(dagId,
        new TaskStartedEvent(tezTaskID, "test", time, time)));
    historyEvents.add(new DAGHistoryEvent(dagId,
        new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(tezTaskID, 1), "test", time,
            ContainerId.newContainerId(attemptId, 1), NodeId.newInstance("localhost", 8765), null,
            null, null)));
    historyEvents.add(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, time, time, DAGState.SUCCEEDED, null, null, user, "test", null,
            attemptId, DAGPlan.getDefaultInstance())));
    return historyEvents;
  }
}

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

package org.apache.tez.dag.history.logging.ats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.junit.Test;

public class TestATSV15HistoryLoggingService {
  private static ApplicationId appId = ApplicationId.newInstance(1000l, 1);
  private static ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
  private static String user = "TEST_USER";

  private InMemoryTimelineClient timelineClient;

  @Test(timeout=2000)
  public void testDAGGroupingDefault() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(2, timelineClient.entityLog.size());

    List<TimelineEntity> amEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(1, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNotNull(nonGroupedDagEvents);
    assertEquals(4, nonGroupedDagEvents.size());

    service.stop();
  }

  @Test(timeout=2000)
  public void testDAGGroupingDisabled() throws Exception {
    ATSV15HistoryLoggingService service = createService(1);
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(2, timelineClient.entityLog.size());

    List<TimelineEntity> amEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(1, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNotNull(nonGroupedDagEvents);
    assertEquals(4, nonGroupedDagEvents.size());

    service.stop();
  }

  @Test(timeout=2000)
  public void testDAGGroupingGroupingEnabled() throws Exception {
    int numDagsPerGroup = 100;
    ATSV15HistoryLoggingService service = createService(numDagsPerGroup);
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    TezDAGID dagId2 = TezDAGID.getInstance(appId, numDagsPerGroup - 1);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId2, service)) {
      service.handle(event);
    }

    TezDAGID dagId3 = TezDAGID.getInstance(appId, numDagsPerGroup);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId3, service)) {
      service.handle(event);
    }

    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(dagId1.getGroupId(numDagsPerGroup), dagId2.getGroupId(numDagsPerGroup));
    assertNotEquals(dagId2.getGroupId(numDagsPerGroup), dagId3.getGroupId(numDagsPerGroup));

    assertEquals(3, timelineClient.entityLog.size());

    List<TimelineEntity> amEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(3, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNull(nonGroupedDagEvents);

    List<TimelineEntity> groupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.getGroupId(numDagsPerGroup)));
    assertNotNull(groupedDagEvents);
    assertEquals(8, groupedDagEvents.size());

    nonGroupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId3.toString()));
    assertNull(nonGroupedDagEvents);

    groupedDagEvents = timelineClient.entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId3.getGroupId(numDagsPerGroup)));
    assertNotNull(groupedDagEvents);
    assertEquals(4, groupedDagEvents.size());

    service.stop();
  }

  private ATSV15HistoryLoggingService createService(int numDagsPerGroup) {
    ATSV15HistoryLoggingService service = new ATSV15HistoryLoggingService();
    AppContext appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getHadoopShim()).thenReturn(new HadoopShim() {});
    service.setAppContext(appContext);

    Configuration conf = new Configuration();
    if (numDagsPerGroup != -1) {
      conf.setInt(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP,
          numDagsPerGroup);
    }
    service.init(conf);

    // Set timeline service.
    timelineClient = new InMemoryTimelineClient();
    timelineClient.init(conf);
    service.timelineClient = timelineClient;

    service.start();
    return service;
  }

  private List<DAGHistoryEvent> makeHistoryEvents(TezDAGID dagId,
                                                  ATSV15HistoryLoggingService service) {
    List<DAGHistoryEvent> historyEvents = new ArrayList<>();

    long time = System.currentTimeMillis();
    Configuration conf = new Configuration(service.getConfig());
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
    return historyEvents;
  }

  private static class InMemoryTimelineClient extends TimelineClient {
    Map<TimelineEntityGroupId, List<TimelineEntity>> entityLog = new HashMap<>();

    protected InMemoryTimelineClient() {
      super("InMemoryTimelineClient");
    }

    @Override
    public void flush() throws IOException {
    }

    public static final ApplicationId DEFAULT_APP_ID = ApplicationId.newInstance(0, -1);
    public static final TimelineEntityGroupId DEFAULT_GROUP_ID =
        TimelineEntityGroupId.newInstance(DEFAULT_APP_ID, "");

    @Override
    public synchronized TimelinePutResponse putEntities(TimelineEntity... entities)
        throws IOException, YarnException {
      return putEntities(null, DEFAULT_GROUP_ID, entities);
    }

    @Override
    public TimelinePutResponse putEntities(ApplicationAttemptId appAttemptId,
        TimelineEntityGroupId groupId,
        TimelineEntity... entities) throws IOException, YarnException {
      List<TimelineEntity> groupEntities = entityLog.get(groupId);
      if (groupEntities == null) {
        groupEntities = new ArrayList<>();
        entityLog.put(groupId, groupEntities);
      }
      for (TimelineEntity entity : entities) {
        groupEntities.add(entity);
      }
      return null;
    }

    @Override
    public void putDomain(TimelineDomain domain) throws IOException, YarnException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putDomain(ApplicationAttemptId appAttemptId, TimelineDomain domain)
        throws IOException, YarnException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Token<TimelineDelegationTokenIdentifier> getDelegationToken(String renewer)
        throws IOException, YarnException {
      return null;
    }

    @Override
    public long renewDelegationToken(Token<TimelineDelegationTokenIdentifier> timelineDT)
        throws IOException, YarnException {
      return 0;
    }

    @Override
    public void cancelDelegationToken(Token<TimelineDelegationTokenIdentifier> timelineDT)
        throws IOException, YarnException {
    }
  }
}

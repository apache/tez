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
import static org.junit.Assert.assertTrue;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.security.HistoryACLPolicyManager;
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
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestATSV15HistoryLoggingService {
  private static ApplicationId appId = ApplicationId.newInstance(1000l, 1);
  private static ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
  private static String user = "TEST_USER";

  private TimelineClient timelineClient;
  Map<TimelineEntityGroupId, List<TimelineEntity>> entityLog;
  final TimelineEntityGroupId DEFAULT_GROUP_ID =
    TimelineEntityGroupId.newInstance(ApplicationId.newInstance(0, -1), "");

  private AppContext appContext;

  @Test(timeout=2000)
  public void testDAGGroupingDefault() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    service.start();

    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(2, entityLog.size());

    List<TimelineEntity> amEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(1, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNotNull(nonGroupedDagEvents);
    assertEquals(5, nonGroupedDagEvents.size());

    service.stop();
  }

  @Test(timeout=2000)
  public void testDAGGroupingDisabled() throws Exception {
    ATSV15HistoryLoggingService service = createService(1);
    service.start();

    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(2, entityLog.size());

    List<TimelineEntity> amEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(1, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNotNull(nonGroupedDagEvents);
    assertEquals(5, nonGroupedDagEvents.size());

    service.stop();
  }

  @Test(timeout=2000)
  public void testDAGGroupingGroupingEnabled() throws Exception {
    int numDagsPerGroup = 100;
    ATSV15HistoryLoggingService service = createService(numDagsPerGroup);
    service.start();

    TezDAGID dagId1 = TezDAGID.getInstance(appId, 1);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    TezDAGID dagId2 = TezDAGID.getInstance(appId, numDagsPerGroup );
    for (DAGHistoryEvent event : makeHistoryEvents(dagId2, service)) {
      service.handle(event);
    }

    TezDAGID dagId3 = TezDAGID.getInstance(appId, numDagsPerGroup + 1);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId3, service)) {
      service.handle(event);
    }

    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }

    assertEquals(dagId1.getGroupId(numDagsPerGroup), dagId2.getGroupId(numDagsPerGroup));
    assertNotEquals(dagId2.getGroupId(numDagsPerGroup), dagId3.getGroupId(numDagsPerGroup));

    assertEquals(3, entityLog.size());

    List<TimelineEntity> amEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, appId.toString()));
    assertNotNull(amEvents);
    assertEquals(3, amEvents.size());

    List<TimelineEntity> nonGroupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.toString()));
    assertNull(nonGroupedDagEvents);

    List<TimelineEntity> groupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId1.getGroupId(numDagsPerGroup)));
    assertNotNull(groupedDagEvents);
    assertEquals(10, groupedDagEvents.size());

    nonGroupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId3.toString()));
    assertNull(nonGroupedDagEvents);

    groupedDagEvents = entityLog.get(
        TimelineEntityGroupId.newInstance(appId, dagId3.getGroupId(numDagsPerGroup)));
    assertNotNull(groupedDagEvents);
    assertEquals(5, groupedDagEvents.size());

    service.stop();
  }

  @Test
  public void testNonSessionDomains() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenReturn(Collections.singletonMap(
        TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "session-id"));

    service.start();

    verify(historyACLPolicyManager, times(1))
        .setupSessionACLs((Configuration)any(), eq(appId));

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    // NOTE: Expect 6 invocations for 5 history events because DAG_SUBMITTED becomes two separate timeline events.
    verify(historyACLPolicyManager, times(6)).updateTimelineEntityDomain(any(), eq("session-id"));
    assertTrue(entityLog.size() > 0);

    service.stop();
  }

  @Test
  public void testNonSessionDomainsFailed() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenThrow(new IOException());

    service.start();

    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(), eq(appId));

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // History logging is disabled.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), (String)any());
    assertEquals(0, entityLog.size());

    service.stop();
  }

  @Test
  public void testNonSessionDomainsAclNull() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenReturn(null);

    service.start();

    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(), eq(appId));

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // No domain updates but history logging happened.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), (String)any());
    assertTrue(entityLog.size() > 0);

    service.stop();
  }

  @Test
  public void testSessionDomains() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    when(appContext.isSession()).thenReturn(true);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenReturn(Collections.singletonMap(
        TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "session-id"));

    service.start();

    // Verify that the session domain was created.
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(), eq(appId));

    // Mock dag domain creation.
    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID, "dag-id"));

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // Verify dag domain was created.
    verify(historyACLPolicyManager, times(1))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // calls were made with correct domain ids.
    verify(historyACLPolicyManager, times(1)).updateTimelineEntityDomain(any(), eq("session-id"));
    verify(historyACLPolicyManager, times(5)).updateTimelineEntityDomain(any(), eq("dag-id"));

    service.stop();
  }

  @Test
  public void testSessionDomainsFailed() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    when(appContext.isSession()).thenReturn(true);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenThrow(new IOException());

    service.start();

    // Verify that the session domain was created.
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(), eq(appId));

    // Mock dag domain creation.
    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID, "dag-id"));

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag creation was done.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // No history logging calls were done
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), (String)any());
    assertEquals(0, entityLog.size());

    service.stop();
  }

  @Test
  public void testSessionDomainsDagFailed() throws Exception {
    ATSV15HistoryLoggingService service = createService(-1);

    when(appContext.isSession()).thenReturn(true);

    HistoryACLPolicyManager historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    service.historyACLPolicyManager = historyACLPolicyManager;

    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), eq(appId)))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "session-id"));

    service.start();

    // Verify that the session domain creation was called.
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(), eq(appId));

    // Mock dag domain creation.
    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any()))
    .thenThrow(new IOException());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, service)) {
      service.handle(event);
    }
    while (!service.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // Verify dag domain creation was called.
    verify(historyACLPolicyManager, times(1))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // AM events sent, dag events are not sent.
    verify(historyACLPolicyManager, times(1)).updateTimelineEntityDomain(any(), eq("session-id"));
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), eq("dag-id"));
    assertEquals(1, entityLog.size());

    service.stop();
  }

  private ATSV15HistoryLoggingService createService(int numDagsPerGroup) throws IOException, YarnException {
    ATSV15HistoryLoggingService service = new ATSV15HistoryLoggingService();
    appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getHadoopShim()).thenReturn(new HadoopShim() {});
    service.setAppContext(appContext);

    Configuration conf = new Configuration(false);
    if (numDagsPerGroup != -1) {
      conf.setInt(TezConfiguration.TEZ_HISTORY_LOGGING_TIMELINE_NUM_DAGS_PER_GROUP,
          numDagsPerGroup);
    }
    service.init(conf);

    // Set timeline service.
    timelineClient = mock(TimelineClient.class);
    entityLog = new HashMap<>();
    //timelineClient.init(conf);
    when(timelineClient.getDelegationToken(anyString())).thenReturn(null);
    when(timelineClient.renewDelegationToken(Matchers.<Token<TimelineDelegationTokenIdentifier>>any())).thenReturn(0L);
    when(timelineClient.putEntities(Matchers.<TimelineEntity>anyVararg())).thenAnswer(new Answer() {
      @Override
      public TimelinePutResponse answer(InvocationOnMock invocation) throws Throwable {
        return putEntityHelper(DEFAULT_GROUP_ID, invocation.getArguments(), 0);
      }
    });
    when(timelineClient.putEntities(any(ApplicationAttemptId.class), any(TimelineEntityGroupId.class), Matchers.<TimelineEntity>anyVararg())).thenAnswer(new Answer() {
      @Override
      public TimelinePutResponse answer(InvocationOnMock invocation) throws Throwable {
        return putEntityHelper(invocation.getArgumentAt(1, TimelineEntityGroupId.class), invocation.getArguments(), 2);
      }
    });
    service.timelineClient = timelineClient;

    return service;
  }

  private TimelinePutResponse putEntityHelper(TimelineEntityGroupId groupId, Object[] args, int firstEntityIdx) {
    List<TimelineEntity> groupEntities = entityLog.get(groupId);
    if (groupEntities == null) {
      groupEntities = new ArrayList<>();
      entityLog.put(groupId, groupEntities);
    }
    for (int i = firstEntityIdx; i < args.length; i++) {
      groupEntities.add((TimelineEntity) args[i]);
    }
    return null;
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
            conf, null, "default")));
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
}

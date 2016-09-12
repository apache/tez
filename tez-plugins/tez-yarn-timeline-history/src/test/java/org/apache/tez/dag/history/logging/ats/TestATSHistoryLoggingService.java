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

package org.apache.tez.dag.history.logging.ats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.common.security.HistoryACLPolicyManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestATSHistoryLoggingService {

  private static final Logger LOG = LoggerFactory.getLogger(TestATSHistoryLoggingService.class);

  private ATSHistoryLoggingService atsHistoryLoggingService;
  private AppContext appContext;
  private Configuration conf;
  private int atsInvokeCounter;
  private int atsEntitiesCounter;
  private HistoryACLPolicyManager historyACLPolicyManager;
  private SystemClock clock = new SystemClock();
  private static ApplicationId appId = ApplicationId.newInstance(1000l, 1);
  private static ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

  @Before
  public void setup() throws Exception {
    appContext = mock(AppContext.class);
    historyACLPolicyManager = mock(HistoryACLPolicyManager.class);
    atsHistoryLoggingService = new ATSHistoryLoggingService();
    atsHistoryLoggingService.setAppContext(appContext);
    conf = new Configuration(false);
    conf.setLong(TezConfiguration.YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS,
        1000l);
    conf.setInt(TezConfiguration.YARN_ATS_MAX_EVENTS_PER_BATCH, 2);
    conf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);
    conf.set(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "test-domain");
    atsInvokeCounter = 0;
    atsEntitiesCounter = 0;
    atsHistoryLoggingService.init(conf);
    atsHistoryLoggingService.historyACLPolicyManager = historyACLPolicyManager;
    atsHistoryLoggingService.timelineClient = mock(TimelineClient.class);

    when(appContext.getClock()).thenReturn(clock);
    when(appContext.getCurrentDAGID()).thenReturn(null);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(atsHistoryLoggingService.timelineClient.putEntities(
        Matchers.<TimelineEntity[]>anyVararg())).thenAnswer(
        new Answer<Object>() {
          @Override
          public Object answer(InvocationOnMock invocation) throws Throwable {
            ++atsInvokeCounter;
            atsEntitiesCounter += invocation.getArguments().length;
            try {
              Thread.sleep(500l);
            } catch (InterruptedException e) {
              // do nothing
            }
            return null;
          }
        }
    );
  }

  @After
  public void teardown() {
    atsHistoryLoggingService.stop();
    atsHistoryLoggingService = null;
  }

  @Test(timeout=20000)
  public void testATSHistoryLoggingServiceShutdown() {
    atsHistoryLoggingService.start();
    TezDAGID tezDAGID = TezDAGID.getInstance(
        ApplicationId.newInstance(100l, 1), 1);
    DAGHistoryEvent historyEvent = new DAGHistoryEvent(tezDAGID,
        new DAGStartedEvent(tezDAGID, 1001l, "user1", "dagName1"));

    for (int i = 0; i < 100; ++i) {
      atsHistoryLoggingService.handle(historyEvent);
    }

    try {
      Thread.sleep(2500l);
    } catch (InterruptedException e) {
      // Do nothing
    }
    atsHistoryLoggingService.stop();

    LOG.info("ATS entitiesSent=" + atsEntitiesCounter
        + ", timelineInvocations=" + atsInvokeCounter);

    Assert.assertTrue(atsEntitiesCounter >= 4);
    Assert.assertTrue(atsEntitiesCounter < 20);

  }

  @Test(timeout=20000)
  public void testATSEventBatching() {
    atsHistoryLoggingService.start();
    TezDAGID tezDAGID = TezDAGID.getInstance(
        ApplicationId.newInstance(100l, 1), 1);
    DAGHistoryEvent historyEvent = new DAGHistoryEvent(tezDAGID,
        new DAGStartedEvent(tezDAGID, 1001l, "user1", "dagName1"));

    for (int i = 0; i < 100; ++i) {
      atsHistoryLoggingService.handle(historyEvent);
    }

    try {
      Thread.sleep(1000l);
    } catch (InterruptedException e) {
      // Do nothing
    }
    LOG.info("ATS entitiesSent=" + atsEntitiesCounter
        + ", timelineInvocations=" + atsInvokeCounter);

    Assert.assertTrue(atsEntitiesCounter > atsInvokeCounter);
    Assert.assertEquals(atsEntitiesCounter/2, atsInvokeCounter);
  }

  @Test(timeout=20000)
  public void testTimelineServiceDisable() throws Exception {
    atsHistoryLoggingService.start();
    ATSHistoryLoggingService atsHistoryLoggingService1;
    atsHistoryLoggingService1 = new ATSHistoryLoggingService();

    atsHistoryLoggingService1.setAppContext(appContext);
    atsHistoryLoggingService1.timelineClient = mock(TimelineClient.class);
    when(atsHistoryLoggingService1.timelineClient.putEntities(
      Matchers.<TimelineEntity[]>anyVararg())).thenAnswer(
      new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        ++atsInvokeCounter;
        atsEntitiesCounter += invocation.getArguments().length;
        try {
          Thread.sleep(10l);
        } catch (InterruptedException e) {
          // do nothing
        }
        return null;
      }
    });
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false);
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
      ATSHistoryLoggingService.class.getName());
    atsHistoryLoggingService1.init(conf);
    atsHistoryLoggingService1.start();
    TezDAGID tezDAGID = TezDAGID.getInstance(
         ApplicationId.newInstance(100l, 1), 1);
    DAGHistoryEvent historyEvent = new DAGHistoryEvent(tezDAGID,
    new DAGStartedEvent(tezDAGID, 1001l, "user1", "dagName1"));
    for (int i = 0; i < 100; ++i) {
      atsHistoryLoggingService1.handle(historyEvent);
    }

    try {
        Thread.sleep(20l);
    } catch (InterruptedException e) {
        // Do nothing
    }
    LOG.info("ATS entitiesSent=" + atsEntitiesCounter
         + ", timelineInvocations=" + atsInvokeCounter);
    Assert.assertEquals(atsInvokeCounter, 0);
    Assert.assertEquals(atsEntitiesCounter, 0);
    Assert.assertNull(atsHistoryLoggingService1.timelineClient);
    atsHistoryLoggingService1.close();
  }

  @Test(timeout=10000)
  public void testNonSessionDomains() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "session-id"));
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs(
        (Configuration)any(), (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    Thread.sleep(2500);
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(5)).updateTimelineEntityDomain(any(), eq("session-id"));
  }

  @Test(timeout=10000)
  public void testNonSessionDomainsFailed() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenThrow(new IOException());
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs(
        (Configuration)any(), (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(1000);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), eq("session-id"));
    Assert.assertEquals(0, atsEntitiesCounter);
  }

  @Test(timeout=10000)
  public void testNonSessionDomainsAclNull() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenReturn(null);
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs(
        (Configuration)any(), (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    Thread.sleep(2500);
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), eq("session-id"));
    Assert.assertEquals(5, atsEntitiesCounter);
  }

  @Test(timeout=10000)
  public void testSessionDomains() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "test-domain"));

    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), (ApplicationId)any(), eq("0"), (DAGAccessControls)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID, "dag-domain"));

    when(appContext.isSession()).thenReturn(true);
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(),
        (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    Thread.sleep(2500);
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(1))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(1)).updateTimelineEntityDomain(any(), eq("test-domain"));
    verify(historyACLPolicyManager, times(4)).updateTimelineEntityDomain(any(), eq("dag-domain"));
  }

  @Test(timeout=10000)
  public void testSessionDomainsFailed() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenThrow(new IOException());

    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), (ApplicationId)any(), eq("0"), (DAGAccessControls)any()))
    .thenReturn(
        Collections.singletonMap(TezConfiguration.YARN_ATS_ACL_DAG_DOMAIN_ID, "dag-domain"));

    when(appContext.isSession()).thenReturn(true);
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(),
        (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(1000);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(0))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // No calls were made for domains.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), (String)any());
    Assert.assertEquals(0, atsEntitiesCounter);
  }

  @Test(timeout=10000)
  public void testSessionDomainsDagFailed() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenReturn(Collections.singletonMap(
        TezConfiguration.YARN_ATS_ACL_SESSION_DOMAIN_ID, "session-domain"));

    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), (ApplicationId)any(), eq("0"), (DAGAccessControls)any()))
    .thenThrow(new IOException());

    when(appContext.isSession()).thenReturn(true);
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(),
        (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    Thread.sleep(2500);
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // DAG domain was called once.
    verify(historyACLPolicyManager, times(1))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(1))
        .updateTimelineEntityDomain(any(), eq("session-domain"));
    verify(historyACLPolicyManager, times(1))
        .updateTimelineEntityDomain(any(), (String)any());
    Assert.assertEquals(1, atsEntitiesCounter);
  }

  @Test(timeout=10000)
  public void testSessionDomainsAclNull() throws Exception {
    when(historyACLPolicyManager.setupSessionACLs((Configuration)any(), (ApplicationId)any()))
    .thenReturn(null);

    when(historyACLPolicyManager.setupSessionDAGACLs(
        (Configuration)any(), (ApplicationId)any(), eq("0"), (DAGAccessControls)any()))
    .thenReturn(null);

    when(appContext.isSession()).thenReturn(true);
    atsHistoryLoggingService.start();
    verify(historyACLPolicyManager, times(1)).setupSessionACLs((Configuration)any(),
        (ApplicationId)any());

    // Send the event and wait for completion.
    TezDAGID dagId1 = TezDAGID.getInstance(appId, 0);
    for (DAGHistoryEvent event : makeHistoryEvents(dagId1, atsHistoryLoggingService)) {
      atsHistoryLoggingService.handle(event);
    }
    Thread.sleep(2500);
    while (!atsHistoryLoggingService.eventQueue.isEmpty()) {
      Thread.sleep(100);
    }
    // No dag domain were created.
    verify(historyACLPolicyManager, times(1))
      .setupSessionDAGACLs((Configuration)any(), eq(appId), eq("0"), (DAGAccessControls)any());

    // All calls made with session domain id.
    verify(historyACLPolicyManager, times(0)).updateTimelineEntityDomain(any(), (String)any());
    Assert.assertEquals(5, atsEntitiesCounter);
  }

  private List<DAGHistoryEvent> makeHistoryEvents(TezDAGID dagId,
      ATSHistoryLoggingService service) {
    List<DAGHistoryEvent> historyEvents = new ArrayList<>();

    long time = System.currentTimeMillis();
    Configuration conf = new Configuration(service.getConfig());
    historyEvents.add(new DAGHistoryEvent(null, new AMStartedEvent(attemptId, time, "user")));
    historyEvents.add(new DAGHistoryEvent(dagId, new DAGSubmittedEvent(dagId, time,
        DAGPlan.getDefaultInstance(), attemptId, null, "user", conf, null)));
    TezVertexID vertexID = TezVertexID.getInstance(dagId, 1);
    historyEvents.add(new DAGHistoryEvent(dagId, new VertexStartedEvent(vertexID, time, time)));
    TezTaskID tezTaskID = TezTaskID.getInstance(vertexID, 1);
    historyEvents
        .add(new DAGHistoryEvent(dagId, new TaskStartedEvent(tezTaskID, "test", time, time)));
    historyEvents.add(new DAGHistoryEvent(dagId,
        new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(tezTaskID, 1), "test", time,
            ContainerId.newContainerId(attemptId, 1), NodeId.newInstance("localhost", 8765), null,
            null, null)));
    return historyEvents;
  }
}

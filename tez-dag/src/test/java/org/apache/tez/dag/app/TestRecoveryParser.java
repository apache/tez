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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.RecoveryParser.DAGSummaryData;
import org.apache.tez.dag.app.RecoveryParser.RecoveredDAGData;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.TestDAGImpl;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestRecoveryParser {

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecoveryParser.class.getName() + "-tmpDir";

  private ApplicationId appId;
  private RecoveryParser parser;
  private FileSystem localFS;
  private Configuration conf;
  private Path recoveryPath;
  private DAGAppMaster mockAppMaster;
  private DAGImpl mockDAGImpl;

  @Before
  public void setUp() throws IllegalArgumentException, IOException {
    this.conf = new Configuration();
    this.localFS = FileSystem.getLocal(conf);
    this.appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    this.recoveryPath = new Path(TEST_ROOT_DIR + "/" + appId + "/recovery");
    this.localFS.delete(new Path(TEST_ROOT_DIR), true);
    mockAppMaster = mock(DAGAppMaster.class);
    mockAppMaster.dagNames = new HashSet<String>();
    mockAppMaster.dagIDs = new HashSet<String>();
    when(mockAppMaster.getConfig()).thenReturn(new Configuration());
    mockDAGImpl = mock(DAGImpl.class);
    when(mockAppMaster.createDAG(any(DAGPlan.class), any(TezDAGID.class))).thenReturn(mockDAGImpl);
    parser = new RecoveryParser(mockAppMaster, localFS, recoveryPath, 3);
  }

  private DAGSummaryData createDAGSummaryData(TezDAGID dagId, boolean completed) {
    DAGSummaryData data = new DAGSummaryData(dagId);
    data.completed = completed;
    return data;
  }

  @Test(timeout = 5000)
  public void testGetLastCompletedDAG() {
    Map<TezDAGID, DAGSummaryData> summaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    int lastCompletedDAGId = new Random().nextInt(20) + 1;
    for (int i = 1; i <= lastCompletedDAGId; ++i) {
      ApplicationId appId = ApplicationId.newInstance(1, 1);
      TezDAGID dagId = TezDAGID.getInstance(appId, i);
      summaryDataMap.put(dagId, createDAGSummaryData(dagId, true));
    }

    DAGSummaryData lastCompletedDAG =
        parser.getLastCompletedOrInProgressDAG(summaryDataMap);
    assertEquals(lastCompletedDAGId, lastCompletedDAG.dagId.getId());
  }

  @Test(timeout = 5000)
  public void testGetLastInProgressDAG() {
    Map<TezDAGID, DAGSummaryData> summaryDataMap =
        new HashMap<TezDAGID, DAGSummaryData>();
    int dagNum = 20;
    int lastInProgressDAGId = new Random().nextInt(dagNum) + 1;
    for (int i = 1; i <= dagNum; ++i) {
      ApplicationId appId = ApplicationId.newInstance(1, 1);
      TezDAGID dagId = TezDAGID.getInstance(appId, i);
      if (i == lastInProgressDAGId) {
        summaryDataMap.put(dagId, createDAGSummaryData(dagId, false));
      } else {
        summaryDataMap.put(dagId, createDAGSummaryData(dagId, true));
      }
    }

    DAGSummaryData lastInProgressDAG =
        parser.getLastCompletedOrInProgressDAG(summaryDataMap);
    assertEquals(lastInProgressDAGId, lastInProgressDAG.dagId.getId());
  }

  // skipAllOtherEvents due to non-recoverable (in the middle of commit)
  @Test(timeout = 5000)
  public void testSkipAllOtherEvents_1() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write data in attempt_1
    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration())));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGInitializedEvent(dagID, 1L, "user", dagPlan.getName(), null)));
    // only for testing, DAGCommitStartedEvent is not supposed to happen at this time.
    rService.handle(new DAGHistoryEvent(dagID,new DAGCommitStartedEvent(dagID, System.currentTimeMillis())));
    rService.stop();

    // write data in attempt_2
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/2"));
    rService = new RecoveryService(appContext);
    rService.init(conf);
    rService.start();
    // only for testing, DAGStartedEvent is not supposed to happen at this time.
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGStartedEvent(dagID, 1L, "user", "dag1")));
    rService.stop();

    RecoveredDAGData dagData = parser.parseRecoveryData();
    assertEquals(true, dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("DAG Commit was in progress, not recoverable,"));
    // DAGSubmittedEvent is handled but DAGInitializedEvent and DAGStartedEvent in the next attempt are both skipped
    // due to the dag is not recoerable.
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    verify(dagData.recoveredDAG, never()).restoreFromEvent(isA(DAGInitializedEvent.class));
    verify(dagData.recoveredDAG, never()).restoreFromEvent(isA(DAGStartedEvent.class));
  }

  // skipAllOtherEvents due to dag finished
  @Test (timeout = 5000)
  public void testSkipAllOtherEvents_2() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write data in attempt_1
    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration())));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGInitializedEvent(dagID, 1L, "user", dagPlan.getName(), null)));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGFinishedEvent(dagID, 1L, 2L, DAGState.FAILED, "diag", null, "user", "dag1", null,
            appAttemptId)));
    rService.handle(new DAGHistoryEvent(dagID, new DAGStartedEvent(dagID, 1L, "user", "dag1")));
    rService.stop();

    // write data in attempt_2
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/2"));
    rService = new RecoveryService(appContext);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
       new DAGStartedEvent(dagID, 1L, "user", "dag1")));
    rService.stop();

    RecoveredDAGData dagData = parser.parseRecoveryData();
    assertEquals(false, dagData.nonRecoverable);
    assertEquals(DAGState.FAILED, dagData.dagState);
    assertEquals(true, dagData.isCompleted);
    // DAGSubmittedEvent, DAGInitializedEvent and DAGFinishedEvent is handled
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    // DAGInitializedEvent may not been handled before DAGFinishedEvent,
    // because DAGFinishedEvent's writeToRecoveryImmediately is true
    verify(dagData.recoveredDAG).restoreFromEvent(isA(DAGFinishedEvent.class));
    // DAGStartedEvent is skipped due to it is after DAGFinishedEvent
    verify(dagData.recoveredDAG, never()).restoreFromEvent(isA(DAGStartedEvent.class));
  }

  @Test(timeout = 5000)
  public void testLastCorruptedRecoveryRecord() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write data in attempt_1
    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration())));
    // wait until DAGSubmittedEvent is handled in the RecoveryEventHandling thread
    rService.await();
    rService.outputStreamMap.get(dagID).writeUTF("INVALID_DATA");
    rService.stop();

    // write data in attempt_2
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/2"));
    rService = new RecoveryService(appContext);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGInitializedEvent(dagID, 1L, "user", dagPlan.getName(), null)));
    rService.await();
    rService.outputStreamMap.get(dagID).writeUTF("INVALID_DATA");
    rService.stop();

    // corrupted last records will be skipped but the whole recovery logs will be read
    RecoveredDAGData dagData = parser.parseRecoveryData();
    assertEquals(false, dagData.isCompleted);
    assertEquals(null, dagData.reason);
    assertEquals(false, dagData.nonRecoverable);
    // verify DAGSubmitedEvent & DAGInititlizedEvent is handled.
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    verify(dagData.recoveredDAG).restoreFromEvent(isA(DAGInitializedEvent.class));
  }

  @Test(timeout = 5000)
  public void testLastCorruptedSummaryRecord() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());

    // write data in attempt_1
    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration())));
    // write an corrupted SummaryEvent
    rService.summaryStream.writeChars("INVALID_DATA");
    rService.stop();

    try {
      // Corrupted SummaryEvent will cause recovery fail (throw exception here)
      parser.parseRecoveryData();
      fail();
    } catch (IOException e) {
      // exception when parsing protobuf object
      e.printStackTrace();
    }
  }

}

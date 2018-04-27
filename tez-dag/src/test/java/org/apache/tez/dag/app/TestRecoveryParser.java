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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Sets;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.RecoveryParser.DAGRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.DAGSummaryData;
import org.apache.tez.dag.app.RecoveryParser.TaskAttemptRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.TaskRecoveryData;
import org.apache.tez.dag.app.RecoveryParser.VertexRecoveryData;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.dag.impl.TestDAGImpl;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitFinishedEvent;
import org.apache.tez.dag.history.events.VertexGroupCommitStartedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.*;

import com.google.common.collect.Lists;

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
  // Protobuf message limit is 64 MB by default
  private static final int PROTOBUF_DEFAULT_SIZE_LIMIT = 64 << 20;

  @Before
  public void setUp() throws IllegalArgumentException, IOException {
    this.conf = new Configuration();
    this.localFS = FileSystem.getLocal(conf);
    this.appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    this.recoveryPath = new Path(TEST_ROOT_DIR + "/" + appId + "/recovery");
    this.localFS.delete(new Path(TEST_ROOT_DIR), true);
    mockAppMaster = mock(DAGAppMaster.class);
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
            null, "user", new Configuration(), null, null)));
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

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(true, dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("DAG Commit was in progress, not recoverable,"));
    // DAGSubmittedEvent is handled but DAGInitializedEvent and DAGStartedEvent in the next attempt are both skipped
    // due to the dag is not recoerable.
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    assertNull(dagData.getDAGInitializedEvent());
    assertNull(dagData.getDAGStartedEvent());
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
            null, "user", new Configuration(), null, null)));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGInitializedEvent(dagID, 1L, "user", dagPlan.getName(), null)));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGFinishedEvent(dagID, 1L, 2L, DAGState.FAILED, "diag", null, "user", "dag1", null,
            appAttemptId, dagPlan)));
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

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(false, dagData.nonRecoverable);
    assertEquals(DAGState.FAILED, dagData.dagState);
    assertEquals(true, dagData.isCompleted);
    // DAGSubmittedEvent, DAGInitializedEvent and DAGFinishedEvent is handled
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    // DAGInitializedEvent may not been handled before DAGFinishedEvent,
    // because DAGFinishedEvent's writeToRecoveryImmediately is true
    assertNotNull(dagData.getDAGFinishedEvent());
    assertNull(dagData.getDAGStartedEvent());
  }

  @Test(timeout = 5000)
  public void testLastCorruptedRecoveryRecord() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write data in attempt_1
    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // wait until DAGSubmittedEvent is handled in the RecoveryEventHandling thread
    rService.await();
    rService.outputStreamMap.get(dagID).write("INVALID_DATA".getBytes("UTF-8"));
    rService.stop();

    // write data in attempt_2
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/2"));
    rService = new RecoveryService(appContext);
    rService.init(conf);
    rService.start();
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGInitializedEvent(dagID, 1L, "user", dagPlan.getName(), null)));
    rService.await();
    rService.outputStreamMap.get(dagID).write("INVALID_DATA".getBytes("UTF-8"));
    rService.stop();

    // corrupted last records will be skipped but the whole recovery logs will be read
    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(false, dagData.isCompleted);
    assertEquals(null, dagData.reason);
    assertEquals(false, dagData.nonRecoverable);
    // verify DAGSubmitedEvent & DAGInititlizedEvent is handled.
    verify(mockAppMaster).createDAG(any(DAGPlan.class),any(TezDAGID.class));
    assertNotNull(dagData.getDAGInitializedEvent());
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
            null, "user", new Configuration(), null, null)));
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

  @Test(timeout=5000)
  public void testRecoverableSummary_DAGInCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGCommitStartedEvent(dagID, 0L)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertTrue(dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("DAG Commit was in progress"));
  }
  
  @Test(timeout=5000)
  public void testRecoverableSummary_DAGFinishCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGCommitStartedEvent(dagID, 0L)));
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGFinishedEvent(dagID, 1L, 2L, DAGState.FAILED, "diag", null, "user", "dag1", null,
            appAttemptId, dagPlan)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertEquals(DAGState.FAILED, dagData.dagState);
    assertFalse(dagData.nonRecoverable);
    assertNull(dagData.reason);
    assertTrue(dagData.isCompleted);
  }

  @Test(timeout=5000)
  public void testRecoverableSummary_VertexInCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexCommitStartedEvent(TezVertexID.getInstance(dagID, 0), 0L)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertTrue(dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("Vertex Commit was in progress"));
  }

  @Test(timeout=5000)
  public void testRecoverableSummary_VertexFinishCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    TezVertexID vertexId = TezVertexID.getInstance(dagID, 0);
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexCommitStartedEvent(vertexId, 0L)));
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexFinishedEvent(vertexId, "v1", 10, 0L, 0L, 
            0L, 0L, 0L, VertexState.SUCCEEDED, 
            "", null, null, null, null)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertFalse(dagData.nonRecoverable);
  }

  @Test(timeout=5000)
  public void testRecoverableSummary_VertexGroupInCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexGroupCommitStartedEvent(dagID, "group_1", 
            Lists.newArrayList(TezVertexID.getInstance(dagID, 0), TezVertexID.getInstance(dagID, 1)), 0L)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertTrue(dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("Vertex Group Commit was in progress"));
  }
  
  @Test(timeout=5000)
  public void testRecoverableSummary_VertexGroupFinishCommitting() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    TezVertexID v0 = TezVertexID.getInstance(dagID, 0);
    TezVertexID v1 = TezVertexID.getInstance(dagID, 1);
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexGroupCommitStartedEvent(dagID, "group_1", 
            Lists.newArrayList(v0, v1), 0L)));
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexGroupCommitFinishedEvent(dagID, "group_1", 
            Lists.newArrayList(v0, v1), 0L)));
    // also write VertexFinishedEvent, otherwise it is still non-recoverable
    // when checking with non-summary event
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexFinishedEvent(v0, "v1", 10, 0L, 0L, 
            0L, 0L, 0L, VertexState.SUCCEEDED, 
            "", null, null, null, null)));
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexFinishedEvent(v1, "v1", 10, 0L, 0L, 
            0L, 0L, 0L, VertexState.SUCCEEDED, 
            "", null, null, null, null)));
    rService.stop();
    
    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertEquals(dagID, dagData.recoveredDagID);
    assertFalse(dagData.nonRecoverable);
  }
  
  @Test(timeout=5000)
  public void testRecoverableNonSummary1() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    // MockRecoveryService will skip the non-summary event
    MockRecoveryService rService = new MockRecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    TezVertexID vertexId = TezVertexID.getInstance(dagID, 0);
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexCommitStartedEvent(vertexId, 0L)));
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexFinishedEvent(vertexId, "v1", 10, 0L, 0L, 
            0L, 0L, 0L, VertexState.SUCCEEDED, 
            "", null, null, null, null)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertTrue(dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("Vertex has been committed, but its full recovery events are not seen"));
  }
  
  @Test(timeout=5000)
  public void testRecoverableNonSummary2() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);

    // MockRecoveryService will skip the non-summary event
    MockRecoveryService rService = new MockRecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // write a DAGSubmittedEvent first to initialize summaryStream
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    // It should be fine to skip other events, just for testing.
    TezVertexID vertexId = TezVertexID.getInstance(dagID, 0);
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexGroupCommitStartedEvent(dagID, "group_1", 
            Lists.newArrayList(TezVertexID.getInstance(dagID, 0), TezVertexID.getInstance(dagID, 1)), 0L)));
    rService.handle(new DAGHistoryEvent(dagID,
        new VertexGroupCommitFinishedEvent(dagID, "group_1", 
            Lists.newArrayList(TezVertexID.getInstance(dagID, 0), TezVertexID.getInstance(dagID, 1)), 0L)));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertTrue(dagData.nonRecoverable);
    assertTrue(dagData.reason.contains("Vertex has been committed as member of vertex group"
              + ", but its full recovery events are not seen"));
  }

  @Test(timeout=20000)
  public void testRecoveryLargeEventData() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // DAG  DAGSubmittedEvent -> DAGInitializedEvent -> DAGStartedEvent
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagID, 100L,
        "user", "dagName", null);
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagID, 0L, "user", "dagName");
    rService.handle(new DAGHistoryEvent(dagID, dagInitedEvent));
    rService.handle(new DAGHistoryEvent(dagID, dagStartedEvent));

    // Create a Recovery event larger than 64 MB to verify default max protobuf size
    ArrayList<TaskLocationHint> taskLocationHints = new ArrayList<>(100000);
    TaskLocationHint taskLocationHint = TaskLocationHint.createTaskLocationHint(
        Sets.newHashSet("aaaaaaaaaaaaaaa.aaaaaaaaaaaaaaa.aaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbb.bbbbbbbbbbbbbbb.bbbbbbbbbbbbbbb",
            "ccccccccccccccc.ccccccccccccccc.ccccccccccccccc",
            "ddddddddddddddd.ddddddddddddddd.ddddddddddddddd",
            "eeeeeeeeeeeeeee.eeeeeeeeeeeeeee.eeeeeeeeeeeeeee",
            "fffffffffffffff.fffffffffffffff.fffffffffffffff",
            "ggggggggggggggg.ggggggggggggggg.ggggggggggggggg",
            "hhhhhhhhhhhhhhh.hhhhhhhhhhhhhhh.hhhhhhhhhhhhhhh",
            "iiiiiiiiiiiiiii.iiiiiiiiiiiiiii.iiiiiiiiiiiiiii",
            "jjjjjjjjjjjjjjj.jjjjjjjjjjjjjjj.jjjjjjjjjjjjjjj",
            "kkkkkkkkkkkkkkk.kkkkkkkkkkkkkkk.kkkkkkkkkkkkkkk",
            "lllllllllllllll.lllllllllllllll.lllllllllllllll",
            "mmmmmmmmmmmmmmm.mmmmmmmmmmmmmmm.mmmmmmmmmmmmmmm",
            "nnnnnnnnnnnnnnn.nnnnnnnnnnnnnnn.nnnnnnnnnnnnnnn"),
        Sets.newHashSet("rack1", "rack2", "rack3"));
    for (int i = 0; i < 100000; i++) {
      taskLocationHints.add(taskLocationHint);
    }

    TezVertexID v0Id = TezVertexID.getInstance(dagID, 0);
    VertexLocationHint vertexLocationHint = VertexLocationHint.create(taskLocationHints);
    VertexConfigurationDoneEvent vertexConfigurationDoneEvent = new VertexConfigurationDoneEvent(
        v0Id, 0, 100000, vertexLocationHint, null, null, false);
    // Verify large protobuf message
    assertTrue(vertexConfigurationDoneEvent.toProto().getSerializedSize() > PROTOBUF_DEFAULT_SIZE_LIMIT );
    rService.handle(new DAGHistoryEvent(dagID, vertexConfigurationDoneEvent));
    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    VertexRecoveryData v0data = dagData.getVertexRecoveryData(v0Id);
    assertNotNull("Vertex Recovery Data should be non-null", v0data);
    VertexConfigurationDoneEvent parsedVertexConfigurationDoneEvent = v0data.getVertexConfigurationDoneEvent();
    assertNotNull("Vertex Configuration Done Event should be non-null", parsedVertexConfigurationDoneEvent);
    VertexLocationHint parsedVertexLocationHint = parsedVertexConfigurationDoneEvent.getVertexLocationHint();
    assertNotNull("Vertex Location Hint should be non-null", parsedVertexLocationHint);
    assertEquals(parsedVertexLocationHint.getTaskLocationHints().size(), 100000);
  }

  @Test(timeout=5000)
  public void testRecoveryData() throws IOException {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    TezDAGID dagID = TezDAGID.getInstance(appId, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(recoveryPath+"/1"));
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(mockDAGImpl.getID()).thenReturn(dagID);
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    RecoveryService rService = new RecoveryService(appContext);
    Configuration conf = new Configuration();
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    rService.init(conf);
    rService.start();

    DAGPlan dagPlan = TestDAGImpl.createTestDAGPlan();
    // DAG  DAGSubmittedEvent -> DAGInitializedEvent -> DAGStartedEvent
    rService.handle(new DAGHistoryEvent(dagID,
        new DAGSubmittedEvent(dagID, 1L, dagPlan, ApplicationAttemptId.newInstance(appId, 1),
            null, "user", new Configuration(), null, null)));
    DAGInitializedEvent dagInitedEvent = new DAGInitializedEvent(dagID, 100L, 
        "user", "dagName", null);
    DAGStartedEvent dagStartedEvent = new DAGStartedEvent(dagID, 0L, "user", "dagName");
    rService.handle(new DAGHistoryEvent(dagID, dagInitedEvent));
    rService.handle(new DAGHistoryEvent(dagID, dagStartedEvent));
    
    // 3 vertices of this dag: v0, v1, v2
    TezVertexID v0Id = TezVertexID.getInstance(dagID, 0);
    TezVertexID v1Id = TezVertexID.getInstance(dagID, 1);
    TezVertexID v2Id = TezVertexID.getInstance(dagID, 2);
    // v0 VertexInitializedEvent
    VertexInitializedEvent v0InitedEvent =  new VertexInitializedEvent(
        v0Id, "v0", 200L, 400L, 2, null, null, null, null);
    rService.handle(new DAGHistoryEvent(dagID, v0InitedEvent));
    // v1 VertexFinishedEvent(KILLED)
    VertexFinishedEvent v1FinishedEvent = new VertexFinishedEvent(v1Id, "v1", 2, 300L, 400L, 
        500L, 600L, 700L, VertexState.KILLED, 
        "", null, null, null, null);
    rService.handle(new DAGHistoryEvent(dagID, v1FinishedEvent));
    // v2 VertexInitializedEvent -> VertexStartedEvent
    List<TezEvent> initGeneratedEvents = Lists.newArrayList(
        new TezEvent(DataMovementEvent.create(ByteBuffer.wrap(new byte[0])), null));
    VertexInitializedEvent v2InitedEvent = new VertexInitializedEvent(v2Id, "v2", 200L, 300L,
        2, null, null, initGeneratedEvents, null);
    VertexStartedEvent v2StartedEvent = new VertexStartedEvent(v2Id, 0L, 0L);
    rService.handle(new DAGHistoryEvent(dagID, v2InitedEvent));
    rService.handle(new DAGHistoryEvent(dagID, v2StartedEvent));

    // 3 tasks of v2
    TezTaskID t0v2Id = TezTaskID.getInstance(v2Id, 0);
    TezTaskID t1v2Id = TezTaskID.getInstance(v2Id, 1);
    TezTaskID t2v2Id = TezTaskID.getInstance(v2Id, 2);
    // t0v2 TaskStartedEvent
    TaskStartedEvent t0v2StartedEvent = new TaskStartedEvent(t0v2Id, "v2", 400L, 5000L);
    rService.handle(new DAGHistoryEvent(dagID, t0v2StartedEvent));
    // t1v2 TaskFinishedEvent
    TaskFinishedEvent t1v2FinishedEvent = new TaskFinishedEvent(t1v2Id, "v1",
        0L, 0L, null, TaskState.KILLED, "", null, 4);
    rService.handle(new DAGHistoryEvent(dagID, t1v2FinishedEvent));
    // t2v2 TaskStartedEvent -> TaskFinishedEvent
    TaskStartedEvent t2v2StartedEvent = new TaskStartedEvent(t2v2Id, "v2", 400L, 500L);
    rService.handle(new DAGHistoryEvent(dagID, t2v2StartedEvent));
    TaskFinishedEvent t2v2FinishedEvent = new TaskFinishedEvent(t2v2Id, "v1",
        0L, 0L, null, TaskState.SUCCEEDED, "", null, 4);
    rService.handle(new DAGHistoryEvent(dagID, t2v2FinishedEvent));

    // attempts under t0v2
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    NodeId nodeId = NodeId.newInstance("localhost", 9999);
    TezTaskAttemptID ta0t0v2Id = TezTaskAttemptID.getInstance(t0v2Id, 0);
    TaskAttemptStartedEvent ta0t0v2StartedEvent = new TaskAttemptStartedEvent(
        ta0t0v2Id, "v1", 0L, containerId, 
        nodeId, "", "", "");
    rService.handle(new DAGHistoryEvent(dagID, ta0t0v2StartedEvent));
    // attempts under t2v2
    TezTaskAttemptID ta0t2v2Id = TezTaskAttemptID.getInstance(t2v2Id, 0);
    TaskAttemptStartedEvent ta0t2v2StartedEvent = new TaskAttemptStartedEvent(
        ta0t2v2Id, "v1", 500L, containerId, 
        nodeId, "", "", "");
    rService.handle(new DAGHistoryEvent(dagID, ta0t2v2StartedEvent));
    TaskAttemptFinishedEvent ta0t2v2FinishedEvent = new TaskAttemptFinishedEvent(
        ta0t2v2Id, "v1", 500L, 600L, 
        TaskAttemptState.SUCCEEDED, null, null, "", null,
        null, null, 0L, null, 0L, null, null, null, null, null);
    rService.handle(new DAGHistoryEvent(dagID, ta0t2v2FinishedEvent));

    rService.stop();

    DAGRecoveryData dagData = parser.parseRecoveryData();
    assertFalse(dagData.nonRecoverable);
    // There's no equals method for the history event, so here only verify the init/start/finish time of each event for simplicity
    assertEquals(dagInitedEvent.getInitTime(), dagData.getDAGInitializedEvent().getInitTime());
    assertEquals(dagStartedEvent.getStartTime(), dagData.getDAGStartedEvent().getStartTime());
    assertNull(dagData.getDAGFinishedEvent());

    VertexRecoveryData v0Data = dagData.getVertexRecoveryData(v0Id);
    VertexRecoveryData v1Data = dagData.getVertexRecoveryData(v1Id);
    VertexRecoveryData v2Data = dagData.getVertexRecoveryData(v2Id);
    assertNotNull(v0Data);
    assertNotNull(v1Data);
    assertNotNull(v2Data);
    assertEquals(v0InitedEvent.getInitedTime(), v0Data.getVertexInitedEvent().getInitedTime());
    assertNull(v0Data.getVertexStartedEvent());
    assertNull(v1Data.getVertexInitedEvent());
    assertEquals(v1FinishedEvent.getFinishTime(), v1Data.getVertexFinishedEvent().getFinishTime());
    assertEquals(v2InitedEvent.getInitedTime(), v2Data.getVertexInitedEvent().getInitedTime());
    assertEquals(v2StartedEvent.getStartTime(), v2Data.getVertexStartedEvent().getStartTime());

    TaskRecoveryData t0v2Data = dagData.getTaskRecoveryData(t0v2Id);
    TaskRecoveryData t1v2Data = dagData.getTaskRecoveryData(t1v2Id);
    TaskRecoveryData t2v2Data = dagData.getTaskRecoveryData(t2v2Id);
    assertNotNull(t0v2Data);
    assertNotNull(t1v2Data);
    assertNotNull(t2v2Data);
    assertEquals(t0v2StartedEvent.getStartTime(), t0v2Data.getTaskStartedEvent().getStartTime());
    assertNull(t0v2Data.getTaskFinishedEvent());
    assertEquals(t1v2FinishedEvent.getFinishTime(), t1v2Data.getTaskFinishedEvent().getFinishTime());
    assertNull(t1v2Data.getTaskStartedEvent());
    assertEquals(t2v2StartedEvent.getStartTime(), t2v2Data.getTaskStartedEvent().getStartTime());
    assertEquals(t2v2FinishedEvent.getFinishTime(), t2v2Data.getTaskFinishedEvent().getFinishTime());

    TaskAttemptRecoveryData ta0t0v2Data = dagData.getTaskAttemptRecoveryData(ta0t0v2Id);
    TaskAttemptRecoveryData ta0t2v2Data = dagData.getTaskAttemptRecoveryData(ta0t2v2Id);
    assertNotNull(ta0t0v2Data);
    assertNotNull(ta0t2v2Data);
    assertEquals(ta0t0v2StartedEvent.getStartTime(), ta0t0v2Data.getTaskAttemptStartedEvent().getStartTime());
    assertNull(ta0t0v2Data.getTaskAttemptFinishedEvent());
    assertEquals(ta0t2v2StartedEvent.getStartTime(), ta0t2v2Data.getTaskAttemptStartedEvent().getStartTime());
    assertEquals(ta0t2v2FinishedEvent.getFinishTime(), ta0t2v2Data.getTaskAttemptFinishedEvent().getFinishTime());
  }

  // Simulate the behavior that summary event is written 
  // but non-summary is not written to hdfs
  public static class MockRecoveryService extends RecoveryService{

    public MockRecoveryService(AppContext appContext) {
      super(appContext);
    }

    @Override
    protected void handleRecoveryEvent(DAGHistoryEvent event)
        throws IOException {
      // skip the non-summary events
    }
  }
}

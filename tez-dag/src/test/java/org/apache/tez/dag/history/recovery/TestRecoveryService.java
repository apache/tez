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
package org.apache.tez.dag.history.recovery;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class TestRecoveryService {

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecoveryService.class.getName() + "-tmpDir";

  private Configuration conf;

  @Before
  public void setUp() throws IllegalArgumentException, IOException {
    this.conf = new Configuration();
    FileSystem localFS = FileSystem.getLocal(conf);
    localFS.delete(new Path(TEST_ROOT_DIR), true);
  }

  @Test(timeout = 5000)
  public void testDrainEvents() throws IOException {
    Configuration conf = new Configuration();
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(TEST_ROOT_DIR));
    when(appContext.getClock()).thenReturn(new SystemClock());

    MockRecoveryService recoveryService = new MockRecoveryService(appContext);
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    recoveryService.init(conf);
    recoveryService.start();
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(System.currentTimeMillis(), 1),1);
    int randEventCount = new Random().nextInt(100) + 100;
    for (int i=0; i< randEventCount; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new TaskStartedEvent(TezTaskID.getInstance(TezVertexID.getInstance(dagId, 1), 1), "v1", 0L, 0L)));
    }
    recoveryService.stop();
    assertEquals(randEventCount, recoveryService.processedRecoveryEventCounter.get());
  }

  @Test(timeout = 5000)
  public void testMultipleDAGFinishedEvent() throws IOException {
    Configuration conf = new Configuration();
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(TEST_ROOT_DIR));
    when(appContext.getClock()).thenReturn(new SystemClock());

    MockRecoveryService recoveryService = new MockRecoveryService(appContext);
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    recoveryService.init(conf);
    recoveryService.start();
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(System.currentTimeMillis(), 1),1);
    int randEventCount = new Random().nextInt(100) + 100;
    for (int i=0; i< randEventCount; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new TaskStartedEvent(TezTaskID.getInstance(TezVertexID.getInstance(dagId, 1), 1), "v1", 0L, 0L)));
    }
    recoveryService.await();
    assertTrue(recoveryService.outputStreamMap.containsKey(dagId));
    // 2 DAGFinishedEvent
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.FAILED, "diag", null, "user", "dag1", null,
            appAttemptId)));
    // outputStream removed
    assertFalse(recoveryService.outputStreamMap.containsKey(dagId));
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId)));
    // no new outputStream opened
    assertEquals(recoveryService.outputStreamMap.size(), 0);
    assertFalse(recoveryService.outputStreamMap.containsKey(dagId));
    recoveryService.stop();
  }

  @Test(timeout = 5000)
  public void testSummaryPathExisted() throws IOException {
    Configuration conf = new Configuration();
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(TEST_ROOT_DIR));
    when(appContext.getClock()).thenReturn(new SystemClock());

    MockRecoveryService recoveryService = new MockRecoveryService(appContext);
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    recoveryService.init(conf);
    recoveryService.start();
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(System.currentTimeMillis(), 1),1);
    Path dagRecoveryPath = TezCommonUtils.getSummaryRecoveryPath(recoveryService.recoveryPath);
    touchFile(dagRecoveryPath);
    assertFalse(recoveryService.hasRecoveryFailed());
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId)));
    assertTrue(recoveryService.hasRecoveryFailed());
    // be able to handle event after fatal error
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId)));
  }

  @Test(timeout = 5000)
  public void testRecoveryPathExisted() throws IOException {
    Configuration conf = new Configuration();
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(TEST_ROOT_DIR));
    when(appContext.getClock()).thenReturn(new SystemClock());

    MockRecoveryService recoveryService = new MockRecoveryService(appContext);
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    recoveryService.init(conf);
    recoveryService.start();
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(System.currentTimeMillis(), 1),1);
    Path dagRecoveryPath = TezCommonUtils.getDAGRecoveryPath(recoveryService.recoveryPath, dagId.toString());
    touchFile(dagRecoveryPath);
    assertFalse(recoveryService.hasRecoveryFailed());
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new TaskStartedEvent(TezTaskID.getInstance(TezVertexID.getInstance(dagId, 1), 1), "v1", 0L, 0L)));
    // wait for recovery event to be handled
    recoveryService.await();
    assertTrue(recoveryService.hasRecoveryFailed());
    // be able to handle recovery event after fatal error
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new TaskStartedEvent(TezTaskID.getInstance(TezVertexID.getInstance(dagId, 1), 1), "v1", 0L, 0L)));
  }

  private void touchFile(Path path) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.create(path).close();
  }

  private static class MockRecoveryService extends RecoveryService {

    public AtomicInteger processedRecoveryEventCounter = new AtomicInteger(0);

    public MockRecoveryService(AppContext appContext) {
      super(appContext);
    }

    @Override
    protected void handleRecoveryEvent(DAGHistoryEvent event)
        throws IOException {
      super.handleRecoveryEvent(event);
      processedRecoveryEventCounter.addAndGet(1);
    }
  }
}

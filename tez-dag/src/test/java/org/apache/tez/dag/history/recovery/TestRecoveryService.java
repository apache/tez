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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.DAGCommitStartedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.junit.Test;

public class TestRecoveryService {

  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecoveryService.class.getName() + "-tmpDir";

  private static final long startTime = System.currentTimeMillis();
  private static final ApplicationId appId = ApplicationId.newInstance(startTime, 1);
  private static final ApplicationAttemptId appAttemptId =
      ApplicationAttemptId.newInstance(appId, 1);
  private static final TezDAGID dagId = TezDAGID.getInstance(appId, 1);
  private static final TezVertexID vertexId = TezVertexID.getInstance(dagId, 1);
  private static final TezTaskID tezTaskId = TezTaskID.getInstance(vertexId, 1);

  private Configuration conf;
  private AppContext appContext;
  private MockRecoveryService recoveryService;
  private Path dagRecoveryPath;
  private Path summaryPath;
  private FileSystem fs;
  private FSDataOutputStream dagFos;
  private FSDataOutputStream summaryFos;

  private void setup(boolean useMockFs, String[][] configs) throws Exception {
    conf = new Configuration();
    if (configs != null) {
      for (String[] config : configs) {
        conf.set(config[0], config[1]);
      }
    }

    appContext = mock(AppContext.class);
    when(appContext.getClock()).thenReturn(new SystemClock());
    when(appContext.getHadoopShim()).thenReturn(new DefaultHadoopShim());
    when(appContext.getApplicationID()).thenReturn(appId);

    if (useMockFs) {
      fs = mock(FileSystem.class);
      when(appContext.getCurrentRecoveryDir()).thenReturn(new Path("mockfs:///"));
      conf.set("fs.mockfs.impl", MockFileSystem.class.getName());
      MockFileSystem.delegate = fs;
      dagFos = spy(new FSDataOutputStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {}
      }, null));
      summaryFos = spy(new FSDataOutputStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {}
      }, null));
    } else {
      when(appContext.getCurrentRecoveryDir()).thenReturn(new Path(TEST_ROOT_DIR));
      fs = FileSystem.getLocal(conf);
      fs.delete(new Path(TEST_ROOT_DIR), true);
    }

    recoveryService = new MockRecoveryService(appContext);
    conf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);
    recoveryService.init(conf);

    summaryPath = TezCommonUtils.getSummaryRecoveryPath(recoveryService.recoveryPath);
    dagRecoveryPath = TezCommonUtils.getDAGRecoveryPath(
        recoveryService.recoveryPath, dagId.toString());
    if (useMockFs) {
      when(fs.create(eq(dagRecoveryPath), eq(false), anyInt())).thenReturn(dagFos);
      when(fs.create(eq(summaryPath), eq(false), anyInt())).thenReturn(summaryFos);
    }
  }

  @Test(timeout = 5000)
  public void testDrainEvents() throws Exception {
    setup(false, null);
    recoveryService.start();
    int randEventCount = new Random().nextInt(100) + 100;
    for (int i=0; i< randEventCount; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new TaskStartedEvent(tezTaskId, "v1", 0L, 0L)));
    }
    recoveryService.stop();
    assertEquals(randEventCount, recoveryService.processedRecoveryEventCounter.get());
  }

  @Test(timeout = 5000)
  public void testMultipleDAGFinishedEvent() throws Exception {
    setup(false, null);
    recoveryService.start();
    int randEventCount = new Random().nextInt(100) + 100;
    for (int i=0; i< randEventCount; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new TaskStartedEvent(tezTaskId, "v1", 0L, 0L)));
    }
    recoveryService.await();
    assertTrue(recoveryService.outputStreamMap.containsKey(dagId));
    // 2 DAGFinishedEvent
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.FAILED, "diag", null, "user", "dag1", null,
            appAttemptId, null)));
    // outputStream removed
    assertFalse(recoveryService.outputStreamMap.containsKey(dagId));
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId, null)));
    // no new outputStream opened
    assertEquals(recoveryService.outputStreamMap.size(), 0);
    assertFalse(recoveryService.outputStreamMap.containsKey(dagId));
    recoveryService.stop();
  }

  @Test(timeout = 5000)
  public void testSummaryPathExisted() throws Exception {
    setup(false, null);
    recoveryService.start();
    touchFile(summaryPath);
    assertFalse(recoveryService.hasRecoveryFailed());
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId, null)));
    assertTrue(recoveryService.hasRecoveryFailed());
    // be able to handle event after fatal error
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGFinishedEvent(dagId, 1L, 2L, DAGState.ERROR, "diag", null, "user", "dag1", null,
            appAttemptId, null)));
    recoveryService.stop();
  }

  @Test(timeout = 5000)
  public void testRecoveryPathExisted() throws Exception {
    setup(false, null);
    recoveryService.start();
    touchFile(dagRecoveryPath);
    assertFalse(recoveryService.hasRecoveryFailed());
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new TaskStartedEvent(tezTaskId, "v1", 0L, 0L)));
    // wait for recovery event to be handled
    recoveryService.await();
    assertTrue(recoveryService.hasRecoveryFailed());
    // be able to handle recovery event after fatal error
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new TaskStartedEvent(tezTaskId, "v1", 0L, 0L)));
    recoveryService.stop();
  }

  @Test(timeout=5000)
  public void testRecoveryFlushOnMaxEvents() throws Exception {
    setup(true, new String[][] {
        {TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, "10"},
        {TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS, "-1"}
      });
    recoveryService.start();

    // Send 1 event less, wait for drain
    for (int i = 0; i < 9; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    }
    waitForDrain(-1);
    verify(dagFos, times(0)).hflush();

    // This event should cause the flush.
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    waitForDrain(-1);
    verify(dagFos, times(1)).hflush();

    recoveryService.stop();
  }

  @Test(timeout=10000)
  public void testRecoveryFlushOnTimeoutEvents() throws Exception {
    setup(true, new String[][] {
      {TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, "-1"},
      {TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS, "5"}
    });
    recoveryService.start();

    // Send lot of events.
    for (int i = 0; i < TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS_DEFAULT; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    }
    // wait for timeout.
    Thread.sleep(5000);
    assertTrue(recoveryService.eventQueue.isEmpty());
    verify(fs, times(1)).create(eq(dagRecoveryPath), eq(false), anyInt());
    verify(dagFos, times(0)).hflush();

    // The flush is trigged by sending 1 event after the timeout.
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    waitForDrain(1000);
    verify(dagFos, times(1)).hflush();

    recoveryService.stop();
  }

  @Test(timeout=10000)
  public void testRecoveryFlush() throws Exception {
    setup(true, new String[][] {
      {TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, "10"},
      {TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS, "5"}
    });
    recoveryService.start();

    // 5 second flush
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    Thread.sleep(5000);
    assertTrue(recoveryService.eventQueue.isEmpty());
    verify(fs, times(1)).create(eq(dagRecoveryPath), eq(false), anyInt());
    verify(dagFos, times(0)).hflush();
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    waitForDrain(1000);
    verify(dagFos, times(1)).hflush();

    // Number of events flush.
    for (int i = 0; i < 9; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    }
    waitForDrain(-1);
    verify(dagFos, times(1)).hflush();
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    waitForDrain(-1);
    verify(dagFos, times(2)).hflush();

    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));

    recoveryService.stop();
  }

  @Test(timeout=50000)
  public void testRecoveryFlushOnStop() throws Exception {
    setup(true, new String[][] {
      {TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, "-1"},
      {TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS, "-1"}
    });
    recoveryService.start();

    // Does not flush on event counts.
    for (int i = 0; i < TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS_DEFAULT; ++i) {
      recoveryService.handle(new DAGHistoryEvent(dagId,
          new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    }
    waitForDrain(-1);
    verify(dagFos, times(0)).hflush();

    // Does not flush on timeout.
    Thread.sleep(TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS_DEFAULT * 1000);
    recoveryService.handle(new DAGHistoryEvent(dagId,
        new DAGStartedEvent(dagId, startTime, "nobody", "test-dag")));
    waitForDrain(-1);
    verify(dagFos, times(0)).hflush();

    // Does flush on stop.
    recoveryService.stop();
    verify(dagFos, times(1)).hflush();
  }

  @Test(timeout=5000)
  public void testRecoveryFlushOnSummaryEvent() throws Exception {
    setup(true, new String[][] {
      {TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, "-1"},
      {TezConfiguration.DAG_RECOVERY_FLUSH_INTERVAL_SECS, "-1"}
    });
    recoveryService.start();

    DAGPlan dagPlan = DAGPlan.newBuilder().setName("test_dag").build();
    // This writes to recovery immediately.
    recoveryService.handle(new DAGHistoryEvent(dagId, new DAGSubmittedEvent(
        dagId, startTime, dagPlan, appAttemptId, null, "nobody", conf, null, "default")));
    waitForDrain(-1);
    verify(summaryFos, times(1)).hflush();
    verify(dagFos, times(1)).hflush();

    // This does not write to recovery immediately.
    recoveryService.handle(new DAGHistoryEvent(dagId, new DAGCommitStartedEvent(dagId, startTime)));
    waitForDrain(-1);
    verify(summaryFos, times(2)).hflush();
    verify(dagFos, times(1)).hflush();

    // Does flush on stop.
    recoveryService.stop();
    verify(dagFos, times(2)).hflush();
  }

  private void waitForDrain(int limit) throws Exception {
    long maxTime = System.currentTimeMillis() + limit;
    while (!recoveryService.eventQueue.isEmpty()) {
      Thread.sleep(10);
      if (limit != -1 && System.currentTimeMillis() > maxTime) {
        break;
      }
    }
  }

  private void touchFile(Path path) throws IOException {
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

  // Public access to ensure it can be created through reflection.
  public static class MockFileSystem extends FileSystem {
    // Should be set to a mock fs in the test, only one instance of this class can run.
    static FileSystem delegate;

    static URI uri = URI.create("mockfs:///");

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return delegate.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
      return delegate.create(f, overwrite, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize, Progressable progress)
        throws IOException {
      return delegate.create(f, permission, overwrite, bufferSize, replication, blockSize,
          progress);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        throws IOException {
      return delegate.append(f, bufferSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      return delegate.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return delegate.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
      return delegate.listStatus(f);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
      delegate.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory() {
      return delegate.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      return delegate.mkdirs(f, permission);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return delegate.getFileStatus(f);
    }
  }
}

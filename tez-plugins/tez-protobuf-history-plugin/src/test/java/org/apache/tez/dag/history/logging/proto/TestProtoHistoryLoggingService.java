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

package org.apache.tez.dag.history.logging.proto;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.ManifestEntryProto;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestProtoHistoryLoggingService {
  private static ApplicationId appId = ApplicationId.newInstance(1000l, 1);
  private static ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
  private static String user = "TEST_USER";
  private Clock clock;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testService() throws Exception {
    ProtoHistoryLoggingService service = createService();
    service.start();
    TezDAGID dagId = TezDAGID.getInstance(appId, 0);
    List<HistoryEventProto> protos = new ArrayList<>();
    for (DAGHistoryEvent event : makeHistoryEvents(dagId, service)) {
      protos.add(new HistoryEventProtoConverter().convert(event.getHistoryEvent()));
      service.handle(event);
    }
    service.stop();

    TezProtoLoggers loggers = new TezProtoLoggers();
    Assert.assertTrue(loggers.setup(service.getConfig(), clock));

    // Verify dag events are logged.
    DatePartitionedLogger<HistoryEventProto> dagLogger = loggers.getDagEventsLogger();
    Path dagFilePath = dagLogger.getPathForDate(LocalDate.ofEpochDay(0), dagId.toString() + "_" + 1);
    ProtoMessageReader<HistoryEventProto> reader = dagLogger.getReader(dagFilePath);
    HistoryEventProto evt = reader.readEvent();
    int ind = 1;
    while (evt != null) {
      Assert.assertEquals(protos.get(ind), evt);
      ind++;
      try {
        evt = reader.readEvent();
      } catch (EOFException e) {
        evt = null;
      }
    }
    reader.close();

    // Verify app events are logged.
    DatePartitionedLogger<HistoryEventProto> appLogger = loggers.getAppEventsLogger();
    Path appFilePath = appLogger.getPathForDate(LocalDate.ofEpochDay(0), attemptId.toString());
    ProtoMessageReader<HistoryEventProto> appReader = appLogger.getReader(appFilePath);
    long appOffset = appReader.getOffset();
    Assert.assertEquals(protos.get(0), appReader.readEvent());
    reader.close();

    // Verify manifest events are logged.
    DatePartitionedLogger<ManifestEntryProto> manifestLogger = loggers.getManifestEventsLogger();
    Path manifestFilePath = manifestLogger.getPathForDate(
        LocalDate.ofEpochDay(0), attemptId.toString());
    ProtoMessageReader<ManifestEntryProto> reader2 = manifestLogger.getReader(manifestFilePath);
    ManifestEntryProto manifest = reader2.readEvent();
    Assert.assertEquals(appId.toString(), manifest.getAppId());
    Assert.assertEquals(dagId.toString(), manifest.getDagId());
    Assert.assertEquals(dagFilePath.toString(), manifest.getDagFilePath());
    Assert.assertEquals(appFilePath.toString(), manifest.getAppFilePath());
    Assert.assertEquals(appOffset, manifest.getAppLaunchedEventOffset());

    // Verify offsets in manifest logger.
    reader = dagLogger.getReader(new Path(manifest.getDagFilePath()));
    reader.setOffset(manifest.getDagSubmittedEventOffset());
    evt = reader.readEvent();
    Assert.assertNotNull(evt);
    Assert.assertEquals(HistoryEventType.DAG_SUBMITTED.name(), evt.getEventType());

    reader.setOffset(manifest.getDagFinishedEventOffset());
    evt = reader.readEvent();
    Assert.assertNotNull(evt);
    Assert.assertEquals(HistoryEventType.DAG_FINISHED.name(), evt.getEventType());

    // Verify manifest file scanner.
    DagManifesFileScanner scanner = new DagManifesFileScanner(manifestLogger);
    Assert.assertEquals(manifest, scanner.getNext());
    Assert.assertNull(scanner.getNext());
    scanner.close();
  }

  private List<DAGHistoryEvent> makeHistoryEvents(TezDAGID dagId,
      ProtoHistoryLoggingService service) {
    List<DAGHistoryEvent> historyEvents = new ArrayList<>();
    DAGPlan dagPlan = DAGPlan.newBuilder().setName("DAGPlanMock").build();

    long time = System.currentTimeMillis();
    Configuration conf = new Configuration(service.getConfig());
    historyEvents.add(new DAGHistoryEvent(null, new AppLaunchedEvent(appId, time, time, user, conf,
        new VersionInfo("component", "1.1.0", "rev1", "20120101", "git.apache.org") {})));
    historyEvents.add(new DAGHistoryEvent(dagId, new DAGSubmittedEvent(dagId, time,
        DAGPlan.getDefaultInstance(), attemptId, null, user, conf, null, "default")));
    TezVertexID vertexID = TezVertexID.getInstance(dagId, 1);
    historyEvents.add(new DAGHistoryEvent(dagId, new VertexStartedEvent(vertexID, time, time)));
    TezTaskID tezTaskID = TezTaskID.getInstance(vertexID, 1);
    historyEvents
        .add(new DAGHistoryEvent(dagId, new TaskStartedEvent(tezTaskID, "test", time, time)));
    historyEvents.add(new DAGHistoryEvent(dagId,
        new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(tezTaskID, 1), "test", time,
            ContainerId.newContainerId(attemptId, 1), NodeId.newInstance("localhost", 8765), null,
            null, null)));
    historyEvents.add(new DAGHistoryEvent(dagId, new DAGFinishedEvent(dagId, time, time,
        DAGState.ERROR, "diagnostics", null, user, dagPlan.getName(),
        new HashMap<String, Integer>(), attemptId, dagPlan)));
    return historyEvents;
  }

  private static class FixedClock implements Clock {
    final Clock clock = new SystemClock();
    final long diff;

    public FixedClock(long startTime) {
      diff = clock.getTime() - startTime;
    }

    @Override
    public long getTime() {
      return clock.getTime() - diff;
    }
  }

  private ProtoHistoryLoggingService createService() throws IOException {
    ProtoHistoryLoggingService service = new ProtoHistoryLoggingService();
    clock = new FixedClock(0); // Start time is always first day, easier to write tests.
    AppContext appContext = mock(AppContext.class);
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getApplicationAttemptId()).thenReturn(attemptId);
    when(appContext.getUser()).thenReturn(user);
    when(appContext.getHadoopShim()).thenReturn(new HadoopShim() {});
    when(appContext.getClock()).thenReturn(clock);
    service.setAppContext(appContext);
    Configuration conf = new Configuration(false);
    String basePath = tempFolder.newFolder().getAbsolutePath();
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR, basePath);
    service.init(conf);
    return service;
  }
}

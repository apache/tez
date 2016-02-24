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
package org.apache.tez.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.RecoveryParser;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexConfigurationDoneEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.examples.TezExampleBase;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.test.RecoveryServiceWithEventHandlingHook.SimpleRecoveryEventHook;
import org.apache.tez.test.RecoveryServiceWithEventHandlingHook.SimpleShutdownCondition;
import org.apache.tez.test.RecoveryServiceWithEventHandlingHook.SimpleShutdownCondition.TIMING;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TestRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(TestRecovery.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster = null;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestRecovery.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem remoteFs = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini clusters");
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestRecovery.class.getName(), 1, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 4);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();
    }
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    if (miniTezCluster != null) {
      try {
        LOG.info("Stopping MiniTezCluster");
        miniTezCluster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (dfsCluster != null) {
      try {
        LOG.info("Stopping DFSCluster");
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test(timeout=1800000)
  public void testRecovery_OrderedWordCount() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId0 = TezVertexID.getInstance(dagId, 0);
    TezVertexID vertexId1 = TezVertexID.getInstance(dagId, 1);
    TezVertexID vertexId2 = TezVertexID.getInstance(dagId, 2);
    ContainerId containerId = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(appId, 1), 1);
    NodeId nodeId = NodeId.newInstance("localhost", 10);
    
    List<TezEvent> initGeneratedEvents = Lists.newArrayList(
            new TezEvent(InputDataInformationEvent.createWithObjectPayload(0, new Object()), null));

    List<SimpleShutdownCondition> shutdownConditions = Lists
        .newArrayList(
            new SimpleShutdownCondition(TIMING.POST, new DAGInitializedEvent(
                dagId, 0L, "username", "dagName", null)),
            new SimpleShutdownCondition(TIMING.POST, new DAGStartedEvent(dagId,
                0L, "username", "dagName")),
            new SimpleShutdownCondition(TIMING.POST, new DAGFinishedEvent(
                dagId, 0L, 0L, DAGState.SUCCEEDED, "", new TezCounters(),
                "username", "dagName", new HashMap<String, Integer>(),
                ApplicationAttemptId.newInstance(appId, 1), null)),
            new SimpleShutdownCondition(TIMING.POST,
                new VertexInitializedEvent(vertexId0, "Tokenizer", 0L, 0L, 0,
                    "", null, initGeneratedEvents)),
            new SimpleShutdownCondition(TIMING.POST,
                new VertexInitializedEvent(vertexId1, "Summation", 0L, 0L, 0,
                    "", null, null)),
            new SimpleShutdownCondition(TIMING.POST,
                new VertexInitializedEvent(vertexId2, "Sorter", 0L, 0L, 0, "",
                    null, null)),

            new SimpleShutdownCondition(TIMING.POST,
                new VertexConfigurationDoneEvent(vertexId0, 0L, 2, null, null,
                    null, true)),
                        
            new SimpleShutdownCondition(TIMING.POST,
                new VertexConfigurationDoneEvent(vertexId1, 0L, 2, null, null,
                    null, true)),

            new SimpleShutdownCondition(TIMING.POST,
                new VertexConfigurationDoneEvent(vertexId2, 0L, 2, null, null,
                    null, true)),
            new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
                vertexId0, 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
                vertexId1, 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
                vertexId2, 0L, 0L)),

            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                vertexId0, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                VertexState.SUCCEEDED, "", new TezCounters(),
                new VertexStats(), new HashMap<String, Integer>())),
            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                vertexId1, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                VertexState.SUCCEEDED, "", new TezCounters(),
                new VertexStats(), new HashMap<String, Integer>())),
            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                vertexId2, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                VertexState.SUCCEEDED, "", new TezCounters(),
                new VertexStats(), new HashMap<String, Integer>())),

            new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(
                TezTaskID.getInstance(vertexId0, 0), "vertexName", 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(
                TezTaskID.getInstance(vertexId1, 0), "vertexName", 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(
                TezTaskID.getInstance(vertexId2, 0), "vertexName", 0L, 0L)),

            new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
                TezTaskID.getInstance(vertexId0, 0), "vertexName", 0L, 0L,
                null, TaskState.SUCCEEDED, "", new TezCounters(), 0)),
            new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
                TezTaskID.getInstance(vertexId1, 0), "vertexName", 0L, 0L,
                null, TaskState.SUCCEEDED, "", new TezCounters(), 0)),
            new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
                TezTaskID.getInstance(vertexId2, 0), "vertexName", 0L, 0L,
                null, TaskState.SUCCEEDED, "", new TezCounters(), 0)),

            new SimpleShutdownCondition(TIMING.POST,
                new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                    TezTaskID.getInstance(vertexId0, 0), 0), "vertexName", 0L,
                    containerId, nodeId, "", "", "")),
            new SimpleShutdownCondition(TIMING.POST,
                new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                    TezTaskID.getInstance(vertexId1, 0), 0), "vertexName", 0L,
                    containerId, nodeId, "", "", "")),
            new SimpleShutdownCondition(TIMING.POST,
                new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                    TezTaskID.getInstance(vertexId2, 0), 0), "vertexName", 0L,
                    containerId, nodeId, "", "", ""))

        );

    Random rand = new Random();
    for (int i = 0; i < shutdownConditions.size(); i++) {
      // randomly choose half of the test scenario to avoid
      // timeout.
      if (rand.nextDouble() < 0.5) {
        // generate split in client side when HistoryEvent type is VERTEX_STARTED (TEZ-2976)
        testOrderedWordCount(shutdownConditions.get(i), true,
            shutdownConditions.get(i).getHistoryEvent().getEventType() == HistoryEventType.VERTEX_STARTED);
      }
    }
  }

  private void testOrderedWordCount(SimpleShutdownCondition shutdownCondition,
      boolean enableAutoParallelism, boolean generateSplitInClient) throws Exception {
    LOG.info("shutdownCondition:" + shutdownCondition.getEventType()
        + ", event=" + shutdownCondition.getEvent());
    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    TestTezJobs.generateOrderedWordCountInput(inputDir, remoteFs);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.set(TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS,
        RecoveryServiceWithEventHandlingHook.class.getName());
    tezConf.set(
        RecoveryServiceWithEventHandlingHook.AM_RECOVERY_SERVICE_HOOK_CLASS,
        SimpleRecoveryEventHook.class.getName());
    tezConf.set(SimpleRecoveryEventHook.SIMPLE_SHUTDOWN_CONDITION,
        shutdownCondition.serialize());
    tezConf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        enableAutoParallelism);
    tezConf.setBoolean(
        RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, false);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    tezConf.setBoolean(
        TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, false);
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "INFO;org.apache.tez=DEBUG");
    OrderedWordCount job = new OrderedWordCount();
    if (generateSplitInClient) {
      Assert
          .assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{
              "-generateSplitInClient", inputDirStr, outputDirStr, "5"}, null) == 0);
    } else {
      Assert
          .assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{
              inputDirStr, outputDirStr, "5"}, null) == 0);
    }
    TestTezJobs.verifyOutput(outputDir, remoteFs);
    List<HistoryEvent> historyEventsOfAttempt1 = RecoveryParser
        .readRecoveryEvents(tezConf, job.getAppId(), 1);
    HistoryEvent lastEvent = historyEventsOfAttempt1
        .get(historyEventsOfAttempt1.size() - 1);
    assertEquals(shutdownCondition.getEvent().getEventType(),
        lastEvent.getEventType());
    assertTrue(shutdownCondition.match(lastEvent));

  }

  private void testOrderedWordCountMultipleRoundRecoverying(
          RecoveryServiceWithEventHandlingHook.MultipleRoundShutdownCondition shutdownCondition,
          boolean enableAutoParallelism, boolean generateSplitInClient) throws Exception {

    for (int i=0; i<shutdownCondition.size(); i++) {
      SimpleShutdownCondition condition = shutdownCondition.getSimpleShutdownCondition(i);
      LOG.info("ShutdownCondition:" + condition.getEventType()
              + ", event=" + condition.getEvent());
    }

    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    TestTezJobs.generateOrderedWordCountInput(inputDir, remoteFs);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.set(TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS,
            RecoveryServiceWithEventHandlingHook.class.getName());
    tezConf.set(
            RecoveryServiceWithEventHandlingHook.AM_RECOVERY_SERVICE_HOOK_CLASS,
            RecoveryServiceWithEventHandlingHook.MultipleRoundRecoveryEventHook.class.getName());
    tezConf.set(RecoveryServiceWithEventHandlingHook.MultipleRoundRecoveryEventHook.MULTIPLE_ROUND_SHUTDOWN_CONDITION,
            shutdownCondition.serialize());
    tezConf.setBoolean(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            enableAutoParallelism);
    tezConf.setBoolean(
            RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, false);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    tezConf.setBoolean(
            TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, false);
    OrderedWordCount job = new OrderedWordCount();
    if (generateSplitInClient) {
      Assert
              .assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{
                      "-generateSplitInClient", inputDirStr, outputDirStr, "5"}, null) == 0);
    } else {
      Assert
              .assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{
                      inputDirStr, outputDirStr, "5"}, null) == 0);
    }
    TestTezJobs.verifyOutput(outputDir, remoteFs);
  }

  @Test(timeout = 1800000)
  public void testRecovery_HashJoin() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
        1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId0 = TezVertexID.getInstance(dagId, 0);
    TezVertexID vertexId1 = TezVertexID.getInstance(dagId, 1);
    TezVertexID vertexId2 = TezVertexID.getInstance(dagId, 2);
    ContainerId containerId = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(appId, 1), 1);
    NodeId nodeId = NodeId.newInstance("localhost", 10);
    List<TezEvent> initGeneratedEvents = Lists.newArrayList(
        new TezEvent(InputDataInformationEvent.createWithObjectPayload(0, new Object()), null));

    List<SimpleShutdownCondition> shutdownConditions = Lists.newArrayList(

        new SimpleShutdownCondition(TIMING.POST, new DAGInitializedEvent(dagId,
            0L, "username", "dagName", null)),
        new SimpleShutdownCondition(TIMING.POST, new DAGStartedEvent(dagId, 0L,
            "username", "dagName")),
        new SimpleShutdownCondition(TIMING.POST, new DAGFinishedEvent(dagId,
            0L, 0L, DAGState.SUCCEEDED, "", new TezCounters(), "username",
            "dagName", new HashMap<String, Integer>(), ApplicationAttemptId
                .newInstance(appId, 1), null)),
        new SimpleShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId0, "hashSide", 0L, 0L, 0, "", null, initGeneratedEvents)),
        new SimpleShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId1, "streamingSide", 0L, 0L, 0, "", null, null)),
        new SimpleShutdownCondition(TIMING.POST, new VertexInitializedEvent(
            vertexId2, "joiner", 0L, 0L, 0, "", null, null)),

        new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
            vertexId0, 0L, 0L)),
        new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
            vertexId1, 0L, 0L)),
        new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
            vertexId2, 0L, 0L)),

        new SimpleShutdownCondition(TIMING.POST,
            new VertexConfigurationDoneEvent(vertexId0, 0L, 2, null, null,
                null, true)),
                    
        new SimpleShutdownCondition(TIMING.POST,
            new VertexConfigurationDoneEvent(vertexId1, 0L, 2, null, null,
                null, true)),

        new SimpleShutdownCondition(TIMING.POST,
            new VertexConfigurationDoneEvent(vertexId2, 0L, 2, null, null,
                null, true)),
                    
        new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
            vertexId0, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),
        new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
            vertexId1, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),
        new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
            vertexId2, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
            VertexState.SUCCEEDED, "", new TezCounters(), new VertexStats(),
            new HashMap<String, Integer>())),

        new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId0, 0), "vertexName", 0L, 0L)),
        new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId1, 0), "vertexName", 0L, 0L)),
        new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(TezTaskID
            .getInstance(vertexId2, 0), "vertexName", 0L, 0L)),

        new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
            TezTaskID.getInstance(vertexId0, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),
        new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
            TezTaskID.getInstance(vertexId1, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),
        new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
            TezTaskID.getInstance(vertexId2, 0), "vertexName", 0L, 0L, null,
            TaskState.SUCCEEDED, "", new TezCounters(), 0)),

        new SimpleShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId0, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", "")),
        new SimpleShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId1, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", "")),
        new SimpleShutdownCondition(TIMING.POST,
            new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                TezTaskID.getInstance(vertexId2, 0), 0), "vertexName", 0L,
                containerId, nodeId, "", "", ""))

    );

    Random rand = new Random();
    for (int i = 0; i < shutdownConditions.size(); i++) {
      // randomly choose half of the test scenario to avoid
      // timeout.
      if (rand.nextDouble() < 0.5) {
        // generate split in client side when HistoryEvent type is VERTEX_STARTED (TEZ-2976)
        testHashJoinExample(shutdownConditions.get(i), true,
            shutdownConditions.get(i).getHistoryEvent().getEventType() == HistoryEventType.VERTEX_STARTED);
      }
    }
  }

  private void testHashJoinExample(SimpleShutdownCondition shutdownCondition,
      boolean enableAutoParallelism, boolean generateSplitInClient) throws Exception {
    HashJoinExample hashJoinExample = new HashJoinExample();
    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.set(TezConfiguration.TEZ_AM_RECOVERY_SERVICE_CLASS,
        RecoveryServiceWithEventHandlingHook.class.getName());
    tezConf.set(
        RecoveryServiceWithEventHandlingHook.AM_RECOVERY_SERVICE_HOOK_CLASS,
        SimpleRecoveryEventHook.class.getName());
    tezConf.set(SimpleRecoveryEventHook.SIMPLE_SHUTDOWN_CONDITION,
        shutdownCondition.serialize());
    tezConf.setBoolean(
        ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
        enableAutoParallelism);
    tezConf.setBoolean(
        RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, false);
    tezConf.setBoolean(
        TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, false);
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "INFO;org.apache.tez=DEBUG");

    hashJoinExample.setConf(tezConf);
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    Path inPath1 = new Path("/tmp/hashJoin/inPath1");
    Path inPath2 = new Path("/tmp/hashJoin/inPath2");
    Path outPath = new Path("/tmp/hashJoin/outPath");
    remoteFs.delete(outPath, true);
    remoteFs.mkdirs(inPath1);
    remoteFs.mkdirs(inPath2);
    remoteFs.mkdirs(stagingDirPath);

    Set<String> expectedResult = new HashSet<String>();

    FSDataOutputStream out1 = remoteFs.create(new Path(inPath1, "file"));
    FSDataOutputStream out2 = remoteFs.create(new Path(inPath2, "file"));
    BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(out1));
    BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(out2));
    for (int i = 0; i < 20; i++) {
      String term = "term" + i;
      writer1.write(term);
      writer1.newLine();
      if (i % 2 == 0) {
        writer2.write(term);
        writer2.newLine();
        expectedResult.add(term);
      }
    }
    writer1.close();
    writer2.close();
    out1.close();
    out2.close();

    String[] args = null;
    if (generateSplitInClient) {
      args = new String[]{
          "-D" + TezConfiguration.TEZ_AM_STAGING_DIR + "="
              + stagingDirPath.toString(),
          "-generateSplitInClient",
          inPath1.toString(), inPath2.toString(), "1", outPath.toString()};
    } else {
      args = new String[]{
          "-D" + TezConfiguration.TEZ_AM_STAGING_DIR + "="
              + stagingDirPath.toString(),
          inPath1.toString(), inPath2.toString(), "1", outPath.toString()};
    }
    assertEquals(0, hashJoinExample.run(args));

    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());

    List<HistoryEvent> historyEventsOfAttempt1 = RecoveryParser
        .readRecoveryEvents(tezConf, hashJoinExample.getAppId(), 1);
    HistoryEvent lastEvent = historyEventsOfAttempt1
        .get(historyEventsOfAttempt1.size() - 1);
    assertEquals(shutdownCondition.getEvent().getEventType(),
        lastEvent.getEventType());
    assertTrue(shutdownCondition.match(lastEvent));
  }

  @Test(timeout = 1800000)
  public void testTwoRoundsRecoverying() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(),
            1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vertexId0 = TezVertexID.getInstance(dagId, 0);
    TezVertexID vertexId1 = TezVertexID.getInstance(dagId, 1);
    TezVertexID vertexId2 = TezVertexID.getInstance(dagId, 2);
    ContainerId containerId = ContainerId.newInstance(
            ApplicationAttemptId.newInstance(appId, 1), 1);
    NodeId nodeId = NodeId.newInstance("localhost", 10);
    List<TezEvent> initGeneratedEvents = Lists.newArrayList(
            new TezEvent(InputDataInformationEvent.createWithObjectPayload(0, new Object()), null));


    List<SimpleShutdownCondition> shutdownConditions = Lists.newArrayList(

            new SimpleShutdownCondition(TIMING.POST, new DAGInitializedEvent(
                    dagId, 0L, "username", "dagName", null)),
            new SimpleShutdownCondition(TIMING.POST, new DAGStartedEvent(dagId,
                    0L, "username", "dagName")),
            new SimpleShutdownCondition(TIMING.POST,
                    new VertexInitializedEvent(vertexId0, "Tokenizer", 0L, 0L, 0,
                            "", null, initGeneratedEvents)),
            new SimpleShutdownCondition(TIMING.POST, new VertexStartedEvent(
                    vertexId0, 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST,
                    new VertexConfigurationDoneEvent(vertexId0, 0L, 2, null, null,
                            null, true)),
            new SimpleShutdownCondition(TIMING.POST, new TaskStartedEvent(
                    TezTaskID.getInstance(vertexId0, 0), "vertexName", 0L, 0L)),
            new SimpleShutdownCondition(TIMING.POST,
                    new TaskAttemptStartedEvent(TezTaskAttemptID.getInstance(
                            TezTaskID.getInstance(vertexId0, 0), 0), "vertexName", 0L,
                            containerId, nodeId, "", "", "")),
            new SimpleShutdownCondition(TIMING.POST, new TaskFinishedEvent(
                    TezTaskID.getInstance(vertexId0, 0), "vertexName", 0L, 0L,
                    null, TaskState.SUCCEEDED, "", new TezCounters(), 0)),
            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                    vertexId0, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                    VertexState.SUCCEEDED, "", new TezCounters(),
                    new VertexStats(), new HashMap<String, Integer>())),
            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                    vertexId1, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                    VertexState.SUCCEEDED, "", new TezCounters(),
                    new VertexStats(), new HashMap<String, Integer>())),
            new SimpleShutdownCondition(TIMING.POST, new VertexFinishedEvent(
                    vertexId2, "vertexName", 1, 0L, 0L, 0L, 0L, 0L,
                    VertexState.SUCCEEDED, "", new TezCounters(),
                    new VertexStats(), new HashMap<String, Integer>())),
            new SimpleShutdownCondition(TIMING.POST, new DAGFinishedEvent(
                    dagId, 0L, 0L, DAGState.SUCCEEDED, "", new TezCounters(),
                    "username", "dagName", new HashMap<String, Integer>(),
                    ApplicationAttemptId.newInstance(appId, 1), null))

    );

    Random rand = new Random();
    for (int i = 0; i < shutdownConditions.size() - 1; i++) {
      // randomly choose half of the test scenario to avoid
      // timeout.
      if (rand.nextDouble()<0.5) {
        int nextSimpleConditionIndex = i + 1 + rand.nextInt(shutdownConditions.size() - i - 1);
        if (nextSimpleConditionIndex == shutdownConditions.size() - 1) {
          testOrderedWordCountMultipleRoundRecoverying(
                  new RecoveryServiceWithEventHandlingHook.MultipleRoundShutdownCondition(
                          Lists.newArrayList(shutdownConditions.get(i), shutdownConditions.get(nextSimpleConditionIndex)))
                  , true,
                  shutdownConditions.get(i).getHistoryEvent().getEventType() == HistoryEventType.VERTEX_STARTED);
        }
      }
    }
  }
}

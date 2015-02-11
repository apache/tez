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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.app.RecoveryParser;
import org.apache.tez.dag.app.dag.impl.ImmediateStartVertexManager;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestAMRecovery {

  private static final Log LOG = LogFactory.getLog(TestAMRecovery.class);

  private static Configuration conf = new Configuration();
  private static TezConfiguration tezConf;
  private static int MAX_AM_ATTEMPT = 10;
  private static MiniTezCluster miniTezCluster = null;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestAMRecovery.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static TezClient tezSession = null;
  private static FileSystem remoteFs = null;
  private static String FAIL_ON_PARTIAL_FINISHED = "FAIL_ON_PARTIAL_COMPLETED";
  private static String FAIL_ON_ATTEMPT = "FAIL_ON_ATTEMPT";

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("Starting mini clusters");
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true)
              .racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster =
          new MiniTezCluster(TestAMRecovery.class.getName(), 1, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, MAX_AM_ATTEMPT);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();
    }
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    if (tezSession != null) {
      try {
        LOG.info("Stopping Tez Session");
        tezSession.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (miniTezCluster != null) {
      try {
        LOG.info("Stopping MiniTezCluster");
        miniTezCluster.stop();
        miniTezCluster = null;
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

  @Before
  public void setup() throws Exception {
    LOG.info("Starting session");
    Path remoteStagingDir =
        remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
            .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

    tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "INFO");
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());
    tezConf
        .setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, MAX_AM_ATTEMPT);
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    tezConf.setBoolean(
        TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, false);
    tezConf.setBoolean(
        RecoveryService.TEZ_AM_RECOVERY_HANDLE_REMAINING_EVENT_WHEN_STOPPED,
        true);
    tezSession = TezClient.create("TestDAGRecovery", tezConf);
    tezSession.start();
  }

  @After
  public void teardown() throws InterruptedException {
    if (tezSession != null) {
      try {
        LOG.info("Stopping Tez Session");
        tezSession.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    tezSession = null;
  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is not started. History flush happens. AM dies. Once AM is recovered, task 0 is
   * not re-run. Task 1 is re-run. (Broadcast)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexPartiallyFinished_Broadcast() throws Exception {
    DAG dag =
        createDAG("VertexPartiallyFinished_Broadcast", ControlledImmediateStartVertexManager.class,
            DataMovementType.BROADCAST, true);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());
  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is also done. History flush happens. AM dies. Once AM is recovered, task 0
   * and Task 1 is not re-run. (Broadcast)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexCompletelyFinished_Broadcast() throws Exception {
    DAG dag =
        createDAG("VertexCompletelyFinished_Broadcast", ControlledImmediateStartVertexManager.class,
            DataMovementType.BROADCAST, false);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());
  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is not started. History flush happens. AM dies. Once AM is recovered, task 0 is
   * not re-run. Task 1 is re-run. (ONE_TO_ONE)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexPartialFinished_One2One() throws Exception {
    DAG dag =
        createDAG("VertexPartialFinished_One2One", ControlledInputReadyVertexManager.class,
            DataMovementType.ONE_TO_ONE, true);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());

  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is also done. History flush happens. AM dies. Once AM is recovered, task 0
   * and Task 1 is not re-run. (ONE_TO_ONE)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexCompletelyFinished_One2One() throws Exception {
    DAG dag =
        createDAG("VertexCompletelyFinished_One2One", ControlledInputReadyVertexManager.class,
            DataMovementType.ONE_TO_ONE, false);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());

  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is not started. History flush happens. AM dies. Once AM is recovered, task 0 is
   * not re-run. Task 1 is re-run. (SCATTER_GATHER)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexPartiallyFinished_ScatterGather() throws Exception {
    DAG dag =
        createDAG("VertexPartiallyFinished_ScatterGather", ControlledShuffleVertexManager.class,
            DataMovementType.SCATTER_GATHER, true);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());

  }

  /**
   * Fine-grained recovery task-level, In a vertex (v1), task 0 is done task 1
   * is also done. History flush happens. AM dies. Once AM is recovered, task 0
   * and Task 1 is not re-run. (SCATTER_GATHER)
   *
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testVertexCompletelyFinished_ScatterGather() throws Exception {
    DAG dag =
        createDAG("VertexCompletelyFinished_ScatterGather", ControlledShuffleVertexManager.class,
            DataMovementType.SCATTER_GATHER, false);
    TezCounters counters = runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
    assertEquals(4, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());
    assertEquals(2, counters.findCounter(TestCounter.Counter_1).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    printHistoryEvents(historyEvents1, 1);
    printHistoryEvents(historyEvents1, 2);
    // task_0 of v1 is finished in attempt 1, task_1 of v1 is not finished in
    // attempt 1
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 1).size());

    // task_0 of v1 is finished in attempt 1 and not rerun, task_1 of v1 is
    // finished in attempt 2
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 1).size());
  }

  /**
   * Set AM max attempt to high number. Kill many attempts. Last AM can still be
   * recovered with latest AM history data.
   *
   * @throws Exception
   */
  @Test(timeout = 600000)
  public void testHighMaxAttempt() throws Exception {
    Random rand = new Random();
    tezConf.set(FAIL_ON_ATTEMPT, rand.nextInt(MAX_AM_ATTEMPT) + "");
    LOG.info("Set FAIL_ON_ATTEMPT=" + tezConf.get(FAIL_ON_ATTEMPT));
    DAG dag =
        createDAG("HighMaxAttempt", FailOnAttemptVertexManager.class,
            DataMovementType.SCATTER_GATHER, false);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);

  }

  TezCounters runDAGAndVerify(DAG dag, DAGStatus.State finalState) throws Exception {
    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus =
        dagClient.waitForCompletionWithStatusUpdates(EnumSet
            .of(StatusGetOpts.GET_COUNTERS));
    Assert.assertEquals(finalState, dagStatus.getState());
    return dagStatus.getDAGCounters();
  }

  /**
   * v1 --> v2 <br>
   * v1 has a customized VM to control whether to schedule only one second task when it is partiallyFinished test case.
   * v2 has a customized VM which could control when to kill AM
   *
   * @param vertexManagerClass
   * @param dmType
   * @param failOnParitialCompleted
   * @return
   * @throws IOException
   */
  private DAG createDAG(String dagName, Class vertexManagerClass, DataMovementType dmType,
      boolean failOnParitialCompleted) throws IOException {
    if (failOnParitialCompleted) {
      tezConf.set(FAIL_ON_PARTIAL_FINISHED, "true");
    } else {
      tezConf.set(FAIL_ON_PARTIAL_FINISHED, "false");
    }
    DAG dag = DAG.create(dagName);
    UserPayload payload = UserPayload.create(null);
    Vertex v1 = Vertex.create("v1", MyProcessor.getProcDesc(), 2);
    v1.setVertexManagerPlugin(VertexManagerPluginDescriptor.create(
        ScheduleControlledVertexManager.class.getName()).setUserPayload(
        TezUtils.createUserPayloadFromConf(tezConf)));
    Vertex v2 = Vertex.create("v2", DoNothingProcessor.getProcDesc(), 2);
    v2.setVertexManagerPlugin(VertexManagerPluginDescriptor.create(
        vertexManagerClass.getName()).setUserPayload(
        TezUtils.createUserPayloadFromConf(tezConf)));

    dag.addVertex(v1).addVertex(v2);
    dag.addEdge(Edge.create(v1, v2, EdgeProperty.create(dmType,
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        TestOutput.getOutputDesc(payload), TestInput.getInputDesc(payload))));
    return dag;
  }

  private List<TaskAttemptFinishedEvent> findTaskAttemptFinishedEvent(
      List<HistoryEvent> historyEvents, int vertexId, int taskId) {
    List<TaskAttemptFinishedEvent> resultEvents =
        new ArrayList<TaskAttemptFinishedEvent>();
    for (HistoryEvent historyEvent : historyEvents) {
      if (historyEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
        TaskAttemptFinishedEvent taFinishedEvent =
            (TaskAttemptFinishedEvent) historyEvent;
        if (taFinishedEvent.getTaskAttemptID().getTaskID().getVertexID()
            .getId() == vertexId
            && taFinishedEvent.getTaskAttemptID().getTaskID().getId() == taskId) {
          resultEvents.add(taFinishedEvent);
        }
      }
    }
    return resultEvents;
  }

  private List<HistoryEvent> readRecoveryLog(int attemptNum) throws IOException {
    ApplicationId appId = tezSession.getAppMasterApplicationId();
    Path tezSystemStagingDir =
        TezCommonUtils.getTezSystemStagingPath(tezConf, appId.toString());
    Path recoveryDataDir =
        TezCommonUtils.getRecoveryPath(tezSystemStagingDir, tezConf);
    FileSystem fs = tezSystemStagingDir.getFileSystem(tezConf);
    Path currentAttemptRecoveryDataDir =
        TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, attemptNum);
    Path recoveryFilePath =
        new Path(currentAttemptRecoveryDataDir, appId.toString().replace(
            "application", "dag")
            + "_1" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
    return RecoveryParser.parseDAGRecoveryFile(fs.open(recoveryFilePath));
  }

  private void printHistoryEvents(List<HistoryEvent> historyEvents, int attemptId) {
    LOG.info("RecoveryLogs from attempt:" + attemptId);
    for(HistoryEvent historyEvent : historyEvents) {
      LOG.info("Parsed event from recovery stream"
          + ", eventType=" + historyEvent.getEventType()
          + ", event=" + historyEvent);
    }
    LOG.info("");
  }

  public static class ControlledInputReadyVertexManager extends
      InputReadyVertexManager {

    private Configuration conf;
    private int completedTaskNum = 0;

    public ControlledInputReadyVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      super.initialize();
      try {
        conf =
            TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      super.onSourceTaskCompleted(srcVertexName, taskId);
      completedTaskNum ++;
      if (getContext().getDAGAttemptNumber() == 1) {
        if (conf.getBoolean(FAIL_ON_PARTIAL_FINISHED, true)) {
          if (completedTaskNum == 1) {
            System.exit(-1);
          }
        } else {
          if (completedTaskNum == getContext().getVertexNumTasks(srcVertexName)) {
            System.exit(-1);
          }
        }
      }
    }
  }

  public static class ControlledShuffleVertexManager extends
      ShuffleVertexManager {

    private Configuration conf;
    private int completedTaskNum = 0;

    public ControlledShuffleVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      super.initialize();
      try {
        conf =
            TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      super.onSourceTaskCompleted(srcVertexName, taskId);
      completedTaskNum ++;
      if (getContext().getDAGAttemptNumber() == 1) {
        if (conf.getBoolean(FAIL_ON_PARTIAL_FINISHED, true)) {
          if (completedTaskNum == 1) {
            System.exit(-1);
          }
        } else {
          if (completedTaskNum == getContext().getVertexNumTasks(srcVertexName)) {
            System.exit(-1);
          }
        }
      }
    }
  }

  public static class ControlledImmediateStartVertexManager extends
      ImmediateStartVertexManager {

    private Configuration conf;
    private int completedTaskNum = 0;

    public ControlledImmediateStartVertexManager(
        VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      super.initialize();
      try {
        conf =
            TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      super.onSourceTaskCompleted(srcVertexName, taskId);
      completedTaskNum ++;
      if (getContext().getDAGAttemptNumber() == 1) {
        if (conf.getBoolean(FAIL_ON_PARTIAL_FINISHED, true)) {
          if (completedTaskNum == 1) {
            System.exit(-1);
          }
        } else {
          if (completedTaskNum == getContext().getVertexNumTasks(srcVertexName)) {
            System.exit(-1);
          }
        }
      }
    }
  }

  
  /**
   * VertexManager which control schedule only one task when it is test case of partially-finished.
   *
   */
  public static class ScheduleControlledVertexManager extends VertexManagerPlugin {

    private Configuration conf;

    public ScheduleControlledVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      try {
        conf =
            TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions)
        throws Exception {
      if (getContext().getDAGAttemptNumber() == 1) {
        // only schedule one task if it is partiallyFinished case
        if (conf.getBoolean(FAIL_ON_PARTIAL_FINISHED, true)) {
          getContext().scheduleVertexTasks(Lists.newArrayList(new TaskWithLocationHint(0, null)));
          return ;
        }
      }
      // schedule all tasks when it is not partiallyFinished
      int taskNum = getContext().getVertexNumTasks(getContext().getVertexName());
      List<TaskWithLocationHint> taskWithLocationHints = new ArrayList<TaskWithLocationHint>();
      for (int i=0;i<taskNum;++i) {
        taskWithLocationHints.add(new TaskWithLocationHint(i, null));
      }
      getContext().scheduleVertexTasks(taskWithLocationHints);
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId)
        throws Exception {
      
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent)
        throws Exception {
      
    }

    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) throws Exception {
      
    }
  }

  /**
   * VM which could control fail on attempt less than a specified number
   *
   */
  public static class FailOnAttemptVertexManager extends ShuffleVertexManager {

    private Configuration conf;

    public FailOnAttemptVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      super.initialize();
      try {
        conf =
            TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      int curAttempt = getContext().getDAGAttemptNumber();
      super.onSourceTaskCompleted(srcVertexName, taskId);
      int failOnAttempt = conf.getInt(FAIL_ON_ATTEMPT, 1);
      LOG.info("failOnAttempt:" + failOnAttempt);
      LOG.info("curAttempt:" + curAttempt);
      if (curAttempt < failOnAttempt) {
        System.exit(-1);
      }
    }
  }

  public static enum TestCounter {
    Counter_1,
  }

  public static class MyProcessor extends SimpleProcessor {

    public MyProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      getContext().getCounters().findCounter(TestCounter.Counter_1).increment(1);
    }

    public static ProcessorDescriptor getProcDesc() {
      return ProcessorDescriptor.create(MyProcessor.class.getName());
    }
  }

  public static class DoNothingProcessor extends SimpleProcessor {

    public DoNothingProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      // Sleep 3 second in vertex2 to avoid that vertex2 completed 
      // before vertex2 get the SourceVertexTaskAttemptCompletedEvent.
      // SourceVertexTaskAttemptCompletedEvent will been ingored if vertex in SUCCEEDED,
      // so AM won't been killed in the VM of vertex2
      Thread.sleep(3000);
    }

    public static ProcessorDescriptor getProcDesc() {
      return ProcessorDescriptor.create(DoNothingProcessor.class.getName());
    }
  }

}

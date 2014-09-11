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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.dag.app.RecoveryParser;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.VertexDataMovementEventsGeneratedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.test.dag.MultiAttemptDAG;
import org.apache.tez.test.dag.MultiAttemptDAG.FailingInputInitializer;
import org.apache.tez.test.dag.MultiAttemptDAG.NoOpInput;
import org.apache.tez.test.dag.MultiAttemptDAG.TestRootInputInitializer;
import org.apache.tez.test.dag.SimpleVTestDAG;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class TestDAGRecovery {

  private static final Log LOG = LogFactory.getLog(TestDAGRecovery.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster = null;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestDAGRecovery.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static TezClient tezSession = null;
  private static FileSystem remoteFs = null;
  private static TezConfiguration tezConf = null;

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
      miniTezCluster = new MiniTezCluster(TestDAGRecovery.class.getName(),
          1, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 4);
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
    Thread.sleep(10000);
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

  @Before
  public void setup()  throws Exception {
    LOG.info("Starting session");
    Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
        .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

    tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG");
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, "false");

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
    Thread.sleep(10000);
  }

  void runDAGAndVerify(DAG dag, DAGStatus.State finalState) throws Exception {
    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for dag to complete. Sleeping for 500ms."
          + " DAG name: " + dag.getName()
          + " DAG appContext: " + dagClient.getExecutionContext()
          + " Current state: " + dagStatus.getState());
      Thread.sleep(100);
      dagStatus = dagClient.getDAGStatus(null);
    }

    Assert.assertEquals(finalState, dagStatus.getState());
  }

  private void verifyRecoveryLog() throws IOException{
    ApplicationId appId = tezSession.getAppMasterApplicationId();
    Path tezSystemStagingDir = TezCommonUtils.getTezSystemStagingPath(tezConf, appId.toString());
    Path recoveryDataDir = TezCommonUtils.getRecoveryPath(tezSystemStagingDir, tezConf);

    FileSystem fs = tezSystemStagingDir.getFileSystem(tezConf);
    for (int i=1; i<=3; ++i) {
      Path currentAttemptRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir,i);
      Path recoveryFilePath = new Path(currentAttemptRecoveryDataDir,
      appId.toString().replace("application", "dag") + "_1" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
      List<HistoryEvent> historyEvents = RecoveryParser.parseDAGRecoveryFile(
          fs.open(recoveryFilePath));

      int inputInfoEventIndex = -1;
      int vertexInitedEventIndex = -1;
      for (int j=0;j<historyEvents.size(); ++j) {
        HistoryEvent historyEvent = historyEvents.get(j);
        LOG.info("Parsed event from recovery stream"
            + ", eventType=" + historyEvent.getEventType()
            + ", event=" + historyEvent);
        if (historyEvent.getEventType() ==  HistoryEventType.VERTEX_DATA_MOVEMENT_EVENTS_GENERATED) {
          VertexDataMovementEventsGeneratedEvent dmEvent =
              (VertexDataMovementEventsGeneratedEvent)historyEvent;
          // TODO do not need to check whether it is -1 after Tez-1521 is resolved
          if (dmEvent.getVertexID().getId() == 0 && inputInfoEventIndex == -1) {
            inputInfoEventIndex = j;
          }
        }
        if (historyEvent.getEventType() == HistoryEventType.VERTEX_INITIALIZED) {
          VertexInitializedEvent vInitedEvent = (VertexInitializedEvent) historyEvent;
          if (vInitedEvent.getVertexID().getId() == 0) {
            vertexInitedEventIndex = j;
          }
        }
      }
      // v1's init events must be logged before its VertexInitializedEvent (Tez-1345)
      Assert.assertTrue("can not find VERTEX_DATA_MOVEMENT_EVENTS_GENERATED for v1", inputInfoEventIndex != -1);
      Assert.assertTrue("can not find VERTEX_INITIALIZED for v1", vertexInitedEventIndex != -1);
      Assert.assertTrue("VERTEX_DATA_MOVEMENT_EVENTS_GENERATED is logged before VERTEX_INITIALIZED for v1",
          inputInfoEventIndex < vertexInitedEventIndex);
    }
  }

  @Test(timeout=120000)
  public void testBasicRecovery() throws Exception {
    DAG dag = MultiAttemptDAG.createDAG("TestBasicRecovery", null);
    // add input to v1 to make sure that there will be init events for v1 (TEZ-1345)
    DataSourceDescriptor dataSource =
        DataSourceDescriptor.create(InputDescriptor.create(NoOpInput.class.getName()),
           InputInitializerDescriptor.create(TestRootInputInitializer.class.getName()), null);
    dag.getVertex("v1").addDataSource("Input", dataSource);

    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);

    verifyRecoveryLog();

    // it should fail if submitting same dags in recovery mode (TEZ-1064)
    try {
      DAGClient dagClient = tezSession.submitDAG(dag);
      Assert.fail("Expected DAG submit to fail on duplicate dag name");
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage().contains("Duplicate dag name"));
    }
  }

  @Test(timeout=120000)
  public void testDelayedInit() throws Exception {
    DAG dag = SimpleVTestDAG.createDAG("DelayedInitDAG", null);
    dag.getVertex("v1").addDataSource(
        "i1",
        DataSourceDescriptor.create(
            InputDescriptor.create(NoOpInput.class.getName()),
            InputInitializerDescriptor.create(FailingInputInitializer.class
                .getName()), null));
    runDAGAndVerify(dag, State.SUCCEEDED);
  }

}

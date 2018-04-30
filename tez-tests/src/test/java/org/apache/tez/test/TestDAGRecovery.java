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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
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
import java.util.Random;

public class TestDAGRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(TestDAGRecovery.class);

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
      miniTezconf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
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
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "INFO");
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, "false");
    tezConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,0);
    tezConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    tezConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,1000);

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

  void runDAGAndVerify(DAG dag, DAGStatus.State finalState) throws Exception {
    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null, 10);
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

  @Test(timeout=120000)
  public void testBasicRecovery() throws Exception {
    DAG dag = MultiAttemptDAG.createDAG("TestBasicRecovery", null);
    // add input to v1 to make sure that there will be init events for v1 (TEZ-1345)
    DataSourceDescriptor dataSource =
        DataSourceDescriptor.create(InputDescriptor.create(NoOpInput.class.getName()),
           InputInitializerDescriptor.create(TestRootInputInitializer.class.getName()), null);
    dag.getVertex("v1").addDataSource("Input", dataSource);

    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
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

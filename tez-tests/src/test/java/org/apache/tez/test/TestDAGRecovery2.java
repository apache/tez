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
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.test.dag.MultiAttemptDAG;
import org.apache.tez.test.dag.SimpleVTestDAG;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class TestDAGRecovery2 {

  private static final Log LOG = LogFactory.getLog(TestDAGRecovery2.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestDAGRecovery2.class.getName() + "-tmpDir";
  protected static MiniDFSCluster dfsCluster;

  private static TezSession tezSession = null;

  @BeforeClass
  public static void setup() throws Exception {
    LOG.info("Starting mini clusters");
    FileSystem remoteFs = null;
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestDAGRecovery2.class.getName(),
          1, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 4);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();

      Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
          .valueOf(new Random().nextInt(100000))));
      TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

      TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
      tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 10);
      tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG");
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
          remoteStagingDir.toString());
      tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
      tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
      tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
      tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS, " -Xmx256m");

      AMConfiguration amConfig = new AMConfiguration(
          new HashMap<String, String>(), new HashMap<String, LocalResource>(),
          tezConf, null);
      TezSessionConfiguration tezSessionConfig =
          new TezSessionConfiguration(amConfig, tezConf);
      tezSession = new TezSession("TestDAGRecovery2", tezSessionConfig);
      tezSession.start();
    }
  }
  void runDAGAndVerify(DAG dag, DAGStatus.State finalState) throws Exception {
    TezSessionStatus status = tezSession.getSessionStatus();
    while (status != TezSessionStatus.READY && status != TezSessionStatus.SHUTDOWN) {
      LOG.info("Waiting for session to be ready. Current: " + status);
      Thread.sleep(100);
      status = tezSession.getSessionStatus();
    }
    if (status == TezSessionStatus.SHUTDOWN) {
      throw new TezUncheckedException("Unexpected Session shutdown");
    }
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for dag to complete. Sleeping for 500ms."
          + " DAG name: " + dag.getName()
          + " DAG appId: " + dagClient.getApplicationId()
          + " Current state: " + dagStatus.getState());
      Thread.sleep(100);
      dagStatus = dagClient.getDAGStatus(null);
    }

    Assert.assertEquals(finalState, dagStatus.getState());
  }

  @Test(timeout=120000)
  public void testBasicRecovery() throws Exception {
    DAG dag = SimpleVTestDAG.createDAG("FailingCommitterDAG", null);
    OutputDescriptor od =
        new OutputDescriptor(MultiAttemptDAG.NoOpOutput.class.getName());
    od.setUserPayload(new
        MultiAttemptDAG.FailingOutputCommitter.FailingOutputCommitterConfig(true)
            .toUserPayload());
    dag.getVertex("v3").addOutput("FailingOutput", od,
        MultiAttemptDAG.FailingOutputCommitter.class);
    runDAGAndVerify(dag, State.FAILED);
  }

}

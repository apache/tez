/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.app.launcher.TezTestServiceNoOpContainerLauncher;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerService;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorImpl;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.JoinValidate;
import org.apache.tez.service.MiniTezTestServiceCluster;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExternalTezServices {

  private static final Log LOG = LogFactory.getLog(TestExternalTezServices.class);

  private static final String EXT_PUSH_ENTITY_NAME = "ExtServiceTestPush";

  private static MiniTezCluster tezCluster;
  private static MiniDFSCluster dfsCluster;
  private static MiniTezTestServiceCluster tezTestServiceCluster;

  private static Configuration clusterConf = new Configuration();
  private static Configuration confForJobs;

  private static FileSystem remoteFs;
  private static FileSystem localFs;

  private static TezClient sharedTezClient;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestExternalTezServices.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException, TezException, InterruptedException {

    localFs = FileSystem.getLocal(clusterConf);

    try {
      clusterConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster =
          new MiniDFSCluster.Builder(clusterConf).numDataNodes(1).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
      LOG.info("MiniDFSCluster started");
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    tezCluster = new MiniTezCluster(TestExternalTezServices.class.getName(), 1, 1, 1);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    tezCluster.init(conf);
    tezCluster.start();
    LOG.info("MiniTezCluster started");

    clusterConf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    for (Map.Entry<String, String> entry : tezCluster.getConfig()) {
      clusterConf.set(entry.getKey(), entry.getValue());
    }
    long jvmMax = Runtime.getRuntime().maxMemory();

    tezTestServiceCluster = MiniTezTestServiceCluster
        .create(TestExternalTezServices.class.getSimpleName(), 3, ((long) (jvmMax * 0.5d)), 1);
    tezTestServiceCluster.init(clusterConf);
    tezTestServiceCluster.start();
    LOG.info("MiniTezTestServer started");

    confForJobs = new Configuration(clusterConf);
    for (Map.Entry<String, String> entry : tezTestServiceCluster
        .getClusterSpecificConfiguration()) {
      confForJobs.set(entry.getKey(), entry.getValue());
    }

    // TODO TEZ-2003 Once per vertex configuration is possible, run separate tests for push vs pull (regular threaded execution)

    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    // This is currently configured to push tasks into the Service, and then use the standard RPC
    confForJobs.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    confForJobs.set(TezConfiguration.TEZ_AM_TASK_SCHEDULERS,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceTaskSchedulerService.class.getName());
    confForJobs.set(TezConfiguration.TEZ_AM_CONTAINER_LAUNCHERS,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceNoOpContainerLauncher.class.getName());
    confForJobs.set(TezConfiguration.TEZ_AM_TASK_COMMUNICATORS,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceTaskCommunicatorImpl.class.getName());

    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_TASK_SCHEDULER_NAME, EXT_PUSH_ENTITY_NAME);
    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_CONTAINER_LAUNCHER_NAME, EXT_PUSH_ENTITY_NAME);
    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_TASK_COMMUNICATOR_NAME, EXT_PUSH_ENTITY_NAME);


    TezConfiguration tezConf = new TezConfiguration(confForJobs);

    sharedTezClient = TezClient.create(TestExternalTezServices.class.getSimpleName() + "_session",
        tezConf, true);
    sharedTezClient.start();
    LOG.info("Shared TezSession started");
    sharedTezClient.waitTillReady();
    LOG.info("Shared TezSession ready for submission");

  }

  @AfterClass
  public static void tearDown() throws IOException, TezException {
    if (sharedTezClient != null) {
      sharedTezClient.stop();
      sharedTezClient = null;
    }

    if (tezTestServiceCluster != null) {
      tezTestServiceCluster.stop();
      tezTestServiceCluster = null;
    }

    if (tezCluster != null) {
      tezCluster.stop();
      tezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    // TODO Add cleanup code.
  }


  @Test(timeout = 60000)
  public void test1() throws Exception {
    Path testDir = new Path("/tmp/testHashJoinExample");

    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(confForJobs);

    JoinDataGen dataGen = new JoinDataGen();
    String[] dataGenArgs = new String[]{
        dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
        expectedOutputPath.toString(), "2"};
    assertEquals(0, dataGen.run(tezConf, dataGenArgs, sharedTezClient));

    HashJoinExample joinExample = new HashJoinExample();
    String[] args = new String[]{
        dataPath1.toString(), dataPath2.toString(), "2", outPath.toString()};
    assertEquals(0, joinExample.run(tezConf, args, sharedTezClient));

    JoinValidate joinValidate = new JoinValidate();
    String[] validateArgs = new String[]{
        expectedOutputPath.toString(), outPath.toString(), "3"};
    assertEquals(0, joinValidate.run(tezConf, validateArgs, sharedTezClient));

    // Ensure this was actually submitted to the external cluster
    assertTrue(tezTestServiceCluster.getNumSubmissions() > 0);
  }
}

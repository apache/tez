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

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.app.launcher.TezTestServiceNoOpContainerLauncher;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerService;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorImpl;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.JoinValidateConfigured;
import org.apache.tez.service.MiniTezTestServiceCluster;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExternalTezServices {

  private static final Log LOG = LogFactory.getLog(TestExternalTezServices.class);

  private static final String EXT_PUSH_ENTITY_NAME = "ExtServiceTestPush";

  private static volatile MiniTezCluster tezCluster;
  private static volatile MiniDFSCluster dfsCluster;
  private static volatile MiniTezTestServiceCluster tezTestServiceCluster;

  private static volatile Configuration clusterConf = new Configuration();
  private static volatile Configuration confForJobs;

  private static volatile FileSystem remoteFs;
  private static volatile FileSystem localFs;

  private static volatile TezClient sharedTezClient;

  private static final Path SRC_DATA_DIR = new Path("/tmp/" + TestExternalTezServices.class.getSimpleName());
  private static final Path HASH_JOIN_EXPECTED_RESULT_PATH = new Path(SRC_DATA_DIR, "expectedOutputPath");
  private static final Path HASH_JOIN_OUTPUT_PATH = new Path(SRC_DATA_DIR, "outPath");

  private static final Map<String, String> PROPS_EXT_SERVICE_PUSH = Maps.newHashMap();
  private static final Map<String, String> PROPS_REGULAR_CONTAINERS = Maps.newHashMap();
  private static final Map<String, String> PROPS_IN_AM = Maps.newHashMap();

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestExternalTezServices.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws Exception {

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

    confForJobs.setStrings(TezConfiguration.TEZ_AM_TASK_SCHEDULERS,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceTaskSchedulerService.class.getName());

    confForJobs.setStrings(TezConfiguration.TEZ_AM_CONTAINER_LAUNCHERS,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceNoOpContainerLauncher.class.getName());

    confForJobs.setStrings(TezConfiguration.TEZ_AM_TASK_COMMUNICATORS,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT,
        EXT_PUSH_ENTITY_NAME + ":" + TezTestServiceTaskCommunicatorImpl.class.getName());

    // Default all jobs to run via the service. Individual tests override this on a per vertex/dag level.
    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_TASK_SCHEDULER_NAME, EXT_PUSH_ENTITY_NAME);
    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_CONTAINER_LAUNCHER_NAME, EXT_PUSH_ENTITY_NAME);
    confForJobs.set(TezConfiguration.TEZ_AM_VERTEX_TASK_COMMUNICATOR_NAME, EXT_PUSH_ENTITY_NAME);

    // Setup various executor sets
    PROPS_REGULAR_CONTAINERS.put(TezConfiguration.TEZ_AM_VERTEX_TASK_SCHEDULER_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT);
    PROPS_REGULAR_CONTAINERS.put(TezConfiguration.TEZ_AM_VERTEX_CONTAINER_LAUNCHER_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT);
    PROPS_REGULAR_CONTAINERS.put(TezConfiguration.TEZ_AM_VERTEX_TASK_COMMUNICATOR_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT);

    PROPS_EXT_SERVICE_PUSH.put(TezConfiguration.TEZ_AM_VERTEX_TASK_SCHEDULER_NAME, EXT_PUSH_ENTITY_NAME);
    PROPS_EXT_SERVICE_PUSH.put(TezConfiguration.TEZ_AM_VERTEX_CONTAINER_LAUNCHER_NAME, EXT_PUSH_ENTITY_NAME);
    PROPS_EXT_SERVICE_PUSH.put(TezConfiguration.TEZ_AM_VERTEX_TASK_COMMUNICATOR_NAME, EXT_PUSH_ENTITY_NAME);

    PROPS_IN_AM.put(TezConfiguration.TEZ_AM_VERTEX_TASK_SCHEDULER_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT);
    PROPS_IN_AM.put(TezConfiguration.TEZ_AM_VERTEX_CONTAINER_LAUNCHER_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT);
    PROPS_IN_AM.put(TezConfiguration.TEZ_AM_VERTEX_TASK_COMMUNICATOR_NAME,
        TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT);


    // Create a session to use for all tests.
    TezConfiguration tezClientConf = new TezConfiguration(confForJobs);

    sharedTezClient = TezClient.create(TestExternalTezServices.class.getSimpleName() + "_session",
        tezClientConf, true);
    sharedTezClient.start();
    LOG.info("Shared TezSession started");
    sharedTezClient.waitTillReady();
    LOG.info("Shared TezSession ready for submission");

    // Generate the join data set used for each run.
    // Can a timeout be enforced here ?
    remoteFs.mkdirs(SRC_DATA_DIR);
    Path dataPath1 = new Path(SRC_DATA_DIR, "inPath1");
    Path dataPath2 = new Path(SRC_DATA_DIR, "inPath2");
    TezConfiguration tezConf = new TezConfiguration(confForJobs);
    //   Generate join data - with 2 tasks.
    JoinDataGen dataGen = new JoinDataGen();
    String[] dataGenArgs = new String[]{
        dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
        HASH_JOIN_EXPECTED_RESULT_PATH.toString(), "2"};
    assertEquals(0, dataGen.run(tezConf, dataGenArgs, sharedTezClient));
    //    Run the actual join - with 2 reducers
    HashJoinExample joinExample = new HashJoinExample();
    String[] args = new String[]{
        dataPath1.toString(), dataPath2.toString(), "2", HASH_JOIN_OUTPUT_PATH.toString()};
    assertEquals(0, joinExample.run(tezConf, args, sharedTezClient));

    LOG.info("Completed generating Data - Expected Hash Result and Actual Join Result");
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
  public void testAllInService() throws Exception {
    int expectedExternalSubmissions = 4 + 3; //4 for 4 src files, 3 for num reducers.
    runJoinValidate("AllInService", expectedExternalSubmissions, PROPS_EXT_SERVICE_PUSH,
        PROPS_EXT_SERVICE_PUSH, PROPS_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testAllInContainers() throws Exception {
    int expectedExternalSubmissions = 0; // All in containers
    runJoinValidate("AllInContainers", expectedExternalSubmissions, PROPS_REGULAR_CONTAINERS,
        PROPS_REGULAR_CONTAINERS, PROPS_REGULAR_CONTAINERS);
  }

  @Test(timeout = 60000)
  public void testAllInAM() throws Exception {
    int expectedExternalSubmissions = 0; // All in AM
    runJoinValidate("AllInAM", expectedExternalSubmissions, PROPS_IN_AM,
        PROPS_IN_AM, PROPS_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed1() throws Exception { // M-ExtService, R-containers
    int expectedExternalSubmissions = 4 + 0; //4 for 4 src files, 0 for num reducers.
    runJoinValidate("Mixed1", expectedExternalSubmissions, PROPS_EXT_SERVICE_PUSH,
        PROPS_EXT_SERVICE_PUSH, PROPS_REGULAR_CONTAINERS);
  }

  @Test(timeout = 60000)
  public void testMixed2() throws Exception { // M-Containers, R-ExtService
    int expectedExternalSubmissions = 0 + 3; // 3 for num reducers.
    runJoinValidate("Mixed2", expectedExternalSubmissions, PROPS_REGULAR_CONTAINERS,
        PROPS_REGULAR_CONTAINERS, PROPS_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testMixed3() throws Exception { // M - service, R-AM
    int expectedExternalSubmissions = 4 + 0; //4 for 4 src files, 0 for num reducers (in-AM).
    runJoinValidate("Mixed3", expectedExternalSubmissions, PROPS_EXT_SERVICE_PUSH,
        PROPS_EXT_SERVICE_PUSH, PROPS_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed4() throws Exception { // M - containers, R-AM
    int expectedExternalSubmissions = 0 + 0; // Nothing in external service.
    runJoinValidate("Mixed4", expectedExternalSubmissions, PROPS_REGULAR_CONTAINERS,
        PROPS_REGULAR_CONTAINERS, PROPS_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed5() throws Exception { // M1 - containers, M2-extservice, R-AM
    int expectedExternalSubmissions = 2 + 0; // 2 for M2
    runJoinValidate("Mixed5", expectedExternalSubmissions, PROPS_REGULAR_CONTAINERS,
        PROPS_EXT_SERVICE_PUSH, PROPS_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed6() throws Exception { // M - AM, R - Service
    int expectedExternalSubmissions = 0 + 3; // 3 for R in service
    runJoinValidate("Mixed6", expectedExternalSubmissions, PROPS_IN_AM,
        PROPS_IN_AM, PROPS_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testMixed7() throws Exception { // M - AM, R - Containers
    int expectedExternalSubmissions = 0; // Nothing in ext service
    runJoinValidate("Mixed7", expectedExternalSubmissions, PROPS_IN_AM,
        PROPS_IN_AM, PROPS_REGULAR_CONTAINERS);
  }


  private void runJoinValidate(String name, int extExpectedCount, Map<String, String> lhsProps,
                               Map<String, String> rhsProps,
                               Map<String, String> validateProps) throws
      Exception {
    int externalSubmissionCount = tezTestServiceCluster.getNumSubmissions();

    TezConfiguration tezConf = new TezConfiguration(confForJobs);
    JoinValidateConfigured joinValidate =
        new JoinValidateConfigured(lhsProps, rhsProps,
            validateProps, name);
    String[] validateArgs = new String[]{"-disableSplitGrouping",
        HASH_JOIN_EXPECTED_RESULT_PATH.toString(), HASH_JOIN_OUTPUT_PATH.toString(), "3"};
    assertEquals(0, joinValidate.run(tezConf, validateArgs, sharedTezClient));

    // Ensure this was actually submitted to the external cluster
    assertEquals(extExpectedCount,
        (tezTestServiceCluster.getNumSubmissions() - externalSubmissionCount));
  }
}

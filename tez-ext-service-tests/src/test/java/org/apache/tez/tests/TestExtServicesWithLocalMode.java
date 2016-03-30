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

package org.apache.tez.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.app.launcher.TezTestServiceNoOpContainerLauncher;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerService;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorImpl;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.JoinValidateConfigured;
import org.apache.tez.service.MiniTezTestServiceCluster;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExtServicesWithLocalMode {

  private static final Logger LOG = LoggerFactory.getLogger(TestExtServicesWithLocalMode.class);


  private static final String EXT_PUSH_ENTITY_NAME = "ExtServiceTestPush";

  private static String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TestExtServicesWithLocalMode.class.getName()
          + "-tmpDir";

  private static final Path SRC_DATA_DIR = new Path(TEST_ROOT_DIR + Path.SEPARATOR + "data");
  private static final Path HASH_JOIN_EXPECTED_RESULT_PATH =
      new Path(SRC_DATA_DIR, "expectedOutputPath");
  private static final Path HASH_JOIN_OUTPUT_PATH = new Path(SRC_DATA_DIR, "outPath");

  private static final Vertex.VertexExecutionContext EXECUTION_CONTEXT_EXT_SERVICE_PUSH =
      Vertex.VertexExecutionContext.create(
          EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);

  private static volatile Configuration clusterConf = new Configuration();
  private static volatile FileSystem localFs;
  private static volatile MiniTezTestServiceCluster tezTestServiceCluster;

  private static volatile Configuration confForJobs;

  @BeforeClass
  public static void setup() throws Exception {

    localFs = FileSystem.getLocal(clusterConf).getRaw();
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
    confForJobs.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
  }

  @AfterClass
  public static void tearDown() throws IOException, TezException {
    if (tezTestServiceCluster != null) {
      tezTestServiceCluster.stop();
      tezTestServiceCluster = null;
    }

    Path testRootDirPath = new Path(TEST_ROOT_DIR);
    testRootDirPath = localFs.makeQualified(testRootDirPath);
    LOG.info("CLeaning up path: " + testRootDirPath);
    localFs.delete(testRootDirPath, true);
  }


  @Test(timeout = 30000)
  public void test1() throws Exception {

    UserPayload userPayload = TezUtils.createUserPayloadFromConf(confForJobs);

    TaskSchedulerDescriptor[] taskSchedulerDescriptors = new TaskSchedulerDescriptor[]{
        TaskSchedulerDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceTaskSchedulerService.class.getName())
            .setUserPayload(userPayload)};

    ContainerLauncherDescriptor[] containerLauncherDescriptors = new ContainerLauncherDescriptor[]{
        ContainerLauncherDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceNoOpContainerLauncher.class.getName())
            .setUserPayload(userPayload)};

    TaskCommunicatorDescriptor[] taskCommunicatorDescriptors = new TaskCommunicatorDescriptor[]{
        TaskCommunicatorDescriptor
            .create(EXT_PUSH_ENTITY_NAME, TezTestServiceTaskCommunicatorImpl.class.getName())
            .setUserPayload(userPayload)};

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor.create(true, false,
        taskSchedulerDescriptors, containerLauncherDescriptors, taskCommunicatorDescriptors);


    TezConfiguration tezConf = new TezConfiguration(confForJobs);

    TezClient tezClient = TezClient.newBuilder("test1", tezConf).setIsSession(true)
        .setServicePluginDescriptor(servicePluginsDescriptor).build();
    try {
      tezClient.start();


      Path dataPath1 = new Path(SRC_DATA_DIR, "inPath1");
      Path dataPath2 = new Path(SRC_DATA_DIR, "inPath2");

      Path expectedResultPath = new Path(SRC_DATA_DIR, "expectedOutputPath");


      JoinDataGen dataGen = new JoinDataGen();
      String[] dataGenArgs = new String[]{
          dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
          expectedResultPath.toString(), "2"};

      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezClient));

      Path outputPath = new Path(SRC_DATA_DIR, "outPath");
      HashJoinExample joinExample = new HashJoinExample();
      String[] args = new String[]{
          dataPath1.toString(), dataPath2.toString(), "2", outputPath.toString()};
      assertEquals(0, joinExample.run(tezConf, args, tezClient));
      LOG.info("Completed generating Data - Expected Hash Result and Actual Join Result");

      assertEquals(0, tezTestServiceCluster.getNumSubmissions());

      // ext can consume from ext.
      runJoinValidate(tezClient, "allInExt", 7, EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
          EXECUTION_CONTEXT_EXT_SERVICE_PUSH, EXECUTION_CONTEXT_EXT_SERVICE_PUSH);
      LOG.info("Completed allInExt");

      // uber can consume from uber.
      runJoinValidate(tezClient, "noneInExt", 0, null, null, null);
      LOG.info("Completed noneInExt");

      // uber can consume from ext
      runJoinValidate(tezClient, "lhsInExt", 2, EXECUTION_CONTEXT_EXT_SERVICE_PUSH, null, null);
      LOG.info("Completed lhsInExt");

      // ext cannot consume from uber in this mode since there's no shuffle handler working,
      // and the local data transfer semantics may not match.

    } finally {
      tezClient.stop();
    }

  }

  private void runJoinValidate(TezClient tezClient, String name, int extExpectedCount,
                               Vertex.VertexExecutionContext lhsContext,
                               Vertex.VertexExecutionContext rhsContext,
                               Vertex.VertexExecutionContext validateContext) throws
      Exception {
    int externalSubmissionCount = tezTestServiceCluster.getNumSubmissions();

    TezConfiguration tezConf = new TezConfiguration(confForJobs);
    JoinValidateConfigured joinValidate =
        new JoinValidateConfigured(null, lhsContext, rhsContext,
            validateContext, name);
    String[] validateArgs = new String[]{"-disableSplitGrouping",
        HASH_JOIN_EXPECTED_RESULT_PATH.toString(), HASH_JOIN_OUTPUT_PATH.toString(), "3"};
    assertEquals(0, joinValidate.run(tezConf, validateArgs, tezClient));

    // Ensure this was actually submitted to the external cluster
    assertEquals(extExpectedCount,
        (tezTestServiceCluster.getNumSubmissions() - externalSubmissionCount));
  }
}

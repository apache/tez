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

import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.launcher.TezTestServiceNoOpContainerLauncher;
import org.apache.tez.dag.app.rm.TezTestServiceTaskSchedulerService;
import org.apache.tez.dag.app.taskcomm.TezTestServiceTaskCommunicatorImpl;
import org.apache.tez.examples.JoinValidateConfigured;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.service.impl.ContainerRunnerImpl;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestExternalTezServices {

  private static final Logger LOG = LoggerFactory.getLogger(TestExternalTezServices.class);

  private static final String EXT_PUSH_ENTITY_NAME = "ExtServiceTestPush";

  private static ExternalTezServiceTestHelper extServiceTestHelper;

  private static final Path SRC_DATA_DIR = new Path("/tmp/" + TestExternalTezServices.class.getSimpleName());
  private static final Path HASH_JOIN_EXPECTED_RESULT_PATH = new Path(SRC_DATA_DIR, "expectedOutputPath");
  private static final Path HASH_JOIN_OUTPUT_PATH = new Path(SRC_DATA_DIR, "outPath");

  private static final VertexExecutionContext EXECUTION_CONTEXT_EXT_SERVICE_PUSH =
      VertexExecutionContext.create(
          EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME, EXT_PUSH_ENTITY_NAME);
  private static final VertexExecutionContext EXECUTION_CONTEXT_REGULAR_CONTAINERS =
      VertexExecutionContext.createExecuteInContainers(true);
  private static final VertexExecutionContext EXECUTION_CONTEXT_IN_AM =
      VertexExecutionContext.createExecuteInAm(true);

  private static final VertexExecutionContext EXECUTION_CONTEXT_DEFAULT = EXECUTION_CONTEXT_EXT_SERVICE_PUSH;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestExternalTezServices.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws Exception {

    extServiceTestHelper = new ExternalTezServiceTestHelper(TEST_ROOT_DIR);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(extServiceTestHelper.getConfForJobs());

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

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor.create(true, true,
        taskSchedulerDescriptors, containerLauncherDescriptors, taskCommunicatorDescriptors);


    extServiceTestHelper.setupSharedTezClient(servicePluginsDescriptor);


    // Generate the join data set used for each run.
    // Can a timeout be enforced here ?
    Path dataPath1 = new Path(SRC_DATA_DIR, "inPath1");
    Path dataPath2 = new Path(SRC_DATA_DIR, "inPath2");
    extServiceTestHelper
        .setupHashJoinData(SRC_DATA_DIR, dataPath1, dataPath2, HASH_JOIN_EXPECTED_RESULT_PATH, HASH_JOIN_OUTPUT_PATH);
  }

  @AfterClass
  public static void tearDown() throws IOException, TezException {
    extServiceTestHelper.tearDownAll();
  }


  @Test(timeout = 60000)
  public void testAllInService() throws Exception {
    int expectedExternalSubmissions = 4 + 3; //4 for 4 src files, 3 for num reducers.
    runJoinValidate("AllInService", expectedExternalSubmissions, EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
        EXECUTION_CONTEXT_EXT_SERVICE_PUSH, EXECUTION_CONTEXT_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testAllInContainers() throws Exception {
    int expectedExternalSubmissions = 0; // All in containers
    runJoinValidate("AllInContainers", expectedExternalSubmissions, EXECUTION_CONTEXT_REGULAR_CONTAINERS,
        EXECUTION_CONTEXT_REGULAR_CONTAINERS, EXECUTION_CONTEXT_REGULAR_CONTAINERS);
  }

  @Test(timeout = 60000)
  public void testAllInAM() throws Exception {
    int expectedExternalSubmissions = 0; // All in AM
    runJoinValidate("AllInAM", expectedExternalSubmissions, EXECUTION_CONTEXT_IN_AM,
        EXECUTION_CONTEXT_IN_AM, EXECUTION_CONTEXT_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed1() throws Exception { // M-ExtService, R-containers
    int expectedExternalSubmissions = 4 + 0; //4 for 4 src files, 0 for num reducers.
    runJoinValidate("Mixed1", expectedExternalSubmissions, EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
        EXECUTION_CONTEXT_EXT_SERVICE_PUSH, EXECUTION_CONTEXT_REGULAR_CONTAINERS);
  }

  @Test(timeout = 60000)
  public void testMixed2() throws Exception { // M-Containers, R-ExtService
    int expectedExternalSubmissions = 0 + 3; // 3 for num reducers.
    runJoinValidate("Mixed2", expectedExternalSubmissions, EXECUTION_CONTEXT_REGULAR_CONTAINERS,
        EXECUTION_CONTEXT_REGULAR_CONTAINERS, EXECUTION_CONTEXT_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testMixed3() throws Exception { // M - service, R-AM
    int expectedExternalSubmissions = 4 + 0; //4 for 4 src files, 0 for num reducers (in-AM).
    runJoinValidate("Mixed3", expectedExternalSubmissions, EXECUTION_CONTEXT_EXT_SERVICE_PUSH,
        EXECUTION_CONTEXT_EXT_SERVICE_PUSH, EXECUTION_CONTEXT_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed4() throws Exception { // M - containers, R-AM
    int expectedExternalSubmissions = 0 + 0; // Nothing in external service.
    runJoinValidate("Mixed4", expectedExternalSubmissions, EXECUTION_CONTEXT_REGULAR_CONTAINERS,
        EXECUTION_CONTEXT_REGULAR_CONTAINERS, EXECUTION_CONTEXT_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed5() throws Exception { // M1 - containers, M2-extservice, R-AM
    int expectedExternalSubmissions = 2 + 0; // 2 for M2
    runJoinValidate("Mixed5", expectedExternalSubmissions, EXECUTION_CONTEXT_REGULAR_CONTAINERS,
        EXECUTION_CONTEXT_EXT_SERVICE_PUSH, EXECUTION_CONTEXT_IN_AM);
  }

  @Test(timeout = 60000)
  public void testMixed6() throws Exception { // M - AM, R - Service
    int expectedExternalSubmissions = 0 + 3; // 3 for R in service
    runJoinValidate("Mixed6", expectedExternalSubmissions, EXECUTION_CONTEXT_IN_AM,
        EXECUTION_CONTEXT_IN_AM, EXECUTION_CONTEXT_EXT_SERVICE_PUSH);
  }

  @Test(timeout = 60000)
  public void testMixed7() throws Exception { // M - AM, R - Containers
    int expectedExternalSubmissions = 0; // Nothing in ext service
    runJoinValidate("Mixed7", expectedExternalSubmissions, EXECUTION_CONTEXT_IN_AM,
        EXECUTION_CONTEXT_IN_AM, EXECUTION_CONTEXT_REGULAR_CONTAINERS);
  }

  @Test(timeout = 60000)
  public void testErrorPropagation() throws TezException, InterruptedException, IOException {
    runExceptionSimulation();
  }



  private void runExceptionSimulation() throws IOException, TezException, InterruptedException {
    DAG dag = DAG.create(ContainerRunnerImpl.DAG_NAME_INSTRUMENTED_FAILURES);
    Vertex v =Vertex.create("Vertex1", ProcessorDescriptor.create(SleepProcessor.class.getName()),
        3);
    v.setExecutionContext(EXECUTION_CONTEXT_EXT_SERVICE_PUSH);
    dag.addVertex(v);

    DAGClient dagClient = extServiceTestHelper.getSharedTezClient().submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    assertEquals(1, dagStatus.getDAGProgress().getFailedTaskAttemptCount());
    assertEquals(1, dagStatus.getDAGProgress().getRejectedTaskAttemptCount());

  }

  private void runJoinValidate(String name, int extExpectedCount, VertexExecutionContext lhsContext,
                               VertexExecutionContext rhsContext,
                               VertexExecutionContext validateContext) throws
      Exception {
    int externalSubmissionCount = extServiceTestHelper.getTezTestServiceCluster().getNumSubmissions();

    TezConfiguration tezConf = new TezConfiguration(extServiceTestHelper.getConfForJobs());
    JoinValidateConfigured joinValidate =
        new JoinValidateConfigured(EXECUTION_CONTEXT_DEFAULT, lhsContext, rhsContext,
            validateContext, name);
    String[] validateArgs = new String[]{"-disableSplitGrouping",
        HASH_JOIN_EXPECTED_RESULT_PATH.toString(), HASH_JOIN_OUTPUT_PATH.toString(), "3"};
    assertEquals(0, joinValidate.run(tezConf, validateArgs, extServiceTestHelper.getSharedTezClient()));

    // Ensure this was actually submitted to the external cluster
    assertEquals(extExpectedCount,
        (extServiceTestHelper.getTezTestServiceCluster().getNumSubmissions() - externalSubmissionCount));
  }
}

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.test.dag.SixLevelsFailingDAG;
import org.apache.tez.test.dag.ThreeLevelsFailingDAG;
import org.apache.tez.test.dag.TwoLevelsFailingDAG;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFaultTolerance {
  private static final Log LOG = LogFactory.getLog(TestFaultTolerance.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestFaultTolerance.class.getName() + "-tmpDir";
  protected static MiniDFSCluster dfsCluster;
  
  private static TezSession tezSession = null;
  
  @BeforeClass
  public static void setup() throws Exception {
    LOG.info("Starting mini clusters");
    FileSystem remoteFs = null;
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestFaultTolerance.class.getName(),
          4, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();
      
      Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
          .valueOf(new Random().nextInt(100000))));
      TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);
      
      TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
          remoteStagingDir.toString());
      tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);

      AMConfiguration amConfig = new AMConfiguration(
          new HashMap<String, String>(), new HashMap<String, LocalResource>(),
          tezConf, null);
      TezSessionConfiguration tezSessionConfig =
          new TezSessionConfiguration(amConfig, tezConf);
      tezSession = new TezSession("TestFaultTolerance", tezSessionConfig);
      tezSession.start();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("Stopping mini clusters");
    if (tezSession != null) {
      tezSession.stop();
    }
    if (miniTezCluster != null) {
      miniTezCluster.stop();
      miniTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
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
  
  @Test (timeout=60000)
  public void testBasicSuccessScatterGather() throws Exception {
    DAG dag = SimpleTestDAG.createDAG("testBasicSuccessScatterGather", null);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testBasicSuccessBroadcast() throws Exception {
    DAG dag = new DAG("testBasicSuccessBroadcast");
    Vertex v1 = new Vertex("v1", TestProcessor.getProcDesc(null), 2, SimpleTestDAG.defaultResource);
    Vertex v2 = new Vertex("v2", TestProcessor.getProcDesc(null), 2, SimpleTestDAG.defaultResource);
    dag.addVertex(v1).addVertex(v2).addEdge(new Edge(v1, v2, 
        new EdgeProperty(DataMovementType.BROADCAST, 
            DataSourceType.PERSISTED, 
            SchedulingType.SEQUENTIAL, 
            TestOutput.getOutputDesc(null), 
            TestInput.getInputDesc(null))));
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testBasicTaskFailure() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), 0);
    
    DAG dag = SimpleTestDAG.createDAG("testBasicTaskFailure", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testTaskMultipleFailures() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), 1);
    
    DAG dag = SimpleTestDAG.createDAG("testTaskMultipleFailures", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testTaskMultipleFailuresDAGFail() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), -1);
    
    DAG dag = SimpleTestDAG.createDAG("testTaskMultipleFailuresDAGFail", testConf);
    runDAGAndVerify(dag, DAGStatus.State.FAILED);
  }
  
  @Test (timeout=60000)
  public void testBasicInputFailureWithExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    
    DAG dag = SimpleTestDAG.createDAG("testBasicInputFailureWithExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testBasicInputFailureWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    
    DAG dag = SimpleTestDAG.createDAG("testBasicInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testMultipleInputFailureWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0,1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "-1");
    
    DAG dag = SimpleTestDAG.createDAG("testMultipleInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testMultiVersionInputFailureWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);
    
    DAG dag = SimpleTestDAG.createDAG("testMultiVersionInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testTwoLevelsFailingDAGSuccess() throws Exception {
    Configuration testConf = new Configuration();
    DAG dag = TwoLevelsFailingDAG.createDAG("testTwoLevelsFailingDAGSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testThreeLevelsFailingDAGSuccess() throws Exception {
    Configuration testConf = new Configuration();
    DAG dag = ThreeLevelsFailingDAG.createDAG("testThreeLevelsFailingDAGSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testSixLevelsFailingDAGSuccess() throws Exception {
    Configuration testConf = new Configuration();
    DAG dag = SixLevelsFailingDAG.createDAG("testSixLevelsFailingDAGSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=60000)
  public void testThreeLevelsFailingDAG2VerticesHaveFailedAttemptsDAGSucceeds() throws Exception {
    Configuration testConf = new Configuration();
    //set maximum number of task attempts to 4
    testConf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_ATTEMPTS, 4);
    //l2v1 failure
    testConf.setBoolean(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "l2v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "l2v1"), "1");
    //3 attempts fail
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "l2v1"), 2);
    
    //l3v1 failure
    testConf.setBoolean(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "l3v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "l3v1"), "0");
    //3 attempts fail
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "l3v1"), 2);
    DAG dag = ThreeLevelsFailingDAG.createDAG("testThreeLevelsFailingDAG2VerticesHaveFailedAttemptsDAGSucceeds", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  /**
   * Test input failure.
   * v1-task0    v1-task1
   * |       \ /     |
   * v2-task0    v2-task1
   * 
   * Use maximum allowed failed attempt of 4 (default value during session creation).
   * v1-task1-attempt0 fails. Attempt 1 succeeds.
   * v2-task0-attempt0 runs. Its input1-inputversion0 fails. 
   * This will trigger rerun of v1-task1.
   * v1-task1-attempt2 is re-run and succeeds.
   * v2-task0-attempt0 (no attempt bump) runs. Check its input1. 
   * The input version is now 1. The attempt will now succeed.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testInputFailureCausesRerunAttemptWithinMaxAttemptSuccess() throws Exception {
    Configuration testConf = new Configuration();
    //at v1, task 1 has attempt 0 failing. Attempt 1 succeeds. 1 attempt fails so far.
    testConf.setBoolean(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "1");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), 0);
    //at v2, task 0 attempt 0 input 1 input-version 0 fails.
    //This will trigger re-run of v1's task 1. 
    //At v1, attempt 2 will kicks off. This attempt is still ok because 
    //failed attempt so far at v1-task1 is 1 (not greater than 4).
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    //at v2, attempt 0 have input failures.
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "1");
    //at v2-task0-attempt0/1-input1 has input failure at input version 0 only.
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);
  
    DAG dag = SimpleTestDAG.createDAG(
            "testInputFailureCausesRerunAttemptWithinMaxAttemptSuccess", testConf);
    //Job should succeed.
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  /**
   * Sets configuration for cascading input failure tests that
   * use SimpleTestDAG3Vertices.
   * @param testConf configuration
   * @param failAndExit whether input failure should trigger attempt exit 
   */
  private void setCascadingInputFailureConfig(Configuration testConf, 
                                              boolean failAndExit) {
    // v2 attempt0 succeeds.
    // v2 task0 attempt1 input0 fails up to version 0.
    testConf.setBoolean(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), failAndExit);
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "1");
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"),
            0);

    //v3 all-tasks attempt0 input0 fails up to version 0.
    testConf.setBoolean(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v3"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v3"), failAndExit);
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v3"), "-1");
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v3"), "0");
    testConf.set(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v3"), "0");
    testConf.setInt(TestInput.getVertexConfName(
            TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v3"),
            0);
  }
  
  /**
   * Test cascading input failure without exit. Expecting success.
   * v1 -- v2 -- v3
   * v3 all-tasks attempt0 input0 fails. Wait. Triggering v2 rerun.
   * v2 task0 attempt1 input0 fails. Wait. Triggering v1 rerun.
   * v1 attempt1 rerun and succeeds. v2 accepts v1 attempt1 output. v2 attempt1 succeeds.
   * v3 attempt0 accepts v2 attempt1 output.
   * 
   * AM vertex succeeded order is v1, v2, v1, v2, v3.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testCascadingInputFailureWithoutExitSuccess() throws Exception {
    Configuration testConf = new Configuration(false);
    setCascadingInputFailureConfig(testConf, false);
    DAG dag = SimpleTestDAG3Vertices.createDAG(
              "testCascadingInputFailureWithoutExitSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  /**
   * Test cascading input failure with exit. Expecting success.
   * v1 -- v2 -- v3
   * v3 all-tasks attempt0 input0 fails. v3 attempt0 exits. Triggering v2 rerun.
   * v2 task0 attempt1 input0 fails. v2 attempt1 exits. Triggering v1 rerun.
   * v1 attempt1 rerun and succeeds. v2 accepts v1 attempt1 output. v2 attempt2 succeeds.
   * v3 attempt1 accepts v2 attempt2 output.
   * 
   * AM vertex succeeded order is v1, v2, v3, v1, v2, v3.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testCascadingInputFailureWithExitSuccess() throws Exception {
    Configuration testConf = new Configuration(false);
    setCascadingInputFailureConfig(testConf, true);
    DAG dag = SimpleTestDAG3Vertices.createDAG(
              "testCascadingInputFailureWithExitSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }

}

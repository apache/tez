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
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.test.dag.SimpleReverseVTestDAG;
import org.apache.tez.test.dag.SimpleVTestDAG;
import org.apache.tez.test.dag.SixLevelsFailingDAG;
import org.apache.tez.test.dag.ThreeLevelsFailingDAG;
import org.apache.tez.test.dag.TwoLevelsFailingDAG;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFaultTolerance {
  private static final Logger LOG = LoggerFactory.getLogger(TestFaultTolerance.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestFaultTolerance.class.getName() + "-tmpDir";
  protected static MiniDFSCluster dfsCluster;
  
  private static TezClient tezSession = null;
  
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

      tezSession = TezClient.create("TestFaultTolerance", tezConf, true);
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
    runDAGAndVerify(dag, finalState, -1);
  }

  void runDAGAndVerify(DAG dag, DAGStatus.State finalState, int checkFailedAttempts) throws Exception {
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

    if (checkFailedAttempts > 0) {
      Assert.assertEquals(checkFailedAttempts,
          dagStatus.getDAGProgress().getFailedTaskAttemptCount());
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
    DAG dag = DAG.create("testBasicSuccessBroadcast");
    Vertex v1 =
        Vertex.create("v1", TestProcessor.getProcDesc(null), 2, SimpleTestDAG.defaultResource);
    Vertex v2 =
        Vertex.create("v2", TestProcessor.getProcDesc(null), 2, SimpleTestDAG.defaultResource);
    dag.addVertex(v1).addVertex(v2).addEdge(Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.BROADCAST,
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
    
    //verify value at v2 task1
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "1");
    //value of v2 task1 is 4.
    //v1 attempt0 has value of 1 (attempt index + 1). 
    //v1 attempt1 has value of 2 (attempt index + 1).
    //v3 attempt0 verifies value of 1 + 2 (values from input vertices) 
    // + 1 (attempt index + 1) = 4
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 4);

    DAG dag = SimpleTestDAG.createDAG("testBasicTaskFailure", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, 1);
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
    
    //v1 task0,1 attempt 2 succeed. Input sum = 6. Plus one (v2 attempt0).
    //ending sum is 7.
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 7);
    
    DAG dag = SimpleTestDAG.createDAG("testTaskMultipleFailures", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, 4);
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
    
    //v2 task1 attempt0 index0 fails and exits.
    //v1 task0 attempt1 reruns. 
    //v2 task1 attempt1 has:
    // v1 task0 attempt1 (value = 2) + v1 task1 attempt0 (value = 1)
    // + its own value, attempt + 1 (value = 2). Total is 5.
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 5);
    //v2 task0 attempt 0 succeeds instantly.
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 3);
    
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
    
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 4);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 3);
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
    
    //v2 task0 attempt0 input0,1 fails. wait.
    //v1 task0 attempt1 reruns. v1 task1 attempt1 reruns.
    //2 + 2 + 1 = 5
    //same number for v2 task1
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 5);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 5);
    
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
    
    //v2 task1 attempt0 input0 input-attempt0 fails. Wait. v1 task0 attempt1 reruns.
    //v2 task1 attempt0 input0 input-attempt1 fails. Wait. v1 task0 attempt2 reruns.
    //v2 task1 attempt0 input0 input-attempt2 succeeds.
    //input values (3 + 1) + 1 = 5 
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 5);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 3);
    
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
    testConf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 4);
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
    
    //l2v1: task0 attempt0 succeeds. task1 attempt3 succeeds. 
    //l3v1 finally task0 attempt3 will succeed.
    //l1v1 outputs 1. l1v2 outputs 2.
    //l2v1 task0 attempt0 output = 2. 
    //l2v2 output: attempt0 (l1v2+self = 2+1) * 3 tasks = 9
    //l3v1 task0 attempt3 = l2v1 (2) + l2v2 (9) + self (4) = 15
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "l3v1"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "l3v1", 0), 15);
    
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
   * The input version is now 2. The attempt will now succeed.
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
    
    //v2-task1-attempt0 takes v1-task0-attempt0 input and v1-task1-attempt1 input.
    //v2-task1 does not take v1-task1-attempt2 (re-run caused by input failure 
    //triggered by v2-task0) output.
    //1 + 2 + 1 = 4
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 5);
    // Work-around till TEZ-877 gets fixed
    //testConf.setInt(TestProcessor.getVertexConfName(
    //        TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 4);
  
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
    
    //v2 task0 attempt1 value = v1 task0 attempt1 (2) + v1 task1 attempt0 (1) + 2 = 5
    //v3 all-tasks attempt0 takes v2 task0 attempt1 value (5) + v2 task1 attempt0 (3) + 1 = 9
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v3"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 0), 9);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 1), 9);
    
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
    
    //v2 task0 attempt2 value = v1 task0 attempt1 (2) + v1 task1 attempt0 (1) + 3 = 6
    //v3 all-tasks attempt1 takes v2 task0 attempt2 value (6) + v2 task1 attempt0 (3) + 2 = 11
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v3"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 0), 11);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 1), 11);
    
    DAG dag = SimpleTestDAG3Vertices.createDAG(
              "testCascadingInputFailureWithExitSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  /**
   * Input failure of v3 causes rerun of both both v1 and v2 vertices. 
   *   v1  v2
   *    \ /
   *    v3
   * 
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testInputFailureCausesRerunOfTwoVerticesWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v3"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v3"), false);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v3"), "0,1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v3"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v3"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v3"), "1");
    
    //v3 attempt0:
    //v1 task0,1 attempt2 = 6. v2 task0,1 attempt2 = 6.
    //total = 6 + 6 + 1 = 13
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v3"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 0), 13);
    
    DAG dag = SimpleVTestDAG.createDAG(
            "testInputFailureCausesRerunOfTwoVerticesWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  /**
   * Downstream(v3) attempt failure of a vertex connected with 
   * 2 upstream vertices.. 
   *   v1  v2
   *    \ /
   *    v3
   * 
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testAttemptOfDownstreamVertexConnectedWithTwoUpstreamVerticesFailure() throws Exception {
    Configuration testConf = new Configuration(false);
    
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v3"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v3"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v3"), 1);
    
    //v1 input = 2. v2 input = 2
    //v3 attempt2 value = 2 + 2 + 3 = 7
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v3"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 0), 7);
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 1), 7);
    
    DAG dag = SimpleVTestDAG.createDAG(
            "testAttemptOfDownstreamVertexConnectedWithTwoUpstreamVerticesFailure", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
    
  /**
   * Input failure of v2,v3 trigger v1 rerun. 
   * Reruns can send output to 2 downstream vertices. 
   *     v1
   *    /  \
   *   v2   v3 
   * 
   * Also covers multiple consumer vertices report failure against same producer task.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testInputFailureRerunCanSendOutputToTwoDownstreamVertices() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), false);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), "0");
    
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v3"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v3"), false);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v3"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v3"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v3"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v3"), "0");
    
    //both vertices trigger v1 rerun. v1 attempt1 output is 2 * 2 tasks = 4.
    //v2 attempt0 = 4 + 1 = 5
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 5);
    //v3 attempt0 = 4 + 1 = 5
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v3"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v3", 0), 5);
        
    DAG dag = SimpleReverseVTestDAG.createDAG(
            "testInputFailureRerunCanSendOutputToTwoDownstreamVertices", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  
  /**
   * SimpleTestDAG (v1,v2) has v2 task0/1 input failures triggering v1 rerun
   * upto version 1.
   * 
   * v1 attempt0 succeeds.
   * v2-task0-attempt0 rejects v1 version0/1. Trigger v1 attempt1.
   * v2-task1-attempt0 rejects v1 version0/1. Trigger v1 attempt2.
   * DAG succeeds with v1 attempt2.
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testTwoTasksHaveInputFailuresSuccess() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), false);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0,1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);
    
    //v2 task0 accepts v1 task0 attempt2(3) and v1 task1 attempt0(1) = 4
    //v2 task0 attempt0 = 1
    //total = 5
    testConf.set(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, "v2"), "0,1");
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 0), 5);
    //similarly for v2 task1
    testConf.setInt(TestProcessor.getVertexConfName(
            TestProcessor.TEZ_FAILING_PROCESSOR_VERIFY_VALUE, "v2", 1), 5);
    
    DAG dag = SimpleTestDAG.createDAG("testTwoTasksHaveInputFailuresSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=240000)
  public void testRandomFailingTasks() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestProcessor.TEZ_FAILING_PROCESSOR_DO_RANDOM_FAIL, true);
    testConf.setFloat(TestProcessor.TEZ_FAILING_PROCESSOR_RANDOM_FAIL_PROBABILITY, 0.5f);
    DAG dag = SixLevelsFailingDAG.createDAG("testRandomFailingTasks", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
  @Test (timeout=240000)
  public void testRandomFailingInputs() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setBoolean(TestInput.TEZ_FAILING_INPUT_DO_RANDOM_FAIL, true);
    testConf.setFloat(TestInput.TEZ_FAILING_INPUT_RANDOM_FAIL_PROBABILITY, 0.5f);
    DAG dag = SixLevelsFailingDAG.createDAG("testRandomFailingInputs", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }
  
}

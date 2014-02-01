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

import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFaultTolerance {
  private static final Log LOG = LogFactory.getLog(TestFaultTolerance.class);

  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster;
  private static FileSystem remoteFs;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestFaultTolerance.class.getName() + "-tmpDir";
  
  private static TezSession tezSession = null;
  
  @BeforeClass
  public static void setup() throws Exception {
    LOG.info("Starting mini clusters");
    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestFaultTolerance.class.getName(),
          4, 1, 1);
      Configuration miniTezconf = new Configuration(conf);
      remoteFs = new RawLocalFileSystem();
      miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(miniTezconf);
      miniTezCluster.start();
      
      Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
          .valueOf(new Random().nextInt(100000))));
      TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);
      
      TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
          remoteStagingDir.toString());

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
    if (remoteFs != null) {
      remoteFs.close();
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
        TestInput.TEZ_AM_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
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
        TestInput.TEZ_AM_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
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
        TestInput.TEZ_AM_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0,1");
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
        TestInput.TEZ_AM_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);
    
    DAG dag = SimpleTestDAG.createDAG("testMultiVersionInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED);
  }

}

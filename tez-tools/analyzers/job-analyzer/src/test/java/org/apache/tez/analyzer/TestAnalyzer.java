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

package org.apache.tez.analyzer;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathDependency;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathStep;
import org.apache.tez.analyzer.plugins.CriticalPathAnalyzer.CriticalPathStep.EntityType;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.history.ATSImportTool;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.test.SimpleTestDAG;
import org.apache.tez.test.SimpleTestDAG3Vertices;
import org.apache.tez.test.TestInput;
import org.apache.tez.test.TestProcessor;
import org.apache.tez.test.dag.SimpleReverseVTestDAG;
import org.apache.tez.test.dag.SimpleVTestDAG;
import org.apache.tez.tests.MiniTezClusterWithTimeline;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TestAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(TestAnalyzer.class);

  private static String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TestAnalyzer.class.getName() + "-tmpDir";
  private static String DOWNLOAD_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "download";
  private final static String SIMPLE_HISTORY_DIR = "/tmp/simplehistory/";

  private static MiniDFSCluster dfsCluster;
  private static MiniTezClusterWithTimeline miniTezCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  
  private static TezClient tezSession = null;
  
  private static int numDAGs = 0;

  @BeforeClass
  public static void setupClass() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH, false);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
    dfsCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    fs = dfsCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());

    setupTezCluster();
    numDAGs = 0;
  }
  
  @AfterClass
  public static void tearDownClass() throws Exception {
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
    
  public CriticalPathAnalyzer setupCPAnalyzer() {
    Configuration analyzerConf = new Configuration(false);
    analyzerConf.setBoolean(CriticalPathAnalyzer.DRAW_SVG, false);
    CriticalPathAnalyzer cp = new CriticalPathAnalyzer();
    cp.setConf(analyzerConf);
    return cp;
  }

  public static void setupTezCluster() throws Exception {
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3 * 1000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3 * 1000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT, 2);

    //Enable per edge counters
    conf.setBoolean(TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, ATSHistoryLoggingService
        .class.getName());

    conf.set(TezConfiguration.TEZ_SIMPLE_HISTORY_LOGGING_DIR, SIMPLE_HISTORY_DIR);

    miniTezCluster =
        new MiniTezClusterWithTimeline(TestAnalyzer.class.getName(), 4, 1, 1, true);

    miniTezCluster.init(conf);
    miniTezCluster.start();

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    tezConf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "0.0.0.0:8188");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);
    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        ATSHistoryLoggingService.class.getName());
    
    Path remoteStagingDir = dfsCluster.getFileSystem().makeQualified(new Path(TEST_ROOT_DIR, String
        .valueOf(new Random().nextInt(100000))));
    
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);

    tezSession = TezClient.create("TestFaultTolerance", tezConf, true);
    tezSession.start();

  }
  
  StepCheck createStep(String attempt, CriticalPathDependency reason) {
    return new StepCheck(attempt, reason);
  }
  
  class StepCheck {
    String attempt; // attempt is the TaskAttemptInfo short name with regex
    CriticalPathDependency reason;
    StepCheck(String attempt, CriticalPathDependency reason) {
      this.attempt = attempt;
      this.reason = reason;
    }
    String getAttemptDetail() {
      return attempt;
    }
    CriticalPathDependency getReason() {
      return reason;
    }
  }

  DagInfo runDAGAndVerify(DAG dag, DAGStatus.State finalState, List<StepCheck[]> steps) throws Exception {
    tezSession.waitTillReady();
    numDAGs++;
    LOG.info("ABC Running DAG name: " + dag.getName());
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
    
    String dagId = TezDAGID.getInstance(tezSession.getAppMasterApplicationId(), numDAGs).toString();
    DagInfo dagInfo = getDagInfo(dagId);
    
    verifyCriticalPath(dagInfo, steps);
    return dagInfo;
  }
  
  DagInfo getDagInfo(String dagId) throws Exception {
    // sleep for a bit to let ATS events be sent from AM
    Thread.sleep(1000);
    //Export the data from ATS
    String[] args = { "--dagId=" + dagId, "--downloadDir=" + DOWNLOAD_DIR };

    int result = ATSImportTool.process(args);
    assertTrue(result == 0);

    //Parse ATS data and verify results
    //Parse downloaded contents
    File downloadedFile = new File(DOWNLOAD_DIR
        + Path.SEPARATOR + dagId
        + Path.SEPARATOR + dagId + ".zip");
    ATSFileParser parser = new ATSFileParser(downloadedFile);
    DagInfo dagInfo = parser.getDAGData(dagId);
    assertTrue(dagInfo.getDagId().equals(dagId));
    return dagInfo;
  }
  
  void verifyCriticalPath(DagInfo dagInfo, List<StepCheck[]> stepsOptions) throws Exception {
    CriticalPathAnalyzer cp = setupCPAnalyzer();
    cp.analyze(dagInfo);
    
    List<CriticalPathStep> criticalPath = cp.getCriticalPath();

    for (CriticalPathStep step : criticalPath) {
      LOG.info("ABC Step: " + step.getType());
      if (step.getType() == EntityType.ATTEMPT) {
        LOG.info("ABC Attempt: " + step.getAttempt().getShortName() + " " + step.getAttempt().getDetailedStatus());
      }
      LOG.info("ABC Reason: " + step.getReason());
      String notes = Joiner.on(";").join(step.getNotes());
      LOG.info("ABC Notes: " + notes);
    }

    boolean foundMatchingLength = false;
    for (StepCheck[] steps : stepsOptions) {
      if (steps.length + 2 == criticalPath.size()) {
        foundMatchingLength = true;
        Assert.assertEquals(CriticalPathStep.EntityType.VERTEX_INIT, criticalPath.get(0).getType());
        Assert.assertEquals(criticalPath.get(1).getAttempt().getShortName(),
            criticalPath.get(0).getAttempt().getShortName());

        for (int i=1; i<criticalPath.size() - 1; ++i) {
          CriticalPathStep step = criticalPath.get(i);
          Assert.assertEquals(CriticalPathStep.EntityType.ATTEMPT, step.getType());
          Assert.assertTrue(steps[i-1].getAttemptDetail(), 
              step.getAttempt().getShortName().matches(steps[i-1].getAttemptDetail()));
          //Assert.assertEquals(steps[i-1].getAttemptDetail(), step.getAttempt().getShortName());
          Assert.assertEquals(steps[i-1].getReason(), step.getReason());
        }
    
        Assert.assertEquals(CriticalPathStep.EntityType.DAG_COMMIT,
            criticalPath.get(criticalPath.size() - 1).getType());
        break;
      }
    }
    
    Assert.assertTrue(foundMatchingLength);
    
  }
  
  @Test (timeout=60000)
  public void testBasicSuccessScatterGather() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    StepCheck[] check = { 
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY)
        };
    DAG dag = SimpleTestDAG.createDAG("testBasicSuccessScatterGather", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }
  
  @Test (timeout=60000)
  public void testBasicTaskFailure() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), 0);

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.RETRY_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
    };
    DAG dag = SimpleTestDAG.createDAG("testBasicTaskFailure", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }
  
  @Test (timeout=60000)
  public void testTaskMultipleFailures() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v1"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v1"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v1"), 1);

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.RETRY_DEPENDENCY),
        createStep("v1 : 000000_2", CriticalPathDependency.RETRY_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
    };    
    
    DAG dag = SimpleTestDAG.createDAG("testTaskMultipleFailures", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }
  
  @Test (timeout=60000)
  public void testBasicInputFailureWithExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_1", CriticalPathDependency.DATA_DEPENDENCY),
      };
    
    DAG dag = SimpleTestDAG.createDAG("testBasicInputFailureWithExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }
  
  @Test (timeout=60000)
  public void testBasicInputFailureWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
      };

    DAG dag = SimpleTestDAG.createDAG("testBasicInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }

  @Test (timeout=60000)
  public void testMultiVersionInputFailureWithExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0,1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_1", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_2", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_2", CriticalPathDependency.DATA_DEPENDENCY),
      };

    DAG dag = SimpleTestDAG.createDAG("testMultiVersionInputFailureWithExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }

  @Test (timeout=60000)
  public void testMultiVersionInputFailureWithoutExit() throws Exception {
    Configuration testConf = new Configuration(false);
    testConf.setInt(SimpleTestDAG.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v2"), true);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v2"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v2"), "0");
    testConf.setInt(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v2"), 1);

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v1 : 000000_2", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
      };

    DAG dag = SimpleTestDAG.createDAG("testMultiVersionInputFailureWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
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
    testConf.setInt(SimpleTestDAG3Vertices.TEZ_SIMPLE_DAG_NUM_TASKS, 1);
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

    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v2 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_1", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
    };
    
    DAG dag = SimpleTestDAG3Vertices.createDAG(
              "testCascadingInputFailureWithoutExitSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
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
    
    StepCheck[] check = {
        createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v2 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v2 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v2 : 000000_2", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v3 : 000000_1", CriticalPathDependency.DATA_DEPENDENCY),
      };

    DAG dag = SimpleTestDAG3Vertices.createDAG(
              "testCascadingInputFailureWithExitSuccess", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
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
    testConf.setInt(SimpleVTestDAG.TEZ_SIMPLE_V_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL, "v3"), true);
    testConf.setBoolean(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, "v3"), false);
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_INDEX, "v3"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, "v3"), "0");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, "v3"), "-1");
    testConf.set(TestInput.getVertexConfName(
        TestInput.TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, "v3"), "1");
    
    StepCheck[] check = {
        // use regex for either vertices being possible on the path
        createStep("v[12] : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v[12] : 000000_[01]", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v[12] : 000000_[012]", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v[12] : 000000_[12]", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v[12] : 000000_2", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
      };

    DAG dag = SimpleVTestDAG.createDAG(
            "testInputFailureCausesRerunOfTwoVerticesWithoutExit", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
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
    testConf.setInt(SimpleVTestDAG.TEZ_SIMPLE_V_DAG_NUM_TASKS, 1);
    testConf.setBoolean(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_DO_FAIL, "v3"), true);
    testConf.set(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, "v3"), "0");
    testConf.setInt(TestProcessor.getVertexConfName(
        TestProcessor.TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, "v3"), 1);
    
    StepCheck[] check = {
        // use regex for either vertices being possible on the path
        createStep("v[12] : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
        createStep("v3 : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
        createStep("v3 : 000000_1", CriticalPathDependency.RETRY_DEPENDENCY),
        createStep("v3 : 000000_2", CriticalPathDependency.RETRY_DEPENDENCY),
      };

    DAG dag = SimpleVTestDAG.createDAG(
            "testAttemptOfDownstreamVertexConnectedWithTwoUpstreamVerticesFailure", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, Collections.singletonList(check));
  }
    
  /**
   * Input failure of v2,v3 trigger v1 rerun.
   * Both v2 and v3 report error on v1 and dont exit. So one of them triggers next
   * version of v1 and also consume the output of the next version. While the other
   * consumes the output of the next version of v1. 
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
    testConf.setInt(SimpleReverseVTestDAG.TEZ_SIMPLE_REVERSE_V_DAG_NUM_TASKS, 1);
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
    
    List<StepCheck[]> stepsOptions = Lists.newLinkedList();
    StepCheck[] check1 = {
        // use regex for either vertices being possible on the path
      createStep("v1 : 000000_0", CriticalPathDependency.INIT_DEPENDENCY),
      createStep("v[23] : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
      createStep("v1 : 000000_1", CriticalPathDependency.OUTPUT_RECREATE_DEPENDENCY),
      createStep("v[23] : 000000_0", CriticalPathDependency.DATA_DEPENDENCY),
    };
    stepsOptions.add(check1);
    DAG dag = SimpleReverseVTestDAG.createDAG(
            "testInputFailureRerunCanSendOutputToTwoDownstreamVertices", testConf);
    runDAGAndVerify(dag, DAGStatus.State.SUCCEEDED, stepsOptions);
  }
  
}
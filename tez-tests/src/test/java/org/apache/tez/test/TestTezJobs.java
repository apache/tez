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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests which do not rely on Map/Reduce processor
 * 
 */
public class TestTezJobs {

  private static final Log LOG = LogFactory.getLog(TestTezJobs.class);

  protected static MiniTezCluster miniTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestTezJobs.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (miniTezCluster == null) {
      miniTezCluster = new MiniTezCluster(TestTezJobs.class.getName(), 1, 1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      miniTezCluster.init(conf);
      miniTezCluster.start();
    }

  }

  @AfterClass
  public static void tearDown() {
    if (miniTezCluster != null) {
      miniTezCluster.stop();
      miniTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    // TODO Add cleanup code.
  }

  @Test(timeout = 60000)
  public void testSleepJob() throws TezException, IOException, InterruptedException {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = new DAG("TezSleepProcessor");
    Vertex vertex = new Vertex("SleepVertex", new ProcessorDescriptor(
        SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(new Random()
        .nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

    TezClient tezClient = new TezClient(tezConf);
    AMConfiguration amConf = new AMConfiguration(new HashMap<String, String>(),
        new HashMap<String, LocalResource>(), tezConf, null);

    DAGClient dagClient = tezClient.submitDAGApplication(dag, amConf);

    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    dagStatus = dagClient.getDAGStatus(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));

    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    assertNotNull(dagStatus.getDAGCounters());
    assertNotNull(dagStatus.getDAGCounters().getGroup(FileSystemCounter.class.getName()));
    assertNotNull(dagStatus.getDAGCounters().findCounter(TaskCounter.GC_TIME_MILLIS));
    ExampleDriver.printDAGStatus(dagClient, new String[] { "SleepVertex" }, true, true);
  }

  @Test
  public void testNonDefaultFSStagingDir() throws Exception {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = new DAG("TezSleepProcessor");
    Vertex vertex = new Vertex("SleepVertex", new ProcessorDescriptor(
        SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    Path stagingDir = new Path(TEST_ROOT_DIR, "testNonDefaultFSStagingDir"
        + String.valueOf(new Random().nextInt(100000)));
    FileSystem localFs = FileSystem.getLocal(tezConf);
    stagingDir = localFs.makeQualified(stagingDir);
    localFs.mkdirs(stagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());

    TezClient tezClient = new TezClient(tezConf);
    AMConfiguration amConf = new AMConfiguration(new HashMap<String, String>(),
        new HashMap<String, LocalResource>(), tezConf, null);

    DAGClient dagClient = tezClient.submitDAGApplication(dag, amConf);

    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    dagStatus = dagClient.getDAGStatus(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));

    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
    assertNotNull(dagStatus.getDAGCounters());
    assertNotNull(dagStatus.getDAGCounters().getGroup(FileSystemCounter.class.getName()));
    assertNotNull(dagStatus.getDAGCounters().findCounter(TaskCounter.GC_TIME_MILLIS));
    ExampleDriver.printDAGStatus(dagClient, new String[] { "SleepVertex" }, true, true);


  }

  // TEZ-1631
  @Test (timeout=100000)
  public void testDuplicatedDAGSubmitWhenAMSessionTimeout() throws Exception {
    Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
        .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);
    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());
    tezConf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.set(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, "10");
    AMConfiguration amConfig = new AMConfiguration(
        new HashMap<String, String>(), new HashMap<String, LocalResource>(),
        tezConf, null);
    TezSessionConfiguration tezSessionConfig =
        new TezSessionConfiguration(amConfig, tezConf);

    TezSession tezSession = new TezSession("TestFaultTolerance", tezSessionConfig);
    tezSession.start();

    DAG dag = new DAG("testDuplciatedDAGSubmitWhenAMSessionTimeout");
    Vertex v1 = new Vertex("v1", TestProcessor.getProcDesc(null), 1, SimpleTestDAG.defaultResource);
    Vertex v2 = new Vertex("v2", TestProcessor.getProcDesc(null), 1, SimpleTestDAG.defaultResource);
    Vertex v3 = new Vertex("v3", TestProcessor.getProcDesc(null), 1, SimpleTestDAG.defaultResource);
    VertexGroup unionVertex = dag.createVertexGroup("union", v1, v2);
    dag.addVertex(v1)
      .addVertex(v2)
      .addVertex(v3)
      .addEdge(
        new GroupInputEdge(unionVertex, v3,
            new EdgeProperty(DataMovementType.BROADCAST,
                DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                TestOutput.getOutputDesc(null),
                TestInput.getInputDesc(null)),
        new InputDescriptor(
            ConcatenatedMergedKeyValuesInput.class.getName())));

    // wait for AM session timeout
    LOG.info("Sleep 30 seconds");
    Thread.sleep(30*1000);
    try {
      tezSession.submitDAG(dag);
      Assert.fail("should fail due to AM session timeout");
    } catch (Exception e) {
      LOG.info(ExceptionUtils.getStackTrace(e));
      Assert.assertTrue(e instanceof SessionNotRunning);
    }
    // create a new TezSession since the previous one has been timeout
    tezSession = new TezSession("TestFaultTolerance", tezSessionConfig);
    tezSession.start();
    runDAGAndVerify(tezSession, dag, DAGStatus.State.SUCCEEDED);
    tezSession.stop();
  }

  void runDAGAndVerify(TezSession tezSession, DAG dag, DAGStatus.State finalState) throws Exception {
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
}

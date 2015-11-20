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

package org.apache.tez.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.dag.history.logging.impl.SimpleHistoryLoggingService;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.MRRSleepJob;
import org.apache.tez.mapreduce.examples.MRRSleepJob.ISleepReducer;
import org.apache.tez.mapreduce.examples.MRRSleepJob.MRRSleepJobPartitioner;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepInputFormat;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepMapper;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepReducer;
import org.apache.tez.mapreduce.examples.UnionExample;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class TestMRRJobsDAGApi {

  private static final Logger LOG = LoggerFactory.getLogger(TestMRRJobsDAGApi.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private Random random = new Random();

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestMRRJobsDAGApi.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    
    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestMRRJobsDAGApi.class.getName(),
          1, 1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      conf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
      mrrTezCluster.init(conf);
      mrrTezCluster.start();
    }

  }

  @AfterClass
  public static void tearDown() {
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
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

    DAG dag = DAG.create("TezSleepProcessor");
    Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
            SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
        .nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

    TezClient tezSession = TezClient.create("TezSleepProcessor", tezConf, false);
    tezSession.start();

    DAGClient dagClient = tezSession.submitDAG(dag);

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
    tezSession.stop();
  }

  @Test(timeout = 60000)
  public void testNonDefaultFSStagingDir() throws Exception {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = DAG.create("TezSleepProcessor");
    Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
            SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    Path stagingDir = new Path(TEST_ROOT_DIR, "testNonDefaultFSStagingDir"
        + String.valueOf(random.nextInt(100000)));
    FileSystem localFs = FileSystem.getLocal(tezConf);
    stagingDir = localFs.makeQualified(stagingDir);
    localFs.mkdirs(stagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());

    TezClient tezSession = TezClient.create("TezSleepProcessor", tezConf, false);
    tezSession.start();

    DAGClient dagClient = tezSession.submitDAG(dag);

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
    tezSession.stop();
  }

  // Submits a simple 5 stage sleep job using tez session. Then kills it.
  @Test(timeout = 60000)
  public void testHistoryLogging() throws IOException,
      InterruptedException, TezException, ClassNotFoundException, YarnException {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = DAG.create("TezSleepProcessorHistoryLogging");
    Vertex vertex = Vertex.create("SleepVertex", ProcessorDescriptor.create(
            SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 2,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
        .nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

    FileSystem localFs = FileSystem.getLocal(tezConf);
    Path historyLogDir = new Path(TEST_ROOT_DIR, "testHistoryLogging");
    localFs.mkdirs(historyLogDir);

    tezConf.set(TezConfiguration.TEZ_SIMPLE_HISTORY_LOGGING_DIR,
        localFs.makeQualified(historyLogDir).toString());

    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
    TezClient tezSession = TezClient.create("TezSleepProcessorHistoryLogging", tezConf);
    tezSession.start();

    DAGClient dagClient = tezSession.submitDAG(dag);

    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
          + dagStatus.getState());
      Thread.sleep(500l);
      dagStatus = dagClient.getDAGStatus(null);
    }
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());

    FileStatus historyLogFileStatus = null;
    for (FileStatus fileStatus : localFs.listStatus(historyLogDir)) {
      if (fileStatus.isDirectory()) {
        continue;
      }
      Path p = fileStatus.getPath();
      if (p.getName().startsWith(SimpleHistoryLoggingService.LOG_FILE_NAME_PREFIX)) {
        historyLogFileStatus = fileStatus;
        break;
      }
    }
    Assert.assertNotNull(historyLogFileStatus);
    Assert.assertTrue(historyLogFileStatus.getLen() > 0);
    tezSession.stop();
  }

  // Submits a simple 5 stage sleep job using the DAG submit API instead of job
  // client.
  @Test(timeout = 60000)
  public void testMRRSleepJobDagSubmit() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    State finalState = testMRRSleepJobDagSubmitCore(false, false, false, false);

    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    // TODO Add additional checks for tracking URL etc. - once it's exposed by
    // the DAG API.
  }

  // Submits a simple 5 stage sleep job using the DAG submit API. Then kills it.
  @Test(timeout = 60000)
  public void testMRRSleepJobDagSubmitAndKill() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    State finalState = testMRRSleepJobDagSubmitCore(false, true, false, false);

    Assert.assertEquals(DAGStatus.State.KILLED, finalState);
    // TODO Add additional checks for tracking URL etc. - once it's exposed by
    // the DAG API.
  }

  // Submits a DAG to AM via RPC after AM has started
  @Test(timeout = 60000)
  public void testMRRSleepJobViaSession() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    State finalState = testMRRSleepJobDagSubmitCore(true, false, false, false);

    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
  }

  // Submit 2 jobs via RPC using a custom initializer. The second job is submitted with an
  // additional local resource, which is verified by the initializer.
  @Test(timeout = 120000)
  public void testAMRelocalization() throws Exception {
    Path relocPath = new Path("/tmp/relocalizationfilefound");
    if (remoteFs.exists(relocPath)) {
      remoteFs.delete(relocPath, true);
    }
    TezClient tezSession = createTezSession();

    State finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, true, MRInputAMSplitGeneratorRelocalizationTest.class, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertFalse(remoteFs.exists(new Path("/tmp/relocalizationfilefound")));

    // Start the second job with some additional resources.

    // Create a test jar directly to HDFS
    LOG.info("Creating jar for relocalization test");
    Path relocFilePath = new Path("/tmp/test.jar");
    relocFilePath = remoteFs.makeQualified(relocFilePath);
    OutputStream os = remoteFs.create(relocFilePath, true);
    createTestJar(os, RELOCALIZATION_TEST_CLASS_NAME);

    // Also upload one of Tez's own JARs to HDFS and add as resource; should be ignored
    Path tezAppJar = new Path(MiniTezCluster.APPJAR);
    Path tezAppJarRemote = remoteFs.makeQualified(new Path("/tmp/" + tezAppJar.getName()));
    remoteFs.copyFromLocalFile(tezAppJar, tezAppJarRemote);

    Map<String, LocalResource> additionalResources = new HashMap<String, LocalResource>();
    additionalResources.put("test.jar", createLrObjFromPath(relocFilePath));
    additionalResources.put("TezAppJar.jar", createLrObjFromPath(tezAppJarRemote));

    Assert.assertEquals(TezAppMasterStatus.READY,
        tezSession.getAppMasterStatus());
    finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, true, MRInputAMSplitGeneratorRelocalizationTest.class, additionalResources);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezAppMasterStatus.READY,
        tezSession.getAppMasterStatus());
    Assert.assertTrue(remoteFs.exists(new Path("/tmp/relocalizationfilefound")));

    stopAndVerifyYarnApp(tezSession);
  }

  private void stopAndVerifyYarnApp(TezClient tezSession) throws TezException,
      IOException, YarnException {
    ApplicationId appId = tezSession.getAppMasterApplicationId();
    tezSession.stop();
    Assert.assertEquals(TezAppMasterStatus.SHUTDOWN,
        tezSession.getAppMasterStatus());

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(mrrTezCluster.getConfig());
    yarnClient.start();

    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState().equals(
          YarnApplicationState.FINISHED)
          || appReport.getYarnApplicationState().equals(
              YarnApplicationState.FAILED)
          || appReport.getYarnApplicationState().equals(
              YarnApplicationState.KILLED)) {
        break;
      }
    }

    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    Assert.assertEquals(YarnApplicationState.FINISHED,
        appReport.getYarnApplicationState());
    Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
        appReport.getFinalApplicationStatus());
  }


  @Test(timeout = 120000)
  public void testAMRelocalizationConflict() throws Exception {
    Path relocPath = new Path("/tmp/relocalizationfilefound");
    if (remoteFs.exists(relocPath)) {
      remoteFs.delete(relocPath, true);
    }

    // Run a DAG w/o a file.
    TezClient tezSession = createTezSession();
    State finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, true, MRInputAMSplitGeneratorRelocalizationTest.class, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertFalse(remoteFs.exists(relocPath));

    // Create a bogus TezAppJar directly to HDFS
    LOG.info("Creating jar for relocalization test");
    Path tezAppJar = new Path(MiniTezCluster.APPJAR);
    Path tezAppJarRemote = remoteFs.makeQualified(new Path("/tmp/" + tezAppJar.getName()));
    OutputStream os = remoteFs.create(tezAppJarRemote, true);
    createTestJar(os, RELOCALIZATION_TEST_CLASS_NAME);

    Map<String, LocalResource> additionalResources = new HashMap<String, LocalResource>();
    additionalResources.put("TezAppJar.jar", createLrObjFromPath(tezAppJarRemote));

    try {
      testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, true, MRInputAMSplitGeneratorRelocalizationTest.class, additionalResources);
      Assert.fail("should have failed");
    } catch (Exception ex) {
      // expected
    }

    stopAndVerifyYarnApp(tezSession);
  }

  private LocalResource createLrObjFromPath(Path filePath) {
    return LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(filePath),
        LocalResourceType.FILE, LocalResourceVisibility.PRIVATE, 0, 0);
  }

  private TezClient createTezSession() throws IOException, TezException {
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

    TezClient tezSession = TezClient.create("testrelocalizationsession", tezConf, true);
    tezSession.start();
    Assert.assertEquals(TezAppMasterStatus.INITIALIZING, tezSession.getAppMasterStatus());
    return tezSession;
  }

  // Submits a DAG to AM via RPC after AM has started
  @Test(timeout = 120000)
  public void testMultipleMRRSleepJobViaSession() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    TezConfiguration tezConf = new TezConfiguration(
        mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());

    TezClient tezSession = TezClient.create("testsession", tezConf, true);
    tezSession.start();
    Assert.assertEquals(TezAppMasterStatus.INITIALIZING,
        tezSession.getAppMasterStatus());

    State finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, false, null, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezAppMasterStatus.READY,
        tezSession.getAppMasterStatus());
    finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, false, null, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezAppMasterStatus.READY,
        tezSession.getAppMasterStatus());

    stopAndVerifyYarnApp(tezSession);
  }

  // Submits a simple 5 stage sleep job using tez session. Then kills it.
  @Test(timeout = 60000)
  public void testMRRSleepJobDagSubmitAndKillViaRPC() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    State finalState = testMRRSleepJobDagSubmitCore(true, true, false, false);

    Assert.assertEquals(DAGStatus.State.KILLED, finalState);
    // TODO Add additional checks for tracking URL etc. - once it's exposed by
    // the DAG API.
  }

  // Create and close a tez session without submitting a job
  @Test(timeout = 60000)
  public void testTezSessionShutdown() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    testMRRSleepJobDagSubmitCore(true, false, true, false);
  }

  @Test(timeout = 60000)
  public void testAMSplitGeneration() throws IOException, InterruptedException,
      TezException, ClassNotFoundException, YarnException {
    testMRRSleepJobDagSubmitCore(true, false, false, true);
  }

  public State testMRRSleepJobDagSubmitCore(
      boolean dagViaRPC,
      boolean killDagWhileRunning,
      boolean closeSessionBeforeSubmit,
      boolean genSplitsInAM) throws IOException,
      InterruptedException, TezException, ClassNotFoundException,
      YarnException {
    return testMRRSleepJobDagSubmitCore(dagViaRPC, killDagWhileRunning,
        closeSessionBeforeSubmit, null, genSplitsInAM, null, null);
  }

  public State testMRRSleepJobDagSubmitCore(
      boolean dagViaRPC,
      boolean killDagWhileRunning,
      boolean closeSessionBeforeSubmit,
      TezClient reUseTezSession,
      boolean genSplitsInAM,
      Class<? extends InputInitializer> initializerClass,
      Map<String, LocalResource> additionalLocalResources) throws IOException,
      InterruptedException, TezException, ClassNotFoundException,
      YarnException {
    LOG.info("\n\n\nStarting testMRRSleepJobDagSubmit().");

    JobConf stage1Conf = new JobConf(mrrTezCluster.getConfig());
    JobConf stage2Conf = new JobConf(mrrTezCluster.getConfig());
    JobConf stage3Conf = new JobConf(mrrTezCluster.getConfig());

    stage1Conf.setLong(MRRSleepJob.MAP_SLEEP_TIME, 1);
    stage1Conf.setInt(MRRSleepJob.MAP_SLEEP_COUNT, 1);
    stage1Conf.setInt(MRJobConfig.NUM_MAPS, 1);
    stage1Conf.set(MRJobConfig.MAP_CLASS_ATTR, SleepMapper.class.getName());
    stage1Conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    stage1Conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    stage1Conf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
        SleepInputFormat.class.getName());
    stage1Conf.set(MRJobConfig.PARTITIONER_CLASS_ATTR,
        MRRSleepJobPartitioner.class.getName());

    stage2Conf.setLong(MRRSleepJob.REDUCE_SLEEP_TIME, 1);
    stage2Conf.setInt(MRRSleepJob.REDUCE_SLEEP_COUNT, 1);
    stage2Conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    stage2Conf
        .set(MRJobConfig.REDUCE_CLASS_ATTR, ISleepReducer.class.getName());
    stage2Conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    stage2Conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    stage2Conf.set(MRJobConfig.PARTITIONER_CLASS_ATTR,
        MRRSleepJobPartitioner.class.getName());

    stage3Conf.setLong(MRRSleepJob.REDUCE_SLEEP_TIME, 1);
    stage3Conf.setInt(MRRSleepJob.REDUCE_SLEEP_COUNT, 1);
    stage3Conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    stage3Conf.set(MRJobConfig.REDUCE_CLASS_ATTR, SleepReducer.class.getName());
    stage3Conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    stage3Conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());

    MRHelpers.translateMRConfToTez(stage1Conf);
    MRHelpers.translateMRConfToTez(stage2Conf);
    MRHelpers.translateMRConfToTez(stage3Conf);
    MRHelpers.configureMRApiUsage(stage1Conf);
    MRHelpers.configureMRApiUsage(stage2Conf);
    MRHelpers.configureMRApiUsage(stage3Conf);

    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

    UserPayload stage1Payload = TezUtils.createUserPayloadFromConf(stage1Conf);
    UserPayload stage2Payload = TezUtils.createUserPayloadFromConf(stage2Conf);
    UserPayload stage3Payload = TezUtils.createUserPayloadFromConf(stage3Conf);
    
    DAG dag = DAG.create("testMRRSleepJobDagSubmit-" + random.nextInt(1000));

    Class<? extends InputInitializer> inputInitializerClazz =
        genSplitsInAM ?
            (initializerClass == null ? MRInputAMSplitGenerator.class : initializerClass)
            : null;
    LOG.info("Using initializer class: " + initializerClass);


    DataSourceDescriptor dsd;
    if (!genSplitsInAM) {
      dsd = MRInputHelpers
          .configureMRInputWithLegacySplitGeneration(stage1Conf, remoteStagingDir, true);
    } else {
      if (initializerClass == null) {
        dsd = MRInputLegacy.createConfigBuilder(stage1Conf, SleepInputFormat.class).build();
      } else {
        InputInitializerDescriptor iid =
            InputInitializerDescriptor.create(inputInitializerClazz.getName());
        dsd = MRInputLegacy.createConfigBuilder(stage1Conf, SleepInputFormat.class)
            .setCustomInitializerDescriptor(iid).build();
      }
    }

    Vertex stage1Vertex = Vertex.create("map", ProcessorDescriptor.create(
            MapProcessor.class.getName()).setUserPayload(stage1Payload),
        dsd.getNumberOfShards(), Resource.newInstance(256, 1));
    stage1Vertex.addDataSource("MRInput", dsd);
    Vertex stage2Vertex = Vertex.create("ireduce", ProcessorDescriptor.create(
            ReduceProcessor.class.getName()).setUserPayload(stage2Payload),
        1, Resource.newInstance(256, 1));
    Vertex stage3Vertex = Vertex.create("reduce", ProcessorDescriptor.create(
            ReduceProcessor.class.getName()).setUserPayload(stage3Payload),
        1, Resource.newInstance(256, 1));
    stage3Conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
        true);
    DataSinkDescriptor dataSinkDescriptor =
        MROutputLegacy.createConfigBuilder(stage3Conf, NullOutputFormat.class).build();
    Assert.assertFalse(dataSinkDescriptor.getOutputDescriptor().getHistoryText().isEmpty());
    stage3Vertex.addDataSink("MROutput", dataSinkDescriptor);

    // TODO env, resources

    dag.addVertex(stage1Vertex);
    dag.addVertex(stage2Vertex);
    dag.addVertex(stage3Vertex);

    Edge edge1 = Edge.create(stage1Vertex, stage2Vertex, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, OutputDescriptor.create(
            OrderedPartitionedKVOutput.class.getName()).setUserPayload(stage2Payload),
        InputDescriptor.create(
            OrderedGroupedInputLegacy.class.getName()).setUserPayload(stage2Payload)));
    Edge edge2 = Edge.create(stage2Vertex, stage3Vertex, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, OutputDescriptor.create(
            OrderedPartitionedKVOutput.class.getName()).setUserPayload(stage3Payload),
        InputDescriptor.create(
            OrderedGroupedInputLegacy.class.getName()).setUserPayload(stage3Payload)));

    dag.addEdge(edge1);
    dag.addEdge(edge2);

    TezConfiguration tezConf = new TezConfiguration(
            mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());

    DAGClient dagClient = null;
    boolean reuseSession = reUseTezSession != null;
    TezClient tezSession = null;
    if (!dagViaRPC) {
      Preconditions.checkArgument(reuseSession == false);
    }
    if (!reuseSession) {
      TezConfiguration tempTezconf = new TezConfiguration(tezConf);
      if (!dagViaRPC) {
        tempTezconf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
      } else {
        tempTezconf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
      }
      tezSession = TezClient.create("testsession", tempTezconf);
      tezSession.start();
    } else {
      tezSession = reUseTezSession;
    }
    if(!dagViaRPC) {
      // TODO Use utility method post TEZ-205 to figure out AM arguments etc.
      dagClient = tezSession.submitDAG(dag);
    }

    if (dagViaRPC && closeSessionBeforeSubmit) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();
      boolean sentKillSession = false;
      while(true) {
        Thread.sleep(500l);
        ApplicationReport appReport =
            yarnClient.getApplicationReport(tezSession.getAppMasterApplicationId());
        if (appReport == null) {
          continue;
        }
        YarnApplicationState appState = appReport.getYarnApplicationState();
        if (!sentKillSession) {
          if (appState == YarnApplicationState.RUNNING) {
            tezSession.stop();
            sentKillSession = true;
          }
        } else {
          if (appState == YarnApplicationState.FINISHED
              || appState == YarnApplicationState.KILLED
              || appState == YarnApplicationState.FAILED) {
            LOG.info("Application completed after sending session shutdown"
                + ", yarnApplicationState=" + appState
                + ", finalAppStatus=" + appReport.getFinalApplicationStatus());
            Assert.assertEquals(YarnApplicationState.FINISHED,
                appState);
            Assert.assertEquals(FinalApplicationStatus.SUCCEEDED,
                appReport.getFinalApplicationStatus());
            break;
          }
        }
      }
      yarnClient.stop();
      return null;
    }

    if(dagViaRPC) {
      LOG.info("Submitting dag to tez session with appId=" + tezSession.getAppMasterApplicationId()
          + " and Dag Name=" + dag.getName());
      if (additionalLocalResources != null) {
        tezSession.addAppMasterLocalFiles(additionalLocalResources);
      }
      dagClient = tezSession.submitDAG(dag);
      Assert.assertEquals(TezAppMasterStatus.RUNNING,
          tezSession.getAppMasterStatus());
    }
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for job to complete. Sleeping for 500ms."
          + " Current state: " + dagStatus.getState());
      Thread.sleep(500l);
      if(killDagWhileRunning
          && dagStatus.getState() == DAGStatus.State.RUNNING) {
        LOG.info("Killing running dag/session");
        if (dagViaRPC) {
          tezSession.stop();
        } else {
          dagClient.tryKillDAG();
        }
      }
      dagStatus = dagClient.getDAGStatus(null);
    }
    if (!reuseSession) {
      tezSession.stop();
    }
    return dagStatus.getState();
  }

  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat
        .getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return LocalResource.newInstance(resourceURL, type, visibility,
        resourceSize, resourceModificationTime);
  }
  
  @Test(timeout = 60000)
  public void testVertexGroups() throws Exception {
    LOG.info("Running Group Test");
    Path inPath = new Path(TEST_ROOT_DIR, "in-groups");
    Path outPath = new Path(TEST_ROOT_DIR, "out-groups");
    FSDataOutputStream out = remoteFs.create(inPath);
    OutputStreamWriter writer = new OutputStreamWriter(out);
    writer.write("abcd ");
    writer.write("efgh ");
    writer.write("abcd ");
    writer.write("efgh ");
    writer.close();
    out.close();
    
    UnionExample job = new UnionExample();
    if (job.run(inPath.toString(), outPath.toString(), mrrTezCluster.getConfig())) {
      LOG.info("Success VertexGroups Test");
    } else {
      throw new TezUncheckedException("VertexGroups Test Failed");
    }
  }
  
  @Test(timeout = 60000)
  public void testBroadcastAndOneToOne() throws Exception {
    LOG.info("Running BroadcastAndOneToOne Test");
    BroadcastAndOneToOneExample job = new BroadcastAndOneToOneExample();
    if (job.run(mrrTezCluster.getConfig(), true)) {
      LOG.info("Success BroadcastAndOneToOne Test");
    } else {
      throw new TezUncheckedException("BroadcastAndOneToOne Test Failed");
    }
  }

  // This class should not be used by more than one test in a single run, since
  // the path it writes to is not dynamic.
  private static String RELOCALIZATION_TEST_CLASS_NAME = "AMClassloadTestDummyClass";
  public static class MRInputAMSplitGeneratorRelocalizationTest extends MRInputAMSplitGenerator {

    public MRInputAMSplitGeneratorRelocalizationTest(
        InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize()  throws Exception {
      MRInputUserPayloadProto userPayloadProto = MRInputHelpers
          .parseMRInputPayload(getContext().getInputUserPayload());
      Configuration conf = TezUtils.createConfFromByteString(userPayloadProto
          .getConfigurationBytes());

      try {
        ReflectionUtils.getClazz(RELOCALIZATION_TEST_CLASS_NAME);
        LOG.info("Class found");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/tmp/relocalizationfilefound"));
      } catch (TezReflectionException e) {
        LOG.info("Class not found");
      }

      return super.initialize();
    }
  }
  
  private static void createTestJar(OutputStream outStream, String dummyClassName)
      throws URISyntaxException, IOException {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    JavaFileObject srcFileObject = new SimpleJavaFileObjectImpl(
        URI.create("string:///" + dummyClassName + Kind.SOURCE.extension), Kind.SOURCE);
    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
    compiler.getTask(null, fileManager, null, null, null, Collections.singletonList(srcFileObject))
        .call();

    JavaFileObject javaFileObject = fileManager.getJavaFileForOutput(StandardLocation.CLASS_OUTPUT,
        dummyClassName, Kind.CLASS, null);

    File classFile = new File(dummyClassName + Kind.CLASS.extension);

    JarOutputStream jarOutputStream = new JarOutputStream(outStream);
    JarEntry jarEntry = new JarEntry(classFile.getName());
    jarEntry.setTime(classFile.lastModified());
    jarOutputStream.putNextEntry(jarEntry);

    InputStream in = javaFileObject.openInputStream();
    byte buffer[] = new byte[4096];
    while (true) {
      int nRead = in.read(buffer, 0, buffer.length);
      if (nRead <= 0)
        break;
      jarOutputStream.write(buffer, 0, nRead);
    }
    in.close();
    jarOutputStream.close();
    javaFileObject.delete();
  }

  private static class SimpleJavaFileObjectImpl extends SimpleJavaFileObject {
    static final String code = "public class AMClassloadTestDummyClass {}";
    SimpleJavaFileObjectImpl(URI uri, Kind kind) {
      super(uri, kind);
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
      return code;
    }
  }
}

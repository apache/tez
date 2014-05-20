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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.common.RuntimeUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.BroadcastAndOneToOneExample;
import org.apache.tez.mapreduce.examples.MRRSleepJob;
import org.apache.tez.mapreduce.examples.MRRSleepJob.ISleepReducer;
import org.apache.tez.mapreduce.examples.MRRSleepJob.MRRSleepJobPartitioner;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepInputFormat;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepMapper;
import org.apache.tez.mapreduce.examples.MRRSleepJob.SleepReducer;
import org.apache.tez.mapreduce.examples.UnionExample;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.api.TezRootInputInitializerContext;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.test.MiniTezCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRRJobsDAGApi {

  private static final Log LOG = LogFactory.getLog(TestMRRJobsDAGApi.class);

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
    TezSession tezSession = createTezSession();

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

    Assert.assertEquals(TezSessionStatus.READY,
        tezSession.getSessionStatus());
    finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, true, MRInputAMSplitGeneratorRelocalizationTest.class, additionalResources);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezSessionStatus.READY,
        tezSession.getSessionStatus());
    Assert.assertTrue(remoteFs.exists(new Path("/tmp/relocalizationfilefound")));

    stopAndVerifyYarnApp(tezSession);
  }

  private void stopAndVerifyYarnApp(TezSession tezSession) throws TezException,
      IOException, YarnException {
    ApplicationId appId = tezSession.getApplicationId();
    tezSession.stop();
    Assert.assertEquals(TezSessionStatus.SHUTDOWN,
        tezSession.getSessionStatus());

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
    TezSession tezSession = createTezSession();
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

  private TezSession createTezSession() throws IOException, TezException {
    Map<String, String> commonEnv = createCommonEnv();
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());

    Map<String, LocalResource> amLocalResources = new HashMap<String, LocalResource>();

    AMConfiguration amConfig = new AMConfiguration(commonEnv, amLocalResources, tezConf, null);
    TezSessionConfiguration tezSessionConfig = new TezSessionConfiguration(amConfig, tezConf);
    TezSession tezSession = new TezSession("testrelocalizationsession", tezSessionConfig);
    tezSession.start();
    Assert.assertEquals(TezSessionStatus.INITIALIZING, tezSession.getSessionStatus());
    return tezSession;
  }

  // Submits a DAG to AM via RPC after AM has started
  @Test(timeout = 120000)
  public void testMultipleMRRSleepJobViaSession() throws IOException,
  InterruptedException, TezException, ClassNotFoundException, YarnException {
    Map<String, String> commonEnv = createCommonEnv();
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    remoteFs.mkdirs(remoteStagingDir);
    TezConfiguration tezConf = new TezConfiguration(
        mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());

    Map<String, LocalResource> amLocalResources =
        new HashMap<String, LocalResource>();

    AMConfiguration amConfig = new AMConfiguration(
        commonEnv, amLocalResources,
        tezConf, null);
    TezSessionConfiguration tezSessionConfig =
        new TezSessionConfiguration(amConfig, tezConf);
    TezSession tezSession = new TezSession("testsession", tezSessionConfig);
    tezSession.start();
    Assert.assertEquals(TezSessionStatus.INITIALIZING,
        tezSession.getSessionStatus());

    State finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, false, null, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezSessionStatus.READY,
        tezSession.getSessionStatus());
    finalState = testMRRSleepJobDagSubmitCore(true, false, false,
        tezSession, false, null, null);
    Assert.assertEquals(DAGStatus.State.SUCCEEDED, finalState);
    Assert.assertEquals(TezSessionStatus.READY,
        tezSession.getSessionStatus());

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

  private Map<String, String> createCommonEnv() {
    Map<String, String> commonEnv = new HashMap<String, String>();
    return commonEnv;
  }

  public State testMRRSleepJobDagSubmitCore(
      boolean dagViaRPC,
      boolean killDagWhileRunning,
      boolean closeSessionBeforeSubmit,
      TezSession reUseTezSession,
      boolean genSplitsInAM,
      Class<? extends TezRootInputInitializer> initializerClass,
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

    JobConf stage22Conf = new JobConf(stage2Conf);
    stage22Conf.setInt(MRJobConfig.NUM_REDUCES, 2);

    stage3Conf.setLong(MRRSleepJob.REDUCE_SLEEP_TIME, 1);
    stage3Conf.setInt(MRRSleepJob.REDUCE_SLEEP_COUNT, 1);
    stage3Conf.setInt(MRJobConfig.NUM_REDUCES, 1);
    stage3Conf.set(MRJobConfig.REDUCE_CLASS_ATTR, SleepReducer.class.getName());
    stage3Conf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    stage3Conf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    stage3Conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        NullOutputFormat.class.getName());

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage1Conf, null);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage2Conf,
        stage1Conf);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage22Conf,
        stage1Conf);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(stage3Conf,
        stage2Conf); // this also works stage22 as it sets up keys etc

    MRHelpers.doJobClientMagic(stage1Conf);
    MRHelpers.doJobClientMagic(stage2Conf);
    MRHelpers.doJobClientMagic(stage22Conf);
    MRHelpers.doJobClientMagic(stage3Conf);

    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String
        .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);
    InputSplitInfo inputSplitInfo = null;
    if (!genSplitsInAM) {
      inputSplitInfo = MRHelpers.generateInputSplits(stage1Conf,
        remoteStagingDir);
    }

    byte[] stage1Payload = MRHelpers.createUserPayloadFromConf(stage1Conf);
    byte[] stage1InputPayload = MRHelpers.createMRInputPayload(stage1Payload, null);
    byte[] stage3Payload = MRHelpers.createUserPayloadFromConf(stage3Conf);
    
    DAG dag = new DAG("testMRRSleepJobDagSubmit-" + random.nextInt(1000));
    int stage1NumTasks = genSplitsInAM ? -1 : inputSplitInfo.getNumTasks();
    Class<? extends TezRootInputInitializer> inputInitializerClazz =
        genSplitsInAM ? (initializerClass == null ? MRInputAMSplitGenerator.class : initializerClass)
        : null;
    LOG.info("Using initializer class: " + initializerClass);
    Vertex stage1Vertex = new Vertex("map", new ProcessorDescriptor(
        MapProcessor.class.getName()).setUserPayload(stage1Payload),
        stage1NumTasks, Resource.newInstance(256, 1));
    MRHelpers.addMRInput(stage1Vertex, stage1InputPayload, inputInitializerClazz);
    Vertex stage2Vertex = new Vertex("ireduce", new ProcessorDescriptor(
        ReduceProcessor.class.getName()).setUserPayload(
        MRHelpers.createUserPayloadFromConf(stage2Conf)),
        1, Resource.newInstance(256, 1));
    Vertex stage3Vertex = new Vertex("reduce", new ProcessorDescriptor(
        ReduceProcessor.class.getName()).setUserPayload(stage3Payload),
        1, Resource.newInstance(256, 1));
    MRHelpers.addMROutputLegacy(stage3Vertex, stage3Payload);

    Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    Map<String, String> commonEnv = createCommonEnv();

    if (!genSplitsInAM) {
      // TODO Use utility method post TEZ-205.
      Map<String, LocalResource> stage1LocalResources = new HashMap<String, LocalResource>();
      stage1LocalResources.put(
          inputSplitInfo.getSplitsFile().getName(),
          createLocalResource(remoteFs, inputSplitInfo.getSplitsFile(),
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      stage1LocalResources.put(
          inputSplitInfo.getSplitsMetaInfoFile().getName(),
          createLocalResource(remoteFs, inputSplitInfo.getSplitsMetaInfoFile(),
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      stage1LocalResources.putAll(commonLocalResources);

      stage1Vertex.setTaskLocalResources(stage1LocalResources);
      stage1Vertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
    } else {
      stage1Vertex.setTaskLocalResources(commonLocalResources);
    }

    stage1Vertex.setJavaOpts(MRHelpers.getMapJavaOpts(stage1Conf));
    stage1Vertex.setTaskEnvironment(commonEnv);

    // TODO env, resources

    stage2Vertex.setJavaOpts(MRHelpers.getReduceJavaOpts(stage2Conf));
    stage2Vertex.setTaskLocalResources(commonLocalResources);
    stage2Vertex.setTaskEnvironment(commonEnv);

    stage3Vertex.setJavaOpts(MRHelpers.getReduceJavaOpts(stage3Conf));
    stage3Vertex.setTaskLocalResources(commonLocalResources);
    stage3Vertex.setTaskEnvironment(commonEnv);

    dag.addVertex(stage1Vertex);
    dag.addVertex(stage2Vertex);
    dag.addVertex(stage3Vertex);

    Edge edge1 = new Edge(stage1Vertex, stage2Vertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, new OutputDescriptor(
        OnFileSortedOutput.class.getName()), new InputDescriptor(
                ShuffledMergedInputLegacy.class.getName())));
    Edge edge2 = new Edge(stage2Vertex, stage3Vertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, new OutputDescriptor(
        OnFileSortedOutput.class.getName()), new InputDescriptor(
                ShuffledMergedInputLegacy.class.getName())));

    dag.addEdge(edge1);
    dag.addEdge(edge2);

    Map<String, LocalResource> amLocalResources =
        new HashMap<String, LocalResource>();
    amLocalResources.putAll(commonLocalResources);

    TezConfiguration tezConf = new TezConfiguration(
            mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        remoteStagingDir.toString());

    TezClient tezClient = new TezClient(tezConf);
    DAGClient dagClient = null;
    TezSession tezSession = null;
    boolean reuseSession = reUseTezSession != null;
    TezSessionConfiguration tezSessionConfig;
    AMConfiguration amConfig = new AMConfiguration(
        commonEnv, amLocalResources,
        tezConf, null);
    if(!dagViaRPC) {
      // TODO Use utility method post TEZ-205 to figure out AM arguments etc.
      dagClient = tezClient.submitDAGApplication(dag, amConfig);
    } else {
      if (reuseSession) {
        tezSession = reUseTezSession;
      } else {
        tezSessionConfig = new TezSessionConfiguration(amConfig, tezConf);
        tezSession = new TezSession("testsession", tezSessionConfig);
        tezSession.start();
      }
    }

    if (dagViaRPC && closeSessionBeforeSubmit) {
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();
      boolean sentKillSession = false;
      while(true) {
        Thread.sleep(500l);
        ApplicationReport appReport =
            yarnClient.getApplicationReport(tezSession.getApplicationId());
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
      LOG.info("Submitting dag to tez session with appId=" + tezSession.getApplicationId()
          + " and Dag Name=" + dag.getName());
      dagClient = tezSession.submitDAG(dag, additionalLocalResources);
      Assert.assertEquals(TezSessionStatus.RUNNING,
          tezSession.getSessionStatus());
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
    if (dagViaRPC && !reuseSession) {
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
    public List<Event> initialize(TezRootInputInitializerContext rootInputContext)  throws Exception {
      MRInputUserPayloadProto userPayloadProto = MRHelpers
          .parseMRInputPayload(rootInputContext.getUserPayload());
      Configuration conf = MRHelpers.createConfFromByteString(userPayloadProto
          .getConfigurationBytes());

      try {
        RuntimeUtils.getClazz(RELOCALIZATION_TEST_CLASS_NAME);
        LOG.info("Class found");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/tmp/relocalizationfilefound"));
      } catch (TezUncheckedException e) {
        LOG.info("Class not found");
      }

      return super.initialize(rootInputContext);
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

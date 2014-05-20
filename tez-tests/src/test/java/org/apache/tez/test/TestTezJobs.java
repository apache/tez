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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.examples.ExampleDriver;
import org.apache.tez.mapreduce.examples.IntersectDataGen;
import org.apache.tez.mapreduce.examples.IntersectExample;
import org.apache.tez.mapreduce.examples.IntersectValidate;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor.SleepProcessorConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests which do not rely on Map/Reduce processor
 * 
 */
public class TestTezJobs {

  private static final Log LOG = LogFactory.getLog(TestTezJobs.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private Random random = new Random();

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

    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestTezJobs.class.getName(), 1, 1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
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

    DAG dag = new DAG("TezSleepProcessor");
    Vertex vertex = new Vertex("SleepVertex", new ProcessorDescriptor(
        SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
        Resource.newInstance(1024, 1));
    dag.addVertex(vertex);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
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

  @Test(timeout = 100000)
  public void testMultipleDAGsWithDuplicateName() throws TezException, IOException,
      InterruptedException {
    TezSession tezSession = null;
    try {
      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      Path remoteStagingDir = remoteFs.makeQualified(new Path("/tmp", String.valueOf(random
          .nextInt(100000))));
      remoteFs.mkdirs(remoteStagingDir);
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
      AMConfiguration amConf = new AMConfiguration(new HashMap<String, String>(),
          new HashMap<String, LocalResource>(), tezConf, null);
      TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConf, tezConf);

      TezClient tezClient = new TezClient(tezConf);
      ApplicationId appId = tezClient.createApplication();
      tezSession = new TezSession("OrderedWordCountSession", appId, sessionConfig);
      tezSession.start();

      SleepProcessorConfig spConf = new SleepProcessorConfig(1);
      for (int dagIndex = 1; dagIndex <= 2; dagIndex++) {
        DAG dag = new DAG("TezSleepProcessor");
        Vertex vertex = new Vertex("SleepVertex", new ProcessorDescriptor(
            SleepProcessor.class.getName()).setUserPayload(spConf.toUserPayload()), 1,
            Resource.newInstance(1024, 1));
        dag.addVertex(vertex);

        DAGClient dagClient = null;
        try {
          dagClient = tezSession.submitDAG(dag);
          if (dagIndex > 1) {
            fail("Should fail due to duplicate dag name for dagIndex: " + dagIndex);
          }
        } catch (TezException tex) {
          if (dagIndex > 1) {
            assertTrue(tex.getMessage().contains("Duplicate dag name "));
            continue;
          }
          fail("DuplicateDAGName exception thrown for 1st DAG submission");
        }
        DAGStatus dagStatus = dagClient.getDAGStatus(null);
        while (!dagStatus.isCompleted()) {
          LOG.debug("Waiting for job to complete. Sleeping for 500ms." + " Current state: "
              + dagStatus.getState());
          Thread.sleep(500l);
          dagStatus = dagClient.getDAGStatus(null);
        }
      }
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
  

  @Test(timeout = 60000)
  public void testIntersectExample() throws Exception {
    IntersectExample intersectExample = new IntersectExample();
    intersectExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    Path inPath1 = new Path("/tmp/inPath1");
    Path inPath2 = new Path("/tmp/inPath2");
    Path outPath = new Path("/tmp/outPath");
    remoteFs.mkdirs(inPath1);
    remoteFs.mkdirs(inPath2);
    remoteFs.mkdirs(stagingDirPath);

    Set<String> expectedResult = new HashSet<String>();

    FSDataOutputStream out1 = remoteFs.create(new Path(inPath1, "file"));
    FSDataOutputStream out2 = remoteFs.create(new Path(inPath2, "file"));
    BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(out1));
    BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(out2));
    for (int i = 0; i < 20; i++) {
      String term = "term" + i;
      writer1.write(term);
      writer1.newLine();
      if (i % 2 == 0) {
        writer2.write(term);
        writer2.newLine();
        expectedResult.add(term);
      }
    }
    writer1.close();
    writer2.close();
    out1.close();
    out2.close();

    String[] args = new String[] {
        "-D" + TezConfiguration.TEZ_AM_STAGING_DIR + "=" + stagingDirPath.toString(),
        inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, intersectExample.run(args));

    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line = null;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  @Test(timeout = 120000)
  public void testIntersect2() throws Exception {

    Path testDir = new Path("/tmp/testIntersect2");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    AMConfiguration amConfiguration = new AMConfiguration(null, null, tezConf, null);
    TezSessionConfiguration sessionConfiguration = new TezSessionConfiguration(amConfiguration,
        tezConf);
    TezSession tezSession = null;
    try {
      tezSession = new TezSession("IntersectExampleSession", sessionConfiguration);
      tezSession.start();

      IntersectDataGen dataGen = new IntersectDataGen();
      String[] dataGenArgs = new String[] {
          dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
          expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      IntersectExample intersect = new IntersectExample();
      String[] intersectArgs = new String[] {
          dataPath1.toString(), dataPath2.toString(), "2", outPath.toString() };
      assertEquals(0, intersect.run(tezConf, intersectArgs, tezSession));

      IntersectValidate intersectValidate = new IntersectValidate();
      String[] intersectValidateArgs = new String[] {
          expectedOutputPath.toString(), outPath.toString(), "3" };
      assertEquals(0, intersectValidate.run(tezConf, intersectValidateArgs, tezSession));

    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
  
  @Test
  public void testNonDefaultFSStagingDir() throws Exception {
    SleepProcessorConfig spConf = new SleepProcessorConfig(1);

    DAG dag = new DAG("TezSleepProcessor");
    Vertex vertex = new Vertex("SleepVertex", new ProcessorDescriptor(
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


}

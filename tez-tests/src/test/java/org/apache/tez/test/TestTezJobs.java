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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.tez.common.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.mapreduce.examples.CartesianProduct;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.examples.SimpleSessionExample;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.HashJoinExample;
import org.apache.tez.examples.JoinValidate;
import org.apache.tez.examples.SortMergeJoinExample;
import org.apache.tez.mapreduce.examples.MultipleCommitsExample;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.apache.tez.test.dag.MultiAttemptDAG;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests for Tez example jobs
 * 
 */
public class TestTezJobs {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezJobs.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;
  private static FileSystem localFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestTezJobs.class.getName()
      + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    localFs = FileSystem.getLocal(conf);
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
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      conf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
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
  public void testHashJoinExample() throws Exception {
    HashJoinExample hashJoinExample = new HashJoinExample();
    hashJoinExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    Path inPath1 = new Path("/tmp/hashJoin/inPath1");
    Path inPath2 = new Path("/tmp/hashJoin/inPath2");
    Path outPath = new Path("/tmp/hashJoin/outPath");
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
        "-counter", inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, hashJoinExample.run(args));

    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  /**
   * test whole {@link HashJoinExample} pipeline as following: <br>
   * {@link JoinDataGen} -> {@link HashJoinExample} -> {@link JoinValidate}
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testHashJoinExampleWithDataViaEvent() throws Exception {

    Path testDir = new Path("/tmp/testHashJoinExampleDataViaEvent");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());

    //turn on the dataViaEvent
    tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED, true);

    TezClient tezSession = null;
    try {
      tezSession = TezClient.create("HashJoinExampleSession", tezConf, true);
      tezSession.start();

      JoinDataGen dataGen = new JoinDataGen();
      String[] dataGenArgs = new String[] {
              "-counter",
              dataPath1.toString(), "1048576", dataPath2.toString(), "8",
              expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      HashJoinExample joinExample = new HashJoinExample();
      String[] args = new String[] {
              dataPath1.toString(), dataPath2.toString(), "1", outPath.toString(),
              "doBroadcast"};

      assertEquals(0, joinExample.run(tezConf, args, tezSession));

      JoinValidate joinValidate = new JoinValidate();
      String[] validateArgs = new String[] {
              "-counter", expectedOutputPath.toString(), outPath.toString(), "3" };
      assertEquals(0, joinValidate.run(tezConf, validateArgs, tezSession));
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }

  @Test(timeout = 60000)
  public void testHashJoinExampleDisableSplitGrouping() throws Exception {
    HashJoinExample hashJoinExample = new HashJoinExample();
    hashJoinExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path(TEST_ROOT_DIR + "/tmp/tez-staging-dir");
    Path inPath1 = new Path(TEST_ROOT_DIR + "/tmp/hashJoin/inPath1");
    Path inPath2 = new Path(TEST_ROOT_DIR + "/tmp/hashJoin/inPath2");
    Path outPath = new Path(TEST_ROOT_DIR + "/tmp/hashJoin/outPath");
    localFs.delete(outPath, true);
    localFs.mkdirs(inPath1);
    localFs.mkdirs(inPath2);
    localFs.mkdirs(stagingDirPath);

    Set<String> expectedResult = new HashSet<String>();

    FSDataOutputStream out1 = localFs.create(new Path(inPath1, "file"));
    FSDataOutputStream out2 = localFs.create(new Path(inPath2, "file"));
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
        "-counter", "-local", "-disableSplitGrouping",
        inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, hashJoinExample.run(args));

    FileStatus[] statuses = localFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = localFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  @Test(timeout = 60000)
  public void testSortMergeJoinExample() throws Exception {
    SortMergeJoinExample sortMergeJoinExample = new SortMergeJoinExample();
    sortMergeJoinExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    Path inPath1 = new Path("/tmp/sortMerge/inPath1");
    Path inPath2 = new Path("/tmp/sortMerge/inPath2");
    Path outPath = new Path("/tmp/sortMerge/outPath");
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
        "-D" + TezConfiguration.TEZ_AM_APPLICATION_PRIORITY + "=" + "2",
        "-counter", inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, sortMergeJoinExample.run(args));

    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  @Test(timeout = 60000)
  public void testPerIOCounterAggregation() throws Exception {
    String baseDir = "/tmp/perIOCounterAgg/";
    Path inPath1 = new Path(baseDir + "inPath1");
    Path inPath2 = new Path(baseDir + "inPath2");
    Path outPath = new Path(baseDir + "outPath");
    final Set<String> expectedResults = generateSortMergeJoinInput(inPath1, inPath2);
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);

    TezConfiguration conf = new TezConfiguration(mrrTezCluster.getConfig());
    conf.setBoolean(TezConfiguration.TEZ_TASK_GENERATE_COUNTERS_PER_IO, true);
    TezClient tezClient = TezClient.create(SortMergeJoinHelper.class.getSimpleName(), conf);
    tezClient.start();

    SortMergeJoinHelper sortMergeJoinHelper = new SortMergeJoinHelper(tezClient);
    sortMergeJoinHelper.setConf(conf);

    String[] args = new String[] {
        "-D" + TezConfiguration.TEZ_AM_STAGING_DIR + "=" + stagingDirPath.toString(),
        "-counter", inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, sortMergeJoinHelper.run(conf, args, tezClient));

    verifySortMergeJoinInput(outPath, expectedResults);

    String joinerVertexName = "joiner";
    String input1Name = "input1";
    String input2Name = "input2";
    String joinOutputName = "joinOutput";
    Set<StatusGetOpts> statusOpts = new HashSet<StatusGetOpts>();
    statusOpts.add(StatusGetOpts.GET_COUNTERS);
    VertexStatus joinerVertexStatus =
        sortMergeJoinHelper.dagClient.getVertexStatus(joinerVertexName, statusOpts);
    final TezCounters joinerCounters = joinerVertexStatus.getVertexCounters();
    final CounterGroup aggregatedGroup = joinerCounters.getGroup(TaskCounter.class.getCanonicalName());
    final CounterGroup input1Group = joinerCounters.getGroup(
        TaskCounter.class.getSimpleName() + "_" + joinerVertexName + "_INPUT_" + input1Name);
    final CounterGroup input2Group = joinerCounters.getGroup(
        TaskCounter.class.getSimpleName() + "_" + joinerVertexName + "_INPUT_" + input2Name);
    assertTrue("aggregated counter group cannot be empty", aggregatedGroup.size() > 0);
    assertTrue("per io group for input1 cannot be empty", input1Group.size() > 0);
    assertTrue("per io group for input1 cannot be empty", input2Group.size() > 0);

    List<TaskCounter> countersToVerifyAgg = Arrays.asList(
        TaskCounter.ADDITIONAL_SPILLS_BYTES_READ,
        TaskCounter.ADDITIONAL_SPILLS_BYTES_WRITTEN,
        TaskCounter.COMBINE_INPUT_RECORDS,
        TaskCounter.MERGED_MAP_OUTPUTS,
        TaskCounter.NUM_DISK_TO_DISK_MERGES,
        TaskCounter.NUM_FAILED_SHUFFLE_INPUTS,
        TaskCounter.NUM_MEM_TO_DISK_MERGES,
        TaskCounter.NUM_SHUFFLED_INPUTS,
        TaskCounter.NUM_SKIPPED_INPUTS,
        TaskCounter.REDUCE_INPUT_GROUPS,
        TaskCounter.REDUCE_INPUT_RECORDS,
        TaskCounter.SHUFFLE_BYTES,
        TaskCounter.SHUFFLE_BYTES_DECOMPRESSED,
        TaskCounter.SHUFFLE_BYTES_DISK_DIRECT,
        TaskCounter.SHUFFLE_BYTES_TO_DISK,
        TaskCounter.SHUFFLE_BYTES_TO_MEM,
        TaskCounter.SPILLED_RECORDS
    );

    int nonZeroCounters = 0;
    // verify that the sum of the counter values for edges add up to the aggregated counter value.
    for(TaskCounter c : countersToVerifyAgg) {
      TezCounter aggregatedCounter = aggregatedGroup.findCounter(c.name(), false);
      TezCounter input1Counter = input1Group.findCounter(c.name(), false);
      TezCounter input2Counter = input2Group.findCounter(c.name(), false);
      assertNotNull("aggregated counter cannot be null " + c.name(), aggregatedCounter);
      assertNotNull("input1 counter cannot be null " + c.name(), input1Counter);
      assertNotNull("input2 counter cannot be null " + c.name(), input2Counter);

      assertEquals("aggregated counter does not match sum of input counters " + c.name(),
          aggregatedCounter.getValue(), input1Counter.getValue() + input2Counter.getValue());

      if (aggregatedCounter.getValue() > 0) {
        nonZeroCounters++;
      }
    }

    // ensure that at least one of the counters tested above were non-zero.
    assertTrue("At least one of the counter should be non-zero. invalid test ", nonZeroCounters > 0);

    CounterGroup joinerOutputGroup = joinerCounters.getGroup(
        TaskCounter.class.getSimpleName() + "_" + joinerVertexName + "_OUTPUT_" + joinOutputName);
    String outputCounterName = TaskCounter.OUTPUT_RECORDS.name();
    TezCounter aggregateCounter = aggregatedGroup.findCounter(outputCounterName, false);
    TezCounter joinerOutputCounter = joinerOutputGroup.findCounter(outputCounterName, false);
    assertNotNull("aggregated counter cannot be null " + outputCounterName, aggregateCounter);
    assertNotNull("output counter cannot be null " + outputCounterName, joinerOutputCounter);
    assertTrue("counter value is zero. test is invalid", aggregateCounter.getValue() > 0);
    assertEquals("aggregated counter does not match sum of output counters " + outputCounterName,
        aggregateCounter.getValue(), joinerOutputCounter.getValue());
  }


  @Test(timeout = 60000)
  public void testSortMergeJoinExampleDisableSplitGrouping() throws Exception {
    SortMergeJoinExample sortMergeJoinExample = new SortMergeJoinExample();
    sortMergeJoinExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path(TEST_ROOT_DIR + "/tmp/tez-staging-dir");
    Path inPath1 = new Path(TEST_ROOT_DIR + "/tmp/sortMerge/inPath1");
    Path inPath2 = new Path(TEST_ROOT_DIR + "/tmp/sortMerge/inPath2");
    Path outPath = new Path(TEST_ROOT_DIR + "/tmp/sortMerge/outPath");
    localFs.delete(outPath, true);
    localFs.mkdirs(inPath1);
    localFs.mkdirs(inPath2);
    localFs.mkdirs(stagingDirPath);

    Set<String> expectedResult = new HashSet<String>();

    FSDataOutputStream out1 = localFs.create(new Path(inPath1, "file"));
    FSDataOutputStream out2 = localFs.create(new Path(inPath2, "file"));
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
        "-counter", "-local","-disableSplitGrouping",
        inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, sortMergeJoinExample.run(args));

    FileStatus[] statuses = localFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = localFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  /**
   * test whole {@link HashJoinExample} pipeline as following: <br>
   * {@link JoinDataGen} -> {@link HashJoinExample} -> {@link JoinValidate}
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testHashJoinExamplePipeline() throws Exception {

    Path testDir = new Path("/tmp/testHashJoinExample");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;
    try {
      tezSession = TezClient.create("HashJoinExampleSession", tezConf, true);
      tezSession.start();

      JoinDataGen dataGen = new JoinDataGen();
      String[] dataGenArgs = new String[] {
          "-counter",
          dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
          expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      HashJoinExample joinExample = new HashJoinExample();
      String[] args = new String[] {
          dataPath1.toString(), dataPath2.toString(), "2", outPath.toString() };
      assertEquals(0, joinExample.run(tezConf, args, tezSession));

      JoinValidate joinValidate = new JoinValidate();
      String[] validateArgs = new String[] {
          "-counter", expectedOutputPath.toString(), outPath.toString(), "3" };
      assertEquals(0, joinValidate.run(tezConf, validateArgs, tezSession));

    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }

  /**
   * test whole {@link SortMergeJoinExample} pipeline as following: <br>
   * {@link JoinDataGen} -> {@link SortMergeJoinExample} -> {@link JoinValidate}
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testSortMergeJoinExamplePipeline() throws Exception {

    Path testDir = new Path("/tmp/testSortMergeExample");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;
    try {
      tezSession = TezClient.create("SortMergeExampleSession", tezConf, true);
      tezSession.start();

      JoinDataGen dataGen = new JoinDataGen();
      String[] dataGenArgs = new String[] {
          dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
          expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      SortMergeJoinExample joinExample = new SortMergeJoinExample();
      String[] args = new String[] {
          dataPath1.toString(), dataPath2.toString(), "2", outPath.toString() };
      assertEquals(0, joinExample.run(tezConf, args, tezSession));

      JoinValidate joinValidate = new JoinValidate();
      String[] validateArgs = new String[] {
          expectedOutputPath.toString(), outPath.toString(), "3" };
      assertEquals(0, joinValidate.run(tezConf, validateArgs, tezSession));

    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }

  public static void generateOrderedWordCountInput(Path inputDir, FileSystem fs) throws IOException {
    Path dataPath1 = new Path(inputDir, "inPath1");
    Path dataPath2 = new Path(inputDir, "inPath2");

    FSDataOutputStream f1 = null;
    FSDataOutputStream f2 = null;
    try {
      f1 = fs.create(dataPath1);
      f2 = fs.create(dataPath2);

      final String prefix = "a";
      for (int i = 1; i <= 10; ++i) {
        final String word = prefix + "_" + i;
        for (int j = 10; j >= i; --j) {
          LOG.info("Writing " + word + " to input files");
          f1.write(word.getBytes());
          f1.writeChars("\t");
          f2.write(word.getBytes());
          f2.writeChars("\t");
        }
      }
      f1.hsync();
      f2.hsync();
    } finally {
      if (f1 != null) {
        f1.close();
      }
      if (f2 != null) {
        f2.close();
      }
    }
  }

  public static void verifyOrderedWordCountOutput(Path resultFile, FileSystem fs) throws IOException {
    FSDataInputStream inputStream = fs.open(resultFile);
    final String prefix = "a";
    int currentCounter = 10;

    byte[] buffer = new byte[4096];
    int bytesRead = inputStream.read(buffer, 0, 4096);

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer, 0, bytesRead)));

    String line;
    while ((line = reader.readLine()) != null) {
      LOG.info("Line: " + line + ", counter=" + currentCounter);
      int pos = line.indexOf("\t");
      String word = line.substring(0, pos-1);
      Assert.assertEquals(prefix + "_" + currentCounter, word);
      String val = line.substring(pos+1, line.length());
      Assert.assertEquals((long)(11 - currentCounter) * 2, (long)Long.valueOf(val));
      currentCounter--;
    }

    Assert.assertEquals(0, currentCounter);
  }
  
  public static void verifyOutput(Path outputDir, FileSystem fs) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(outputDir);
    Path resultFile = null;
    boolean foundResult = false;
    boolean foundSuccessFile = false;
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isFile()) {
        continue;
      }
      if (fileStatus.getPath().getName().equals("_SUCCESS")) {
        foundSuccessFile = true;
        continue;
      }
      if (fileStatus.getPath().getName().startsWith("part-")) {
        if (foundResult) {
          fail("Found 2 part files instead of 1"
              + ", paths=" + resultFile + "," + fileStatus.getPath());
        }
        foundResult = true;
        resultFile = fileStatus.getPath();
        LOG.info("Found output at " + resultFile);
      }
    }
    assertTrue(foundResult);
    assertTrue(resultFile != null);
    assertTrue(foundSuccessFile);
    verifyOrderedWordCountOutput(resultFile, fs);
  }
  
  @Test(timeout = 60000)
  public void testOrderedWordCount() throws Exception {
    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    generateOrderedWordCountInput(inputDir, remoteFs);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {

      OrderedWordCount job = new OrderedWordCount();
      Assert.assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{"-counter", inputDirStr, outputDirStr, "2"}, null)==0);
      verifyOutput(outputDir, remoteFs);

    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }

  }
  
  @Test(timeout = 60000)
  public void testOrderedWordCountDisableSplitGrouping() throws Exception {
    String inputDirStr = TEST_ROOT_DIR + "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path(TEST_ROOT_DIR + "/tmp/owc-staging-dir");
    localFs.mkdirs(inputDir);
    localFs.mkdirs(stagingDirPath);
    generateOrderedWordCountInput(inputDir, localFs);

    String outputDirStr = TEST_ROOT_DIR + "/tmp/owc-output/";
    localFs.delete(new Path(outputDirStr), true);
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(conf);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {

      OrderedWordCount job = new OrderedWordCount();
      Assert.assertTrue("OrderedWordCount failed", job.run(tezConf, new String[]{"-counter", "-local", "-disableSplitGrouping",
          inputDirStr, outputDirStr, "2"}, null)==0);
      verifyOutput(outputDir, localFs);

    } finally {
      localFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }

  }

  @Test(timeout = 60000)
  public void testSimpleSessionExample() throws Exception {
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(stagingDirPath);

    int numIterations = 2;
    String[] inputPaths = new String[numIterations];
    String[] outputPaths = new String[numIterations];
    Path[] outputDirs = new Path[numIterations];
    for (int i=0; i<numIterations; ++i) {
      String inputDirStr = "/tmp/owc-input-" + i + "/";
      inputPaths[i] = inputDirStr;
      Path inputDir = new Path(inputDirStr);
      remoteFs.mkdirs(inputDir);
      generateOrderedWordCountInput(inputDir, remoteFs);
      String outputDirStr = "/tmp/owc-output-" + i + "/"; 
      outputPaths[i] = outputDirStr;
      Path outputDir = new Path(outputDirStr);
      outputDirs[i] = outputDir;
    }


    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    YarnClient yarnClient = YarnClient.createYarnClient();

    try {
      
      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();
      
      List<ApplicationReport> apps = yarnClient.getApplications();
      int appsBeforeCount = apps != null ? apps.size() : 0;

      SimpleSessionExample job = new SimpleSessionExample();
      tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
      Assert.assertTrue(
          "SimpleSessionExample failed",
          job.run(tezConf, new String[] { StringUtils.join(",", inputPaths),
              StringUtils.join(",", outputPaths), "2" }, null) == 0);

      for (int i=0; i<numIterations; ++i) {
        verifyOutput(outputDirs[i], remoteFs);
      }
      
      apps = yarnClient.getApplications();
      int appsAfterCount = apps != null ? apps.size() : 0;
      
      // Running in session mode. So should only create 1 more app.
      Assert.assertEquals(appsBeforeCount + 1, appsAfterCount);
    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }

  }

  @Test(timeout = 60000)
  public void testInvalidQueueSubmission() throws Exception {

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    YarnClient yarnClient = YarnClient.createYarnClient();
    try {

      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();

      SimpleSessionExample job = new SimpleSessionExample();
      tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
      tezConf.set(TezConfiguration.TEZ_QUEUE_NAME, "nonexistent");

      String[] inputPaths = new String[1];
      String[] outputPaths = new String[1];
      String inputDirStr = "/tmp/owc-input";
      inputPaths[0] = inputDirStr;
      Path inputDir = new Path(inputDirStr);
      remoteFs.mkdirs(inputDir);
      String outputDirStr = "/tmp/owc-output";
      outputPaths[0] = outputDirStr;
      int result = job.run(tezConf, new String[] { StringUtils.join(",", inputPaths),
          StringUtils.join(",", outputPaths), "2" }, null);
      Assert.assertTrue("Job should have failed", result != 0);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to submit application"));
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }
  }

  @Test(timeout = 60000)
  public void testInvalidQueueSubmissionToSession() throws Exception {

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    YarnClient yarnClient = YarnClient.createYarnClient();
    try {

      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();

      SimpleSessionExample job = new SimpleSessionExample();
      tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
      tezConf.set(TezConfiguration.TEZ_QUEUE_NAME, "nonexistent");

      String[] inputPaths = new String[1];
      String[] outputPaths = new String[1];
      String inputDirStr = "/tmp/owc-input";
      inputPaths[0] = inputDirStr;
      Path inputDir = new Path(inputDirStr);
      remoteFs.mkdirs(inputDir);
      String outputDirStr = "/tmp/owc-output";
      outputPaths[0] = outputDirStr;
      job.run(tezConf, new String[] { StringUtils.join(",", inputPaths),
          StringUtils.join(",", outputPaths), "2" }, null);
      fail("Job submission should have failed");
    } catch (SessionNotRunning e) {
      // Expected
      LOG.info("Session not running", e);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to submit application"));
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }

  }


  @Test (timeout=60000)
  public void testVertexOrder() throws Exception {
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    TezClient tezClient = TezClient.create("TestVertexOrder", tezConf);
    tezClient.start();

    try {
    DAG dag = SimpleTestDAG.createDAGForVertexOrder("dag1", conf);
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.getDAGStatus(null);
    while (!dagStatus.isCompleted()) {
      LOG.info("Waiting for dag to complete. Sleeping for 500ms."
          + " DAG name: " + dag.getName()
          + " DAG context: " + dagClient.getExecutionContext()
          + " Current state: " + dagStatus.getState());
      Thread.sleep(100);
      dagStatus = dagClient.getDAGStatus(null);
    }

    Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());

    // verify vertex order
    Set<String> resultVertices = dagStatus.getVertexProgress().keySet();
    Assert.assertEquals(6, resultVertices.size());
    int i = 0;
    for (String vertexName : resultVertices){
      if (i <= 1){
        Assert.assertTrue( vertexName.equals("v1") || vertexName.equals("v2"));
      } else if (i == 2){
        Assert.assertTrue( vertexName.equals("v3"));
      } else if (i <= 4){
        Assert.assertTrue( vertexName.equals("v4") || vertexName.equals("v5"));
      } else {
        Assert.assertTrue( vertexName.equals("v6"));
      }
      i++;
    }
    } finally {
      if (tezClient != null) {
        tezClient.stop();
      }
    }
  }

  @Test(timeout = 60000)
  public void testInputInitializerEvents() throws TezException, InterruptedException, IOException {

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    TezClient tezClient = TezClient.create("TestInputInitializerEvents", tezConf);
    tezClient.start();

    try {
      DAG dag = DAG.create("TestInputInitializerEvents");
      Vertex vertex1 = Vertex.create(VERTEX_WITH_INITIALIZER_NAME, ProcessorDescriptor.create(
          SleepProcessor.class.getName())
          .setUserPayload(new SleepProcessor.SleepProcessorConfig(1).toUserPayload()), 1)
          .addDataSource(INPUT1_NAME,
              DataSourceDescriptor
                  .create(InputDescriptor.create(MultiAttemptDAG.NoOpInput.class.getName()),
                      InputInitializerDescriptor.create(InputInitializerForTest.class.getName()),
                      null));
      Vertex vertex2 = Vertex.create(EVENT_GENERATING_VERTEX_NAME,
          ProcessorDescriptor.create(InputInitializerEventGeneratingProcessor.class.getName()), 5);

      dag.addVertex(vertex1).addVertex(vertex2);

      DAGClient dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletion();
      Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    } finally {
      tezClient.stop();
    }
  }

  @Test(timeout = 60000)
  public void testMultipleCommits_OnDAGSuccess() throws Exception {
    Path stagingDirPath = new Path("/tmp/commit-staging-dir");
    Random rand = new Random();
    String v1OutputPathPrefix = "/tmp/commit-output-v1";
    int v1OutputNum = rand.nextInt(10) + 1;
    String v2OutputPathPrefix = "/tmp/commit-output-v2";
    int v2OutputNum = rand.nextInt(10) + 1;
    String uv12OutputPathPrefix = "/tmp/commit-output-uv12";
    int uv12OutputNum = rand.nextInt(10) + 1;
    String v3OutputPathPrefix = "/tmp/commit-output-v3";
    int v3OutputNum = rand.nextInt(10) + 1;
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {
      MultipleCommitsExample job = new MultipleCommitsExample();
      Assert.assertTrue("MultipleCommitsExample failed", job.run(tezConf,
          new String[]{ v1OutputPathPrefix, v1OutputNum + "", v2OutputPathPrefix, v2OutputNum + "",
          uv12OutputPathPrefix, uv12OutputNum + "", v3OutputPathPrefix, v3OutputNum + ""}, null)==0);
      verifyCommits(v1OutputPathPrefix, v1OutputNum);
      verifyCommits(v2OutputPathPrefix, v2OutputNum);
      verifyCommits(uv12OutputPathPrefix, uv12OutputNum);
      verifyCommits(v3OutputPathPrefix, v3OutputNum);
    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
  
  @Test(timeout = 60000)
  public void testMultipleCommits_OnVertexSuccess() throws Exception {
    Path stagingDirPath = new Path("/tmp/commit-staging-dir");
    Random rand = new Random();
    String v1OutputPathPrefix = "/tmp/commit-output-v1";
    int v1OutputNum = rand.nextInt(10) + 1;
    String v2OutputPathPrefix = "/tmp/commit-output-v2";
    int v2OutputNum = rand.nextInt(10) + 1;
    String uv12OutputPathPrefix = "/tmp/commit-output-uv12";
    int uv12OutputNum = rand.nextInt(10) + 1;
    String v3OutputPathPrefix = "/tmp/commit-output-v3";
    int v3OutputNum = rand.nextInt(10) + 1;
    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {
      MultipleCommitsExample job = new MultipleCommitsExample();
      Assert.assertTrue("MultipleCommitsExample failed", job.run(tezConf,
          new String[]{ v1OutputPathPrefix, v1OutputNum + "", v2OutputPathPrefix, v2OutputNum + "",
          uv12OutputPathPrefix, uv12OutputNum + "", v3OutputPathPrefix, v3OutputNum + "",
          MultipleCommitsExample.CommitOnVertexSuccessOption}, null)==0);
      verifyCommits(v1OutputPathPrefix, v1OutputNum);
      verifyCommits(v2OutputPathPrefix, v2OutputNum);
      verifyCommits(uv12OutputPathPrefix, uv12OutputNum);
      verifyCommits(v3OutputPathPrefix, v3OutputNum);
    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
  
  private void verifyCommits(String outputPrefix, int outputNum) throws IllegalArgumentException, IOException {
    for (int i=0; i< outputNum; ++i) {
      String outputDir = outputPrefix + "_" + i;
      Assert.assertTrue("Output of " + outputDir + " is not succeeded",
          remoteFs.exists(new Path( outputDir + "/_SUCCESS")));
    }
  }

  private static final String VERTEX_WITH_INITIALIZER_NAME = "VertexWithInitializer";
  private static final String EVENT_GENERATING_VERTEX_NAME = "EventGeneratingVertex";
  private static final String INPUT1_NAME = "Input1";

  public static class InputInitializerEventGeneratingProcessor extends SimpleProcessor {

    public InputInitializerEventGeneratingProcessor(
        ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      if (getContext().getTaskIndex() == 1 && getContext().getTaskAttemptNumber() == 0) {
        throw new IOException("Failing task 2, attempt 0");
      }

      InputInitializerEvent initializerEvent = InputInitializerEvent.create(
          VERTEX_WITH_INITIALIZER_NAME, INPUT1_NAME,
          ByteBuffer.allocate(4).putInt(0, getContext().getTaskIndex()));
      List<Event> events = Lists.newArrayList();
      events.add(initializerEvent);
      getContext().sendEvents(events);
    }
  }

  public static class FailingAttemptProcessor extends SimpleProcessor {

    public FailingAttemptProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      if (getContext().getTaskIndex() == 0) {
        LOG.info("Failing task " + getContext().getTaskIndex() + ", attempt " + getContext().getTaskAttemptNumber());
        throw new IOException("Failing task " + getContext().getTaskIndex() + ", attempt " + getContext().getTaskAttemptNumber());
      }
    }
  }

  public static class InputInitializerForTest extends InputInitializer {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final BitSet eventsSeen = new BitSet();

    public InputInitializerForTest(
        InputInitializerContext initializerContext) {
      super(initializerContext);
      getContext().registerForVertexStateUpdates(EVENT_GENERATING_VERTEX_NAME, EnumSet.of(
          VertexState.SUCCEEDED));
    }

    @Override
    public List<Event> initialize() throws Exception {
      lock.lock();
      try {
        condition.await();
      } finally {
        lock.unlock();
      }
      return null;
    }


    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
      lock.lock();
      try {
        for (InputInitializerEvent event : events) {
          Preconditions.checkArgument(
              event.getSourceVertexName().equals(EVENT_GENERATING_VERTEX_NAME));
          int index = event.getUserPayload().getInt(0);
          Preconditions.checkState(!eventsSeen.get(index));
          eventsSeen.set(index);
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
      lock.lock();
      try {
        Preconditions.checkArgument(stateUpdate.getVertexState() == VertexState.SUCCEEDED);
        if (eventsSeen.cardinality() ==
            getContext().getVertexNumTasks(EVENT_GENERATING_VERTEX_NAME)) {
          condition.signal();
        } else {
          throw new IllegalStateException(
              "Received VertexState SUCCEEDED before receiving all InputInitializerEvents");
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private static class SortMergeJoinHelper extends SortMergeJoinExample {
    private final TezClient tezClientInternal;
    private DAGClient dagClient;

    public SortMergeJoinHelper(TezClient tezClient) {
      this.tezClientInternal = tezClient;
    }

    @Override
    public int runDag(DAG dag, boolean printCounters, Logger logger) throws TezException,
        InterruptedException, IOException {
      tezClientInternal.waitTillReady();
      dagClient = tezClientInternal.submitDAG(dag);
      Set<StatusGetOpts> getOpts = new HashSet<StatusGetOpts>();
      if (printCounters) {
        getOpts.add(StatusGetOpts.GET_COUNTERS);
      }

      DAGStatus dagStatus;
      dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
        return -1;
      }
      return 0;
    }
  }

  private Set<String> generateSortMergeJoinInput(Path inPath1, Path inPath2) throws
      IOException {
    remoteFs.mkdirs(inPath1);
    remoteFs.mkdirs(inPath2);

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

    return expectedResult;
  }

  private void verifySortMergeJoinInput(Path outPath, Set<String> expectedResult) throws
      IOException {
    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  @Test(timeout = 60000)
  public void testAMClientHeartbeatTimeout() throws Exception {
    Path stagingDirPath = new Path("/tmp/timeout-staging-dir");
    remoteFs.mkdirs(stagingDirPath);

    YarnClient yarnClient = YarnClient.createYarnClient();

    try {

      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();

      List<ApplicationReport> apps = yarnClient.getApplications();
      int appsBeforeCount = apps != null ? apps.size() : 0;

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
      tezConf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, 5);
      TezClient tezClient = TezClient.create("testAMClientHeartbeatTimeout", tezConf, true);
      tezClient.start();
      tezClient.cancelAMKeepAlive(true);

      ApplicationId appId = tezClient.getAppMasterApplicationId();

      apps = yarnClient.getApplications();
      int appsAfterCount = apps != null ? apps.size() : 0;

      // Running in session mode. So should only create 1 more app.
      Assert.assertEquals(appsBeforeCount + 1, appsAfterCount);

      ApplicationReport report;
      while (true) {
        report = yarnClient.getApplicationReport(appId);
        if (report.getYarnApplicationState() == YarnApplicationState.FINISHED
            || report.getYarnApplicationState() == YarnApplicationState.FAILED
            || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
          break;
        }
        Thread.sleep(1000);
      }
      // Add a sleep because YARN is not consistent in terms of reporting uptodate diagnostics
      Thread.sleep(2000);
      report = yarnClient.getApplicationReport(appId);
      LOG.info("App Report for appId=" + appId
          + ", report=" + report);
      Assert.assertTrue("Actual diagnostics: " + report.getDiagnostics(),
          report.getDiagnostics().contains("Client-to-AM Heartbeat timeout interval expired"));

    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }
  }

  @Test(timeout = 60000)
  public void testSessionTimeout() throws Exception {
    Path stagingDirPath = new Path("/tmp/sessiontimeout-staging-dir");
    remoteFs.mkdirs(stagingDirPath);

    YarnClient yarnClient = YarnClient.createYarnClient();

    try {

      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();

      List<ApplicationReport> apps = yarnClient.getApplications();
      int appsBeforeCount = apps != null ? apps.size() : 0;

      TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
      tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
      tezConf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, 5);
      TezClient tezClient = TezClient.create("testSessionTimeout", tezConf, true);
      tezClient.start();

      ApplicationId appId = tezClient.getAppMasterApplicationId();

      apps = yarnClient.getApplications();
      int appsAfterCount = apps != null ? apps.size() : 0;

      // Running in session mode. So should only create 1 more app.
      Assert.assertEquals(appsBeforeCount + 1, appsAfterCount);

      ApplicationReport report;
      while (true) {
        report = yarnClient.getApplicationReport(appId);
        if (report.getYarnApplicationState() == YarnApplicationState.FINISHED
            || report.getYarnApplicationState() == YarnApplicationState.FAILED
            || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
          break;
        }
        Thread.sleep(1000);
      }
      // Add a sleep because YARN is not consistent in terms of reporting uptodate diagnostics
      Thread.sleep(2000);
      report = yarnClient.getApplicationReport(appId);
      LOG.info("App Report for appId=" + appId
          + ", report=" + report);
      Assert.assertTrue("Actual diagnostics: " + report.getDiagnostics(),
          report.getDiagnostics().contains("Session timed out"));

    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }
  }

  @Test(timeout = 60000)
  public void testVertexFailuresMaxPercent() throws TezException, InterruptedException, IOException {

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_VERTEX_FAILURES_MAXPERCENT, "50.0f");
    tezConf.setInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 1);
    TezClient tezClient = TezClient.create("TestVertexFailuresMaxPercent", tezConf);
    tezClient.start();

    try {
      DAG dag = DAG.create("TestVertexFailuresMaxPercent");
      Vertex vertex1 = Vertex.create("Parent", ProcessorDescriptor.create(
          FailingAttemptProcessor.class.getName()), 2);
      Vertex vertex2 = Vertex.create("Child", ProcessorDescriptor.create(FailingAttemptProcessor.class.getName()), 2);

      OrderedPartitionedKVEdgeConfig edgeConfig = OrderedPartitionedKVEdgeConfig
          .newBuilder(Text.class.getName(), IntWritable.class.getName(),
              HashPartitioner.class.getName())
          .setFromConfiguration(tezConf)
          .build();
      dag.addVertex(vertex1)
          .addVertex(vertex2)
          .addEdge(Edge.create(vertex1, vertex2, edgeConfig.createDefaultEdgeProperty()));

      DAGClient dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletion();
      Assert.assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    } finally {
      tezClient.stop();
    }
  }

  @Test(timeout = 60000)
  public void testCartesianProduct() throws Exception {
    LOG.info("Running CartesianProduct Test");
    CartesianProduct job = new CartesianProduct();

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.setInt(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM, 10);
    tezConf.setInt(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER, 25);
    Assert.assertEquals("CartesianProduct failed", job.run(tezConf, null, null), 0);
  }
}

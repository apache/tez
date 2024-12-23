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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.RecoveryParser;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAMRecoveryAggregationBroadcast {
  private static final Logger LOG = LoggerFactory.getLogger(TestAMRecoveryAggregationBroadcast.class);
  private static final String INPUT1 = "Input";
  private static final String INPUT2 = "Input";
  private static final String OUTPUT = "Output";
  private static final String TABLE_SCAN = "TableScan";
  private static final String AGGREGATION = "Aggregation";
  private static final String MAP_JOIN = "MapJoin";
  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestAMRecoveryAggregationBroadcast.class.getName() + "-tmpDir";
  private static final Path INPUT_FILE = new Path(TEST_ROOT_DIR, "input.csv");
  private static final Path OUT_PATH = new Path(TEST_ROOT_DIR, "out-groups");
  private static final String EXPECTED_OUTPUT = "1-5\n1-5\n1-5\n1-5\n1-5\n"
      + "2-4\n2-4\n2-4\n2-4\n" + "3-3\n3-3\n3-3\n" + "4-2\n4-2\n" + "5-1\n";
  private static final String TABLE_SCAN_SLEEP = "tez.test.table.scan.sleep";
  private static final String AGGREGATION_SLEEP = "tez.test.aggregation.sleep";
  private static final String MAP_JOIN_SLEEP = "tez.test.map.join.sleep";

  private static Configuration dfsConf;
  private static MiniDFSCluster dfsCluster;
  private static MiniTezCluster tezCluster;
  private static FileSystem remoteFs;

  private TezConfiguration tezConf;
  private TezClient tezSession;

  @BeforeClass
  public static void setupAll() {
    try {
      dfsConf = new Configuration();
      dfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(dfsConf).numDataNodes(3).format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
      createSampleFile();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }

    if (tezCluster == null) {
      tezCluster = new MiniTezCluster(TestAMRecoveryAggregationBroadcast.class.getName(), 1, 1, 1);
      Configuration conf = new Configuration(dfsConf);
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      conf.setInt("yarn.nodemanager.delete.debug-delay-sec", 20000);
      conf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
      tezCluster.init(conf);
      tezCluster.start();
    }
  }

  private static void createSampleFile() throws IOException {
    FSDataOutputStream out = remoteFs.create(INPUT_FILE);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    // 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5
    for (int i = 1; i <= 5; i++) {
      for (int j = 0; j <= 5 - i; j++) {
        writer.write(String.valueOf(i));
        writer.newLine();
      }
    }
    writer.close();
  }

  @AfterClass
  public static void tearDownAll() {
    if (tezCluster != null) {
      tezCluster.stop();
      tezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown(true);
      dfsCluster = null;
    }
  }

  @Before
  public void setup() throws Exception {
    Path remoteStagingDir = remoteFs.makeQualified(new Path(TEST_ROOT_DIR, String
        .valueOf(new Random().nextInt(100000))));
    TezClientUtils.ensureStagingDirExists(dfsConf, remoteStagingDir);

    tezConf = new TezConfiguration(tezCluster.getConfig());
    tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "INFO");
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE, false);
    tezConf.setBoolean(RecoveryService.TEZ_TEST_RECOVERY_DRAIN_EVENTS_WHEN_STOPPED, true);

    tezSession = TezClient.create("TestAMRecoveryAggregationBroadcast", tezConf);
    tezSession.start();
  }

  @After
  public void teardown() throws InterruptedException {
    if (tezSession != null) {
      try {
        LOG.info("Stopping Tez Session");
        tezSession.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop Tez session", e);
      }
    }
    tezSession = null;
  }

  @Test(timeout = 120000)
  public void testSucceed() throws Exception {
    DAG dag = createDAG("Succeed");
    TezCounters counters = runDAGAndVerify(dag, false);
    assertEquals(3, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 1, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 2, 0).size());

    // No retry happens
    assertEquals(Collections.emptyList(), readRecoveryLog(2));
  }

  @Test(timeout = 120000)
  public void testTableScanTemporalFailure() throws Exception {
    tezConf.setBoolean(TABLE_SCAN_SLEEP, true);
    DAG dag = createDAG("TableScanTemporalFailure");
    TezCounters counters = runDAGAndVerify(dag, true);
    assertEquals(3, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 1, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 2, 0).size());

    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 1, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 2, 0).size());

    assertEquals(Collections.emptyList(), readRecoveryLog(3));
  }

  @Test(timeout = 120000)
  public void testAggregationTemporalFailure() throws Exception {
    tezConf.setBoolean(AGGREGATION_SLEEP, true);
    DAG dag = createDAG("AggregationTemporalFailure");
    TezCounters counters = runDAGAndVerify(dag, true);
    assertEquals(3, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 1, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 2, 0).size());

    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 1, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 2, 0).size());

    assertEquals(Collections.emptyList(), readRecoveryLog(3));
  }

  @Test(timeout = 120000)
  public void testMapJoinTemporalFailure() throws Exception {
    tezConf.setBoolean(MAP_JOIN_SLEEP, true);
    DAG dag = createDAG("MapJoinTemporalFailure");
    TezCounters counters = runDAGAndVerify(dag, true);
    assertEquals(3, counters.findCounter(DAGCounter.NUM_SUCCEEDED_TASKS).getValue());

    List<HistoryEvent> historyEvents1 = readRecoveryLog(1);
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 0, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents1, 1, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents1, 2, 0).size());

    List<HistoryEvent> historyEvents2 = readRecoveryLog(2);
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents2, 0, 0).size());
    assertEquals(0, findTaskAttemptFinishedEvent(historyEvents2, 1, 0).size());
    assertEquals(1, findTaskAttemptFinishedEvent(historyEvents2, 2, 0).size());

    assertEquals(Collections.emptyList(), readRecoveryLog(3));
  }

  /**
   * v1 scans lines and emit (line -> 1), imitating a simple Map vertex.
   * v2 aggregates the lines and emit (line -> # of duplicated values), imitating an aggregation.
   * v3 joins the output of v2 with another input. v2 broadcasts its output.
   * (input1)
   *    \
   *    v1
   *     \
   *     v2  (input2)
   *      \  /
   *       v3
   */
  private DAG createDAG(String dagName) throws Exception {
    UserPayload payload = TezUtils.createUserPayloadFromConf(tezConf);
    DataSourceDescriptor dataSource = MRInput
        .createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
            INPUT_FILE.toString())
        .build();
    // each line -> 1
    Vertex tableScanVertex = Vertex
        .create(TABLE_SCAN, ProcessorDescriptor.create(TableScanProcessor.class.getName())
            .setUserPayload(payload))
        .addDataSource(INPUT1, dataSource);

    // key -> num keys
    Vertex aggregationVertex = Vertex
        .create(AGGREGATION, ProcessorDescriptor
            .create(AggregationProcessor.class.getName()).setUserPayload(payload), 1);

    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class,
            OUT_PATH.toString())
        .build();
    // Broadcast Hash Join
    Vertex mapJoinVertex = Vertex
        .create(MAP_JOIN, ProcessorDescriptor.create(MapJoinProcessor.class.getName())
            .setUserPayload(payload))
        .addDataSource(INPUT2, dataSource)
        .addDataSink(OUTPUT, dataSink);

    EdgeProperty orderedEdge = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build()
        .createDefaultEdgeProperty();
    EdgeProperty broadcastEdge = UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName())
        .setFromConfiguration(tezConf)
        .build()
        .createDefaultBroadcastEdgeProperty();

    DAG dag = DAG.create("TestAMRecoveryAggregationBroadcast_" + dagName);
    dag.addVertex(tableScanVertex)
        .addVertex(aggregationVertex)
        .addVertex(mapJoinVertex)
        .addEdge(Edge.create(tableScanVertex, aggregationVertex, orderedEdge))
        .addEdge(Edge.create(aggregationVertex, mapJoinVertex, broadcastEdge));
    return dag;
  }

  TezCounters runDAGAndVerify(DAG dag, boolean killAM) throws Exception {
    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);

    if (killAM) {
      TimeUnit.SECONDS.sleep(10);
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(tezConf);
      yarnClient.start();
      ApplicationAttemptId id = ApplicationAttemptId.newInstance(tezSession.getAppMasterApplicationId(), 1);
      yarnClient.failApplicationAttempt(id);
      yarnClient.close();
    }
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(EnumSet.of(StatusGetOpts.GET_COUNTERS));
    LOG.info("Diagnosis: " + dagStatus.getDiagnostics());
    Assert.assertEquals(State.SUCCEEDED, dagStatus.getState());

    FSDataInputStream in = remoteFs.open(new Path(OUT_PATH, "part-v002-o000-r-00000"));
    ByteBuffer buf = ByteBuffer.allocate(100);
    in.read(buf);
    buf.flip();
    Assert.assertEquals(EXPECTED_OUTPUT, StandardCharsets.UTF_8.decode(buf).toString());
    return dagStatus.getDAGCounters();
  }

  private List<HistoryEvent> readRecoveryLog(int attemptNum) throws IOException {
    ApplicationId appId = tezSession.getAppMasterApplicationId();
    Path tezSystemStagingDir = TezCommonUtils.getTezSystemStagingPath(tezConf, appId.toString());
    Path recoveryDataDir = TezCommonUtils.getRecoveryPath(tezSystemStagingDir, tezConf);
    FileSystem fs = tezSystemStagingDir.getFileSystem(tezConf);
    List<HistoryEvent> historyEvents = new ArrayList<>();
    Path currentAttemptRecoveryDataDir = TezCommonUtils.getAttemptRecoveryPath(recoveryDataDir, attemptNum);
    Path recoveryFilePath =
        new Path(currentAttemptRecoveryDataDir, appId.toString().replace("application", "dag")
            + "_1" + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
    if (fs.exists(recoveryFilePath)) {
      LOG.info("Read recovery file:" + recoveryFilePath);
      historyEvents.addAll(RecoveryParser.parseDAGRecoveryFile(fs.open(recoveryFilePath)));
    }
    printHistoryEvents(historyEvents, attemptNum);
    return historyEvents;
  }

  private void printHistoryEvents(List<HistoryEvent> historyEvents, int attemptId) {
    LOG.info("RecoveryLogs from attempt:" + attemptId);
    for(HistoryEvent historyEvent : historyEvents) {
      LOG.info("Parsed event from recovery stream"
          + ", eventType=" + historyEvent.getEventType()
          + ", event=" + historyEvent);
    }
    LOG.info("");
  }

  private List<TaskAttemptFinishedEvent> findTaskAttemptFinishedEvent(
      List<HistoryEvent> historyEvents, int vertexId, int taskId) {
    List<TaskAttemptFinishedEvent> resultEvents = new ArrayList<>();
    for (HistoryEvent historyEvent : historyEvents) {
      if (historyEvent.getEventType() == HistoryEventType.TASK_ATTEMPT_FINISHED) {
        TaskAttemptFinishedEvent taFinishedEvent = (TaskAttemptFinishedEvent) historyEvent;
        if (taFinishedEvent.getState() == TaskAttemptState.KILLED) {
          continue;
        }
        if (taFinishedEvent.getVertexID().getId() == vertexId && taFinishedEvent.getTaskID().getId() == taskId) {
          resultEvents.add(taFinishedEvent);
        }
      }
    }
    return resultEvents;
  }

  public static class TableScanProcessor extends SimpleMRProcessor {
    private static final IntWritable one = new IntWritable(1);

    private final boolean sleep;

    public TableScanProcessor(ProcessorContext context) {
      super(context);
      try {
        Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        sleep = conf.getBoolean(TABLE_SCAN_SLEEP, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() throws Exception {
      if (getContext().getDAGAttemptNumber() == 1 && sleep) {
        TimeUnit.SECONDS.sleep(60);
      }
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT1).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(AGGREGATION).getWriter();
      while (kvReader.next()) {
        Text line = (Text) kvReader.getCurrentValue();
        kvWriter.write(line, one);
      }
    }
  }

  public static class AggregationProcessor extends SimpleMRProcessor {
    private final boolean sleep;

    public AggregationProcessor(ProcessorContext context) {
      super(context);
      try {
        Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        sleep = conf.getBoolean(AGGREGATION_SLEEP, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() throws Exception {
      if (getContext().getDAGAttemptNumber() == 1 && sleep) {
        TimeUnit.SECONDS.sleep(60);
      }

      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TABLE_SCAN).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(MAP_JOIN).getWriter();
      while (kvReader.next()) {
        Text word = (Text) kvReader.getCurrentKey();
        int sum = 0;
        for (Object value : kvReader.getCurrentValues()) {
          sum += ((IntWritable) value).get();
        }
        kvWriter.write(word, new IntWritable(sum));
      }
    }
  }

  public static class MapJoinProcessor extends SimpleMRProcessor {
    private final boolean sleep;

    public MapJoinProcessor(ProcessorContext context) {
      super(context);
      try {
        Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        sleep = conf.getBoolean(MAP_JOIN_SLEEP, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() throws Exception {
      if (getContext().getDAGAttemptNumber() == 1 && sleep) {
        TimeUnit.SECONDS.sleep(60);
      }

      Preconditions.checkArgument(getInputs().size() == 2);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueReader broadcastKvReader = (KeyValueReader) getInputs().get(AGGREGATION).getReader();
      HashMap<String, Integer> countMap = new HashMap<>();
      while (broadcastKvReader.next()) {
        String key = broadcastKvReader.getCurrentKey().toString();
        int value = ((IntWritable) broadcastKvReader.getCurrentValue()).get();
        countMap.put(key, value);
      }

      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT2).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
      while (kvReader.next()) {
        String line = kvReader.getCurrentValue().toString();
        int count = countMap.getOrDefault(line, 0);
        kvWriter.write(NullWritable.get(), String.format("%s-%d", line, count));
      }
    }
  }
}

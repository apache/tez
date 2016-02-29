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

package org.apache.tez.history;

import com.google.common.collect.Sets;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService;
import org.apache.tez.dag.history.logging.impl.SimpleHistoryLoggingService;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.examples.WordCount;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.SimpleHistoryParser;
import org.apache.tez.history.parser.datamodel.BaseInfo;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.history.parser.datamodel.EdgeInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo.DataDependencyEvent;
import org.apache.tez.history.parser.datamodel.TaskInfo;
import org.apache.tez.history.parser.datamodel.VersionInfo;
import org.apache.tez.history.parser.datamodel.VertexInfo;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.tests.MiniTezClusterWithTimeline;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestHistoryParser {

  private static MiniDFSCluster miniDFSCluster;
  private static MiniTezClusterWithTimeline miniTezCluster;

  //location within miniHDFS cluster's hdfs
  private static Path inputLoc = new Path("/tmp/sample.txt");

  private final static String INPUT = "Input";
  private final static String OUTPUT = "Output";
  private final static String TOKENIZER = "Tokenizer";
  private final static String SUMMATION = "Summation";
  private final static String SIMPLE_HISTORY_DIR = "/tmp/simplehistory/";
  private final static String HISTORY_TXT = "history.txt";

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  private static String TEST_ROOT_DIR =
      "target" + Path.SEPARATOR + TestHistoryParser.class.getName() + "-tmpDir";
  private static String TEZ_BASE_DIR =
      "target" + Path.SEPARATOR + TestHistoryParser.class.getName() + "-tez";
  private static String DOWNLOAD_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "download";

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH, false);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    fs = miniDFSCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());

    setupTezCluster();
  }

  @AfterClass
  public static void shutdownCluster() {
    try {
      if (miniDFSCluster != null) {
        miniDFSCluster.shutdown();
      }
      if (miniTezCluster != null) {
        miniTezCluster.stop();
      }
    } finally {
      try {
        FileUtils.deleteDirectory(new File(TEST_ROOT_DIR));
        FileUtils.deleteDirectory(new File(TEZ_BASE_DIR));
      } catch (IOException e) {
        //safe to ignore
      }
    }
  }

  // @Before
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
        new MiniTezClusterWithTimeline(TEZ_BASE_DIR, 1, 1, 1, true);

    miniTezCluster.init(conf);
    miniTezCluster.start();

    createSampleFile(inputLoc);

    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    tezConf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "0.0.0.0:8188");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);
    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
        ATSHistoryLoggingService.class.getName());

  }


  /**
   * Run a word count example in mini cluster and check if it is possible to download
   * data from ATS and parse it. Also, run with SimpleHistoryLogging option and verify
   * if it matches with ATS data.
   *
   * @throws Exception
   */
  @Test
  public void testParserWithSuccessfulJob() throws Exception {
    //Run basic word count example.
    String dagId = runWordCount(WordCount.TokenProcessor.class.getName(),
        WordCount.SumProcessor.class.getName(), "WordCount", true);

    //Export the data from ATS
    String[] args = { "--dagId=" + dagId, "--downloadDir=" + DOWNLOAD_DIR };

    int result = ATSImportTool.process(args);
    assertTrue(result == 0);

    //Parse ATS data and verify results
    DagInfo dagInfoFromATS = getDagInfo(dagId);
    verifyDagInfo(dagInfoFromATS, true);
    verifyJobSpecificInfo(dagInfoFromATS);

    //Now run with SimpleHistoryLogging
    dagId = runWordCount(WordCount.TokenProcessor.class.getName(),
        WordCount.SumProcessor.class.getName(), "WordCount", false);
    Thread.sleep(10000); //For all flushes to happen and to avoid half-cooked download.

    DagInfo shDagInfo = getDagInfoFromSimpleHistory(dagId);
    verifyDagInfo(shDagInfo, false);
    verifyJobSpecificInfo(shDagInfo);

    //Compare dagInfo by parsing ATS data with DagInfo obtained by parsing SimpleHistoryLog
    isDAGEqual(dagInfoFromATS, shDagInfo);
  }

  private DagInfo getDagInfoFromSimpleHistory(String dagId) throws TezException, IOException {
    TezDAGID tezDAGID = TezDAGID.fromString(dagId);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(tezDAGID
        .getApplicationId(), 1);
    Path historyPath = new Path(conf.get("fs.defaultFS")
        + SIMPLE_HISTORY_DIR + HISTORY_TXT + "."
        + applicationAttemptId);
    FileSystem fs = historyPath.getFileSystem(conf);

    Path localPath = new Path(DOWNLOAD_DIR, HISTORY_TXT);
    fs.copyToLocalFile(historyPath, localPath);
    File localFile = new File(DOWNLOAD_DIR, HISTORY_TXT);

    //Now parse via SimpleHistory
    SimpleHistoryParser parser = new SimpleHistoryParser(localFile);
    DagInfo dagInfo = parser.getDAGData(dagId);
    assertTrue(dagInfo.getDagId().equals(dagId));
    return dagInfo;
  }

  private void verifyJobSpecificInfo(DagInfo dagInfo) {
    //Job specific
    assertTrue(dagInfo.getNumVertices() == 2);
    assertTrue(dagInfo.getName().equals("WordCount"));
    assertTrue(dagInfo.getVertex(TOKENIZER).getProcessorClassName().equals(
        WordCount.TokenProcessor.class.getName()));
    assertTrue(dagInfo.getVertex(SUMMATION).getProcessorClassName()
        .equals(WordCount.SumProcessor.class.getName()));
    assertTrue(dagInfo.getFinishTime() > dagInfo.getStartTime());
    assertTrue(dagInfo.getEdges().size() == 1);
    EdgeInfo edgeInfo = dagInfo.getEdges().iterator().next();
    assertTrue(edgeInfo.getDataMovementType().
        equals(EdgeProperty.DataMovementType.SCATTER_GATHER.toString()));
    assertTrue(edgeInfo.getSourceVertex().getVertexName().equals(TOKENIZER));
    assertTrue(edgeInfo.getDestinationVertex().getVertexName().equals(SUMMATION));
    assertTrue(edgeInfo.getInputVertexName().equals(TOKENIZER));
    assertTrue(edgeInfo.getOutputVertexName().equals(SUMMATION));
    assertTrue(edgeInfo.getEdgeSourceClass().equals(OrderedPartitionedKVOutput.class.getName()));
    assertTrue(edgeInfo.getEdgeDestinationClass().equals(OrderedGroupedKVInput.class.getName()));
    assertTrue(dagInfo.getVertices().size() == 2);
    String lastSourceTA = null;
    String lastDataEventSourceTA = null;
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      assertTrue(vertexInfo.getKilledTasksCount() == 0);
      assertTrue(vertexInfo.getInitRequestedTime() > 0);
      assertTrue(vertexInfo.getInitTime() > 0);
      assertTrue(vertexInfo.getStartRequestedTime() > 0);
      assertTrue(vertexInfo.getStartTime() > 0);
      assertTrue(vertexInfo.getFinishTime() > 0);
      assertTrue(vertexInfo.getFinishTime() > vertexInfo.getStartTime());
      long finishTime = 0;
      for (TaskInfo taskInfo : vertexInfo.getTasks()) {
        assertTrue(taskInfo.getNumberOfTaskAttempts() == 1);
        assertTrue(taskInfo.getMaxTaskAttemptDuration() >= 0);
        assertTrue(taskInfo.getMinTaskAttemptDuration() >= 0);
        assertTrue(taskInfo.getAvgTaskAttemptDuration() >= 0);
        assertTrue(taskInfo.getLastTaskAttemptToFinish() != null);
        assertTrue(taskInfo.getContainersMapping().size() > 0);
        assertTrue(taskInfo.getSuccessfulTaskAttempts().size() > 0);
        assertTrue(taskInfo.getFailedTaskAttempts().size() == 0);
        assertTrue(taskInfo.getKilledTaskAttempts().size() == 0);
        assertTrue(taskInfo.getFinishTime() > taskInfo.getStartTime());
        List<TaskAttemptInfo> attempts = taskInfo.getTaskAttempts();
        if (vertexInfo.getVertexName().equals(TOKENIZER)) {
          // get the last task to finish and track its successful attempt
          if (finishTime < taskInfo.getFinishTime()) {
            finishTime = taskInfo.getFinishTime();
            lastSourceTA = taskInfo.getSuccessfulAttemptId();
          }
        } else {
          for (TaskAttemptInfo attempt : attempts) {
            DataDependencyEvent item = attempt.getLastDataEvents().get(0);
            assertTrue(item.getTimestamp() > 0);
            
            if (lastDataEventSourceTA == null) {
              lastDataEventSourceTA = item.getTaskAttemptId();
            } else {
              // all attempts should have the same last data event source TA
              assertTrue(lastDataEventSourceTA.equals(item.getTaskAttemptId()));
            }
          }
        }
        for (TaskAttemptInfo attemptInfo : taskInfo.getTaskAttempts()) {
          assertTrue(attemptInfo.getCreationTime() > 0);
          assertTrue(attemptInfo.getAllocationTime() > 0);
          assertTrue(attemptInfo.getStartTime() > 0);
          assertTrue(attemptInfo.getFinishTime() > attemptInfo.getStartTime());
        }
      }
      assertTrue(vertexInfo.getLastTaskToFinish() != null);
      if (vertexInfo.getVertexName().equals(TOKENIZER)) {
        assertTrue(vertexInfo.getInputEdges().size() == 0);
        assertTrue(vertexInfo.getOutputEdges().size() == 1);
        assertTrue(vertexInfo.getOutputVertices().size() == 1);
        assertTrue(vertexInfo.getInputVertices().size() == 0);
      } else {
        assertTrue(vertexInfo.getInputEdges().size() == 1);
        assertTrue(vertexInfo.getOutputEdges().size() == 0);
        assertTrue(vertexInfo.getOutputVertices().size() == 0);
        assertTrue(vertexInfo.getInputVertices().size() == 1);
      }
    }
    assertTrue(lastSourceTA.equals(lastDataEventSourceTA));
  }

  /**
   * Run a word count example in mini cluster.
   * Provide invalid URL for ATS.
   *
   * @throws Exception
   */
  @Test
  public void testParserWithSuccessfulJob_InvalidATS() throws Exception {
    //Run basic word count example.
    String dagId =  runWordCount(WordCount.TokenProcessor.class.getName(),
        WordCount.SumProcessor.class.getName(), "WordCount-With-WrongATS-URL", true);

    //Export the data from ATS
    String atsAddress = "--atsAddress=http://atsHost:8188";
    String[] args = { "--dagId=" + dagId,
        "--downloadDir=" + DOWNLOAD_DIR,
        atsAddress
      };

    try {
      int result = ATSImportTool.process(args);
      fail("Should have failed with processException");
    } catch(ParseException e) {
      //expects exception
    }
  }

  /**
   * Run a failed job and parse the data from ATS
   */
  @Test
  public void testParserWithFailedJob() throws Exception {
    //Run a job which would fail
    String dagId = runWordCount(WordCount.TokenProcessor.class.getName(), FailProcessor.class
        .getName(), "WordCount-With-Exception", true);

    //Export the data from ATS
    String[] args = { "--dagId=" + dagId, "--downloadDir=" + DOWNLOAD_DIR };

    int result = ATSImportTool.process(args);
    assertTrue(result == 0);

    //Parse ATS data
    DagInfo dagInfo = getDagInfo(dagId);

    //Verify DAGInfo. Verifies vertex, task, taskAttempts in recursive manner
    verifyDagInfo(dagInfo, true);

    //Dag specific
    VertexInfo summationVertex = dagInfo.getVertex(SUMMATION);
    assertTrue(summationVertex.getFailedTasks().size() == 1); //1 task, 4 attempts failed
    assertTrue(summationVertex.getFailedTasks().get(0).getFailedTaskAttempts().size() == 4);
    assertTrue(summationVertex.getStatus().equals(VertexState.FAILED.toString()));

    assertTrue(dagInfo.getFailedVertices().size() == 1);
    assertTrue(dagInfo.getFailedVertices().get(0).getVertexName().equals(SUMMATION));
    assertTrue(dagInfo.getSuccessfullVertices().size() == 1);
    assertTrue(dagInfo.getSuccessfullVertices().get(0).getVertexName().equals(TOKENIZER));

    assertTrue(dagInfo.getStatus().equals(DAGState.FAILED.toString()));

    verifyCounter(dagInfo.getCounter(DAGCounter.NUM_FAILED_TASKS.toString()), null, 4);
    verifyCounter(dagInfo.getCounter(DAGCounter.NUM_SUCCEEDED_TASKS.toString()), null, 1);
    verifyCounter(dagInfo.getCounter(DAGCounter.TOTAL_LAUNCHED_TASKS.toString()), null, 5);

    verifyCounter(dagInfo.getCounter(TaskCounter.INPUT_RECORDS_PROCESSED.toString()),
        "TaskCounter_Tokenizer_INPUT_Input", 10);
    verifyCounter(dagInfo.getCounter(TaskCounter.ADDITIONAL_SPILLS_BYTES_READ.toString()),
        "TaskCounter_Tokenizer_OUTPUT_Summation", 0);
    verifyCounter(dagInfo.getCounter(TaskCounter.OUTPUT_RECORDS.toString()),
        "TaskCounter_Tokenizer_OUTPUT_Summation",
        20); //Every line has 2 words. 10 lines x 2 words = 20
    verifyCounter(dagInfo.getCounter(TaskCounter.SPILLED_RECORDS.toString()),
        "TaskCounter_Tokenizer_OUTPUT_Summation", 20); //Same as above

    for (TaskInfo taskInfo : summationVertex.getTasks()) {
      TaskAttemptInfo lastAttempt = null;
      for (TaskAttemptInfo attemptInfo : taskInfo.getTaskAttempts()) {
        if (lastAttempt != null) {
          // failed attempt should be causal TA of next attempt
          assertTrue(lastAttempt.getTaskAttemptId().equals(attemptInfo.getCreationCausalTA()));
          assertTrue(lastAttempt.getTerminationCause() != null);
        }
        lastAttempt = attemptInfo;
      }
    }

    //TODO: Need to check for SUMMATION vertex counters. Since all attempts are failed, counters are not getting populated.
    //TaskCounter.REDUCE_INPUT_RECORDS

    //Verify if the processor exception is given in diagnostics
    assertTrue(dagInfo.getDiagnostics().contains("Failing this processor for some reason"));

  }

  /**
   * Adding explicit equals here instead of in DAG/Vertex/Edge where hashCode also needs to
   * change. Also, some custom comparisons are done here for unit testing.
   */
  private void isDAGEqual(DagInfo dagInfo1, DagInfo dagInfo2) {
    assertNotNull(dagInfo1);
    assertNotNull(dagInfo2);
    assertEquals(dagInfo1.getStatus(), dagInfo2.getStatus());
    isEdgeEqual(dagInfo1.getEdges(), dagInfo2.getEdges());
    isVertexEqual(dagInfo1.getVertices(), dagInfo2.getVertices());
  }

  private void isVertexEqual(VertexInfo vertexInfo1, VertexInfo vertexInfo2) {
    assertTrue(vertexInfo1 != null);
    assertTrue(vertexInfo2 != null);
    assertTrue(vertexInfo1.getVertexName().equals(vertexInfo2.getVertexName()));
    assertTrue(vertexInfo1.getProcessorClassName().equals(vertexInfo2.getProcessorClassName()));
    assertTrue(vertexInfo1.getNumTasks() == vertexInfo2.getNumTasks());
    assertTrue(vertexInfo1.getCompletedTasksCount() == vertexInfo2.getCompletedTasksCount());
    assertTrue(vertexInfo1.getStatus().equals(vertexInfo2.getStatus()));

    isEdgeEqual(vertexInfo1.getInputEdges(), vertexInfo2.getInputEdges());
    isEdgeEqual(vertexInfo1.getOutputEdges(), vertexInfo2.getOutputEdges());

    assertTrue(vertexInfo1.getInputVertices().size() == vertexInfo2.getInputVertices().size());
    assertTrue(vertexInfo1.getOutputVertices().size() == vertexInfo2.getOutputVertices().size());

    assertTrue(vertexInfo1.getNumTasks() == vertexInfo2.getNumTasks());
    isTaskEqual(vertexInfo1.getTasks(), vertexInfo2.getTasks());
  }

  private void isVertexEqual(List<VertexInfo> vertexList1, List<VertexInfo> vertexList2) {
    assertTrue("Vertices sizes should be the same", vertexList1.size() == vertexList2.size());
    Iterator<VertexInfo> it1 = vertexList1.iterator();
    Iterator<VertexInfo> it2 = vertexList2.iterator();
    while (it1.hasNext()) {
      assertTrue(it2.hasNext());
      VertexInfo info1 = it1.next();
      VertexInfo info2 = it2.next();
      isVertexEqual(info1, info2);
    }
  }

  private void isEdgeEqual(EdgeInfo edgeInfo1, EdgeInfo edgeInfo2) {
    assertTrue(edgeInfo1 != null);
    assertTrue(edgeInfo2 != null);
    String info1 = edgeInfo1.toString();
    String info2 = edgeInfo1.toString();
    assertTrue(info1.equals(info2));
  }

  private void isEdgeEqual(Collection<EdgeInfo> info1, Collection<EdgeInfo> info2) {
    assertTrue("sizes should be the same", info1.size() == info1.size());
    Iterator<EdgeInfo> it1 = info1.iterator();
    Iterator<EdgeInfo> it2 = info2.iterator();
    while (it1.hasNext()) {
      assertTrue(it2.hasNext());
      isEdgeEqual(it1.next(), it2.next());
    }
  }

  private void isTaskEqual(Collection<TaskInfo> info1, Collection<TaskInfo> info2) {
    assertTrue("sizes should be the same", info1.size() == info1.size());
    Iterator<TaskInfo> it1 = info1.iterator();
    Iterator<TaskInfo> it2 = info2.iterator();
    while (it1.hasNext()) {
      assertTrue(it2.hasNext());
      isTaskEqual(it1.next(), it2.next());
    }
  }

  private void isTaskEqual(TaskInfo taskInfo1, TaskInfo taskInfo2) {
    assertTrue(taskInfo1 != null);
    assertTrue(taskInfo2 != null);
    assertTrue(taskInfo1.getVertexInfo() != null);
    assertTrue(taskInfo2.getVertexInfo() != null);
    assertTrue(taskInfo1.getStatus().equals(taskInfo2.getStatus()));
    assertTrue(
        taskInfo1.getVertexInfo().getVertexName()
            .equals(taskInfo2.getVertexInfo().getVertexName()));
    isTaskAttemptEqual(taskInfo1.getTaskAttempts(), taskInfo2.getTaskAttempts());

    //Verify counters
    isCountersSame(taskInfo1, taskInfo2);
  }

  private void isCountersSame(BaseInfo info1, BaseInfo info2) {
    isCounterSame(info1.getCounter(TaskCounter.ADDITIONAL_SPILL_COUNT.name()),
        info2.getCounter(TaskCounter.ADDITIONAL_SPILL_COUNT.name()));

    isCounterSame(info1.getCounter(TaskCounter.SPILLED_RECORDS.name()),
        info2.getCounter(TaskCounter.SPILLED_RECORDS.name()));

    isCounterSame(info1.getCounter(TaskCounter.OUTPUT_RECORDS.name()),
        info2.getCounter(TaskCounter.OUTPUT_RECORDS.name()));

    isCounterSame(info1.getCounter(TaskCounter.OUTPUT_BYTES.name()),
        info2.getCounter(TaskCounter.OUTPUT_BYTES.name()));

    isCounterSame(info1.getCounter(TaskCounter.OUTPUT_RECORDS.name()),
        info2.getCounter(TaskCounter.OUTPUT_RECORDS.name()));

    isCounterSame(info1.getCounter(TaskCounter.REDUCE_INPUT_GROUPS.name()),
        info2.getCounter(TaskCounter.REDUCE_INPUT_GROUPS.name()));

    isCounterSame(info1.getCounter(TaskCounter.REDUCE_INPUT_RECORDS.name()),
        info2.getCounter(TaskCounter.REDUCE_INPUT_RECORDS.name()));
  }

  private void isCounterSame(Map<String, TezCounter> counter1, Map<String, TezCounter> counter2) {
    for (Map.Entry<String, TezCounter> entry : counter1.entrySet()) {
      String source = entry.getKey();
      long val = entry.getValue().getValue();

      //check if other counter has the same value
      assertTrue(counter2.containsKey(entry.getKey()));
      assertTrue(counter2.get(entry.getKey()).getValue() == val);
    }
  }

  private void isTaskAttemptEqual(Collection<TaskAttemptInfo> info1,
      Collection<TaskAttemptInfo> info2) {
    assertTrue("sizes should be the same", info1.size() == info1.size());
    Iterator<TaskAttemptInfo> it1 = info1.iterator();
    Iterator<TaskAttemptInfo> it2 = info2.iterator();
    while (it1.hasNext()) {
      assertTrue(it2.hasNext());
      isTaskAttemptEqual(it1.next(), it2.next());
    }
  }

  private void isTaskAttemptEqual(TaskAttemptInfo info1, TaskAttemptInfo info2) {
    assertTrue(info1 != null);
    assertTrue(info2 != null);
    assertTrue(info1.getTaskInfo() != null);
    assertTrue(info2.getTaskInfo() != null);
    assertTrue(info1.getStatus().equals(info2.getStatus()));
    assertTrue(info1.getTaskInfo().getVertexInfo().getVertexName().equals(info2.getTaskInfo()
        .getVertexInfo().getVertexName()));

    //Verify counters
    isCountersSame(info1, info2);
  }


  /**
   * Create sample file for wordcount program
   *
   * @param inputLoc
   * @throws IOException
   */
  private static void createSampleFile(Path inputLoc) throws IOException {
    fs.deleteOnExit(inputLoc);
    FSDataOutputStream out = fs.create(inputLoc);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    for (int i = 0; i < 10; i++) {
      writer.write("Sample " + RandomStringUtils.randomAlphanumeric(5));
      writer.newLine();
    }
    writer.close();
  }

  private DagInfo getDagInfo(String dagId) throws TezException {
    //Parse downloaded contents
    File downloadedFile = new File(DOWNLOAD_DIR
        + Path.SEPARATOR + dagId + ".zip");
    ATSFileParser parser = new ATSFileParser(downloadedFile);
    DagInfo dagInfo = parser.getDAGData(dagId);
    assertTrue(dagInfo.getDagId().equals(dagId));
    return dagInfo;
  }

  private void verifyCounter(Map<String, TezCounter> counterMap,
      String counterGroupName, long expectedVal) {
    //Iterate through group-->tezCounter
    for (Map.Entry<String, TezCounter> entry : counterMap.entrySet()) {
      if (counterGroupName != null) {
        if (entry.getKey().equals(counterGroupName)) {
          assertTrue(entry.getValue().getValue() == expectedVal);
        }
      } else {
        assertTrue(entry.getValue().getValue() == expectedVal);
      }
    }
  }

  TezClient getTezClient(boolean withTimeline) throws Exception {
    TezConfiguration tezConf = new TezConfiguration(miniTezCluster.getConfig());
    if (withTimeline) {
      tezConf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, withTimeline);
      tezConf.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS, "0.0.0.0:8188");
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          ATSHistoryLoggingService.class.getName());
    } else {
      tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS,
          SimpleHistoryLoggingService.class.getName());
    }
    tezConf.setBoolean(TezConfiguration.TEZ_AM_ALLOW_DISABLED_TIMELINE_DOMAINS, true);

    TezClient tezClient = TezClient.create("WordCount", tezConf, false);
    tezClient.start();
    tezClient.waitTillReady();
    return tezClient;
  }

  private String runWordCount(String tokenizerProcessor, String summationProcessor,
      String dagName, boolean withTimeline)
      throws Exception {
    //HDFS path
    Path outputLoc = new Path("/tmp/outPath_" + System.currentTimeMillis());

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(conf,
        TextInputFormat.class, inputLoc.toString()).build();

    DataSinkDescriptor dataSink =
        MROutput.createConfigBuilder(conf, TextOutputFormat.class, outputLoc.toString()).build();

    Vertex tokenizerVertex = Vertex.create(TOKENIZER, ProcessorDescriptor.create(
        tokenizerProcessor)).addDataSource(INPUT, dataSource);

    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName()).build();

    Vertex summationVertex = Vertex.create(SUMMATION,
        ProcessorDescriptor.create(summationProcessor), 1).addDataSink(OUTPUT, dataSink);

    // Create DAG and add the vertices. Connect the producer and consumer vertices via the edge
    DAG dag = DAG.create(dagName);
    dag.addVertex(tokenizerVertex).addVertex(summationVertex).addEdge(
        Edge.create(tokenizerVertex, summationVertex, edgeConf.createDefaultEdgeProperty()));

    TezClient tezClient = getTezClient(withTimeline);

    // Update Caller Context
    CallerContext callerContext = CallerContext.create("TezExamples", "Tez WordCount Example Job");
    ApplicationId appId = tezClient.getAppMasterApplicationId();
    if (appId == null) {
      appId = ApplicationId.newInstance(1001l, 1);
    }
    callerContext.setCallerIdAndType(appId.toString(), "TezApplication");
    dag.setCallerContext(callerContext);

    DAGClient client = tezClient.submitDAG(dag);
    client.waitForCompletionWithStatusUpdates(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
    TezDAGID tezDAGID = TezDAGID.getInstance(tezClient.getAppMasterApplicationId(), 1);

    if (tezClient != null) {
      tezClient.stop();
    }
    return tezDAGID.toString();
  }

  /**
   * Processor which would just throw exception.
   */
  public static class FailProcessor extends SimpleMRProcessor {
    public FailProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      throw new Exception("Failing this processor for some reason");
    }
  }

  private void verifyDagInfo(DagInfo dagInfo, boolean ats) {
    if (ats) {
      VersionInfo versionInfo = dagInfo.getVersionInfo();
      assertTrue(versionInfo != null); //should be present post 0.5.4
      assertTrue(versionInfo.getVersion() != null);
      assertTrue(versionInfo.getRevision() != null);
      assertTrue(versionInfo.getBuildTime() != null);
    }

    assertTrue(dagInfo.getUserName() != null);
    assertTrue(!dagInfo.getUserName().isEmpty());
    assertTrue(dagInfo.getStartTime() > 0);
    assertTrue(dagInfo.getFinishTimeInterval() > 0);
    assertTrue(dagInfo.getStartTimeInterval() == 0);
    assertTrue(dagInfo.getStartTime() > 0);
    if (dagInfo.getStatus().equalsIgnoreCase(DAGState.SUCCEEDED.toString())) {
      assertTrue(dagInfo.getFinishTime() >= dagInfo.getStartTime());
    }
    assertTrue(dagInfo.getFinishTimeInterval() > dagInfo.getStartTimeInterval());

    assertTrue(dagInfo.getStartTime() > dagInfo.getSubmitTime());
    assertTrue(dagInfo.getTimeTaken() > 0);

    assertNotNull(dagInfo.getCallerContext());
    assertEquals("TezExamples", dagInfo.getCallerContext().getContext());
    assertEquals("Tez WordCount Example Job", dagInfo.getCallerContext().getBlob());
    assertNotNull(dagInfo.getCallerContext().getCallerId());
    assertEquals("TezApplication", dagInfo.getCallerContext().getCallerType());

    //Verify all vertices
    for (VertexInfo vertexInfo : dagInfo.getVertices()) {
      verifyVertex(vertexInfo, vertexInfo.getFailedTasksCount() > 0);
    }

    VertexInfo fastestVertex = dagInfo.getFastestVertex();
    assertTrue(fastestVertex != null);

    if (dagInfo.getStatus().equals(DAGState.SUCCEEDED)) {
      assertTrue(dagInfo.getSlowestVertex() != null);
    }
  }

  private void verifyVertex(VertexInfo vertexInfo, boolean hasFailedTasks) {
    assertTrue(vertexInfo != null);
    if (hasFailedTasks) {
      assertTrue(vertexInfo.getFailedTasksCount() > 0);
    }
    assertTrue(vertexInfo.getStartTimeInterval() > 0);
    assertTrue(vertexInfo.getStartTime() > 0);
    assertTrue(vertexInfo.getFinishTimeInterval() > 0);
    assertTrue(vertexInfo.getStartTimeInterval() < vertexInfo.getFinishTimeInterval());
    assertTrue(vertexInfo.getVertexName() != null);
    if (!hasFailedTasks) {
      assertTrue(vertexInfo.getFinishTime() > 0);
      assertTrue(vertexInfo.getFailedTasks().size() == 0);
      assertTrue(vertexInfo.getSucceededTasksCount() == vertexInfo.getSuccessfulTasks().size());
      assertTrue(vertexInfo.getFailedTasksCount() == 0);
      assertTrue(vertexInfo.getAvgTaskDuration() > 0);
      assertTrue(vertexInfo.getMaxTaskDuration() > 0);
      assertTrue(vertexInfo.getMinTaskDuration() > 0);
      assertTrue(vertexInfo.getTimeTaken() > 0);
      assertTrue(vertexInfo.getStatus().equalsIgnoreCase(VertexState.SUCCEEDED.toString()));
      assertTrue(vertexInfo.getCompletedTasksCount() > 0);
      assertTrue(vertexInfo.getFirstTaskToStart() != null);
      assertTrue(vertexInfo.getSucceededTasksCount() > 0);
      assertTrue(vertexInfo.getTasks().size() > 0);
      assertTrue(vertexInfo.getFinishTime() > vertexInfo.getStartTime());
    }

    for (TaskInfo taskInfo : vertexInfo.getTasks()) {
      if (taskInfo.getStatus().equals(TaskState.SUCCEEDED.toString())) {
        verifyTask(taskInfo, false);
      }
    }

    for (TaskInfo taskInfo : vertexInfo.getFailedTasks()) {
      verifyTask(taskInfo, true);
    }

    assertTrue(vertexInfo.getProcessorClassName() != null);
    assertTrue(vertexInfo.getStatus() != null);
    assertTrue(vertexInfo.getDagInfo() != null);
    assertTrue(vertexInfo.getInitTimeInterval() > 0);
    assertTrue(vertexInfo.getNumTasks() > 0);
  }

  private void verifyTask(TaskInfo taskInfo, boolean hasFailedAttempts) {
    assertTrue(taskInfo != null);
    assertTrue(taskInfo.getStatus() != null);
    assertTrue(taskInfo.getStartTimeInterval() > 0);

    //Not testing for killed attempts. So if there are no failures, it should succeed
    if (!hasFailedAttempts) {
      assertTrue(taskInfo.getStatus().equals(TaskState.SUCCEEDED.toString()));
      assertTrue(taskInfo.getFinishTimeInterval() > 0 && taskInfo.getFinishTime() > taskInfo
          .getFinishTimeInterval());
      assertTrue(
          taskInfo.getStartTimeInterval() > 0 && taskInfo.getStartTime() > taskInfo.getStartTimeInterval());
      assertTrue(taskInfo.getSuccessfulAttemptId() != null);
      assertTrue(taskInfo.getSuccessfulTaskAttempt() != null);
      assertTrue(taskInfo.getFinishTime() > taskInfo.getStartTime());
    }
    assertTrue(taskInfo.getTaskId() != null);

    for (TaskAttemptInfo attemptInfo : taskInfo.getTaskAttempts()) {
      verifyTaskAttemptInfo(attemptInfo);
    }
  }

  private void verifyTaskAttemptInfo(TaskAttemptInfo attemptInfo) {
    if (attemptInfo.getStatus() != null && attemptInfo.getStatus()
        .equals(TaskAttemptState.SUCCEEDED)) {
      assertTrue(attemptInfo.getStartTimeInterval() > 0);
      assertTrue(attemptInfo.getFinishTimeInterval() > 0);
      assertTrue(attemptInfo.getCreationTime() > 0);
      assertTrue(attemptInfo.getAllocationTime() > 0);
      assertTrue(attemptInfo.getStartTime() > 0);
      assertTrue(attemptInfo.getFinishTime() > 0);
      assertTrue(attemptInfo.getFinishTime() > attemptInfo.getStartTime());
      assertTrue(attemptInfo.getFinishTime() > attemptInfo.getFinishTimeInterval());
      assertTrue(attemptInfo.getStartTime() > attemptInfo.getStartTimeInterval());
      assertTrue(attemptInfo.getNodeId() != null);
      assertTrue(attemptInfo.getTimeTaken() != -1);
      assertTrue(attemptInfo.getEvents() != null);
      assertTrue(attemptInfo.getTezCounters() != null);
      assertTrue(attemptInfo.getContainer() != null);
    }
    assertTrue(attemptInfo.getTaskInfo() != null);
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPipelinedShuffle {

  private static MiniDFSCluster miniDFSCluster;
  private static MiniTezCluster miniTezCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem fs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestPipelinedShuffle.class.getName() + "-tmpDir";

  private static final int KEYS_PER_MAPPER = 5000;

  @BeforeClass
  public static void setupDFSCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH, false);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    fs = miniDFSCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
  }

  @AfterClass
  public static void shutdownDFSCluster() {
    if (miniDFSCluster != null) {
      //shutdown
      miniDFSCluster.shutdown();
    }
  }

  @Before
  public void setupTezCluster() throws Exception {
    //With 1 MB sort buffer and with good amount of dataset, it would spill records
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_MB, 1);

    //Enable PipelinedShuffle
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, true);

    //Enable local fetch
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

    // 3 seconds should be good enough in local machine
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3 * 1000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3 * 1000);
    //set to low value so that it can detect failures quickly
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT, 2);

    miniTezCluster = new MiniTezCluster(TestPipelinedShuffle.class.getName(), 1, 1, 1);

    miniTezCluster.init(conf);
    miniTezCluster.start();
  }

  @After
  public void shutdownTezCluster() throws IOException {
    if (miniTezCluster != null) {
      miniTezCluster.stop();
    }
  }

  @Test
  public void baseTest() throws Exception {
    PipelinedShuffleJob pipelinedShuffle = new PipelinedShuffleJob();
    pipelinedShuffle.setConf(new Configuration(miniTezCluster.getConfig()));

    String[] args = new String[] { };
    assertEquals(0, pipelinedShuffle.run(args));
  }

  /**
   *
   * mapper1 --\
   *            --> reducer
   * mapper2 --/
   *
   * Mappers just generate dummy data, but ensures that there is enough spills.
   * Reducer should process them correctly and validate the total number of records.
   * Only record count is validated in the reducer which is fine for this test.
   */
  public static class PipelinedShuffleJob extends Configured implements Tool {
    private TezConfiguration tezConf;

    public static class DataGenerator extends SimpleMRProcessor {

      public DataGenerator(ProcessorContext context) {
        super(context);
      }

      @Override public void run() throws Exception {
        Preconditions.checkArgument(getInputs().size() == 0);
        Preconditions.checkArgument(getOutputs().size() == 1);
        KeyValueWriter writer = (KeyValueWriter) getOutputs().get("reducer").getWriter();

        for (int i = 0; i < KEYS_PER_MAPPER; i++) {
          writer.write(new Text(RandomStringUtils.randomAlphanumeric(1000)),
              new Text(RandomStringUtils.randomAlphanumeric(1000)));
        }
      }
    }

    public static class SimpleReduceProcessor extends SimpleMRProcessor {

      public SimpleReduceProcessor(ProcessorContext context) {
        super(context);
      }

      private long readData(KeyValuesReader reader) throws IOException {
        long records = 0;
        while (reader.next()) {
          reader.getCurrentKey();
          for (Object val : reader.getCurrentValues()) {
            records++;
          }
        }
        return records;
      }

      @Override
      public void run() throws Exception {
        Preconditions.checkArgument(getInputs().size() == 2);

        long totalRecords = 0;

        KeyValuesReader reader1 = (KeyValuesReader) getInputs().get("mapper1").getReader();
        totalRecords += readData(reader1);

        KeyValuesReader reader2 = (KeyValuesReader) getInputs().get("mapper2").getReader();
        totalRecords += readData(reader2);

        //Verify if correct number of records are retrieved.
        assertEquals(2 * KEYS_PER_MAPPER, totalRecords);
      }
    }

    @Override
    public int run(String[] args) throws Exception {
      this.tezConf = new TezConfiguration(getConf());
      String dagName = "pipelinedShuffleTest";
      DAG dag = DAG.create(dagName);

      Vertex m1_Vertex = Vertex.create("mapper1",
          ProcessorDescriptor.create(DataGenerator.class.getName()), 1);

      Vertex m2_Vertex = Vertex.create("mapper2",
          ProcessorDescriptor.create(DataGenerator.class.getName()), 1);

      Vertex reducerVertex = Vertex.create("reducer",
          ProcessorDescriptor.create(SimpleReduceProcessor.class.getName()), 1);

      Edge mapper1_to_reducer = Edge.create(m1_Vertex, reducerVertex,
          OrderedPartitionedKVEdgeConfig
              .newBuilder(Text.class.getName(), Text.class.getName(),
                  HashPartitioner.class.getName())
              .setFromConfiguration(tezConf).build().createDefaultEdgeProperty());

      Edge mapper2_to_reducer = Edge.create(m2_Vertex, reducerVertex,
          OrderedPartitionedKVEdgeConfig
              .newBuilder(Text.class.getName(), Text.class.getName(),
                  HashPartitioner.class.getName())
              .setFromConfiguration(tezConf).build().createDefaultEdgeProperty());

      dag.addVertex(m1_Vertex);
      dag.addVertex(m2_Vertex);
      dag.addVertex(reducerVertex);

      dag.addEdge(mapper1_to_reducer).addEdge(mapper2_to_reducer);

      TezClient client = TezClient.create(dagName, tezConf);
      client.start();
      client.waitTillReady();

      DAGClient dagClient = client.submitDAG(dag);
      Set<StatusGetOpts> getOpts = Sets.newHashSet();
      getOpts.add(StatusGetOpts.GET_COUNTERS);

      DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

      System.out.println(dagStatus.getDAGCounters());
      TezCounters counters = dagStatus.getDAGCounters();

      //Ensure that atleast 10 spills were there in this job.
      assertTrue(counters.findCounter(TaskCounter.ADDITIONAL_SPILL_COUNT).getValue() > 10);

      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
        return -1;
      }
      return 0;
    }
  }

}

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

package org.apache.tez.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.examples.HashJoinExample.ForwardingProcessor;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.google.common.base.Preconditions;

/**
 * Simple example of joining 2 data sets using <a
 * href="http://en.wikipedia.org/wiki/Sort-merge_join">Sort-Merge Join</a><br>
 * There're 2 differences between {@link SortMergeJoinExample} and
 * {@link HashJoinExample}. <li>We always load one data set(hashFile) in memory
 * in {@link HashJoinExample} which require one dataset(hashFile) must be small
 * enough to fit into memory, while in {@link SortMergeJoinExample}, it does not
 * load one data set into memory, it just sort the output of the datasets before
 * feeding to {@link SortMergeJoinProcessor}, just like the sort phase before
 * reduce in traditional MapReduce. Then we could move forward the iterators of
 * two inputs in {@link SortMergeJoinProcessor} to find the joined keys since
 * they are both sorted already. <br> <li>Because of the sort implemention
 * difference we describe above, the data requirement is also different for
 * these 2 sort algorithms. For {@link HashJoinExample} It is required that keys
 * in the hashFile are unique. while for {@link SortMergeJoinExample} it is
 * required that keys in the both 2 datasets are unique.
 */
public class SortMergeJoinExample extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(SortMergeJoinExample.class);

  private static final String input1 = "input1";
  private static final String input2 = "input2";
  private static final String inputFile = "inputFile";
  private static final String joiner = "joiner";
  private static final String joinOutput = "joinOutput";

  public static void main(String[] args) throws Exception {
    SortMergeJoinExample job = new SortMergeJoinExample();
    int status = ToolRunner.run(new Configuration(), job, args);
    System.exit(status);
  }

  private static void printUsage() {
    System.err.println("Usage: "
        + "sortmergejoin <file1> <file2> <numPartitions> <outPath>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs);
  }

  public int run(Configuration conf, String[] args, TezClient tezClient)
      throws Exception {
    setConf(conf);
    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs, tezClient);
  }

  private int validateArgs(String[] otherArgs) {
    if (otherArgs.length != 4) {
      printUsage();
      return 2;
    }
    return 0;
  }

  private int execute(String[] args) throws TezException, IOException,
      InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    TezClient tezClient = null;
    try {
      tezClient = createTezClient(tezConf);
      return execute(args, tezConf, tezClient);
    } finally {
      if (tezClient != null) {
        tezClient.stop();
      }
    }
  }

  private int execute(String[] args, TezClient tezClient) throws IOException,
      TezException, InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    return execute(args, tezConf, tezClient);
  }

  private TezClient createTezClient(TezConfiguration tezConf)
      throws TezException, IOException {
    TezClient tezClient = TezClient.create("SortMergeJoinExample", tezConf);
    tezClient.start();
    return tezClient;
  }

  private int execute(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws IOException, TezException,
      InterruptedException {
    LOG.info("Running SortMergeJoinExample");

    UserGroupInformation.setConfiguration(tezConf);

    String inputDir1 = args[0];
    String inputDir2 = args[1];
    int numPartitions = Integer.parseInt(args[2]);
    String outputDir = args[3];

    Path inputPath1 = new Path(inputDir1);
    Path inputPath2 = new Path(inputDir2);
    Path outputPath = new Path(outputDir);

    // Verify output path existence
    FileSystem fs = FileSystem.get(tezConf);
    if (fs.exists(outputPath)) {
      System.err.println("Output directory: " + outputDir + " already exists");
      return 3;
    }
    if (numPartitions <= 0) {
      System.err.println("NumPartitions must be > 0");
      return 4;
    }

    DAG dag =
        createDag(tezConf, inputPath1, inputPath2, outputPath, numPartitions);

    tezClient.waitTillReady();
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;

  }

  /**
   * v1 v2 <br>
   * &nbsp;\&nbsp;/ <br>
   * &nbsp;&nbsp;v3 <br>
   * 
   * @param tezConf
   * @param inputPath1
   * @param inputPath2
   * @param outPath
   * @param numPartitions
   * @return
   * @throws IOException
   */
  private DAG createDag(TezConfiguration tezConf, Path inputPath1,
      Path inputPath2, Path outPath, int numPartitions) throws IOException {
    DAG dag = DAG.create("SortMergeJoinExample");

    /**
     * This vertex represents the one side of the join. It reads text data using
     * the TextInputFormat. ForwardingProcessor simply forwards the data
     * downstream as is.
     */
    Vertex inputVertex1 =
        Vertex.create("input1",
            ProcessorDescriptor.create(ForwardingProcessor.class.getName()))
            .addDataSource(
                inputFile,
                MRInput
                    .createConfigBuilder(new Configuration(tezConf),
                        TextInputFormat.class, inputPath1.toUri().toString())
                    .groupSplits(false).build());

    /**
     * The other vertex represents the other side of the join. It reads text
     * data using the TextInputFormat. ForwardingProcessor simply forwards the
     * data downstream as is.
     */
    Vertex inputVertex2 =
        Vertex.create("input2",
            ProcessorDescriptor.create(ForwardingProcessor.class.getName()))
            .addDataSource(
                inputFile,
                MRInput
                    .createConfigBuilder(new Configuration(tezConf),
                        TextInputFormat.class, inputPath2.toUri().toString())
                    .groupSplits(false).build());

    /**
     * This vertex represents the join operation. It writes the join output as
     * text using the TextOutputFormat. The JoinProcessor is going to perform
     * the join of the two sorted output from inputVertex1 and inputVerex2. It
     * is load balanced across numPartitions.
     */
    Vertex joinVertex = Vertex
        .create(joiner, ProcessorDescriptor.create(SortMergeJoinProcessor.class.getName()),
            numPartitions)
        .setVertexManagerPlugin(
            ShuffleVertexManager.createConfigBuilder(tezConf).setAutoReduceParallelism(true)
                .build())
        .addDataSink(
            joinOutput,
            MROutput.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class,
                outPath.toUri().toString()).build());

    /**
     * The output of inputVertex1 and inputVertex2 will be partitioned into
     * fragments with the same keys going to the same fragments using hash
     * partitioning. The data to be joined is the key itself and so the value is
     * null. And these outputs will be sorted before feeding them to
     * JoinProcessor. The number of fragments is initially inferred from the
     * number of tasks running in the join vertex because each task will be
     * handling one fragment.
     * Edge config options are derived from client-side tez-site.xml (recommended). Optionally
     * invoke setFromConfiguration to override these config options via commandline arguments.
     */
    OrderedPartitionedKVEdgeConfig edgeConf =
        OrderedPartitionedKVEdgeConfig
            .newBuilder(Text.class.getName(), NullWritable.class.getName(),
                HashPartitioner.class.getName()).setFromConfiguration(tezConf)
            .build();

    /**
     * Connect the join vertex with inputVertex1 with the EdgeProperty created
     * from {@link OrderedPartitionedKVEdgeConfig} so that the output of
     * inputVertex1 is sorted before feeding it to JoinProcessor
     */
    Edge e1 =
        Edge.create(inputVertex1, joinVertex,
            edgeConf.createDefaultEdgeProperty());
    /**
     * Connect the join vertex with inputVertex2 with the EdgeProperty created
     * from {@link OrderedPartitionedKVEdgeConfig} so that the output of
     * inputVertex1 is sorted before feeding it to JoinProcessor
     */
    Edge e2 =
        Edge.create(inputVertex2, joinVertex,
            edgeConf.createDefaultEdgeProperty());

    dag.addVertex(inputVertex1).addVertex(inputVertex2).addVertex(joinVertex)
        .addEdge(e1).addEdge(e2);
    return dag;
  }

  /**
   * Join 2 inputs which has already been sorted. Check the algorithm here <a
   * href="http://en.wikipedia.org/wiki/Sort-merge_join">Sort-Merge Join</a><br>
   * It require the keys in both datasets are unique. <br>
   * Disclaimer: The join code here is written as a tutorial for the APIs and
   * not for performance.
   */
  public static class SortMergeJoinProcessor extends SimpleMRProcessor {

    public SortMergeJoinProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 2);
      Preconditions.checkState(getOutputs().size() == 1);
      // Get the input data for the 2 sides of the join from the 2 inputs
      LogicalInput logicalInput1 = getInputs().get(input1);
      LogicalInput logicalInput2 = getInputs().get(input2);
      Reader inputReader1 = logicalInput1.getReader();
      Reader inputReader2 = logicalInput2.getReader();
      Preconditions.checkState(inputReader1 instanceof KeyValuesReader);
      Preconditions.checkState(inputReader2 instanceof KeyValuesReader);
      LogicalOutput lo = getOutputs().get(joinOutput);
      Preconditions.checkState(lo.getWriter() instanceof KeyValueWriter);
      KeyValueWriter writer = (KeyValueWriter) lo.getWriter();

      join((KeyValuesReader) inputReader1, (KeyValuesReader) inputReader2,
          writer);
    }

    /**
     * Join 2 sorted inputs both from {@link KeyValuesReader} and write output
     * using {@link KeyValueWriter}
     * 
     * @param inputReader1
     * @param inputReader2
     * @param writer
     * @throws IOException
     */
    private void join(KeyValuesReader inputReader1,
        KeyValuesReader inputReader2, KeyValueWriter writer) throws IOException {

      while (inputReader1.next() && inputReader2.next()) {
        Text value1 = (Text) inputReader1.getCurrentKey();
        Text value2 = (Text) inputReader2.getCurrentKey();
        boolean reachEnd = false;
        // move the cursor of 2 inputs forward until find the same values or one
        // of them reach the end.
        while (value1.compareTo(value2) != 0) {
          if (value1.compareTo(value2) > 0) {
            if (inputReader2.next()) {
              value2 = (Text) inputReader2.getCurrentKey();
            } else {
              reachEnd = true;
              break;
            }
          } else {
            if (inputReader1.next()) {
              value1 = (Text) inputReader1.getCurrentKey();
            } else {
              reachEnd = true;
              break;
            }
          }
        }

        if (reachEnd) {
          break;
        } else {
          writer.write(value1, NullWritable.get());
        }
      }
    }
  }
}

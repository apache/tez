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
import java.util.HashSet;
import java.util.Set;

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
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

/**
 * Simple example of joining 2 data sets.
 * <br>The example shows a vertex with multiple inputs that represent the two
 * data sets that need to be joined.
 * <br>The join can be performed using a broadcast (or replicate-fragment) join in 
 * which the small side of the join is broadcast in total to fragments of the 
 * larger side. Each fragment of the larger side can perform the join operation
 * independently using the full data of the smaller side. This shows the usage 
 * of the broadcast edge property in Tez.
 * <br>The join can be performed using the regular repartition join where both 
 * sides are partitioned according to the same scheme into the same number of 
 * fragments. Then the keys in the same fragment are joined with each other. This 
 * is the default join strategy.
 *
 */
public class JoinExample extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(JoinExample.class);
  
  private static final String broadcastOption = "doBroadcast";
  private static final String streamingSide = "streamingSide";
  private static final String hashSide = "hashSide";
  private static final String inputFile = "inputFile";
  private static final String joiner = "joiner";
  private static final String joinOutput = "joinOutput";
  

  public static void main(String[] args) throws Exception {
    JoinExample job = new JoinExample();
    int status = ToolRunner.run(new Configuration(), job, args);
    System.exit(status);
  }

  private static void printUsage() {
    System.err.println("Usage: " + "joinexample <file1> <file2> <numPartitions> <outPath> [" 
                       + broadcastOption + "(default false)]");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs);
  }
  
  public int run(Configuration conf, String[] args, TezClient tezClient) throws Exception {
    setConf(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs, tezClient);
  }
  
  private int validateArgs(String[] otherArgs) {
    if (!(otherArgs.length == 4 || otherArgs.length == 5)) {
      printUsage();
      return 2;
    }
    return 0;
  }

  private int execute(String[] args) throws TezException, IOException, InterruptedException {
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
  
  private int execute(String[] args, TezClient tezClient) throws IOException, TezException,
      InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    return execute(args, tezConf, tezClient);
  }

  private TezClient createTezClient(TezConfiguration tezConf) throws TezException, IOException {
    TezClient tezClient = TezClient.create("JoinExample", tezConf);
    tezClient.start();
    return tezClient;
  }
  
  private int execute(String[] args, TezConfiguration tezConf, TezClient tezClient)
      throws IOException, TezException, InterruptedException {
    boolean doBroadcast = args.length == 5 && args[4].equals(broadcastOption) ? true : false;
    LOG.info("Running JoinExample" + (doBroadcast ? "-WithBroadcast" : ""));

    UserGroupInformation.setConfiguration(tezConf);

    String streamInputDir = args[0];
    String hashInputDir = args[1];
    int numPartitions = Integer.parseInt(args[2]);
    String outputDir = args[3];

    Path streamInputPath = new Path(streamInputDir);
    Path hashInputPath = new Path(hashInputDir);
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

    DAG dag = createDag(tezConf, streamInputPath, hashInputPath, outputPath, numPartitions, doBroadcast);

    tezClient.waitTillReady();
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;

  }

  private DAG createDag(TezConfiguration tezConf, Path streamPath, Path hashPath, Path outPath,
      int numPartitions, boolean doBroadcast) throws IOException {
    DAG dag = DAG.create("JoinExample" + (doBroadcast ? "-WithBroadcast" : ""));

    /**
     * This vertex represents the side of the join that will be accumulated in a hash 
     * table in order to join it against the other side. It reads text data using the
     * TextInputFormat. ForwardingProcessor simply forwards the data downstream as is.
     */
    Vertex hashFileVertex = Vertex.create(hashSide, ProcessorDescriptor.create(
        ForwardingProcessor.class.getName())).addDataSource(
        inputFile,
        MRInput
            .createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
                hashPath.toUri().toString()).groupSplits(false).build());

    /**
     * This vertex represents that side of the data that will be streamed and joined 
     * against the other side that has been accumulated into a hash table. It reads 
     * text data using the TextInputFormat. ForwardingProcessor simply forwards the data 
     * downstream as is.
     */
    Vertex streamFileVertex = Vertex.create(streamingSide, ProcessorDescriptor.create(
        ForwardingProcessor.class.getName())).addDataSource(
        inputFile,
        MRInput
            .createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
                streamPath.toUri().toString()).groupSplits(false).build());

    /**
     * This vertex represents the join operation. It writes the join output as text using
     * the TextOutputFormat. The JoinProcessor is going to perform the join of the 
     * streaming side and the hash side. It is load balanced across numPartitions 
     */
    Vertex joinVertex = Vertex.create(joiner, ProcessorDescriptor.create(
        JoinProcessor.class.getName()), numPartitions).addDataSink(joinOutput,
        MROutput.createConfigBuilder(new Configuration(tezConf),
            TextOutputFormat.class, outPath.toUri().toString()).build());

    /**
     * The streamed side will be partitioned into fragments with the same keys going to 
     * the same fragments using hash partitioning. The data to be joined is the key itself
     * and so the value is null. The number of fragments is initially inferred from the 
     * number of tasks running in the join vertex because each task will be handling one
     * fragment.
     */
    UnorderedPartitionedKVEdgeConfig streamConf =
        UnorderedPartitionedKVEdgeConfig
            .newBuilder(Text.class.getName(), NullWritable.class.getName(),
                HashPartitioner.class.getName()).build();

    /**
     * Connect the join vertex with the stream side
     */
    Edge e1 = Edge.create(streamFileVertex, joinVertex, streamConf.createDefaultEdgeProperty());
    
    EdgeProperty hashSideEdgeProperty = null;
    if (doBroadcast) {
      /**
       * This option can be used when the hash side is small. We can broadcast the entire data to 
       * all fragments of the stream side. This avoids re-partitioning the fragments of the stream 
       * side to match the partitioning scheme of the hash side and avoids costly network data 
       * transfer. However, in this example the stream side is being partitioned in both cases for 
       * brevity of code. The join task can perform the join of its fragment of keys with all the 
       * keys of the hash side.
       * Using an unpartitioned edge to transfer the complete output of the hash side to be 
       * broadcasted to all fragments of the streamed side. Again, since the data is the key, the 
       * value is null.
       */
      UnorderedKVEdgeConfig broadcastConf = UnorderedKVEdgeConfig.newBuilder(Text.class.getName(),
          NullWritable.class.getName()).build();
      hashSideEdgeProperty = broadcastConf.createDefaultBroadcastEdgeProperty();
    } else {
      /**
       * The hash side is also being partitioned into fragments with the same key going to the same
       * fragment using hash partitioning. This way all keys with the same hash value will go to the
       * same fragment from both sides. Thus the join task handling that fragment can join both data
       * set fragments. 
       */
      hashSideEdgeProperty = streamConf.createDefaultEdgeProperty();
    }

    /**
     * Connect the join vertex to the hash side.
     * The join vertex is connected with 2 upstream vertices that provide it with inputs
     */
    Edge e2 = Edge.create(hashFileVertex, joinVertex, hashSideEdgeProperty);

    /**
     * Connect everything up by adding them to the DAG
     */
    dag.addVertex(streamFileVertex).addVertex(hashFileVertex).addVertex(joinVertex)
        .addEdge(e1).addEdge(e2);
    return dag;
  }

  /**
   * Reads key-values from the source and forwards the value as the key for the output
   */
  public static class ForwardingProcessor extends SimpleProcessor {
    public ForwardingProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 1);
      Preconditions.checkState(getOutputs().size() == 1);
      // not looking up inputs and outputs by name because there is just one
      // instance and this processor is used in many different DAGs with 
      // different names for inputs and outputs
      LogicalInput input = getInputs().values().iterator().next();
      Reader rawReader = input.getReader();
      Preconditions.checkState(rawReader instanceof KeyValueReader);
      LogicalOutput output = getOutputs().values().iterator().next();

      KeyValueReader reader = (KeyValueReader) rawReader;
      KeyValueWriter writer = (KeyValueWriter) output.getWriter();

      while (reader.next()) {
        Object val = reader.getCurrentValue();
        // The data value itself is the join key. Simply write it out as the key.
        // The output value is null.
        writer.write(val, NullWritable.get());
      }
    }
  }

  public static class JoinProcessor extends SimpleMRProcessor {

    public JoinProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 2);
      Preconditions.checkState(getOutputs().size() == 1);
      // Get the input data for the 2 sides of the join from the 2 inputs
      LogicalInput streamInput = getInputs().get(streamingSide);
      LogicalInput hashInput = getInputs().get(hashSide);
      Reader rawStreamReader = streamInput.getReader();
      Reader rawHashReader = hashInput.getReader();
      Preconditions.checkState(rawStreamReader instanceof KeyValueReader);
      Preconditions.checkState(rawHashReader instanceof KeyValueReader);
      LogicalOutput lo = getOutputs().get(joinOutput);
      Preconditions.checkState(lo.getWriter() instanceof KeyValueWriter);
      KeyValueWriter writer = (KeyValueWriter) lo.getWriter();

      // create a hash table for the hash side
      KeyValueReader hashKvReader = (KeyValueReader) rawHashReader;
      Set<Text> keySet = new HashSet<Text>();
      while (hashKvReader.next()) {
        keySet.add(new Text((Text) hashKvReader.getCurrentKey()));
      }

      // read the stream side and join it using the hash table
      KeyValueReader streamKvReader = (KeyValueReader) rawStreamReader;
      while (streamKvReader.next()) {
        Text key = (Text) streamKvReader.getCurrentKey();
        if (keySet.contains(key)) {
          writer.write(key, NullWritable.get());
        }
      }
    }
  }
}

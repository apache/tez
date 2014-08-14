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

package org.apache.tez.mapreduce.examples;

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
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class IntersectExample extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(IntersectExample.class);

  public static void main(String[] args) throws Exception {
    IntersectExample intersect = new IntersectExample();
    int status = ToolRunner.run(new Configuration(), intersect, args);
    System.exit(status);
  }

  private static void printUsage() {
    System.err.println("Usage: " + "intersect <file1> <file2> <numPartitions> <outPath>");
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
  
  public int run(Configuration conf, String[] args, TezClient tezSession) throws Exception {
    setConf(conf);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int result = validateArgs(otherArgs);
    if (result != 0) {
      return result;
    }
    return execute(otherArgs, tezSession);
  }
  
  private int validateArgs(String[] otherArgs) {
    if (otherArgs.length != 4) {
      printUsage();
      return 2;
    }
    return 0;
  }

  private int execute(String[] args) throws TezException, IOException, InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    TezClient tezSession = null;
    try {
      tezSession = createTezSession(tezConf);
      return execute(args, tezConf, tezSession);
    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }
  
  private int execute(String[] args, TezClient tezSession) throws IOException, TezException,
      InterruptedException {
    TezConfiguration tezConf = new TezConfiguration(getConf());
    return execute(args, tezConf, tezSession);
  }
  
  private TezClient createTezSession(TezConfiguration tezConf) throws TezException, IOException {
    TezClient tezSession = new TezClient("IntersectExampleSession", tezConf);
    tezSession.start();
    return tezSession;
  }
  
  private int execute(String[] args, TezConfiguration tezConf, TezClient tezSession)
      throws IOException, TezException, InterruptedException {
    LOG.info("Running IntersectExample");

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

    DAG dag = createDag(tezConf, streamInputPath, hashInputPath, outputPath, numPartitions);

    tezSession.waitTillReady();
    DAGClient dagClient = tezSession.submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;

  }

  private DAG createDag(TezConfiguration tezConf, Path streamPath, Path hashPath, Path outPath,
      int numPartitions) throws IOException {
    DAG dag = new DAG("IntersectExample");

    // Configuration for intermediate output - shared by Vertex1 and Vertex2
    // This should only be setting selective keys from the underlying conf. Fix after there's a
    // better mechanism to configure the IOs.

    UnorderedPartitionedKVEdgeConfigurer edgeConf =
        UnorderedPartitionedKVEdgeConfigurer
            .newBuilder(Text.class.getName(), NullWritable.class.getName(),
                HashPartitioner.class.getName()).build();

    // Change the way resources are setup - no MRHelpers
    Vertex streamFileVertex = new Vertex("partitioner1", new ProcessorDescriptor(
        ForwardingProcessor.class.getName())).addDataSource(
        "streamfile",
        MRInput
            .createConfigurer(new Configuration(tezConf), TextInputFormat.class,
                streamPath.toUri().toString()).groupSplitsInAM(false).create());

    Vertex hashFileVertex = new Vertex("partitioner2", new ProcessorDescriptor(
        ForwardingProcessor.class.getName())).addDataSource(
        "hashfile",
        MRInput
            .createConfigurer(new Configuration(tezConf), TextInputFormat.class,
                hashPath.toUri().toString()).groupSplitsInAM(false).create());

    Vertex intersectVertex = new Vertex("intersect", new ProcessorDescriptor(
        IntersectProcessor.class.getName()), numPartitions).addDataSink("finalOutput",
        MROutput.createConfigurer(new Configuration(tezConf),
            TextOutputFormat.class, outPath.toUri().toString()).create());

    Edge e1 = new Edge(streamFileVertex, intersectVertex, edgeConf.createDefaultEdgeProperty());

    Edge e2 = new Edge(hashFileVertex, intersectVertex, edgeConf.createDefaultEdgeProperty());

    dag.addVertex(streamFileVertex).addVertex(hashFileVertex).addVertex(intersectVertex)
        .addEdge(e1).addEdge(e2);
    return dag;
  }

  // private void obtainTokens(Credentials credentials, Path... paths) throws IOException {
  // TokenCache.obtainTokensForNamenodes(credentials, paths, getConf());
  // }

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
      LogicalInput input = getInputs().values().iterator().next();
      Reader rawReader = input.getReader();
      Preconditions.checkState(rawReader instanceof KeyValueReader);
      LogicalOutput output = getOutputs().values().iterator().next();

      KeyValueReader reader = (KeyValueReader) rawReader;
      KeyValueWriter writer = (KeyValueWriter) output.getWriter();

      while (reader.next()) {
        Object val = reader.getCurrentValue();
        writer.write(val, NullWritable.get());
      }
    }
  }

  public static class IntersectProcessor extends SimpleMRProcessor {

    public IntersectProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 2);
      Preconditions.checkState(getOutputs().size() == 1);
      LogicalInput streamInput = getInputs().get("partitioner1");
      LogicalInput hashInput = getInputs().get("partitioner2");
      Reader rawStreamReader = streamInput.getReader();
      Reader rawHashReader = hashInput.getReader();
      Preconditions.checkState(rawStreamReader instanceof KeyValueReader);
      Preconditions.checkState(rawHashReader instanceof KeyValueReader);
      LogicalOutput lo = getOutputs().values().iterator().next();
      Preconditions.checkState(lo instanceof MROutput);
      MROutput output = (MROutput) lo;
      KeyValueWriter writer = output.getWriter();

      KeyValueReader hashKvReader = (KeyValueReader) rawHashReader;
      Set<Text> keySet = new HashSet<Text>();
      while (hashKvReader.next()) {
        keySet.add(new Text((Text) hashKvReader.getCurrentKey()));
      }

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

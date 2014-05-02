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
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
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
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedPartitionedKVOutput;
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
    System.err.println("Usage: " + "intersectlines <file1> <file2> <numPartitions> <outPath>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 4) {
      printUsage();
      return 2;
    }
    return execute(otherArgs);
  }

  private int execute(String[] args) throws IOException, TezException, InterruptedException {
    LOG.info("Running IntersectExample");
    TezConfiguration tezConf = new TezConfiguration(getConf());
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
      return 2;
    }
    if (numPartitions <= 0) {
      System.err.println("NumPartitions must be > 0");
      return 2;
    }

    AMConfiguration amConfiguration = new AMConfiguration(null, null, tezConf, null);
    TezSessionConfiguration sessionConfiguration = new TezSessionConfiguration(amConfiguration,
        tezConf);
    TezSession tezSession = new TezSession("IntersectExampleSession", sessionConfiguration);
    try {
      tezSession.start();

      DAG dag = createDag(tezConf, streamInputPath, hashInputPath, outputPath, numPartitions);
      setupURIsForCredentials(dag, streamInputPath, hashInputPath, outputPath);

      tezSession.waitTillReady();
      DAGClient dagClient = tezSession.submitDAG(dag);
      DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
        return -1;
      }
      return 0;
    } finally {
      tezSession.stop();
    }
  }

  private DAG createDag(TezConfiguration tezConf, Path streamPath, Path hashPath, Path outPath,
      int numPartitions) throws IOException {
    DAG dag = new DAG("IntersectExample");

    // Configuration for src1
    Configuration streamInputConf = new Configuration(tezConf);
    streamInputConf.set(FileInputFormat.INPUT_DIR, streamPath.toUri().toString());
    byte[] streamInputPayload = MRInput.createUserPayload(streamInputConf,
        TextInputFormat.class.getName(), true, false);

    // Configuration for src2
    Configuration hashInputConf = new Configuration(tezConf);
    hashInputConf.set(FileInputFormat.INPUT_DIR, hashPath.toUri().toString());
    byte[] hashInputPayload = MRInput.createUserPayload(hashInputConf,
        TextInputFormat.class.getName(), true, false);

    // Configuration for intermediate output - shared by Vertex1 and Vertex2
    // This should only be setting selective keys from the underlying conf. Fix after there's a
    // better mechanism to configure the IOs.
    Configuration intermediateOutputConf = new Configuration(tezConf);
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS,
        Text.class.getName());
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS,
        NullWritable.class.getName());
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());
    byte[] intermediateOutputPayload = TezUtils.createUserPayloadFromConf(intermediateOutputConf);

    Configuration intermediateInputConf = new Configuration(tezConf);
    intermediateInputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS,
        Text.class.getName());
    intermediateInputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS,
        NullWritable.class.getName());
    byte[] intermediateInputPayload = TezUtils.createUserPayloadFromConf(intermediateInputConf);

    Configuration finalOutputConf = new Configuration(tezConf);
    finalOutputConf.set(FileOutputFormat.OUTDIR, outPath.toUri().toString());
    byte[] finalOutputPayload = MROutput.createUserPayload(finalOutputConf,
        TextOutputFormat.class.getName(), true);

    // Change the way resources are setup - no MRHelpers
    Vertex streamFileVertex = new Vertex("partitioner1", new ProcessorDescriptor(
        ForwardingProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf)).setJavaOpts(
        MRHelpers.getMapJavaOpts(tezConf)).addInput("streamfile",
        new InputDescriptor(MRInput.class.getName()).setUserPayload(streamInputPayload),
        MRInputAMSplitGenerator.class);

    Vertex hashFileVertex = new Vertex("partitioner2", new ProcessorDescriptor(
        ForwardingProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf)).setJavaOpts(
        MRHelpers.getMapJavaOpts(tezConf)).addInput("hashfile",
        new InputDescriptor(MRInput.class.getName()).setUserPayload(hashInputPayload),
        MRInputAMSplitGenerator.class);

    Vertex intersectVertex = new Vertex("intersect", new ProcessorDescriptor(
        IntersectProcessor.class.getName()), numPartitions, MRHelpers.getReduceResource(tezConf))
        .setJavaOpts(MRHelpers.getReduceJavaOpts(tezConf)).addOutput("finalOutput",
            new OutputDescriptor(MROutput.class.getName()).setUserPayload(finalOutputPayload),
            MROutputCommitter.class);

    Edge e1 = new Edge(streamFileVertex, intersectVertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        new OutputDescriptor(OnFileUnorderedPartitionedKVOutput.class.getName())
            .setUserPayload(intermediateOutputPayload), new InputDescriptor(
            ShuffledUnorderedKVInput.class.getName()).setUserPayload(intermediateInputPayload)));

    Edge e2 = new Edge(hashFileVertex, intersectVertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        new OutputDescriptor(OnFileUnorderedPartitionedKVOutput.class.getName())
            .setUserPayload(intermediateOutputPayload), new InputDescriptor(
            ShuffledUnorderedKVInput.class.getName()).setUserPayload(intermediateInputPayload)));

    dag.addVertex(streamFileVertex).addVertex(hashFileVertex).addVertex(intersectVertex)
        .addEdge(e1).addEdge(e2);
    return dag;
  }

  private void setupURIsForCredentials(DAG dag, Path... paths) throws IOException {
    List<URI> uris = new LinkedList<URI>();
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(getConf());
      Path qPath = fs.makeQualified(path);
      uris.add(qPath.toUri());
    }
    dag.addURIsForCredentials(uris);
  }

  // private void obtainTokens(Credentials credentials, Path... paths) throws IOException {
  // TokenCache.obtainTokensForNamenodes(credentials, paths, getConf());
  // }

  /**
   * Reads key-values from the source and forwards the value as the key for the output
   */
  public static class ForwardingProcessor extends SimpleProcessor {
    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 1);
      Preconditions.checkState(getOutputs().size() == 1);
      LogicalInput input = getInputs().values().iterator().next();
      Reader rawReader = input.getReader();
      Preconditions.checkState(rawReader instanceof KeyValueReader);
      LogicalOutput output = getOutputs().values().iterator().next();
      Preconditions.checkState(output instanceof OnFileUnorderedPartitionedKVOutput);

      KeyValueReader reader = (KeyValueReader) rawReader;
      KeyValueWriter writer = (UnorderedPartitionedKVWriter) output.getWriter();

      while (reader.next()) {
        Object val = reader.getCurrentValue();
        writer.write(val, NullWritable.get());
      }
    }
  }

  public static class IntersectProcessor extends SimpleProcessor {

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

      LOG.info("Completed Processing. Trying to commit");
      while (!getContext().canCommit()) {
        Thread.sleep(100l);
      }
      output.commit();
    }
  }
}
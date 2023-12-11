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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.Preconditions;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.library.vertexmanager.FairShuffleVertexManager;
import org.apache.tez.dag.library.vertexmanager.FairShuffleVertexManager.FairRoutingType;
import org.apache.tez.examples.HashJoinExample.ForwardingProcessor;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MultiMROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriterWithBasePath;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration.ReportPartitionStats;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple example of a demuxer tolerant of data skew problems. The example scans the source files,
 * partitions records by values, and write them in /path/to/{value}-r-{task id}.
 */
public class Demuxer extends TezExampleBase {
  private static final Logger LOG = LoggerFactory.getLogger(Demuxer.class);

  private static final String DEMUXER_OUTPUT = "demuxerOutput";

  public static void main(String[] args) throws Exception {
    Demuxer job = new Demuxer();
    int status = ToolRunner.run(new Configuration(), job, args);
    System.exit(status);
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: "
        + "demuxer <inputPath> <outPath> <numPartitions> [isPrecise(default false)]");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length < 3 || otherArgs.length > 4) {
      return 2;
    }
    return 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient)
      throws Exception {
    String inputDirs = args[0];
    String outputDir = args[1];
    int numPartitions = Integer.parseInt(args[2]);
    if (args.length == 4 && Boolean.parseBoolean(args[3])) {
      tezConf.set(
          TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
          ReportPartitionStats.PRECISE.getType());
    }

    List<Path> inputPaths = Arrays
        .stream(inputDirs.split(","))
        .map(Path::new)
        .collect(Collectors.toList());
    Path outputPath = new Path(outputDir);

    FileSystem fs = outputPath.getFileSystem(tezConf);
    outputPath = fs.makeQualified(outputPath);
    if (fs.exists(outputPath)) {
      System.err.println("Output directory: " + outputDir + " already exists");
      return 3;
    }
    DAG dag = inputPaths.size() == 1
        ? createDag(tezConf, inputPaths.get(0), outputPath, numPartitions)
        : createDagWithUnion(tezConf, inputPaths, outputPath, numPartitions);
    LOG.info("Running Demuxer");
    return runDag(dag, isCountersLog(), LOG);
  }

  private DAG createDag(TezConfiguration tezConf, Path inputPath, Path outputPath,
      int numPartitions) {
    Vertex inputVertex = createInputVertex(tezConf, "input", inputPath);
    Vertex demuxVertex = createDemuxVertex(tezConf, outputPath, numPartitions);
    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(
            Text.class.getName(),
            NullWritable.class.getName(),
            HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build();
    return DAG
        .create("Demuxer")
        .addVertex(inputVertex)
        .addVertex(demuxVertex)
        .addEdge(Edge.create(inputVertex, demuxVertex, edgeConf.createDefaultEdgeProperty()));
  }

  private DAG createDagWithUnion(TezConfiguration tezConf, List<Path> inputPaths, Path outputPath,
      int numPartitions) {
    Vertex[] inputVertices = new Vertex[inputPaths.size()];
    for (int i = 0; i < inputPaths.size(); i++) {
      inputVertices[i] = createInputVertex(tezConf, "input-" + i, inputPaths.get(i));
    }

    Vertex demuxVertex = createDemuxVertex(tezConf, outputPath, numPartitions);

    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), NullWritable.class.getName(),
            HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build();

    DAG dag = DAG.create("Demuxer");
    Arrays.stream(inputVertices).forEach(dag::addVertex);
    return dag
        .addVertex(demuxVertex)
        .addEdge(
            GroupInputEdge.create(
                dag.createVertexGroup("union", inputVertices),
                demuxVertex,
                edgeConf.createDefaultEdgeProperty(),
                InputDescriptor.create(ConcatenatedMergedKeyValuesInput.class.getName())));
  }

  private Vertex createInputVertex(TezConfiguration tezConf, String vertexName, Path path) {
    // This vertex represents an input vertex for the demuxer. It reads text data using the
    // TextInputFormat. ForwardingProcessor simply forwards the data downstream as is.
    return Vertex
        .create(vertexName, ProcessorDescriptor.create(ForwardingProcessor.class.getName()))
        .addDataSource(
            "inputFile",
            MRInput
                .createConfigBuilder(
                    new Configuration(tezConf),
                    TextInputFormat.class,
                    path.toUri().toString())
                .groupSplits(!isDisableSplitGrouping())
                .generateSplitsInAM(!isGenerateSplitInClient()).build());
  }

  private Vertex createDemuxVertex(TezConfiguration tezConf, Path outputPath, int numPartitions) {
    // This vertex demuxes records based on the keys. Multiple reduce tasks can process the same key
    // as fair routing is configured.
    return Vertex
        .create(
            "demuxer",
            ProcessorDescriptor.create(DemuxProcessor.class.getName()), numPartitions)
        .setVertexManagerPlugin(
            FairShuffleVertexManager
                .createConfigBuilder(tezConf)
                .setAutoParallelism(FairRoutingType.FAIR_PARALLELISM)
                // These params demonstrate perfect fair routing
                .setSlowStartMinSrcCompletionFraction(1.0f)
                .setSlowStartMaxSrcCompletionFraction(1.0f)
                .build())
        .addDataSink(
            DEMUXER_OUTPUT,
            MultiMROutput
                .createConfigBuilder(
                    new Configuration(tezConf),
                    TextOutputFormat.class,
                    outputPath.toUri().toString(),
                    false)
                .build());
  }

  public static class DemuxProcessor extends SimpleMRProcessor {
    public DemuxProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(!getInputs().isEmpty());
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueWriterWithBasePath kvWriter = (KeyValueWriterWithBasePath) getOutputs()
          .get(DEMUXER_OUTPUT)
          .getWriter();
      for (LogicalInput input : getInputs().values()) {
        KeyValuesReader kvReader = (KeyValuesReader) input.getReader();
        while (kvReader.next()) {
          Text category = (Text) kvReader.getCurrentKey();
          String path = category.toString();
          for (Object value : kvReader.getCurrentValues()) {
            assert value == NullWritable.get();
            kvWriter.write(category, NullWritable.get(), path);
          }
        }
      }
    }
  }
}

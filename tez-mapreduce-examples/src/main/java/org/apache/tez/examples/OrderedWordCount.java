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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.examples.WordCount.TokenProcessor;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class OrderedWordCount extends Configured implements Tool  {
  
  public static class SumProcessor extends SimpleProcessor {
    public SumProcessor(TezProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next()
          .getReader();
      while (kvReader.next()) {
        Text word = (Text) kvReader.getCurrentKey();
        int sum = 0;
        for (Object value : kvReader.getCurrentValues()) {
          sum += ((IntWritable) value).get();
        }
        kvWriter.write(new IntWritable(sum), word);
      }
    }
  }
  
  /**
   * OrderPartitionedEdge ensures that we get the data sorted by
   * the sum key which means that the result is totally ordered
   */
  public static class NoOpSorter extends SimpleMRProcessor {

    public NoOpSorter(TezProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next()
          .getReader();
      while (kvReader.next()) {
        Object sum = kvReader.getCurrentKey();
        for (Object word : kvReader.getCurrentValues()) {
          kvWriter.write(word, sum);
        }
      }
      // deriving from SimpleMRProcessor takes care of committing the output
    }
  }
  
  private DAG createDAG(TezConfiguration tezConf, Map<String, LocalResource> localResources,
      String inputPath, String outputPath, int numPartitions) throws IOException {

    DataSourceDescriptor dataSource = MRInput.createConfigurer(new Configuration(tezConf),
        TextInputFormat.class, inputPath).create();

    DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
        TextOutputFormat.class, outputPath).create();

    Vertex tokenizerVertex = new Vertex("Tokenizer", new ProcessorDescriptor(
        TokenProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf));
    tokenizerVertex.addDataSource("MRInput", dataSource);

    Vertex summationVertex = new Vertex("Summation",
        new ProcessorDescriptor(
            SumProcessor.class.getName()), numPartitions, MRHelpers.getReduceResource(tezConf));
    
    // 1 task for global sorted order
    Vertex sorterVertex = new Vertex("Sorter",
        new ProcessorDescriptor(
            NoOpSorter.class.getName()), 1, MRHelpers.getReduceResource(tezConf));
    sorterVertex.addDataSink("MROutput", dataSink);

    OrderedPartitionedKVEdgeConfigurer summationEdgeConf = OrderedPartitionedKVEdgeConfigurer
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName()).build();

    OrderedPartitionedKVEdgeConfigurer sorterEdgeConf = OrderedPartitionedKVEdgeConfigurer
        .newBuilder(IntWritable.class.getName(), Text.class.getName(),
            HashPartitioner.class.getName()).build();

    // No need to add jar containing this class as assumed to be part of the tez jars.
    
    DAG dag = new DAG("OrderedWordCount");
    dag.addVertex(tokenizerVertex)
        .addVertex(summationVertex)
        .addVertex(sorterVertex)
        .addEdge(
            new Edge(tokenizerVertex, summationVertex, summationEdgeConf.createDefaultEdgeProperty()))
        .addEdge(
            new Edge(summationVertex, sorterVertex, sorterEdgeConf.createDefaultEdgeProperty()));
    return dag;  
  }
  
  private static void printUsage() {
    System.err.println("Usage: " + " orderedwordcount in out [numPartitions]");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  public boolean run(String inputPath, String outputPath, Configuration conf,
      int numPartitions) throws Exception {
    System.out.println("Running OrderedWordCount");
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    
    TezClient tezClient = new TezClient("OrderedWordCount", tezConf);
    tezClient.start();

    try {
        Map<String, LocalResource> localResources =
          new TreeMap<String, LocalResource>();
        
        DAG dag = createDAG(tezConf, localResources, inputPath, outputPath, numPartitions);

        tezClient.waitTillReady();
        DAGClient dagClient = tezClient.submitDAG(dag);

        // monitoring
        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("OrderedWordCount failed with diagnostics: " + dagStatus.getDiagnostics());
          return false;
        }
        return true;
    } finally {
      tezClient.stop();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 2 || otherArgs.length > 3) {
      printUsage();
      return 2;
    }
    OrderedWordCount job = new OrderedWordCount();
    if (job.run(otherArgs[0], otherArgs[1], conf,
        (otherArgs.length == 3 ? Integer.parseInt(otherArgs[2]) : 1))) {
      return 0;
    }
    return 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new OrderedWordCount(), args);
    System.exit(res);
  }
}

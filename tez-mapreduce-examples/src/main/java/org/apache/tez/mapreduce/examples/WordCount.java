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
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfiguration;

import com.google.common.base.Preconditions;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;


public class WordCount extends Configured implements Tool {
  public static class TokenProcessor extends SimpleMRProcessor {
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      MRInput input = (MRInput) getInputs().values().iterator().next();
      KeyValueReader kvReader = input.getReader();
      Output output = getOutputs().values().iterator().next();
      KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
      while (kvReader.next()) {
        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          kvWriter.write(word, one);
        }
      }
    }

  }

  public static class SumProcessor extends SimpleMRProcessor {
    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      MROutput out = (MROutput) getOutputs().values().iterator().next();
      KeyValueWriter kvWriter = out.getWriter();
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next()
          .getReader();
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

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Map<String, LocalResource> localResources, Path stagingDir,
      String inputPath, String outputPath) throws IOException {

    Configuration inputConf = new Configuration(tezConf);
    inputConf.set(FileInputFormat.INPUT_DIR, inputPath);
    InputDescriptor id = new InputDescriptor(MRInput.class.getName())
        .setUserPayload(MRInput.createUserPayload(inputConf,
            TextInputFormat.class.getName(), true, true));

    Configuration outputConf = new Configuration(tezConf);
    outputConf.set(FileOutputFormat.OUTDIR, outputPath);
    OutputDescriptor od = new OutputDescriptor(MROutput.class.getName())
      .setUserPayload(MROutput.createUserPayload(
          outputConf, TextOutputFormat.class.getName(), true));

    Vertex tokenizerVertex = new Vertex("tokenizer", new ProcessorDescriptor(
        TokenProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf));
    tokenizerVertex.addInput("MRInput", id, MRInputAMSplitGenerator.class);

    Vertex summerVertex = new Vertex("summer",
        new ProcessorDescriptor(
            SumProcessor.class.getName()), 1, MRHelpers.getReduceResource(tezConf));
    summerVertex.addOutput("MROutput", od, MROutputCommitter.class);

    OrderedPartitionedKVEdgeConfiguration edgeConf = OrderedPartitionedKVEdgeConfiguration
        .newBuilder(Text.class.getName(), IntWritable.class.getName()).configureOutput(
            HashPartitioner.class.getName(), null).done().build();

    DAG dag = new DAG("WordCount");
    dag.addVertex(tokenizerVertex)
        .addVertex(summerVertex)
        .addEdge(
            new Edge(tokenizerVertex, summerVertex, edgeConf.createDefaultEdgeProperty()));
    return dag;  
  }

  private static void printUsage() {
    System.err.println("Usage: " + " wordcount <in1> <out1>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  public boolean run(String inputPath, String outputPath, Configuration conf) throws Exception {
    System.out.println("Running WordCount");
    // conf and UGI
    TezConfiguration tezConf;
    if (conf != null) {
      tezConf = new TezConfiguration(conf);
    } else {
      tezConf = new TezConfiguration();
    }
    UserGroupInformation.setConfiguration(tezConf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + Long.toString(System.currentTimeMillis());
    Path stagingDir = new Path(stagingDirStr);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = fs.makeQualified(stagingDir);
    
    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    
    TezClient tezSession = new TezClient("WordCountSession", tezConf);
    tezSession.start();

    DAGClient dagClient = null;

    try {
        if (fs.exists(new Path(outputPath))) {
          throw new FileAlreadyExistsException("Output directory "
              + outputPath + " already exists");
        }
        
        Map<String, LocalResource> localResources =
          new TreeMap<String, LocalResource>();
        
        DAG dag = createDAG(fs, tezConf, localResources,
            stagingDir, inputPath, outputPath);

        tezSession.waitTillReady();
        dagClient = tezSession.submitDAG(dag);

        // monitoring
        DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
          return false;
        }
        return true;
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      printUsage();
      return 2;
    }
    WordCount job = new WordCount();
    job.run(otherArgs[0], otherArgs[1], conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.exit(res);
  }
}

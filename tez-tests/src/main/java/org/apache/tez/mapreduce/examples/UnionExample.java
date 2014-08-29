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
import java.util.EnumSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class UnionExample {

  public static class TokenProcessor extends SimpleMRProcessor {
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      boolean inUnion = true;
      if (getContext().getTaskVertexName().equals("map3")) {
        inUnion = false;
      }
      Preconditions.checkArgument(getOutputs().size() == (inUnion ? 2 : 1));
      Preconditions.checkArgument(getOutputs().containsKey("checker"));
      MRInput input = (MRInput) getInputs().values().iterator().next();
      KeyValueReader kvReader = input.getReader();
      Output output =  getOutputs().get("checker");
      KeyValueWriter kvWriter = (KeyValueWriter) output.getWriter();
      MROutput parts = null;
      KeyValueWriter partsWriter = null;
      if (inUnion) {
        parts = (MROutput) getOutputs().get("parts");
        partsWriter = parts.getWriter();
      }
      while (kvReader.next()) {
        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          kvWriter.write(word, one);
          if (inUnion) {
            partsWriter.write(word, one);
          }
        }
      }
    }

  }

  public static class UnionProcessor extends SimpleMRProcessor {
    IntWritable one = new IntWritable(1);

    public UnionProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 2);
      Preconditions.checkArgument(getOutputs().size() == 2);
      MROutput out = (MROutput) getOutputs().get("union");
      MROutput allParts = (MROutput) getOutputs().get("all-parts");
      KeyValueWriter kvWriter = out.getWriter();
      KeyValueWriter partsWriter = allParts.getWriter();
      Map<String, AtomicInteger> unionKv = Maps.newHashMap();
      LogicalInput union = getInputs().get("union");
      KeyValuesReader kvReader = (KeyValuesReader) union.getReader();
      while (kvReader.next()) {
        String word = ((Text) kvReader.getCurrentKey()).toString();
        IntWritable intVal = (IntWritable) kvReader.getCurrentValues().iterator().next();
        for (int i = 0; i < intVal.get(); ++i) {
          partsWriter.write(word, one);
        }
        AtomicInteger value = unionKv.get(word);
        if (value == null) {
          unionKv.put(word, new AtomicInteger(intVal.get()));
        } else {
          value.addAndGet(intVal.get());
        }
      }
      LogicalInput map3 = getInputs().get("map3");
      kvReader = (KeyValuesReader) map3.getReader();
      while (kvReader.next()) {
        String word = ((Text) kvReader.getCurrentKey()).toString();
        IntWritable intVal = (IntWritable) kvReader.getCurrentValues().iterator().next();
        AtomicInteger value = unionKv.get(word);
        if (value == null) {
          throw new TezUncheckedException("Expected to exist: " + word);
        } else {
          value.getAndAdd(intVal.get() * -2);
        }
      }
      for (AtomicInteger value : unionKv.values()) {
        if (value.get() != 0) {
          throw new TezUncheckedException("Unexpected non-zero value");
        }
      }
      kvWriter.write("Union", new IntWritable(unionKv.size()));
    }

  }

  private DAG createDAG(FileSystem fs, TezConfiguration tezConf,
      Map<String, LocalResource> localResources, Path stagingDir,
      String inputPath, String outputPath) throws IOException {
    DAG dag = DAG.create("UnionExample");
    
    int numMaps = -1;
    Configuration inputConf = new Configuration(tezConf);
    inputConf.setBoolean("mapred.mapper.new-api", false);
    inputConf.set("mapred.input.format.class", TextInputFormat.class.getName());
    inputConf.set(FileInputFormat.INPUT_DIR, inputPath);
    MRInput.MRInputConfigBuilder configurer = MRInput.createConfigBuilder(inputConf, null);
    DataSourceDescriptor dataSource = configurer.generateSplitsInAM(false).build();

    Vertex mapVertex1 = Vertex.create("map1", ProcessorDescriptor.create(
        TokenProcessor.class.getName()), numMaps).addDataSource("MRInput", dataSource);

    Vertex mapVertex2 = Vertex.create("map2", ProcessorDescriptor.create(
        TokenProcessor.class.getName()), numMaps).addDataSource("MRInput", dataSource);

    Vertex mapVertex3 = Vertex.create("map3", ProcessorDescriptor.create(
        TokenProcessor.class.getName()), numMaps).addDataSource("MRInput", dataSource);

    Vertex checkerVertex = Vertex.create("checker", ProcessorDescriptor.create(
        UnionProcessor.class.getName()), 1);

    Configuration outputConf = new Configuration(tezConf);
    outputConf.setBoolean("mapred.reducer.new-api", false);
    outputConf.set("mapred.output.format.class", TextOutputFormat.class.getName());
    outputConf.set(FileOutputFormat.OUTDIR, outputPath);
    DataSinkDescriptor od = MROutput.createConfigBuilder(outputConf, null).build();
    checkerVertex.addDataSink("union", od);
    

    Configuration allPartsConf = new Configuration(tezConf);
    DataSinkDescriptor od2 = MROutput.createConfigBuilder(allPartsConf,
        TextOutputFormat.class, outputPath + "-all-parts").build();
    checkerVertex.addDataSink("all-parts", od2);

    Configuration partsConf = new Configuration(tezConf);    
    DataSinkDescriptor od1 = MROutput.createConfigBuilder(partsConf,
        TextOutputFormat.class, outputPath + "-parts").build();
    VertexGroup unionVertex = dag.createVertexGroup("union", mapVertex1, mapVertex2);
    unionVertex.addDataSink("parts", od1);

    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName()).build();

    dag.addVertex(mapVertex1)
        .addVertex(mapVertex2)
        .addVertex(mapVertex3)
        .addVertex(checkerVertex)
        .addEdge(
            Edge.create(mapVertex3, checkerVertex, edgeConf.createDefaultEdgeProperty()))
        .addEdge(
            GroupInputEdge.create(unionVertex, checkerVertex, edgeConf.createDefaultEdgeProperty(),
                InputDescriptor.create(
                    ConcatenatedMergedKeyValuesInput.class.getName())));
    return dag;  
  }

  private static void printUsage() {
    System.err.println("Usage: " + " unionexample <in1> <out1>");
  }

  public boolean run(String inputPath, String outputPath, Configuration conf) throws Exception {
    System.out.println("Running UnionExample");
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
    
    TezClient tezSession = TezClient.create("UnionExampleSession", tezConf);
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
        DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(EnumSet.of(StatusGetOpts.GET_COUNTERS));
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

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      printUsage();
      System.exit(2);
    }
    UnionExample job = new UnionExample();
    job.run(args[0], args[1], null);
  }
}

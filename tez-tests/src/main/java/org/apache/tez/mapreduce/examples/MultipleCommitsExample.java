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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.examples.TezExampleBase;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValuesInput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  v1 -> v3 <br/>
 *  v2 -> v3 <br/>
 *  (v1,v2) is connected to v3 as vertex group. <br/>
 *  (v1,v2) have multiple shared outputs, each of them have its own multiple outputs. 
 *  And v3 also has multiple outputs. </br>
 */
public class MultipleCommitsExample extends TezExampleBase {

  private static final Logger LOG = LoggerFactory.getLogger(MultipleCommitsExample.class);
  private static final String UV12OutputNamePrefix = "uv12Output";
  private static final String V1OutputNamePrefix = "v1Output";
  private static final String V2OutputNamePrefix = "v2Output";
  private static final String V3OutputNamePrefix = "v3Output";

  public static final String CommitOnVertexSuccessOption = "commitOnVertexSuccess";

  @Override
  protected void printUsage() {
    System.err.println("Usage: "
        + " multiplecommitsExample v1OutputPrefix v1OutputNum v2OutputPrefix v2OutputNum"
        + " uv12OutputPrefix uv12OutputNum v3OutputPrefix v3OutputNum"
        + " [" + CommitOnVertexSuccessOption + "]" + "(default false)");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length != 8 && otherArgs.length != 9) {
      return 2;
    }
    if (otherArgs.length == 9 && !otherArgs[8].equals(CommitOnVertexSuccessOption)) {
      return 2;
    }
    return 0;
  }

  public static class MultipleOutputProcessor extends SimpleMRProcessor {

    MultipleOutputProcessorConfig config;

    public MultipleOutputProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
      super.initialize();
      config = MultipleOutputProcessorConfig.fromUserPayload(getContext().getUserPayload());
    }
    
    @Override
    public void run() throws Exception {
      for (int i=0;i < config.outputNum;++i) {
        KeyValueWriter writer = (KeyValueWriter)
            getOutputs().get(config.outputNamePrefix+"_" + i).getWriter();
        writer.write(NullWritable.get(), new Text("dummy"));
      }
      for (int i=0;i < config.sharedOutputNum; ++i) {
        KeyValueWriter writer = (KeyValueWriter)
            getOutputs().get(config.sharedOutputNamePrefix +"_" + i).getWriter();
        writer.write(NullWritable.get(), new Text("dummy"));
      }
    }
    
    public static class MultipleOutputProcessorConfig implements Writable {
      
      String outputNamePrefix;
      int outputNum;
      String sharedOutputNamePrefix = null;
      int sharedOutputNum;

      public MultipleOutputProcessorConfig(){
        
      }
      
      public MultipleOutputProcessorConfig(String outputNamePrefix, int outputNum) {
        this.outputNamePrefix = outputNamePrefix;
        this.outputNum = outputNum;
      }

      public MultipleOutputProcessorConfig(String outputNamePrefix, int outputNum,
          String sharedOutputNamePrefix, int sharedOutputNum) {
        this.outputNamePrefix = outputNamePrefix;
        this.outputNum = outputNum;
        this.sharedOutputNamePrefix = sharedOutputNamePrefix;
        this.sharedOutputNum = sharedOutputNum;
      }

      @Override
      public void write(DataOutput out) throws IOException {
        new Text(outputNamePrefix).write(out);
        out.writeInt(outputNum);
        if (sharedOutputNamePrefix != null) {
          new BooleanWritable(true).write(out);
          new Text(sharedOutputNamePrefix).write(out);
          out.writeInt(sharedOutputNum);
        } else {
          new BooleanWritable(false).write(out);
        }
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        Text outputNameText = new Text();
        outputNameText.readFields(in);
        outputNamePrefix = outputNameText.toString();
        outputNum = in.readInt();
        BooleanWritable hasSharedOutputs = new BooleanWritable();
        hasSharedOutputs.readFields(in);
        if (hasSharedOutputs.get()) {
          Text sharedOutputNamePrefixText = new Text();
          sharedOutputNamePrefixText.readFields(in);
          sharedOutputNamePrefix = sharedOutputNamePrefixText.toString();
          sharedOutputNum = in.readInt();
        }
      }
      
      public UserPayload toUserPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        this.write(new DataOutputStream(out));
        return UserPayload.create(ByteBuffer.wrap(out.toByteArray()));
      }

      public static MultipleOutputProcessorConfig fromUserPayload(UserPayload payload)
          throws IOException {
        MultipleOutputProcessorConfig config = new MultipleOutputProcessorConfig();
        config.readFields(new DataInputStream(
            new ByteArrayInputStream(payload.deepCopyAsArray())));
        return config;
      }
    }
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    boolean commitOnVertexSuccess =
        args.length == 5 && args[4].equals(CommitOnVertexSuccessOption) ? true : false;
    DAG dag = createDAG(tezConf, args[0], Integer.parseInt(args[1]),
        args[2], Integer.parseInt(args[3]),
        args[4], Integer.parseInt(args[5]),
        args[6], Integer.parseInt(args[7]),
        commitOnVertexSuccess);
    LOG.info("Running MultipleCommitsExample");
    return runDag(dag, false, LOG);
  }

  private DAG createDAG(TezConfiguration tezConf, 
      String v1OutputPathPrefix, int v1OutputNum, String v2OutputPathPrefix, int v2OutputNum,
      String uv12OutputPathPrefix, int uv12OutputNum,
      String v3OutputPathPrefix, int v3OutputNum, boolean commitOnVertexSuccess) throws IOException {
    DAG dag = DAG.create("multipleCommitsDAG");
    dag.setConf(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, !commitOnVertexSuccess + "");
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create(MultipleOutputProcessor.class.getName())
        .setUserPayload(
            new MultipleOutputProcessor.MultipleOutputProcessorConfig(
                V1OutputNamePrefix, v1OutputNum, UV12OutputNamePrefix, uv12OutputNum)
              .toUserPayload()), 2);
    Vertex v2 = Vertex.create("v2", ProcessorDescriptor.create(MultipleOutputProcessor.class.getName())
        .setUserPayload(
            new MultipleOutputProcessor.MultipleOutputProcessorConfig(
                V2OutputNamePrefix, v2OutputNum, UV12OutputNamePrefix, uv12OutputNum)
              .toUserPayload()), 2);
    // add data sinks for v1
    for (int i=0;i<v1OutputNum;++i) {
      DataSinkDescriptor sink = MROutput.createConfigBuilder(
          new Configuration(tezConf), TextOutputFormat.class, v1OutputPathPrefix + "_" + i).build();
      v1.addDataSink(V1OutputNamePrefix + "_" + i, sink);
    }
    // add data sinks for v2
    for (int i=0;i<v2OutputNum;++i) {
      DataSinkDescriptor sink = MROutput.createConfigBuilder(
          new Configuration(tezConf), TextOutputFormat.class, v2OutputPathPrefix + "_" + i).build();
      v2.addDataSink(V2OutputNamePrefix + "_" + i, sink);
    }
    // add data sinks for (v1,v2)
    VertexGroup uv12 = dag.createVertexGroup("uv12", v1,v2);
    for (int i=0;i<uv12OutputNum;++i) {
      DataSinkDescriptor sink = MROutput.createConfigBuilder(
          new Configuration(tezConf), TextOutputFormat.class, uv12OutputPathPrefix + "_" + i).build();
      uv12.addDataSink(UV12OutputNamePrefix + "_" + i, sink);
    }

    Vertex v3 = Vertex.create("v3", ProcessorDescriptor.create(MultipleOutputProcessor.class.getName())
        .setUserPayload(
            new MultipleOutputProcessor.MultipleOutputProcessorConfig(V3OutputNamePrefix, v3OutputNum)
              .toUserPayload()), 2);
    // add data sinks for v3
    for (int i=0;i<v3OutputNum;++i) {
      DataSinkDescriptor sink = MROutput.createConfigBuilder(
          new Configuration(tezConf), TextOutputFormat.class, v3OutputPathPrefix + "_" + i).build();
      v3.addDataSink(V3OutputNamePrefix + "_" + i, sink);
    }

    OrderedPartitionedKVEdgeConfig edgeConfig =
        OrderedPartitionedKVEdgeConfig.newBuilder(
            NullWritable.class.getName(), Text.class.getName(), HashPartitioner.class.getName())
            .setFromConfiguration(tezConf)
            .build();
    GroupInputEdge edge = GroupInputEdge.create(uv12, v3, edgeConfig.createDefaultEdgeProperty(),
        InputDescriptor.create(
            ConcatenatedMergedKeyValuesInput.class.getName()));
    dag.addVertex(v1)
      .addVertex(v2)
      .addVertex(v3)
      .addEdge(edge);
    return dag;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MultipleCommitsExample(), args);
    System.exit(res);
  }
}

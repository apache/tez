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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductConfig;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductEdgeManager;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * This job has three vertices: two Tokenizers and one JoinProcessor. Each Tokenizer handles one
 * input directory and generates tokens. CustomPartitioner separates tokens into 2 partitions
 * according to the parity of token's first char. Then JoinProcessor does cartesian product of
 * partitioned token sets.
 */
public class CartesianProduct extends TezExampleBase {
  private static final String INPUT = "Input1";
  private static final String OUTPUT = "Output";
  private static final String VERTEX1 = "Vertex1";
  private static final String VERTEX2 = "Vertex2";
  private static final String VERTEX3 = "Vertex3";
  private static final String PARTITIONED = "-partitioned";
  private static final String UNPARTITIONED = "-unpartitioned";
  private static final Logger LOG = LoggerFactory.getLogger(CartesianProduct.class);
  private static final int numPartition = 2;
  private static final String[] sourceVertices = new String[] {VERTEX1, VERTEX2};

  public static class TokenProcessor extends SimpleProcessor {
    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(VERTEX3).getWriter();
      while (kvReader.next()) {
        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
        while (itr.hasMoreTokens()) {
          kvWriter.write(new Text(itr.nextToken()), new IntWritable(1));
        }
      }
    }
  }

  public static class JoinProcessor extends SimpleMRProcessor {
    public JoinProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
      KeyValueReader kvReader1 = (KeyValueReader) getInputs().get(VERTEX1).getReader();
      KeyValueReader kvReader2 = (KeyValueReader) getInputs().get(VERTEX2).getReader();
      Set<String> rightSet = new HashSet<>();

      while (kvReader2.next()) {
        rightSet.add(kvReader2.getCurrentKey().toString());
      }

      while (kvReader1.next()) {
        String left = kvReader1.getCurrentKey().toString();
        for (String right : rightSet) {
          kvWriter.write(left, right);
        }
      }
    }
  }

  public static class CustomPartitioner implements Partitioner {
    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      return key.toString().charAt(0) % numPartition;
    }
  }

  private DAG createDAG(TezConfiguration tezConf, String inputPath1, String inputPath2,
                        String outputPath, boolean isPartitioned) throws IOException {
    Vertex v1 = Vertex.create(VERTEX1, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    // turn off groupSplit so that each input file incurs one task
    v1.addDataSource(INPUT,
      MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath1)
             .groupSplits(false).build());
    Vertex v2 = Vertex.create(VERTEX2, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    v2.addDataSource(INPUT,
      MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath2)
              .groupSplits(false).build());
    CartesianProductConfig cartesianProductConfig;
    if (isPartitioned) {
      Map<String, Integer> vertexPartitionMap = new HashMap<>();
      for (String vertex : sourceVertices) {
        vertexPartitionMap.put(vertex, numPartition);
      }
      cartesianProductConfig = new CartesianProductConfig(vertexPartitionMap);
    } else {
      cartesianProductConfig = new CartesianProductConfig(Arrays.asList(sourceVertices));
    }
    UserPayload userPayload = cartesianProductConfig.toUserPayload(tezConf);
    Vertex v3 = Vertex.create(VERTEX3, ProcessorDescriptor.create(JoinProcessor.class.getName()));
    v3.addDataSink(OUTPUT,
      MROutput.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, outputPath)
              .build());
    v3.setVertexManagerPlugin(
      VertexManagerPluginDescriptor.create(CartesianProductVertexManager.class.getName())
                                   .setUserPayload(userPayload));

    DAG dag = DAG.create("CrossProduct").addVertex(v1).addVertex(v2).addVertex(v3);
    EdgeManagerPluginDescriptor edgeManagerDescriptor =
      EdgeManagerPluginDescriptor.create(CartesianProductEdgeManager.class.getName());
    edgeManagerDescriptor.setUserPayload(userPayload);
    EdgeProperty edgeProperty;
    if (isPartitioned) {
      UnorderedPartitionedKVEdgeConfig edgeConf =
        UnorderedPartitionedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName(),
          CustomPartitioner.class.getName()).build();
      edgeProperty = edgeConf.createDefaultCustomEdgeProperty(edgeManagerDescriptor);
    } else {
      UnorderedKVEdgeConfig edgeConf =
        UnorderedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName()).build();
      edgeProperty = edgeConf.createDefaultCustomEdgeProperty(edgeManagerDescriptor);
    }
    dag.addEdge(Edge.create(v1, v3, edgeProperty)).addEdge(Edge.create(v2, v3, edgeProperty));

    return dag;
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: args: ["+PARTITIONED + "|" + UNPARTITIONED
      + " <input_dir1> <input_dir2> <output_dir>");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    return (otherArgs.length != 4 || (!otherArgs[0].equals(PARTITIONED)
      && !otherArgs[0].equals(UNPARTITIONED))) ? -1 : 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    DAG dag = createDAG(tezConf, args[1], args[2],
        args[3], args[0].equals(PARTITIONED));
    return runDag(dag, isCountersLog(), LOG);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CartesianProduct(), args);
    System.exit(res);
  }
}


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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * This DAG does cartesian product of two text inputs and then filters results according to the
 * third text input.
 *
 * V1    V2    V3
 *  \     |    /
 * CP\  CP|   / Broadcast
 *    \   |  /
 *    Vertex 4
 *
 * Vertex 1~3 are tokenizers and each of them tokenizes input from one directory. In partitioned
 * case, CustomPartitioner separates tokens into 2 partitions according to the parity of token's
 * first char. Vertex 4 does cartesian product of input from vertex1 and vertex2, and generates
 * KV pairs where keys are vertex 1 tokens and values are vertex 2 tokens. Then vertex 4 outputs KV
 * pairs whose keys appears in vertex 3 tokens.
 */
public class CartesianProduct extends TezExampleBase {
  private static final String INPUT = "Input1";
  private static final String OUTPUT = "Output";
  private static final String VERTEX1 = "Vertex1";
  private static final String VERTEX2 = "Vertex2";
  private static final String VERTEX3 = "Vertex3";
  private static final String VERTEX4 = "Vertex4";
  private static final String PARTITIONED = "-partitioned";
  private static final String UNPARTITIONED = "-unpartitioned";
  private static final Logger LOG = LoggerFactory.getLogger(CartesianProduct.class);
  private static final int numPartition = 2;
  private static final String[] cpSources = new String[] {VERTEX1, VERTEX2};

  public static class TokenProcessor extends SimpleProcessor {
    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(VERTEX4).getWriter();
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
      KeyValueReader kvReader3 = (KeyValueReader) getInputs().get(VERTEX3).getReader();
      Set<String> v2TokenSet = new HashSet<>();
      Set<String> v3TokenSet = new HashSet<>();

      while (kvReader2.next()) {
        v2TokenSet.add(kvReader2.getCurrentKey().toString());
      }
      while (kvReader3.next()) {
        v3TokenSet.add(kvReader3.getCurrentKey().toString());
      }

      while (kvReader1.next()) {
        String left = kvReader1.getCurrentKey().toString();
        if (v3TokenSet.contains(left)) {
          for (String right : v2TokenSet) {
            kvWriter.write(left, right);
          }
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
                        String inputPath3, String outputPath, boolean isPartitioned)
    throws IOException {
    Vertex v1 = Vertex.create(VERTEX1, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    // turn off groupSplit so that each input file incurs one task
    v1.addDataSource(INPUT,
      MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath1)
             .groupSplits(false).build());
    Vertex v2 = Vertex.create(VERTEX2, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    v2.addDataSource(INPUT,
      MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath2)
              .groupSplits(false).build());
    Vertex v3 = Vertex.create(VERTEX3, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    v3.addDataSource(INPUT,
      MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath3)
        .groupSplits(false).build());
    CartesianProductConfig cartesianProductConfig;
    if (isPartitioned) {
      Map<String, Integer> vertexPartitionMap = new HashMap<>();
      for (String vertex : cpSources) {
        vertexPartitionMap.put(vertex, numPartition);
      }
      cartesianProductConfig = new CartesianProductConfig(vertexPartitionMap);
    } else {
      cartesianProductConfig = new CartesianProductConfig(Arrays.asList(cpSources));
    }
    UserPayload userPayload = cartesianProductConfig.toUserPayload(tezConf);
    Vertex v4 = Vertex.create(VERTEX4, ProcessorDescriptor.create(JoinProcessor.class.getName()));
    v4.addDataSink(OUTPUT,
      MROutput.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, outputPath)
              .build());
    v4.setVertexManagerPlugin(
      VertexManagerPluginDescriptor.create(CartesianProductVertexManager.class.getName())
                                   .setUserPayload(userPayload));

    EdgeManagerPluginDescriptor cpEdgeManager =
      EdgeManagerPluginDescriptor.create(CartesianProductEdgeManager.class.getName());
    cpEdgeManager.setUserPayload(userPayload);
    EdgeProperty cpEdgeProperty;
    if (isPartitioned) {
      UnorderedPartitionedKVEdgeConfig cpEdgeConf =
        UnorderedPartitionedKVEdgeConfig.newBuilder(Text.class.getName(),
          IntWritable.class.getName(), CustomPartitioner.class.getName()).build();
      cpEdgeProperty = cpEdgeConf.createDefaultCustomEdgeProperty(cpEdgeManager);
    } else {
      UnorderedKVEdgeConfig edgeConf =
        UnorderedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName()).build();
      cpEdgeProperty = edgeConf.createDefaultCustomEdgeProperty(cpEdgeManager);
    }

    EdgeProperty broadcastEdgeProperty;
    UnorderedKVEdgeConfig broadcastEdgeConf =
      UnorderedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName()).build();
    broadcastEdgeProperty = broadcastEdgeConf.createDefaultBroadcastEdgeProperty();

    return DAG.create("CartesianProduct")
      .addVertex(v1).addVertex(v2).addVertex(v3).addVertex(v4)
      .addEdge(Edge.create(v1, v4, cpEdgeProperty))
      .addEdge(Edge.create(v2, v4, cpEdgeProperty))
      .addEdge(Edge.create(v3, v4, broadcastEdgeProperty));
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: args: ["+PARTITIONED + "|" + UNPARTITIONED
      + " <input_dir1> <input_dir2> <input_dir3> <output_dir>");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    return (otherArgs.length != 5 || (!otherArgs[0].equals(PARTITIONED)
      && !otherArgs[0].equals(UNPARTITIONED))) ? -1 : 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    DAG dag = createDAG(tezConf, args[1], args[2],
        args[3], args[4], args[0].equals(PARTITIONED));
    return runDag(dag, isCountersLog(), LOG);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CartesianProduct(), args);
    System.exit(res);
  }
}


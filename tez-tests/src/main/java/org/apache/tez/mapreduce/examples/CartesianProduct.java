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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.examples.TezExampleBase;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductConfig;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductEdgeManager;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.RoundRobinPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This job has three vertices: two Tokenizers and one JoinProcessor. Each Tokenizer handles one
 * input directory and generates tokens. CustomPartitioner separates tokens into 2 partitions
 * according to the parity of token's first char. Then JoinProcessor does cartesian product of
 * partitioned token sets.
 */
public class CartesianProduct extends TezExampleBase {
  private static final int srcParallelism = 1;
  private static final int numRecordPerSrc = 10;
  private static final String INPUT = "Input1";
  private static final String OUTPUT = "Output";
  private static final String VERTEX1 = "Vertex1";
  private static final String VERTEX2 = "Vertex2";
  private static final String VERTEX3 = "Vertex3";
  private static final Logger LOG = LoggerFactory.getLogger(CartesianProduct.class);
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
        Object key = kvReader.getCurrentKey();
        Object value = kvReader.getCurrentValue();
        kvWriter.write(new Text((String)key), new IntWritable(1));
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
      Set<Object> leftSet = new HashSet<>();
      Set<Object> rightSet = new HashSet<>();

      while (kvReader1.next()) {
        Text key = (Text)(kvReader1.getCurrentKey());
        leftSet.add(new Text(key));
      }
      while (kvReader2.next()) {
        Text key = (Text)(kvReader2.getCurrentKey());
        rightSet.add(new Text(key));
      }

      for (Object l : leftSet) {
        for (Object r : rightSet) {
          kvWriter.write(l, r);
        }
      }
    }
  }

  public static class FakeInputInitializer extends InputInitializer {

    /**
     * Constructor an instance of the InputInitializer. Classes extending this to create a
     * InputInitializer, must provide the same constructor so that Tez can create an instance of
     * the class at runtime.
     *
     * @param initializerContext initializer context which can be used to access the payload, vertex
     *                           properties, etc
     */
    public FakeInputInitializer(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      List<Event> list = new ArrayList<>();
      list.add(InputConfigureVertexTasksEvent.create(srcParallelism, null, null));
      for (int i = 0; i < srcParallelism; i++) {
        list.add(InputDataInformationEvent.createWithObjectPayload(i, null));
      }
      return list;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {

    }
  }

  public static class FakeInput extends AbstractLogicalInput {

    /**
     * Constructor an instance of the LogicalInput. Classes extending this one to create a
     * LogicalInput, must provide the same constructor so that Tez can create an instance of the
     * class at runtime.
     *
     * @param inputContext      the {@link InputContext} which provides
     *                          the Input with context information within the running task.
     * @param numPhysicalInputs the number of physical inputs that the logical input will
     */
    public FakeInput(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0, null);
      getContext().inputIsReady();
      return null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {

    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public Reader getReader() throws Exception {
      return new KeyValueReader() {
        String[] keys = new String[numRecordPerSrc];

        int i = -1;

        @Override
        public boolean next() throws IOException {
          if (i == -1) {
            for (int j = 0; j < numRecordPerSrc; j++) {
              keys[j] = ""+j;
            }
          }
          i++;
          return i < keys.length;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          return keys[i];
        }

        @Override
        public Object getCurrentValue() throws IOException {
          return keys[i];
        }
      };
    }
  }

  public static class FakeOutputCommitter extends OutputCommitter {

    /**
     * Constructor an instance of the OutputCommitter. Classes extending this to create a
     * OutputCommitter, must provide the same constructor so that Tez can create an instance of
     * the class at runtime.
     *
     * @param committerContext committer context which can be used to access the payload, vertex
     *                         properties, etc
     */
    public FakeOutputCommitter(OutputCommitterContext committerContext) {
      super(committerContext);
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void setupOutput() throws Exception {

    }

    @Override
    public void commitOutput() throws Exception {

    }

    @Override
    public void abortOutput(VertexStatus.State finalState) throws Exception {

    }
  }

  public static class FakeOutput extends AbstractLogicalOutput {

    /**
     * Constructor an instance of the LogicalOutput. Classes extending this one to create a
     * LogicalOutput, must provide the same constructor so that Tez can create an instance of the
     * class at runtime.
     *
     * @param outputContext      the {@link OutputContext} which
     *                           provides
     *                           the Output with context information within the running task.
     * @param numPhysicalOutputs the number of physical outputs that the logical output will
     */
    public FakeOutput(OutputContext outputContext, int numPhysicalOutputs) {
      super(outputContext, numPhysicalOutputs);
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0, null);
      return null;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {

    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public Writer getWriter() throws Exception {
      return new KeyValueWriter() {
        @Override
        public void write(Object key, Object value) throws IOException {
          System.out.println(key + " XXX " + value);
        }
      };
    }
  }

  private DAG createDAG(TezConfiguration tezConf) throws IOException {
    InputDescriptor inputDescriptor = InputDescriptor.create(FakeInput.class.getName());
    InputInitializerDescriptor inputInitializerDescriptor =
      InputInitializerDescriptor.create(FakeInputInitializer.class.getName());
    DataSourceDescriptor dataSourceDescriptor =
      DataSourceDescriptor.create(inputDescriptor, inputInitializerDescriptor, null);

    Vertex v1 = Vertex.create(VERTEX1, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    v1.addDataSource(INPUT, dataSourceDescriptor);
    Vertex v2 = Vertex.create(VERTEX2, ProcessorDescriptor.create(TokenProcessor.class.getName()));
    v2.addDataSource(INPUT, dataSourceDescriptor);

    OutputDescriptor outputDescriptor = OutputDescriptor.create(FakeOutput.class.getName());
    OutputCommitterDescriptor outputCommitterDescriptor =
      OutputCommitterDescriptor.create(FakeOutputCommitter.class.getName());
    DataSinkDescriptor dataSinkDescriptor =
      DataSinkDescriptor.create(outputDescriptor, outputCommitterDescriptor, null);

    CartesianProductConfig cartesianProductConfig =
      new CartesianProductConfig(Arrays.asList(sourceVertices));
    UserPayload userPayload = cartesianProductConfig.toUserPayload(tezConf);

    Vertex v3 = Vertex.create(VERTEX3, ProcessorDescriptor.create(JoinProcessor.class.getName()));
    v3.addDataSink(OUTPUT, dataSinkDescriptor);
    v3.setVertexManagerPlugin(
      VertexManagerPluginDescriptor.create(CartesianProductVertexManager.class.getName())
                                   .setUserPayload(userPayload));

    EdgeManagerPluginDescriptor edgeManagerDescriptor =
      EdgeManagerPluginDescriptor.create(CartesianProductEdgeManager.class.getName());
    edgeManagerDescriptor.setUserPayload(userPayload);
    UnorderedPartitionedKVEdgeConfig edgeConf =
      UnorderedPartitionedKVEdgeConfig.newBuilder(Text.class.getName(), IntWritable.class.getName(),
        RoundRobinPartitioner.class.getName()).build();
    EdgeProperty edgeProperty = edgeConf.createDefaultCustomEdgeProperty(edgeManagerDescriptor);

    return DAG.create("CrossProduct").addVertex(v1).addVertex(v2).addVertex(v3)
      .addEdge(Edge.create(v1, v3, edgeProperty)).addEdge(Edge.create(v2, v3, edgeProperty));
  }

  @Override
  protected void printUsage() {}

  @Override
  protected int validateArgs(String[] otherArgs) {
    return 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    DAG dag = createDAG(tezConf);
    return runDag(dag, isCountersLog(), LOG);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CartesianProduct(), args);
    System.exit(res);
  }
}



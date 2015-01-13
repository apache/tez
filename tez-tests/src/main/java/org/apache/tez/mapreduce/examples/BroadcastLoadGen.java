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
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.examples.TezExampleBase;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class BroadcastLoadGen extends TezExampleBase {

  private static final Log LOG = LogFactory.getLog(RPCLoadGen.class);

  public static class InputGenProcessor extends SimpleProcessor {

    final int bytesToGenerate;

    public InputGenProcessor(ProcessorContext context) {
      super(context);
      bytesToGenerate = context.getUserPayload().getPayload().getInt(0);
    }

    @Override
    public void run() throws Exception {
      Random random = new Random();
      Preconditions.checkArgument(getOutputs().size() == 1);
      LogicalOutput out = getOutputs().values().iterator().next();
      if (out instanceof UnorderedKVOutput) {
        UnorderedKVOutput output = (UnorderedKVOutput) out;
        KeyValueWriter kvWriter = output.getWriter();
        int approxNumInts = bytesToGenerate / 6;
        for (int i = 0 ; i < approxNumInts ; i++) {
          kvWriter.write(NullWritable.get(), new IntWritable(random.nextInt()));
        }
      }
    }
  }

  public static class InputFetchProcessor extends SimpleProcessor {
    public InputFetchProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(inputs.size() == 1);
      KeyValueReader broadcastKvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
      long sum = 0;
      int count = 0;
      while (broadcastKvReader.next()) {
        count++;
        sum += ((IntWritable) broadcastKvReader.getCurrentValue()).get();
      }
      System.err.println("Count = " + getContext().getTaskIndex() + " * " + count + ", Sum=" + sum);
    }
  }

  private DAG createDAG(int numGenTasks, int totalSourceDataSize, int numFetcherTasks) {
    int bytesPerSource = totalSourceDataSize / numGenTasks;
    LOG.info("DataPerSourceTask(bytes)=" + bytesPerSource);
    ByteBuffer payload = ByteBuffer.allocate(4);
    payload.putInt(0, bytesPerSource);

    Vertex broadcastVertex = Vertex.create("DataGen",
        ProcessorDescriptor.create(InputGenProcessor.class.getName())
            .setUserPayload(UserPayload.create(payload)), numGenTasks);
    Vertex fetchVertex = Vertex.create("FetchVertex",
        ProcessorDescriptor.create(InputFetchProcessor.class.getName()), numFetcherTasks);
    UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig.newBuilder(NullWritable.class
    .getName(), IntWritable.class.getName()).setCompression(false, null, null).build();

    DAG dag = DAG.create("BroadcastLoadGen");
    dag.addVertex(broadcastVertex).addVertex(fetchVertex).addEdge(
        Edge.create(broadcastVertex, fetchVertex, edgeConf.createDefaultBroadcastEdgeProperty()));
    return dag;
  }

  @Override
  protected final int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws
      TezException, InterruptedException, IOException {
    LOG.info("Running: " + this.getClass().getSimpleName() + StringUtils.join(args, " "));

    int numSourceTasks = Integer.parseInt(args[0]);
    int totalSourceData = Integer.parseInt(args[1]);
    int numFetcherTasks = Integer.parseInt(args[2]);
    LOG.info("Parameters: numSourceTasks=" + numSourceTasks + ", totalSourceDataSize(bytes)=" + totalSourceData +
        ", numFetcherTasks=" + numFetcherTasks);

    DAG dag = createDAG(numSourceTasks, totalSourceData, numFetcherTasks);
    return runDag(dag, false, LOG);
  }

  @Override
  protected void printUsage() {
    System.err.println(
        "Usage: " + "BroadcastLoadGen <num_source_tasks>  <total_source_data> <num_destination_tasks>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  protected final int validateArgs(String[] otherArgs) {
    return otherArgs.length != 3 ? 2 : 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BroadcastLoadGen(), args);
    System.exit(res);
  }
}
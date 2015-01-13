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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.examples.HashJoinExample.ForwardingProcessor;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class JoinValidate extends TezExampleBase {
  private static final Log LOG = LogFactory.getLog(JoinValidate.class);

  private static final String LHS_INPUT_NAME = "lhsfile";
  private static final String RHS_INPUT_NAME = "rhsfile";

  private static final String COUNTER_GROUP_NAME = "JOIN_VALIDATE";
  private static final String MISSING_KEY_COUNTER_NAME = "MISSING_KEY_EXISTS";

  public static void main(String[] args) throws Exception {
    JoinValidate validate = new JoinValidate();
    int status = ToolRunner.run(new Configuration(), validate, args);
    System.exit(status);
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: " + "joinvalidate <path1> <path2>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {

    LOG.info("Running JoinValidate");

    String lhsDir = args[0];
    String rhsDir = args[1];
    int numPartitions = 1;
    if (args.length == 3) {
      numPartitions = Integer.parseInt(args[2]);
    }

    if (numPartitions <= 0) {
      System.err.println("NumPartitions must be > 0");
      return 4;
    }

    Path lhsPath = new Path(lhsDir);
    Path rhsPath = new Path(rhsDir);

    DAG dag = createDag(tezConf, lhsPath, rhsPath, numPartitions);

    tezClient.waitTillReady();
    DAGClient dagClient = tezClient.submitDAG(dag);
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    } else {
      dagStatus = dagClient.getDAGStatus(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
      TezCounter counter = dagStatus.getDAGCounters().findCounter(COUNTER_GROUP_NAME,
          MISSING_KEY_COUNTER_NAME);
      if (counter == null) {
        LOG.info("Unable to determing equality");
        return -2;
      } else {
        if (counter.getValue() != 0) {
          LOG.info("Validate failed. The two sides are not equivalent");
          return -3;
        } else {
          LOG.info("Validation successful. The two sides are equivalent");
          return 0;
        }
      }
    }
  
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length != 3 && otherArgs.length != 2) {
      return 2;
    }
    return 0;
  }

  private DAG createDag(TezConfiguration tezConf, Path lhs, Path rhs, int numPartitions)
      throws IOException {
    DAG dag = DAG.create("JoinValidate");

    // Configuration for intermediate output - shared by Vertex1 and Vertex2
    // This should only be setting selective keys from the underlying conf. Fix after there's a
    // better mechanism to configure the IOs. The setFromConfiguration call is optional and allows
    // overriding the config options with command line parameters.
    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), NullWritable.class.getName(),
            HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build();

    Vertex lhsVertex = Vertex.create(LHS_INPUT_NAME, ProcessorDescriptor.create(
        ForwardingProcessor.class.getName())).addDataSource("lhs",
        MRInput
            .createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
                lhs.toUri().toString()).groupSplits(false).build());

    Vertex rhsVertex = Vertex.create(RHS_INPUT_NAME, ProcessorDescriptor.create(
        ForwardingProcessor.class.getName())).addDataSource("rhs",
        MRInput
            .createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
                rhs.toUri().toString()).groupSplits(false).build());

    Vertex joinValidateVertex = Vertex.create("joinvalidate", ProcessorDescriptor.create(
        JoinValidateProcessor.class.getName()), numPartitions);

    Edge e1 = Edge.create(lhsVertex, joinValidateVertex, edgeConf.createDefaultEdgeProperty());
    Edge e2 = Edge.create(rhsVertex, joinValidateVertex, edgeConf.createDefaultEdgeProperty());

    dag.addVertex(lhsVertex).addVertex(rhsVertex).addVertex(joinValidateVertex).addEdge(e1)
        .addEdge(e2);
    return dag;
  }

  public static class JoinValidateProcessor extends SimpleProcessor {

    private static final Log LOG = LogFactory.getLog(JoinValidateProcessor.class);

    public JoinValidateProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkState(getInputs().size() == 2);
      Preconditions.checkState(getOutputs().size() == 0);
      LogicalInput lhsInput = getInputs().get(LHS_INPUT_NAME);
      LogicalInput rhsInput = getInputs().get(RHS_INPUT_NAME);
      Reader lhsReaderRaw = lhsInput.getReader();
      Reader rhsReaderRaw = rhsInput.getReader();
      Preconditions.checkState(lhsReaderRaw instanceof KeyValuesReader);
      Preconditions.checkState(rhsReaderRaw instanceof KeyValuesReader);
      KeyValuesReader lhsReader = (KeyValuesReader) lhsReaderRaw;
      KeyValuesReader rhsReader = (KeyValuesReader) rhsReaderRaw;

      TezCounter lhsMissingKeyCounter = getContext().getCounters().findCounter(COUNTER_GROUP_NAME,
          MISSING_KEY_COUNTER_NAME);

      while (lhsReader.next()) {
        if (rhsReader.next()) {
          if (!lhsReader.getCurrentKey().equals(rhsReader.getCurrentKey())) {
            LOG.info("MismatchedKeys: " + "lhs=" + lhsReader.getCurrentKey() + ", rhs=" + rhsReader.getCurrentKey());
            lhsMissingKeyCounter.increment(1);
          }
        } else {
          lhsMissingKeyCounter.increment(1);
          LOG.info("ExtraKey in lhs: " + lhsReader.getClass());
          break;
        }
      }
      if (rhsReader.next()) {
        lhsMissingKeyCounter.increment(1);
        LOG.info("ExtraKey in rhs: " + lhsReader.getClass());
      }
    }
  }
}

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
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.IntersectExample.ForwardingProcessor;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class IntersectValidate extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(IntersectExample.class);

  private static final String LHS_INPUT_NAME = "lhsfile";
  private static final String RHS_INPUT_NAME = "rhsfile";

  private static final String COUNTER_GROUP_NAME = "INTERSECT_VALIDATE";
  private static final String MISSING_KEY_COUNTER_NAME = "MISSING_KEY_EXISTS";

  public static void main(String[] args) throws Exception {
    IntersectValidate validate = new IntersectValidate();
    int status = ToolRunner.run(new Configuration(), validate, args);
    System.exit(status);
  }

  private static void printUsage() {
    System.err.println("Usage: " + "intersectvalidate <path1> <path2>");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 3 && otherArgs.length != 2) {
      printUsage();
      return 2;
    }
    return execute(otherArgs);
  }

  private int execute(String[] args) throws IOException, TezException, InterruptedException {
    LOG.info("Running IntersectValidate");
    TezConfiguration tezConf = new TezConfiguration(getConf());
    UserGroupInformation.setConfiguration(tezConf);

    String lhsDir = args[0];
    String rhsDir = args[1];
    int numPartitions = 1;
    if (args.length == 3) {
      numPartitions = Integer.parseInt(args[2]);
    }

    if (numPartitions <= 0) {
      System.err.println("NumPartitions must be > 0");
      return 2;
    }

    Path lhsPath = new Path(lhsDir);
    Path rhsPath = new Path(rhsDir);

    AMConfiguration amConfiguration = new AMConfiguration(null, null, tezConf, null);
    TezSessionConfiguration sessionConfiguration = new TezSessionConfiguration(amConfiguration,
        tezConf);
    TezSession tezSession = new TezSession("IntersectExampleSession", sessionConfiguration);
    try {
      tezSession.start();

      DAG dag = createDag(tezConf, lhsPath, rhsPath, numPartitions);
      setupURIsForCredentials(dag, lhsPath, rhsPath);

      tezSession.waitTillReady();
      DAGClient dagClient = tezSession.submitDAG(dag);
      DAGStatus dagStatus = dagClient.waitForCompletionWithAllStatusUpdates(null);
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        LOG.info("DAG diagnostics: " + dagStatus.getDiagnostics());
        return -1;
      } else {
        dagStatus = dagClient.getDAGStatus(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
        TezCounter counter = dagStatus.getDAGCounters().findCounter(COUNTER_GROUP_NAME,
            MISSING_KEY_COUNTER_NAME);
        if (counter == null) {
          LOG.info("Unable to determing equality");
          return -1;
        } else {
          if (counter.getValue() != 0) {
            LOG.info("Validate failed. The two sides are not equivalent");
            return -1;
          } else {
            LOG.info("Vlidation successful. The two sides are equivalent");
            return 0;
          }
        }
      }
    } finally {
      tezSession.stop();
    }
  }

  private DAG createDag(TezConfiguration tezConf, Path lhs, Path rhs, int numPartitions)
      throws IOException {
    DAG dag = new DAG("IntersectValidate");

    // Configuration for src1
    Configuration lhsInputConf = new Configuration(tezConf);
    lhsInputConf.set(FileInputFormat.INPUT_DIR, lhs.toUri().toString());
    byte[] streamInputPayload = MRInput.createUserPayload(lhsInputConf,
        TextInputFormat.class.getName(), true, false);

    // Configuration for src2
    Configuration rhsInputConf = new Configuration(tezConf);
    rhsInputConf.set(FileInputFormat.INPUT_DIR, rhs.toUri().toString());
    byte[] hashInputPayload = MRInput.createUserPayload(rhsInputConf,
        TextInputFormat.class.getName(), true, false);

    // Configuration for intermediate output - shared by Vertex1 and Vertex2
    // This should only be setting selective keys from the underlying conf. Fix after there's a
    // better mechanism to configure the IOs.
    Configuration intermediateOutputConf = new Configuration(tezConf);
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS,
        Text.class.getName());
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS,
        NullWritable.class.getName());
    intermediateOutputConf.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS,
        HashPartitioner.class.getName());
    byte[] intermediateOutputPayload = TezUtils.createUserPayloadFromConf(intermediateOutputConf);

    Configuration intermediateInputConf = new Configuration(tezConf);
    intermediateInputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS,
        Text.class.getName());
    intermediateInputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS,
        NullWritable.class.getName());
    byte[] intermediateInputPayload = TezUtils.createUserPayloadFromConf(intermediateInputConf);

    // Change the way resources are setup - no MRHelpers
    Vertex lhsVertex = new Vertex(LHS_INPUT_NAME, new ProcessorDescriptor(
        ForwardingProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf)).setJavaOpts(
        MRHelpers.getMapJavaOpts(tezConf)).addInput("lhs",
        new InputDescriptor(MRInput.class.getName()).setUserPayload(streamInputPayload),
        MRInputAMSplitGenerator.class);

    Vertex rhsVertex = new Vertex(RHS_INPUT_NAME, new ProcessorDescriptor(
        ForwardingProcessor.class.getName()), -1, MRHelpers.getMapResource(tezConf)).setJavaOpts(
        MRHelpers.getMapJavaOpts(tezConf)).addInput("rhs",
        new InputDescriptor(MRInput.class.getName()).setUserPayload(hashInputPayload),
        MRInputAMSplitGenerator.class);

    Vertex intersectValidateVertex = new Vertex("intersectvalidate", new ProcessorDescriptor(
        IntersectValidateProcessor.class.getName()), numPartitions,
        MRHelpers.getReduceResource(tezConf)).setJavaOpts(MRHelpers.getReduceJavaOpts(tezConf));

    Edge e1 = new Edge(lhsVertex, intersectValidateVertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        new OutputDescriptor(OnFileSortedOutput.class.getName())
            .setUserPayload(intermediateOutputPayload), new InputDescriptor(
            ShuffledMergedInput.class.getName()).setUserPayload(intermediateInputPayload)));

    Edge e2 = new Edge(rhsVertex, intersectValidateVertex, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
        new OutputDescriptor(OnFileSortedOutput.class.getName())
            .setUserPayload(intermediateOutputPayload), new InputDescriptor(
            ShuffledMergedInput.class.getName()).setUserPayload(intermediateInputPayload)));

    dag.addVertex(lhsVertex).addVertex(rhsVertex).addVertex(intersectValidateVertex).addEdge(e1)
        .addEdge(e2);
    return dag;
  }

  public static class IntersectValidateProcessor extends SimpleProcessor {

    private static final Log LOG = LogFactory.getLog(IntersectValidateProcessor.class);
    
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

  private void setupURIsForCredentials(DAG dag, Path... paths) throws IOException {
    List<URI> uris = new LinkedList<URI>();
    for (Path path : paths) {
      FileSystem fs = path.getFileSystem(getConf());
      Path qPath = fs.makeQualified(path);
      uris.add(qPath.toUri());
    }
    dag.addURIsForCredentials(uris);
  }
}

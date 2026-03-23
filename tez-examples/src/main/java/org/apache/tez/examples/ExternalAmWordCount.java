/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.examples;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Program inspired for WordCount but to run with External Tez AM with Zookeeper.
 */
public class ExternalAmWordCount {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAmWordCount.class);
  private static final String ZK_ADDRESS = "zookeeper:2181";

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println(
          "Usage: java -cp <classpath> com.github.raghav.ExternalAmWordCount <in_path> <out_path>");
      System.exit(2);
    }

    var inputPath = args[0];
    var outputPath = args[1];

    var conf = new Configuration();
    var tezConf = new TezConfiguration(conf);

    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, "STANDALONE_ZOOKEEPER");
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, ZK_ADDRESS);
    tezConf.set(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT, "30000ms");
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, false);
    tezConf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);

    // Prevent Tez from using the current directory for staging (avoids deleting your custom jar)
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, "/tmp/tez-staging");

    LOG.info("Starting Tez Client with ZK Address: {}", ZK_ADDRESS);

    var tezClient = TezClient.create("ExternalAmWordCount", tezConf, true);

    try {
      tezClient.start();
      LOG.info("Waiting for Tez AM to register");
      tezClient.waitTillReady();
      LOG.info("Tez AM discovered! Submitting DAG...");

      var app = new ExternalAmWordCount();
      var dag = app.createDAG(tezConf, inputPath, outputPath);
      var dagClient = tezClient.submitDAG(dag);

      var dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);

      if (dagStatus.getState() == State.SUCCEEDED) {
        LOG.info("DAG Succeeded");
        System.exit(0);
      } else {
        LOG.error("DAG Failed with state: {}", dagStatus.getState());
        System.exit(1);
      }

    } finally {
      tezClient.stop();
    }
  }

  private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath) {
    var dataSource =
        MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath)
            .build();

    var dataSink =
        MROutput.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, outputPath)
            .build();

    var tokenizerVertex =
        Vertex.create("Tokenizer", ProcessorDescriptor.create(TokenProcessor.class.getName()))
            .addDataSource("Input", dataSource);

    var summerVertex =
        Vertex.create(
                "Summer", ProcessorDescriptor.create(SumProcessor.class.getName()), 1) // 1 Reducer
            .addDataSink("Output", dataSink);

    var edgeConf =
        OrderedPartitionedKVEdgeConfig.newBuilder(
                Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
            .setFromConfiguration(tezConf)
            .build();

    return DAG.create("ZkWordCountDAG")
        .addVertex(tokenizerVertex)
        .addVertex(summerVertex)
        .addEdge(Edge.create(tokenizerVertex, summerVertex, edgeConf.createDefaultEdgeProperty()));
  }

  public static class TokenProcessor extends SimpleMRProcessor {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      // Get inputs/outputs
      var inputs = getInputs();
      var outputs = getOutputs();

      var reader = (org.apache.tez.mapreduce.lib.MRReader) inputs.get("Input").getReader();
      var writer = (KeyValueWriter) outputs.get("Summer").getWriter();

      while (reader.next()) {
        var val = reader.getCurrentValue();
        var line = val.toString();
        var tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          writer.write(word, ONE);
        }
      }
    }
  }

  public static class SumProcessor extends SimpleMRProcessor {
    public SumProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      var inputs = getInputs();
      var outputs = getOutputs();

      var reader = (KeyValuesReader) inputs.get("Tokenizer").getReader();
      var writer = (KeyValueWriter) outputs.get("Output").getWriter();

      while (reader.next()) {
        var key = reader.getCurrentKey();
        var values = reader.getCurrentValues();

        int sum = 0;
        for (var val : values) {
          sum += ((IntWritable) val).get();
        }
        writer.write(key, new IntWritable(sum));
      }
    }
  }
}

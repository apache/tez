/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.examples;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.zookeeper.KeeperException.NoNodeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sample Program inspired for WordCount but to run with External Tez AM with Zookeeper. */
public class ExternalAmWordCount {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAmWordCount.class);
  private static final String ZK_ADDRESS = "zookeeper:2181";
  private static final String ZK_PATH = "/tez-external-sessions/tez_am/server";

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println(
          "Usage: java -cp <classpath> org.apache.tez.examples.ExternalAmWordCount <in_path> <out_path>");
      System.exit(2);
    }

    String inputPath = args[0];
    String outputPath = args[1];

    TezConfiguration tezConf = getTezConf();

    LOG.info(
        "Initializing TezClient to connect to External AM via ZooKeeper quorum at {}", ZK_ADDRESS);

    final TezClient tezClient =
        TezClient.newBuilder("ExternalAmWordCount", tezConf).setIsSession(true).build();

    try {
      LOG.info("Querying ZooKeeper quorum to discover an active Tez AM session");
      String externalAppIdString = fetchAppIdFromZookeeper();

      if (externalAppIdString == null || externalAppIdString.isEmpty()) {
        throw new IllegalStateException(
            "No active Tez Application Master found at path " + ZK_PATH);
      }
      LOG.info(
          "ZooKeeper discovery successful. Extracted Tez Application ID: {}", externalAppIdString);

      ApplicationId appId = ApplicationId.fromString(externalAppIdString);
      tezClient.getClient(appId);
      LOG.info("TezClient is ready for DAG submission.");
    } catch (Exception e) {
      LOG.error("Failure while connecting to the external Tez session.", e);
      throw e;
    }

    ExternalAmWordCount app = new ExternalAmWordCount();
    DAG dag = app.createDAG(tezConf, inputPath, outputPath);

    LOG.info("Submitting DAG to the external Tez AM...");
    DAGClient dagClient = tezClient.submitDAG(dag);

    LOG.info("DAG successfully submitted.");
    DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);

    if (dagStatus.getState() == State.SUCCEEDED) {
      LOG.info("Job completed successfully! WordCount output written to: {}", outputPath);
      System.exit(0);
    } else {
      LOG.error("Job execution failed. Final DAG state: {}.", dagStatus.getState());
      System.exit(1);
    }
  }

  private static TezConfiguration getTezConf() {
    Configuration conf = new Configuration();
    TezConfiguration tezConf = new TezConfiguration(conf);

    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, "STANDALONE_ZOOKEEPER");
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, ZK_ADDRESS);
    tezConf.set(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT, "30000ms");
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, false);
    tezConf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, "/tmp/tez-staging");
    return tezConf;
  }

  /** Discovers the external Tez AM ApplicationId by connecting to the ZooKeeper quorum. */
  public static String fetchAppIdFromZookeeper() throws Exception {
    // Retry up to 3 times, 1s, 2s, 4s (using local variable type inference)
    var retryPolicy = new ExponentialBackoffRetry(1000, 3);

    try (CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, retryPolicy)) {
      client.start();

      LOG.info(
          "Attempting to establish CuratorFramework connection to ZooKeeper at {}...", ZK_ADDRESS);
      // Wait up to 5 seconds to establish a connection to ZooKeeper
      if (!client.blockUntilConnected(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
            "CuratorFramework timeout: Could not connect to ZooKeeper at " + ZK_ADDRESS);
      }
      LOG.info("CuratorFramework connection established. Querying path: {}", ZK_PATH);

      try {
        List<String> children = client.getChildren().forPath(ZK_PATH);
        if (children != null && !children.isEmpty()) {
          String appId = children.getFirst();
          LOG.info("Successfully found AppID {} under ZNode {}", appId, ZK_PATH);
          return appId;
        } else {
          LOG.warn("No application IDs found under ZNode {}", ZK_PATH);
        }
      } catch (NoNodeException e) {
        LOG.warn("ZooKeeper parent path {} does not exist.", ZK_PATH);
      }
    }
    return null;
  }

  /** Constructs the DAG. */
  private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath) {
    DataSourceDescriptor dataSource =
        MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, inputPath)
            .build();

    DataSinkDescriptor dataSink =
        MROutput.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, outputPath)
            .build();

    Vertex tokenizerVertex =
        Vertex.create("Tokenizer", ProcessorDescriptor.create(TokenProcessor.class.getName()))
            .addDataSource("Input", dataSource);

    Vertex summerVertex =
        Vertex.create(
                "Summer", ProcessorDescriptor.create(SumProcessor.class.getName()), 1) // 1 Reducer
            .addDataSink("Output", dataSink);

    OrderedPartitionedKVEdgeConfig edgeConf =
        OrderedPartitionedKVEdgeConfig.newBuilder(
                Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
            .setFromConfiguration(tezConf)
            .build();

    return DAG.create("ZkWordCountDAG")
        .addVertex(tokenizerVertex)
        .addVertex(summerVertex)
        .addEdge(Edge.create(tokenizerVertex, summerVertex, edgeConf.createDefaultEdgeProperty()));
  }

  /** Map-equivalent: reads lines of text, tokenizes them, and emits key-value like (word, 1). */
  public static class TokenProcessor extends SimpleMRProcessor {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      // Get inputs/outputs
      Map<String, LogicalInput> inputs = getInputs();
      Map<String, LogicalOutput> outputs = getOutputs();

      MRReader reader = (MRReader) inputs.get("Input").getReader();
      KeyValueWriter writer = (KeyValueWriter) outputs.get("Summer").getWriter();

      while (reader.next()) {
        Object val = reader.getCurrentValue();
        String line = val.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          writer.write(word, ONE);
        }
      }
    }
  }

  /** Reduce-equivalent: reads (word, [1, 1, ...]) and aggregates the total sum. */
  public static class SumProcessor extends SimpleMRProcessor {
    public SumProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Map<String, LogicalInput> inputs = getInputs();
      Map<String, LogicalOutput> outputs = getOutputs();

      KeyValuesReader reader = (KeyValuesReader) inputs.get("Tokenizer").getReader();
      KeyValueWriter writer = (KeyValueWriter) outputs.get("Output").getWriter();

      while (reader.next()) {
        Object key = reader.getCurrentKey();
        Iterable<Object> values = reader.getCurrentValues();

        int sum = 0;
        for (Object val : values) {
          sum += ((IntWritable) val).get();
        }
        writer.write(key, new IntWritable(sum));
      }
    }
  }
}

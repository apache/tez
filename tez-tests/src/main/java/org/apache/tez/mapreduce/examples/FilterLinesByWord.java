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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.examples.processor.FilterByWordInputProcessor;
import org.apache.tez.mapreduce.examples.processor.FilterByWordOutputProcessor;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

import com.google.common.collect.Sets;

public class FilterLinesByWord extends Configured implements Tool {

  private static Log LOG = LogFactory.getLog(FilterLinesByWord.class);

  public static final String FILTER_PARAM_NAME = "tez.runtime.examples.filterbyword.word";
  
  private boolean exitOnCompletion = false;

  public FilterLinesByWord(boolean exitOnCompletion) {
    this.exitOnCompletion = exitOnCompletion;
  }
  
  private static void printUsage() {
    System.err.println("Usage filtelinesrbyword <in> <out> <filter_word> [-generateSplitsInClient true/<false>]");
    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Credentials credentials = new Credentials();

    boolean generateSplitsInClient = false;

    SplitsInClientOptionParser splitCmdLineParser = new SplitsInClientOptionParser();
    try {
      generateSplitsInClient = splitCmdLineParser.parse(otherArgs, false);
      otherArgs = splitCmdLineParser.getRemainingArgs();
    } catch (ParseException e1) {
      System.err.println("Invalid options");
      printUsage();
      return 2;
    }

    if (otherArgs.length != 3) {
      printUsage();
      return 2;
    }

    String inputPath = otherArgs[0];
    String outputPath = otherArgs[1];
    String filterWord = otherArgs[2];

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputPath))) {
      System.err.println("Output directory : " + outputPath + " already exists");
      return 2;
    }

    TezConfiguration tezConf = new TezConfiguration(conf);

    fs.getWorkingDirectory();
    Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    String jarPath = ClassUtil.findContainingJar(FilterLinesByWord.class);
    if (jarPath == null) {
      throw new TezUncheckedException("Could not find any jar containing"
          + FilterLinesByWord.class.getName() + " in the classpath");
    }

    Path remoteJarPath = fs.makeQualified(new Path(stagingDir, "dag_job.jar"));
    fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
    FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);
    TokenCache.obtainTokensForNamenodes(credentials, new Path[]{remoteJarPath}, conf);

    Map<String, LocalResource> commonLocalResources = new TreeMap<String, LocalResource>();
    LocalResource dagJarLocalRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteJarPath),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
        remoteJarStatus.getLen(), remoteJarStatus.getModificationTime());
    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);



    TezClient tezSession = TezClient.create("FilterLinesByWordSession", tezConf,
        commonLocalResources, credentials);
    tezSession.start(); // Why do I need to start the TezSession.

    Configuration stage1Conf = new JobConf(conf);
    stage1Conf.set(FILTER_PARAM_NAME, filterWord);

    Configuration stage2Conf = new JobConf(conf);
    stage2Conf.set(FileOutputFormat.OUTDIR, outputPath);
    stage2Conf.setBoolean("mapred.mapper.new-api", false);

    UserPayload stage1Payload = TezUtils.createUserPayloadFromConf(stage1Conf);
    // Setup stage1 Vertex
    Vertex stage1Vertex = Vertex.create("stage1", ProcessorDescriptor.create(
        FilterByWordInputProcessor.class.getName()).setUserPayload(stage1Payload))
        .addTaskLocalFiles(commonLocalResources);

    DataSourceDescriptor dsd;
    if (generateSplitsInClient) {
      // TODO TEZ-1406. Dont' use MRInputLegacy
      stage1Conf.set(FileInputFormat.INPUT_DIR, inputPath);
      stage1Conf.setBoolean("mapred.mapper.new-api", false);
      dsd = MRInputHelpers.configureMRInputWithLegacySplitGeneration(stage1Conf, stagingDir, true);
    } else {
      dsd = MRInputLegacy.createConfigBuilder(stage1Conf, TextInputFormat.class, inputPath)
          .groupSplits(false).build();
    }
    stage1Vertex.addDataSource("MRInput", dsd);

    // Setup stage2 Vertex
    Vertex stage2Vertex = Vertex.create("stage2", ProcessorDescriptor.create(
        FilterByWordOutputProcessor.class.getName()).setUserPayload(
        TezUtils.createUserPayloadFromConf(stage2Conf)), 1);
    stage2Vertex.addTaskLocalFiles(commonLocalResources);

    // Configure the Output for stage2
    OutputDescriptor od = OutputDescriptor.create(MROutput.class.getName())
        .setUserPayload(TezUtils.createUserPayloadFromConf(stage2Conf));
    OutputCommitterDescriptor ocd =
        OutputCommitterDescriptor.create(MROutputCommitter.class.getName());
    stage2Vertex.addDataSink("MROutput", DataSinkDescriptor.create(od, ocd, null));

    UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
        .newBuilder(Text.class.getName(), TextLongPair.class.getName()).build();

    DAG dag = DAG.create("FilterLinesByWord");
    Edge edge =
        Edge.create(stage1Vertex, stage2Vertex, edgeConf.createDefaultBroadcastEdgeProperty());
    dag.addVertex(stage1Vertex).addVertex(stage2Vertex).addEdge(edge);

    LOG.info("Submitting DAG to Tez Session");
    DAGClient dagClient = tezSession.submitDAG(dag);
    LOG.info("Submitted DAG to Tez Session");

    DAGStatus dagStatus = null;
    String[] vNames = { "stage1", "stage2" };
    try {
      while (true) {
        dagStatus = dagClient.getDAGStatus(null);
        if(dagStatus.getState() == DAGStatus.State.RUNNING ||
            dagStatus.getState() == DAGStatus.State.SUCCEEDED ||
            dagStatus.getState() == DAGStatus.State.FAILED ||
            dagStatus.getState() == DAGStatus.State.KILLED ||
            dagStatus.getState() == DAGStatus.State.ERROR) {
          break;
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          // continue;
        }
      }

      while (dagStatus.getState() == DAGStatus.State.RUNNING) {
        try {
          ExampleDriver.printDAGStatus(dagClient, vNames);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // continue;
          }
          dagStatus = dagClient.getDAGStatus(null);
        } catch (TezException e) {
          LOG.fatal("Failed to get application progress. Exiting");
          return -1;
        }
      }
      
      dagStatus = dagClient.getDAGStatus(Sets.newHashSet(StatusGetOpts.GET_COUNTERS));
      
    } finally {
      fs.delete(stagingDir, true);
      tezSession.stop();
    }

    ExampleDriver.printDAGStatus(dagClient, vNames, true, true);
    LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
    return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    FilterLinesByWord fl = new FilterLinesByWord(true);
    int status = ToolRunner.run(new Configuration(), fl, args);
    if (fl.exitOnCompletion) {
      System.exit(status);
    }
  }

  public static class TextLongPair implements Writable {

    private Text text;
    private LongWritable longWritable;

    public TextLongPair() {
    }

    public TextLongPair(Text text, LongWritable longWritable) {
      this.text = text;
      this.longWritable = longWritable;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.text.write(out);
      this.longWritable.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.text = new Text();
      this.longWritable = new LongWritable();
      text.readFields(in);
      longWritable.readFields(in);
    }

    @Override
    public String toString() {
      return text.toString() + "\t" + longWritable.get();
    }
  }
}

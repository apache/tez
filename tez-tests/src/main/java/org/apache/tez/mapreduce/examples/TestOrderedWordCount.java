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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * An MRR job built on top of word count to return words sorted by
 * their frequency of occurrence.
 *
 * Use -DUSE_TEZ_SESSION=true to run jobs in a session mode.
 * If multiple input/outputs are provided, this job will process each pair
 * as a separate DAG in a sequential manner.
 * Use -DINTER_JOB_SLEEP_INTERVAL=<N> where N is the sleep interval in seconds
 * between the sequential DAGs.
 */
public class TestOrderedWordCount extends Configured implements Tool {

  private static Logger LOG = LoggerFactory.getLogger(TestOrderedWordCount.class);

  private static final String DAG_VIEW_ACLS = "tez.testorderedwordcount.view-acls";
  private static final String DAG_MODIFY_ACLS = "tez.testorderedwordcount.modify-acls";


  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,IntWritable, Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(result, key);
    }
  }

  /**
   * Shuffle ensures ordering based on count of employees per department
   * hence the final reducer is a no-op and just emits the department name
   * with the employee count per department.
   */
  public static class MyOrderByNoOpReducer
      extends Reducer<IntWritable, Text, Text, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
      for (Text word : values) {
        context.write(word, key);
      }
    }
  }

  private Credentials credentials = new Credentials();

  @VisibleForTesting
  public DAG createDAG(FileSystem fs, Configuration conf,
      Map<String, LocalResource> commonLocalResources, Path stagingDir,
      int dagIndex, String inputPath, String outputPath,
      boolean generateSplitsInClient,
      boolean useMRSettings,
      int intermediateNumReduceTasks) throws Exception {

    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
        TokenizerMapper.class.getName());
    MRHelpers.translateMRConfToTez(mapStageConf, !useMRSettings);

    Configuration iReduceStageConf = new JobConf(conf);
    // TODO replace with auto-reduce parallelism
    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2);
    iReduceStageConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        IntSumReducer.class.getName());
    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
        IntWritable.class.getName());
    iReduceStageConf.setBoolean("mapred.mapper.new-api", true);
    MRHelpers.translateMRConfToTez(iReduceStageConf, !useMRSettings);

    Configuration finalReduceConf = new JobConf(conf);
    finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, 1);
    finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        MyOrderByNoOpReducer.class.getName());
    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    MRHelpers.translateMRConfToTez(finalReduceConf, !useMRSettings);

    MRHelpers.configureMRApiUsage(mapStageConf);
    MRHelpers.configureMRApiUsage(iReduceStageConf);
    MRHelpers.configureMRApiUsage(finalReduceConf);

    List<Vertex> vertices = new ArrayList<Vertex>();

    String mapStageHistoryText = TezUtils.convertToHistoryText("Initial Tokenizer Vertex",
        mapStageConf);
    DataSourceDescriptor dsd;
    if (generateSplitsInClient) {
      mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
          TextInputFormat.class.getName());
      mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
      mapStageConf.setBoolean("mapred.mapper.new-api", true);
      dsd = MRInputHelpers.configureMRInputWithLegacySplitGeneration(mapStageConf, stagingDir,
          true);
    } else {
      dsd = MRInputLegacy.createConfigBuilder(mapStageConf, TextInputFormat.class,
          inputPath).build();
    }
    dsd.getInputDescriptor().setHistoryText(TezUtils.convertToHistoryText(
        "HDFS Input " + inputPath, mapStageConf));

    Map<String, String> mapEnv = Maps.newHashMap();
    MRHelpers.updateEnvBasedOnMRTaskEnv(mapStageConf, mapEnv, true);
    Map<String, String> reduceEnv = Maps.newHashMap();
    MRHelpers.updateEnvBasedOnMRTaskEnv(mapStageConf, reduceEnv, false);

    Vertex mapVertex;
    ProcessorDescriptor mapProcessorDescriptor =
        ProcessorDescriptor.create(MapProcessor.class.getName())
            .setUserPayload(
                TezUtils.createUserPayloadFromConf(mapStageConf))
            .setHistoryText(mapStageHistoryText);
    if (!useMRSettings) {
      mapVertex = Vertex.create("initialmap", mapProcessorDescriptor);
    } else {
      mapVertex = Vertex.create("initialmap", mapProcessorDescriptor, -1,
          MRHelpers.getResourceForMRMapper(mapStageConf));
      mapVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRMapper(mapStageConf));
      mapVertex.setTaskEnvironment(mapEnv);
    }
    mapVertex.addTaskLocalFiles(commonLocalResources)
        .addDataSource("MRInput", dsd);
    vertices.add(mapVertex);

    String iReduceStageHistoryText = TezUtils.convertToHistoryText("Intermediate Summation Vertex",
        iReduceStageConf);
    ProcessorDescriptor iReduceProcessorDescriptor = ProcessorDescriptor.create(
        ReduceProcessor.class.getName())
        .setUserPayload(TezUtils.createUserPayloadFromConf(iReduceStageConf))
        .setHistoryText(iReduceStageHistoryText);

    Vertex intermediateVertex;
    if (!useMRSettings) {
      intermediateVertex = Vertex.create("intermediate_reducer", iReduceProcessorDescriptor,
          intermediateNumReduceTasks);
    } else {
      intermediateVertex = Vertex.create("intermediate_reducer", iReduceProcessorDescriptor,
          intermediateNumReduceTasks, MRHelpers.getResourceForMRReducer(iReduceStageConf));
      intermediateVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(iReduceStageConf));
      intermediateVertex.setTaskEnvironment(reduceEnv);
    }
    intermediateVertex.addTaskLocalFiles(commonLocalResources);
    vertices.add(intermediateVertex);

    String finalReduceStageHistoryText = TezUtils.convertToHistoryText("Final Sorter Vertex",
        finalReduceConf);
    UserPayload finalReducePayload = TezUtils.createUserPayloadFromConf(finalReduceConf);
    Vertex finalReduceVertex;

    ProcessorDescriptor finalReduceProcessorDescriptor =
        ProcessorDescriptor.create(
            ReduceProcessor.class.getName())
            .setUserPayload(finalReducePayload)
            .setHistoryText(finalReduceStageHistoryText);
    if (!useMRSettings) {
      finalReduceVertex = Vertex.create("finalreduce", finalReduceProcessorDescriptor, 1);
    } else {
      finalReduceVertex = Vertex.create("finalreduce", finalReduceProcessorDescriptor, 1,
          MRHelpers.getResourceForMRReducer(finalReduceConf));
      finalReduceVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(finalReduceConf));
      finalReduceVertex.setTaskEnvironment(reduceEnv);
    }
    finalReduceVertex.addTaskLocalFiles(commonLocalResources);
    finalReduceVertex.addDataSink("MROutput",
        MROutputLegacy.createConfigBuilder(finalReduceConf, TextOutputFormat.class, outputPath)
            .build());
    finalReduceVertex.getDataSinks().get(0).getOutputDescriptor().setHistoryText(
        TezUtils.convertToHistoryText("HDFS Output " + outputPath, finalReduceConf));
    vertices.add(finalReduceVertex);

    DAG dag = DAG.create("OrderedWordCount" + dagIndex);
    for (int i = 0; i < vertices.size(); ++i) {
      dag.addVertex(vertices.get(i));
    }

    OrderedPartitionedKVEdgeConfig edgeConf1 = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(iReduceStageConf)
	    .configureInput().useLegacyInput().done().build();
    dag.addEdge(
        Edge.create(dag.getVertex("initialmap"), dag.getVertex("intermediate_reducer"),
            edgeConf1.createDefaultEdgeProperty()));

    OrderedPartitionedKVEdgeConfig edgeConf2 = OrderedPartitionedKVEdgeConfig
        .newBuilder(IntWritable.class.getName(), Text.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(finalReduceConf)
            .configureInput().useLegacyInput().done().build();
    dag.addEdge(
        Edge.create(dag.getVertex("intermediate_reducer"), dag.getVertex("finalreduce"),
            edgeConf2.createDefaultEdgeProperty()));

    updateDAGACls(conf, dag, dagIndex);

    return dag;
  }

  private void updateDAGACls(Configuration conf, DAG dag, int dagIndex) {
    LOG.info("Checking DAG specific ACLS");
    DAGAccessControls accessControls = null;
    String suffix = "." + dagIndex;
    if (conf.get(DAG_VIEW_ACLS + suffix) != null
        || conf.get(DAG_MODIFY_ACLS + suffix) != null) {
      accessControls = new DAGAccessControls(
          conf.get(DAG_VIEW_ACLS + suffix), conf.get(DAG_MODIFY_ACLS + suffix));

    } else if (conf.get(DAG_VIEW_ACLS) != null
      || conf.get(DAG_MODIFY_ACLS) != null) {
      accessControls = new DAGAccessControls(
          conf.get(DAG_VIEW_ACLS), conf.get(DAG_MODIFY_ACLS));
    }
    if (accessControls != null) {
      LOG.info("Setting DAG specific ACLS");
      dag.setAccessControls(accessControls);
    }
  }


  private static void printUsage() {
    String options = " [-generateSplitsInClient true/<false>]";
    System.err.println("Usage: testorderedwordcount <in> <out>" + options);
    System.err.println("Usage (In Session Mode):"
        + " testorderedwordcount <in1> <out1> ... <inN> <outN>" + options);
    ToolRunner.printGenericCommandUsage(System.err);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    boolean generateSplitsInClient;

    SplitsInClientOptionParser splitCmdLineParser = new SplitsInClientOptionParser();
    try {
      generateSplitsInClient = splitCmdLineParser.parse(otherArgs, false);
      otherArgs = splitCmdLineParser.getRemainingArgs();
    } catch (ParseException e1) {
      System.err.println("Invalid options");
      printUsage();
      return 2;
    }

    boolean useTezSession = conf.getBoolean("USE_TEZ_SESSION", true);
    long interJobSleepTimeout = conf.getInt("INTER_JOB_SLEEP_INTERVAL", 0)
        * 1000;

    boolean retainStagingDir = conf.getBoolean("RETAIN_STAGING_DIR", false);
    boolean useMRSettings = conf.getBoolean("USE_MR_CONFIGS", true);
    // TODO needs to use auto reduce parallelism
    int intermediateNumReduceTasks = conf.getInt("IREDUCE_NUM_TASKS", 2);

    if (((otherArgs.length%2) != 0)
        || (!useTezSession && otherArgs.length != 2)) {
      printUsage();
      return 2;
    }

    List<String> inputPaths = new ArrayList<String>();
    List<String> outputPaths = new ArrayList<String>();

    for (int i = 0; i < otherArgs.length; i+=2) {
      inputPaths.add(otherArgs[i]);
      outputPaths.add(otherArgs[i+1]);
    }

    UserGroupInformation.setConfiguration(conf);

    TezConfiguration tezConf = new TezConfiguration(conf);
    TestOrderedWordCount instance = new TestOrderedWordCount();

    FileSystem fs = FileSystem.get(conf);

    String stagingDirStr =  conf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT) + Path.SEPARATOR + 
            Long.toString(System.currentTimeMillis());
    Path stagingDir = new Path(stagingDirStr);
    FileSystem pathFs = stagingDir.getFileSystem(tezConf);
    pathFs.mkdirs(new Path(stagingDirStr));

    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = pathFs.makeQualified(new Path(stagingDirStr));
    
    TokenCache.obtainTokensForNamenodes(instance.credentials, new Path[] {stagingDir}, conf);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    
    if (useTezSession) {
      LOG.info("Creating Tez Session");
      tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    } else {
      tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);
    }
    TezClient tezSession = TezClient.create("OrderedWordCountSession", tezConf,
        null, instance.credentials);
    tezSession.start();

    DAGStatus dagStatus = null;
    DAGClient dagClient = null;
    String[] vNames = { "initialmap", "intermediate_reducer",
      "finalreduce" };

    Set<StatusGetOpts> statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
    try {
      for (int dagIndex = 1; dagIndex <= inputPaths.size(); ++dagIndex) {
        if (dagIndex != 1
            && interJobSleepTimeout > 0) {
          try {
            LOG.info("Sleeping between jobs, sleepInterval="
                + (interJobSleepTimeout/1000));
            Thread.sleep(interJobSleepTimeout);
          } catch (InterruptedException e) {
            LOG.info("Main thread interrupted. Breaking out of job loop");
            break;
          }
        }

        String inputPath = inputPaths.get(dagIndex-1);
        String outputPath = outputPaths.get(dagIndex-1);

        if (fs.exists(new Path(outputPath))) {
          throw new FileAlreadyExistsException("Output directory "
              + outputPath + " already exists");
        }
        LOG.info("Running OrderedWordCount DAG"
            + ", dagIndex=" + dagIndex
            + ", inputPath=" + inputPath
            + ", outputPath=" + outputPath);

        Map<String, LocalResource> localResources =
          new TreeMap<String, LocalResource>();
        
        DAG dag = instance.createDAG(fs, tezConf, localResources,
            stagingDir, dagIndex, inputPath, outputPath,
            generateSplitsInClient, useMRSettings, intermediateNumReduceTasks);
        String callerType = "TestOrderedWordCount";
        String callerId = tezSession.getAppMasterApplicationId() == null ?
            ( "UnknownApp_" + System.currentTimeMillis() + dagIndex ) :
            ( tezSession.getAppMasterApplicationId().toString() + "_" + dagIndex);
        dag.setCallerContext(CallerContext.create("Tez", callerId, callerType,
            "TestOrderedWordCount Job"));

        boolean doPreWarm = dagIndex == 1 && useTezSession
            && conf.getBoolean("PRE_WARM_SESSION", true);
        int preWarmNumContainers = 0;
        if (doPreWarm) {
          preWarmNumContainers = conf.getInt("PRE_WARM_NUM_CONTAINERS", 0);
          if (preWarmNumContainers <= 0) {
            doPreWarm = false;
          }
        }
        if (doPreWarm) {
          LOG.info("Pre-warming Session");
          PreWarmVertex preWarmVertex = PreWarmVertex.create("PreWarm", preWarmNumContainers, dag
              .getVertex("initialmap").getTaskResource());
          preWarmVertex.addTaskLocalFiles(dag.getVertex("initialmap").getTaskLocalFiles());
          preWarmVertex.setTaskEnvironment(dag.getVertex("initialmap").getTaskEnvironment());
          preWarmVertex.setTaskLaunchCmdOpts(dag.getVertex("initialmap").getTaskLaunchCmdOpts());
          
          tezSession.preWarm(preWarmVertex);
        }

        if (useTezSession) {
          LOG.info("Waiting for TezSession to get into ready state");
          waitForTezSessionReady(tezSession);
          LOG.info("Submitting DAG to Tez Session, dagIndex=" + dagIndex);
          dagClient = tezSession.submitDAG(dag);
          LOG.info("Submitted DAG to Tez Session, dagIndex=" + dagIndex);
        } else {
          LOG.info("Submitting DAG as a new Tez Application");
          dagClient = tezSession.submitDAG(dag);
        }

        while (true) {
          dagStatus = dagClient.getDAGStatus(statusGetOpts);
          if (dagStatus.getState() == DAGStatus.State.RUNNING ||
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


        while (dagStatus.getState() != DAGStatus.State.SUCCEEDED &&
            dagStatus.getState() != DAGStatus.State.FAILED &&
            dagStatus.getState() != DAGStatus.State.KILLED &&
            dagStatus.getState() != DAGStatus.State.ERROR) {
          if (dagStatus.getState() == DAGStatus.State.RUNNING) {
            ExampleDriver.printDAGStatus(dagClient, vNames);
          }
          try {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // continue;
            }
            dagStatus = dagClient.getDAGStatus(statusGetOpts);
          } catch (TezException e) {
            LOG.error("Failed to get application progress. Exiting");
            return -1;
          }
        }
        ExampleDriver.printDAGStatus(dagClient, vNames,
            true, true);
        LOG.info("DAG " + dagIndex + " completed. "
            + "FinalState=" + dagStatus.getState());
        if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
          LOG.info("DAG " + dagIndex + " diagnostics: "
            + dagStatus.getDiagnostics());
        }
      }
    } catch (Exception e) {
      LOG.error("Error occurred when submitting/running DAGs", e);
      throw e;
    } finally {
      if (!retainStagingDir) {
        pathFs.delete(stagingDir, true);
      }
      LOG.info("Shutting down session");
      tezSession.stop();
    }

    if (!useTezSession) {
      ExampleDriver.printDAGStatus(dagClient, vNames);
      LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
    }
    return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
  }

  private static void waitForTezSessionReady(TezClient tezSession)
    throws IOException, TezException, InterruptedException {
    tezSession.waitTillReady();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TezConfiguration(), new TestOrderedWordCount(), args);
    System.exit(res);
  }
}

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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.PreWarmContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
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
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.examples.helpers.SplitsInClientOptionParser;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.api.TezRootInputInitializer;
import org.apache.tez.runtime.library.input.ShuffledMergedInputLegacy;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;
import org.apache.tez.runtime.library.processor.SleepProcessor;

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
public class OrderedWordCount extends Configured implements Tool {

  private static Log LOG = LogFactory.getLog(OrderedWordCount.class);

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
  
  private DAG createDAG(FileSystem fs, Configuration conf,
      Map<String, LocalResource> commonLocalResources, Path stagingDir,
      int dagIndex, String inputPath, String outputPath,
      boolean generateSplitsInClient) throws Exception {

    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
        TokenizerMapper.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    if (generateSplitsInClient) {
      mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
          TextInputFormat.class.getName());
    } else {
      mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, 
          TezGroupedSplitsInputFormat.class.getName());
    }
    mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
    mapStageConf.setBoolean("mapred.mapper.new-api", true);

    InputSplitInfo inputSplitInfo = null;
    if (generateSplitsInClient) {
      inputSplitInfo = MRHelpers.generateInputSplits(mapStageConf, stagingDir);
      mapStageConf.setInt(MRJobConfig.NUM_MAPS, inputSplitInfo.getNumTasks());
    }

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(mapStageConf,
        null);

    Configuration iReduceStageConf = new JobConf(conf);
    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2); // TODO NEWTEZ - NOT NEEDED NOW???
    iReduceStageConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        IntSumReducer.class.getName());
    iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        Text.class.getName());
    iReduceStageConf.setBoolean("mapred.mapper.new-api", true);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(iReduceStageConf,
        mapStageConf);

    Configuration finalReduceConf = new JobConf(conf);
    finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, 1);
    finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        MyOrderByNoOpReducer.class.getName());
    finalReduceConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    finalReduceConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    finalReduceConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        TextOutputFormat.class.getName());
    finalReduceConf.set(FileOutputFormat.OUTDIR, outputPath);
    finalReduceConf.setBoolean("mapred.mapper.new-api", true);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(finalReduceConf,
        iReduceStageConf);

    MRHelpers.doJobClientMagic(mapStageConf);
    MRHelpers.doJobClientMagic(iReduceStageConf);
    MRHelpers.doJobClientMagic(finalReduceConf);

    List<Vertex> vertices = new ArrayList<Vertex>();

    byte[] mapPayload = MRHelpers.createUserPayloadFromConf(mapStageConf);
    byte[] mapInputPayload = MRHelpers.createMRInputPayloadWithGrouping(mapPayload,
      TextInputFormat.class.getName());
    int numMaps = generateSplitsInClient ? inputSplitInfo.getNumTasks() : -1;
    Vertex mapVertex = new Vertex("initialmap", new ProcessorDescriptor(
        MapProcessor.class.getName()).setUserPayload(mapPayload),
        numMaps, MRHelpers.getMapResource(mapStageConf));
    mapVertex.setJavaOpts(MRHelpers.getMapJavaOpts(mapStageConf));
    if (generateSplitsInClient) {
      mapVertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
      Map<String, LocalResource> mapLocalResources =
          new HashMap<String, LocalResource>();
      mapLocalResources.putAll(commonLocalResources);
      MRHelpers.updateLocalResourcesForInputSplits(fs, inputSplitInfo,
          mapLocalResources);
      mapVertex.setTaskLocalResources(mapLocalResources);
    } else {
      mapVertex.setTaskLocalResources(commonLocalResources);
    }

    Map<String, String> mapEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(mapStageConf, mapEnv, true);
    mapVertex.setTaskEnvironment(mapEnv);
    Class<? extends TezRootInputInitializer> initializerClazz = generateSplitsInClient ? null
        : MRInputAMSplitGenerator.class;
    MRHelpers.addMRInput(mapVertex, mapInputPayload, initializerClazz);
    vertices.add(mapVertex);

    Vertex ivertex = new Vertex("intermediate_reducer", new ProcessorDescriptor(
        ReduceProcessor.class.getName()).
        setUserPayload(MRHelpers.createUserPayloadFromConf(iReduceStageConf)),
        2,
        MRHelpers.getReduceResource(iReduceStageConf));
    ivertex.setJavaOpts(MRHelpers.getReduceJavaOpts(iReduceStageConf));
    ivertex.setTaskLocalResources(commonLocalResources);
    Map<String, String> ireduceEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(iReduceStageConf, ireduceEnv, false);
    ivertex.setTaskEnvironment(ireduceEnv);
    vertices.add(ivertex);

    byte[] finalReducePayload = MRHelpers.createUserPayloadFromConf(finalReduceConf);
    Vertex finalReduceVertex = new Vertex("finalreduce",
        new ProcessorDescriptor(
            ReduceProcessor.class.getName()).setUserPayload(finalReducePayload),
                1, MRHelpers.getReduceResource(finalReduceConf));
    finalReduceVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(finalReduceConf));
    finalReduceVertex.setTaskLocalResources(commonLocalResources);
    Map<String, String> reduceEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(finalReduceConf, reduceEnv, false);
    finalReduceVertex.setTaskEnvironment(reduceEnv);
    MRHelpers.addMROutputLegacy(finalReduceVertex, finalReducePayload);
    vertices.add(finalReduceVertex);

    DAG dag = new DAG("OrderedWordCount" + dagIndex);
    for (int i = 0; i < vertices.size(); ++i) {
      dag.addVertex(vertices.get(i));
      if (i != 0) {
        dag.addEdge(new Edge(vertices.get(i-1),
            vertices.get(i), new EdgeProperty(
                DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
                SchedulingType.SEQUENTIAL,
                new OutputDescriptor(
                    OnFileSortedOutput.class.getName()),
                new InputDescriptor(
                    ShuffledMergedInputLegacy.class.getName()))));
      }
    }
    return dag;
  }

  private static void printUsage() {
    String options = " [-generateSplitsInClient true/<false>]";
    System.err.println("Usage: orderedwordcount <in> <out>" + options);
    System.err.println("Usage (In Session Mode):"
        + " orderedwordcount <in1> <out1> ... <inN> <outN>" + options);
    ToolRunner.printGenericCommandUsage(System.err);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

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

    boolean useTezSession = conf.getBoolean("USE_TEZ_SESSION", true);
    long interJobSleepTimeout = conf.getInt("INTER_JOB_SLEEP_INTERVAL", 0)
        * 1000;

    boolean retainStagingDir = conf.getBoolean("RETAIN_STAGING_DIR", false);

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
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TezConfiguration tezConf = new TezConfiguration(conf);
    TezClient tezClient = new TezClient(tezConf);
    ApplicationId appId = tezClient.createApplication();
    OrderedWordCount instance = new OrderedWordCount();

    FileSystem fs = FileSystem.get(conf);

    String stagingDirStr = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR
        + Path.SEPARATOR + appId.toString();
    Path stagingDir = new Path(stagingDirStr);
    FileSystem pathFs = stagingDir.getFileSystem(tezConf);
    pathFs.mkdirs(new Path(stagingDirStr));

    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = pathFs.makeQualified(new Path(stagingDirStr));
    
    TokenCache.obtainTokensForNamenodes(instance.credentials, new Path[] {stagingDir}, conf);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    tezConf.set(TezConfiguration.TEZ_AM_JAVA_OPTS,
        MRHelpers.getMRAMJavaOpts(conf));

    // No need to add jar containing this class as assumed to be part of
    // the tez jars.

    // TEZ-674 Obtain tokens based on the Input / Output paths. For now assuming staging dir
    // is the same filesystem as the one used for Input/Output.
    
    TezSession tezSession = null;
    AMConfiguration amConfig = new AMConfiguration(null,
        null, tezConf, instance.credentials);
    if (useTezSession) {
      LOG.info("Creating Tez Session");
      TezSessionConfiguration sessionConfig =
          new TezSessionConfiguration(amConfig, tezConf);
      tezSession = new TezSession("OrderedWordCountSession", appId,
          sessionConfig);
      tezSession.start();
      LOG.info("Created Tez Session");
    }

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
        
        DAG dag = instance.createDAG(fs, conf, localResources,
            stagingDir, dagIndex, inputPath, outputPath,
            generateSplitsInClient);

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
          VertexLocationHint vertexLocationHint =
              new VertexLocationHint(null);
          ProcessorDescriptor sleepProcDescriptor =
            new ProcessorDescriptor(SleepProcessor.class.getName());
          SleepProcessor.SleepProcessorConfig sleepProcessorConfig =
            new SleepProcessor.SleepProcessorConfig(4000);
          sleepProcDescriptor.setUserPayload(
            sleepProcessorConfig.toUserPayload());
          PreWarmContext context = new PreWarmContext(sleepProcDescriptor,
            dag.getVertex("initialmap").getTaskResource(), preWarmNumContainers,
              vertexLocationHint);

          Map<String, LocalResource> contextLocalRsrcs =
            new TreeMap<String, LocalResource>();
          contextLocalRsrcs.putAll(
            dag.getVertex("initialmap").getTaskLocalResources());
          Map<String, String> contextEnv = new TreeMap<String, String>();
          contextEnv.putAll(dag.getVertex("initialmap").getTaskEnvironment());
          String contextJavaOpts =
            dag.getVertex("initialmap").getJavaOpts();
          context
            .setLocalResources(contextLocalRsrcs)
            .setJavaOpts(contextJavaOpts)
            .setEnvironment(contextEnv);

          tezSession.preWarm(context);
        }

        if (useTezSession) {
          LOG.info("Waiting for TezSession to get into ready state");
          waitForTezSessionReady(tezSession);
          LOG.info("Submitting DAG to Tez Session, dagIndex=" + dagIndex);
          dagClient = tezSession.submitDAG(dag);
          LOG.info("Submitted DAG to Tez Session, dagIndex=" + dagIndex);
        } else {
          LOG.info("Submitting DAG as a new Tez Application");
          dagClient = tezClient.submitDAGApplication(dag, amConfig);
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
            LOG.fatal("Failed to get application progress. Exiting");
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
      if (useTezSession) {
        LOG.info("Shutting down session");
        tezSession.stop();
      }
    }

    if (!useTezSession) {
      ExampleDriver.printDAGStatus(dagClient, vNames);
      LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
      return dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1;
    }
    return 0;
  }

  private static void waitForTezSessionReady(TezSession tezSession)
    throws IOException, TezException {
    while (true) {
      TezSessionStatus status = tezSession.getSessionStatus();
      if (status.equals(TezSessionStatus.SHUTDOWN)) {
        throw new RuntimeException("TezSession has already shutdown");
      }
      if (status.equals(TezSessionStatus.READY)) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.info("Interrupted while trying to check session status");
        return;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new OrderedWordCount(), args);
    System.exit(res);
  }
}

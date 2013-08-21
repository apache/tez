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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezSession;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;

/**
 * An MRR job built on top of word count to return words sorted by
 * their frequency of occurrence.
 */
public class OrderedWordCount {

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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    String inputPath = otherArgs[0];
    String outputPath = otherArgs[1];

    boolean useTezSession = conf.getBoolean("USE_TEZ_SESSION", true);

    UserGroupInformation.setConfiguration(conf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    TezConfiguration tezConf = new TezConfiguration(conf);
    TezClient tezClient = new TezClient(tezConf);
    ApplicationId appId = tezClient.createApplication();

    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(outputPath))) {
      throw new FileAlreadyExistsException("Output directory " + outputPath +
          " already exists");
    }

    String baseDir = Path.SEPARATOR + "user" + Path.SEPARATOR
        + user + Path.SEPARATOR+ ".staging" + Path.SEPARATOR;
    Path stagingDir = new Path(baseDir + Path.SEPARATOR + appId.toString());
    stagingDir = fs.makeQualified(stagingDir);
    TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

    List<String> amArgs = new ArrayList<String>();
    amArgs.add(MRHelpers.getMRAMJavaOpts(conf));

    String jarPath = ClassUtil.findContainingJar(OrderedWordCount.class);
    if (jarPath == null)  {
        throw new TezUncheckedException("Could not find any jar containing"
            + " OrderedWordCount.class in the classpath");
    }
    Path remoteJarPath = fs.makeQualified(
        new Path(stagingDir, "dag_job.jar"));
    fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
    FileStatus jarFileStatus = fs.getFileStatus(remoteJarPath);

    Map<String, LocalResource> commonLocalResources =
        new TreeMap<String, LocalResource>();
    LocalResource dagJarLocalRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteJarPath),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        jarFileStatus.getLen(),
        jarFileStatus.getModificationTime());
    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);

    TezSession tezSession = null;
    if (useTezSession) {
      LOG.info("Creating Tez Session");
      tezSession = tezClient.createSession(appId, "OrderedWordCountSession",
          stagingDir, null, "default", amArgs, null, commonLocalResources,
          tezConf);
      LOG.info("Created Tez Session");
    }

    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
        TokenizerMapper.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        Text.class.getName());
    mapStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
        TextInputFormat.class.getName());
    mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
    mapStageConf.setBoolean("mapred.mapper.new-api", true);

    InputSplitInfo inputSplitInfo =
        MRHelpers.generateInputSplits(mapStageConf, stagingDir);
    mapStageConf.setInt(MRJobConfig.NUM_MAPS, inputSplitInfo.getNumTasks());

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(mapStageConf,
        null);

    Configuration iReduceStageConf = new JobConf(conf);
    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2);
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

    Vertex mapVertex = new Vertex("initialmap", new ProcessorDescriptor(
        MapProcessor.class.getName()).setUserPayload(
        MRHelpers.createUserPayloadFromConf(mapStageConf)),
        inputSplitInfo.getNumTasks(),
        MRHelpers.getMapResource(mapStageConf));
    mapVertex.setJavaOpts(MRHelpers.getMapJavaOpts(mapStageConf));
    mapVertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
    Map<String, LocalResource> mapLocalResources =
        new HashMap<String, LocalResource>();
    mapLocalResources.putAll(commonLocalResources);
    MRHelpers.updateLocalResourcesForInputSplits(fs, inputSplitInfo,
        mapLocalResources);
    mapVertex.setTaskLocalResources(mapLocalResources);
    Map<String, String> mapEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(mapStageConf, mapEnv, true);
    mapVertex.setTaskEnvironment(mapEnv);
    vertices.add(mapVertex);

    Vertex ivertex = new Vertex("ivertex1", new ProcessorDescriptor(
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

    Vertex finalReduceVertex = new Vertex("finalreduce",
        new ProcessorDescriptor(
            ReduceProcessor.class.getName()).setUserPayload(
                MRHelpers.createUserPayloadFromConf(finalReduceConf)),
                1,
                MRHelpers.getReduceResource(finalReduceConf));
    finalReduceVertex.setJavaOpts(
        MRHelpers.getReduceJavaOpts(finalReduceConf));
    finalReduceVertex.setTaskLocalResources(commonLocalResources);
    Map<String, String> reduceEnv = new HashMap<String, String>();
    MRHelpers.updateEnvironmentForMRTasks(finalReduceConf, reduceEnv, false);
    finalReduceVertex.setTaskEnvironment(reduceEnv);
    vertices.add(finalReduceVertex);

    DAG dag = new DAG("OrderedWordCount");
    for (int i = 0; i < vertices.size(); ++i) {
      dag.addVertex(vertices.get(i));
      if (i != 0) {
        dag.addEdge(new Edge(vertices.get(i-1),
            vertices.get(i), new EdgeProperty(
                ConnectionPattern.BIPARTITE, SourceType.STABLE,
                new OutputDescriptor(
                    OnFileSortedOutput.class.getName()),
                new InputDescriptor(
                    ShuffledMergedInput.class.getName()))));
      }
    }

    DAGClient dagClient;
    if (useTezSession) {
      LOG.info("Submitting DAG to Tez Session");
      dagClient = tezClient.submitDAG(tezSession, dag);
      LOG.info("Submitted DAG to Tez Session");
    } else {
      LOG.info("Submitting DAG as a new Tez Application");
      dagClient = tezClient.submitDAGApplication(appId, dag, stagingDir, null,
          "default", "OrderedWordCount", amArgs, null, commonLocalResources,
          tezConf);
    }

    DAGStatus dagStatus = null;
    try {
      while (true) {
        dagStatus = dagClient.getDAGStatus();
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
          ExampleDriver.printMRRDAGStatus(dagStatus);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // continue;
          }
          dagStatus = dagClient.getDAGStatus();
        } catch (TezException e) {
          LOG.fatal("Failed to get application progress. Exiting");
          System.exit(-1);
        }
      }
    } finally {
      fs.delete(stagingDir, true);
      if (useTezSession) {
        tezClient.closeSession(tezSession);
      }
    }

    ExampleDriver.printMRRDAGStatus(dagStatus);
    LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
    System.exit(dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1);
  }

}

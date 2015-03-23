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
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

/**
 * Simple example that does a GROUP BY ORDER BY in an MRR job
 * Consider a query such as
 * Select DeptName, COUNT(*) as cnt FROM EmployeeTable
 * GROUP BY DeptName ORDER BY cnt;
 *
 * i.e. List all departments with count of employees in each department
 * and ordered based on department's employee count.
 *
 *  Requires an Input file containing 2 strings per line in format of
 *  <EmployeeName> <DeptName>
 *
 *  For example, use the following:
 *
 *  #/bin/bash
 *
 *  i=1000000
 *  j=1000
 *
 *  id=0
 *  while [[ "$id" -ne "$i" ]]
 *  do
 *    id=`expr $id + 1`
 *    deptId=`expr $RANDOM % $j + 1`
 *    deptName=`echo "ibase=10;obase=16;$deptId" | bc`
 *    echo "$id O$deptName"
 *  done
 *
 */
public class GroupByOrderByMRRTest extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByOrderByMRRTest.class);

  /**
   * Mapper takes in a single line as input containing
   * employee name and department name and then
   * emits department name with count of 1
   */
  public static class MyMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static Text word = new Text();

    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String empName;
      String deptName = "";
      if (itr.hasMoreTokens()) {
        empName = itr.nextToken();
        if (itr.hasMoreTokens()) {
          deptName = itr.nextToken();
        }
        if (!empName.isEmpty()
            && !deptName.isEmpty()) {
          word.set(deptName);
          context.write(word, one);
        }
      }
    }
  }

  /**
   * Intermediate reducer aggregates the total count per department.
   * It takes department name and count as input and emits the final
   * count per department name.
   */
  public static class MyGroupByReducer
      extends Reducer<Text, IntWritable, IntWritable, Text> {
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

  private static DAG createDAG(Configuration conf, Map<String, LocalResource> commonLocalResources,
      Path stagingDir, String inputPath, String outputPath, boolean useMRSettings)
      throws Exception {

    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR,
        MyMapper.class.getName());

    MRHelpers.translateMRConfToTez(mapStageConf);

    Configuration iReduceStageConf = new JobConf(conf);
    // TODO replace with auto-reduce parallelism
    iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, 2);
    iReduceStageConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        MyGroupByReducer.class.getName());
    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    iReduceStageConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
        IntWritable.class.getName());
    iReduceStageConf.setBoolean("mapred.mapper.new-api", true);
    MRHelpers.translateMRConfToTez(iReduceStageConf);

    Configuration finalReduceConf = new JobConf(conf);
    finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, 1);
    finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR,
        MyOrderByNoOpReducer.class.getName());
    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    finalReduceConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    MRHelpers.translateMRConfToTez(finalReduceConf);

    MRHelpers.configureMRApiUsage(mapStageConf);
    MRHelpers.configureMRApiUsage(iReduceStageConf);
    MRHelpers.configureMRApiUsage(finalReduceConf);

    List<Vertex> vertices = new ArrayList<Vertex>();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4096);
    mapStageConf.writeXml(outputStream);
    String mapStageHistoryText = new String(outputStream.toByteArray(), "UTF-8");
    mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
        TextInputFormat.class.getName());
    mapStageConf.set(FileInputFormat.INPUT_DIR, inputPath);
    mapStageConf.setBoolean("mapred.mapper.new-api", true);
    DataSourceDescriptor dsd = MRInputHelpers.configureMRInputWithLegacySplitGeneration(
        mapStageConf, stagingDir, true);

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
    }
    mapVertex.addTaskLocalFiles(commonLocalResources)
        .addDataSource("MRInput", dsd);
    vertices.add(mapVertex);

    ByteArrayOutputStream iROutputStream = new ByteArrayOutputStream(4096);
    iReduceStageConf.writeXml(iROutputStream);
    String iReduceStageHistoryText = new String(iROutputStream.toByteArray(), "UTF-8");

    ProcessorDescriptor iReduceProcessorDescriptor = ProcessorDescriptor.create(
        ReduceProcessor.class.getName())
        .setUserPayload(TezUtils.createUserPayloadFromConf(iReduceStageConf))
        .setHistoryText(iReduceStageHistoryText);

    Vertex intermediateVertex;
    if (!useMRSettings) {
      intermediateVertex = Vertex.create("ireduce1", iReduceProcessorDescriptor, 1);
    } else {
      intermediateVertex = Vertex.create("ireduce1", iReduceProcessorDescriptor,
          1, MRHelpers.getResourceForMRReducer(iReduceStageConf));
      intermediateVertex.setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(iReduceStageConf));
    }
    intermediateVertex.addTaskLocalFiles(commonLocalResources);
    vertices.add(intermediateVertex);

    ByteArrayOutputStream finalReduceOutputStream = new ByteArrayOutputStream(4096);
    finalReduceConf.writeXml(finalReduceOutputStream);
    String finalReduceStageHistoryText = new String(finalReduceOutputStream.toByteArray(), "UTF-8");
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
    }
    finalReduceVertex.addTaskLocalFiles(commonLocalResources);
    finalReduceVertex.addDataSink("MROutput",
        MROutputLegacy.createConfigBuilder(finalReduceConf, TextOutputFormat.class, outputPath)
            .build());
    vertices.add(finalReduceVertex);

    DAG dag = DAG.create("groupbyorderbymrrtest");
    for (Vertex v : vertices) {
      dag.addVertex(v);
    }

    OrderedPartitionedKVEdgeConfig edgeConf1 = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(conf)
        .configureInput().useLegacyInput().done().build();
    dag.addEdge(
        Edge.create(dag.getVertex("initialmap"), dag.getVertex("ireduce1"),
            edgeConf1.createDefaultEdgeProperty()));

    OrderedPartitionedKVEdgeConfig edgeConf2 = OrderedPartitionedKVEdgeConfig
        .newBuilder(IntWritable.class.getName(), Text.class.getName(),
            HashPartitioner.class.getName()).setFromConfiguration(conf)
        .configureInput().useLegacyInput().done().build();
    dag.addEdge(
        Edge.create(dag.getVertex("ireduce1"), dag.getVertex("finalreduce"),
            edgeConf2.createDefaultEdgeProperty()));

    return dag;
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    String[] otherArgs = new GenericOptionsParser(conf, args).
        getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: groupbyorderbymrrtest <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      return 2;
    }

    String inputPath = otherArgs[0];
    String outputPath = otherArgs[1];

    UserGroupInformation.setConfiguration(conf);

    TezConfiguration tezConf = new TezConfiguration(conf);
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(new Path(outputPath))) {
      throw new FileAlreadyExistsException("Output directory "
          + outputPath + " already exists");
    }

    Map<String, LocalResource> localResources =
        new TreeMap<String, LocalResource>();

    String stagingDirStr =  conf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
        TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT) + Path.SEPARATOR +
        Long.toString(System.currentTimeMillis());
    Path stagingDir = new Path(stagingDirStr);
    FileSystem pathFs = stagingDir.getFileSystem(tezConf);
    pathFs.mkdirs(new Path(stagingDirStr));

    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirStr);
    stagingDir = pathFs.makeQualified(new Path(stagingDirStr));

    TezClient tezClient = TezClient.create("groupbyorderbymrrtest", tezConf);
    tezClient.start();

    LOG.info("Submitting groupbyorderbymrrtest DAG as a new Tez Application");

    try {
      DAG dag = createDAG(conf, localResources, stagingDir, inputPath, outputPath, true);

      tezClient.waitTillReady();

      DAGClient dagClient = tezClient.submitDAG(dag);

      DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
      if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
        LOG.error("groupbyorderbymrrtest failed, state=" + dagStatus.getState()
            + ", diagnostics=" + dagStatus.getDiagnostics());
        return -1;
      }
      LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
      return 0;
    } finally {
      tezClient.stop();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    GroupByOrderByMRRTest groupByOrderByMRRTest = new GroupByOrderByMRRTest();
    int status = ToolRunner.run(configuration, groupByOrderByMRRTest, args);
    System.exit(status);
  }
}

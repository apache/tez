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
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;

import com.google.common.annotations.VisibleForTesting;

import org.apache.tez.runtime.library.partitioner.HashPartitioner;

/**
 * Dummy class for testing MR framefork. Sleeps for a defined period
 * of time in mapper and reducer. Generates fake input for map / reduce
 * jobs. Note that generated number of input pairs is in the order
 * of <code>numMappers * mapSleepTime / 100</code>, so the job uses
 * some disk space.
 */
public class MRRSleepJob extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MRRSleepJob.class);

  public static final String MAP_SLEEP_COUNT = "mrr.sleepjob.map.sleep.count";
  public static final String REDUCE_SLEEP_COUNT =
    "mrr.sleepjob.reduce.sleep.count";
  public static final String MAP_SLEEP_TIME = "mrr.sleepjob.map.sleep.time";
  public static final String REDUCE_SLEEP_TIME =
    "mrr.sleepjob.reduce.sleep.time";
  public static final String IREDUCE_SLEEP_COUNT =
      "mrr.sleepjob.ireduce.sleep.count";
  public static final String IREDUCE_SLEEP_TIME =
      "mrr.sleepjob.ireduce.sleep.time";
  public static final String IREDUCE_STAGES_COUNT =
      "mrr.sleepjob.ireduces.stages.count";
  public static final String IREDUCE_TASKS_COUNT =
      "mrr.sleepjob.ireduces.tasks.count";

  // Flags to inject failures
  public static final String MAP_THROW_ERROR = "mrr.sleepjob.map.throw.error";
  public static final String MAP_FATAL_ERROR = "mrr.sleepjob.map.fatal.error";
  public static final String MAP_ERROR_TASK_IDS =
      "mrr.sleepjob.map.error.task.ids";

  public static class MRRSleepJobPartitioner extends
      Partitioner<IntWritable, IntWritable> {
    public int getPartition(IntWritable k, IntWritable v, int numPartitions) {
      return k.get() % numPartitions;
    }
  }

  public static class EmptySplit extends InputSplit implements Writable {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static class SleepInputFormat
      extends InputFormat<IntWritable,IntWritable> {

    public List<InputSplit> getSplits(JobContext jobContext) {
      List<InputSplit> ret = new ArrayList<InputSplit>();
      int numSplits = jobContext.getConfiguration().
                        getInt(MRJobConfig.NUM_MAPS, 1);
      for (int i = 0; i < numSplits; ++i) {
        ret.add(new EmptySplit());
      }
      return ret;
    }

    public RecordReader<IntWritable,IntWritable> createRecordReader(
        InputSplit ignored, TaskAttemptContext taskContext)
        throws IOException {
      Configuration conf = taskContext.getConfiguration();

      final int count = conf.getInt(MAP_SLEEP_COUNT, 1);
      if (count < 0) {
        throw new IOException("Invalid map count: " + count);
      }

      int totalIReduces = conf.getInt(IREDUCE_STAGES_COUNT, 1);

      int reduceTasks = totalIReduces == 0?
          taskContext.getNumReduceTasks() :
            conf.getInt(IREDUCE_TASKS_COUNT, 1);
      int sleepCount = totalIReduces == 0?
          conf.getInt(REDUCE_SLEEP_COUNT,1) :
            conf.getInt(IREDUCE_SLEEP_COUNT,1);
      final int emitPerMapTask = sleepCount * reduceTasks;

      return new RecordReader<IntWritable,IntWritable>() {
        private int records = 0;
        private int emitCount = 0;
        private IntWritable key = null;
        private IntWritable value = null;

        public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        public boolean nextKeyValue()
            throws IOException {
          if (count == 0) {
            return false;
          }
          key = new IntWritable();
          key.set(emitCount);
          int emit = emitPerMapTask / count;
          if ((emitPerMapTask) % count > records) {
            ++emit;
          }
          emitCount += emit;
          value = new IntWritable();
          value.set(emit);
          return records++ < count;
        }
        public IntWritable getCurrentKey() { return key; }
        public IntWritable getCurrentValue() { return value; }
        public void close() throws IOException { }
        public float getProgress() throws IOException {
          return count == 0 ? 100 : records / ((float)count);
        }
      };
    }
  }

  public static class SleepMapper
      extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    private long mapSleepDuration = 100;
    private int mapSleepCount = 1;
    private int count = 0;
    private String vertexName;
    private boolean throwError = false;
    private boolean throwFatal = false;
    private boolean finalAttempt = false;

    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.mapSleepCount =
        conf.getInt(MAP_SLEEP_COUNT, mapSleepCount);
      this.mapSleepDuration = mapSleepCount == 0 ? 0 :
        conf.getLong(MAP_SLEEP_TIME , 100) / mapSleepCount;
      vertexName = conf.get(
          org.apache.tez.mapreduce.hadoop.MRJobConfig.VERTEX_NAME);

      TaskAttemptID taId = context.getTaskAttemptID();

      String[] taskIds = conf.getStrings(MAP_ERROR_TASK_IDS);
      if (taId.getId()+1 >= context.getMaxMapAttempts()) {
        finalAttempt = true;
      }
      boolean found = false;
      if (taskIds != null) {
        if (taskIds.length == 1 && taskIds[0].equals("*")) {
          found = true;
        }
        if (!found) {
          for (String taskId : taskIds) {
            if (Integer.valueOf(taskId).intValue() ==
                taId.getTaskID().getId()) {
              found = true;
              break;
            }
          }
        }
      }
      if (found) {
        if (!finalAttempt) {
          throwError = conf.getBoolean(MAP_THROW_ERROR, false);
        }
        throwFatal = conf.getBoolean(MAP_FATAL_ERROR, false);
      }
    }

    public void map(IntWritable key, IntWritable value, Context context
               ) throws IOException, InterruptedException {
      //it is expected that every map processes mapSleepCount number of records.
      try {
        LOG.info("Reading in " + vertexName
            + " taskid " + context.getTaskAttemptID().getTaskID().getId()
            + " key " + key.get());
        LOG.info("Sleeping in InitialMap"
            + ", vertexName=" + vertexName
            + ", taskAttemptId=" + context.getTaskAttemptID()
            + ", mapSleepDuration=" + mapSleepDuration
            + ", mapSleepCount=" + mapSleepCount
            + ", sleepLeft="
            + (mapSleepDuration * (mapSleepCount - count)));
        context.setStatus("Sleeping... (" +
          (mapSleepDuration * (mapSleepCount - count)) + ") ms left");
        if ((mapSleepCount - count) > 0) {
          Thread.sleep(mapSleepDuration);
        }
        if (throwError || throwFatal) {
          throw new IOException("Throwing a simulated error from map");
        }
      }
      catch (InterruptedException ex) {
        throw (IOException)new IOException(
            "Interrupted while sleeping").initCause(ex);
      }
      ++count;
      // output reduceSleepCount * numReduce number of random values, so that
      // each reducer will get reduceSleepCount number of keys.
      int k = key.get();
      for (int i = 0; i < value.get(); ++i) {
        LOG.info("Writing in " + vertexName
            + " taskid " + context.getTaskAttemptID().getTaskID().getId()
            + " key " + (k+i) + " value 1");
        context.write(new IntWritable(k + i), new IntWritable(1));
      }
    }
  }

  public static class ISleepReducer
  extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private long iReduceSleepDuration = 100;
    private int iReduceSleepCount = 1;
    private int count = 0;
    private String vertexName;

    protected void setup(Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.iReduceSleepCount =
          conf.getInt(IREDUCE_SLEEP_COUNT, iReduceSleepCount);
      this.iReduceSleepDuration = iReduceSleepCount == 0 ? 0 :
        conf.getLong(IREDUCE_SLEEP_TIME , 100) / iReduceSleepCount;
      vertexName = conf.get(
          org.apache.tez.mapreduce.hadoop.MRJobConfig.VERTEX_NAME);
    }

    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context)
            throws IOException, InterruptedException {
      try {
        LOG.info("Reading in " + vertexName
            + " taskid " + context.getTaskAttemptID().getTaskID().getId()
            + " key " + key.get());

        LOG.info("Sleeping in IntermediateReduce"
            + ", vertexName=" + vertexName
            + ", taskAttemptId=" + context.getTaskAttemptID()
            + ", iReduceSleepDuration=" + iReduceSleepDuration
            + ", iReduceSleepCount=" + iReduceSleepCount
            + ", sleepLeft="
            + (iReduceSleepDuration * (iReduceSleepCount - count)));
        context.setStatus("Sleeping... (" +
          (iReduceSleepDuration * (iReduceSleepCount - count)) + ") ms left");
        if ((iReduceSleepCount - count) > 0) {
          Thread.sleep(iReduceSleepDuration);
        }
      }
      catch (InterruptedException ex) {
        throw (IOException)new IOException(
            "Interrupted while sleeping").initCause(ex);
      }
      ++count;
      // output reduceSleepCount * numReduce number of random values, so that
      // each reducer will get reduceSleepCount number of keys.
      int k = key.get();
      for (IntWritable value : values) {
        for (int i = 0; i < value.get(); ++i) {
          LOG.info("Writing in " + vertexName
              + " taskid " + context.getTaskAttemptID().getTaskID().getId()
              + " key " + (k+i) + " value 1");
          context.write(new IntWritable(k + i), new IntWritable(1));
        }
      }
    }
  }

  public static class SleepReducer
      extends Reducer<IntWritable, IntWritable, NullWritable, NullWritable> {
    private long reduceSleepDuration = 100;
    private int reduceSleepCount = 1;
    private int count = 0;
    private String vertexName;

    protected void setup(Context context)
      throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      this.reduceSleepCount =
        conf.getInt(REDUCE_SLEEP_COUNT, reduceSleepCount);
      this.reduceSleepDuration = reduceSleepCount == 0 ? 0 :
        conf.getLong(REDUCE_SLEEP_TIME , 100) / reduceSleepCount;
      vertexName = conf.get(
          org.apache.tez.mapreduce.hadoop.MRJobConfig.VERTEX_NAME);
    }

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context)
      throws IOException {
      try {
        LOG.info("Reading in " + vertexName
            + " taskid " + context.getTaskAttemptID().getTaskID().getId()
            + " key " + key.get());
        LOG.info("Sleeping in FinalReduce"
            + ", vertexName=" + vertexName
            + ", taskAttemptId=" + context.getTaskAttemptID()
            + ", reduceSleepDuration=" + reduceSleepDuration
            + ", reduceSleepCount=" + reduceSleepCount
            + ", sleepLeft="
            + (reduceSleepDuration * (reduceSleepCount - count)));
        context.setStatus("Sleeping... (" +
            (reduceSleepDuration * (reduceSleepCount - count)) + ") ms left");
        if ((reduceSleepCount - count) > 0) {
          Thread.sleep(reduceSleepDuration);
        }
      }
      catch (InterruptedException ex) {
        throw (IOException)new IOException(
          "Interrupted while sleeping").initCause(ex);
      }
      count++;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MRRSleepJob(), args);
    System.exit(res);
  }
  
  private Credentials credentials = new Credentials();

  public DAG createDAG(FileSystem remoteFs, Configuration conf, Path remoteStagingDir,
      int numMapper, int numReducer, int iReduceStagesCount,
      int numIReducer, long mapSleepTime, int mapSleepCount,
      long reduceSleepTime, int reduceSleepCount,
      long iReduceSleepTime, int iReduceSleepCount, boolean writeSplitsToDFS,
      boolean generateSplitsInAM)
      throws IOException, YarnException {


    Configuration mapStageConf = new JobConf(conf);
    mapStageConf.setInt(MRJobConfig.NUM_MAPS, numMapper);
    mapStageConf.setLong(MAP_SLEEP_TIME, mapSleepTime);
    mapStageConf.setLong(REDUCE_SLEEP_TIME, reduceSleepTime);
    mapStageConf.setLong(IREDUCE_SLEEP_TIME, iReduceSleepTime);
    mapStageConf.setInt(MAP_SLEEP_COUNT, mapSleepCount);
    mapStageConf.setInt(REDUCE_SLEEP_COUNT, reduceSleepCount);
    mapStageConf.setInt(IREDUCE_SLEEP_COUNT, iReduceSleepCount);
    mapStageConf.setInt(IREDUCE_STAGES_COUNT, iReduceStagesCount);
    mapStageConf.setInt(IREDUCE_TASKS_COUNT, numIReducer);
    mapStageConf.set(MRJobConfig.MAP_CLASS_ATTR, SleepMapper.class.getName());
    mapStageConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
        SleepInputFormat.class.getName());
    if (numIReducer == 0 && numReducer == 0) {
      mapStageConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
          NullOutputFormat.class.getName());
    }

    MRHelpers.translateMRConfToTez(mapStageConf);

    Configuration[] intermediateReduceStageConfs = null;
    if (iReduceStagesCount > 0
        && numIReducer > 0) {
      intermediateReduceStageConfs = new JobConf[iReduceStagesCount];
      for (int i = 1; i <= iReduceStagesCount; ++i) {
        JobConf iReduceStageConf = new JobConf(conf);
        iReduceStageConf.setLong(MRRSleepJob.REDUCE_SLEEP_TIME, iReduceSleepTime);
        iReduceStageConf.setInt(MRRSleepJob.REDUCE_SLEEP_COUNT, iReduceSleepCount);
        iReduceStageConf.setInt(MRJobConfig.NUM_REDUCES, numIReducer);
        iReduceStageConf
            .set(MRJobConfig.REDUCE_CLASS_ATTR, ISleepReducer.class.getName());
        iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
            IntWritable.class.getName());
        iReduceStageConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
            IntWritable.class.getName());
        iReduceStageConf.set(MRJobConfig.PARTITIONER_CLASS_ATTR,
            MRRSleepJobPartitioner.class.getName());


        MRHelpers.translateMRConfToTez(iReduceStageConf);
        intermediateReduceStageConfs[i-1] = iReduceStageConf;
      }
    }

    Configuration finalReduceConf = null;
    if (numReducer > 0) {
      finalReduceConf = new JobConf(conf);
      finalReduceConf.setLong(MRRSleepJob.REDUCE_SLEEP_TIME, reduceSleepTime);
      finalReduceConf.setInt(MRRSleepJob.REDUCE_SLEEP_COUNT, reduceSleepCount);
      finalReduceConf.setInt(MRJobConfig.NUM_REDUCES, numReducer);
      finalReduceConf.set(MRJobConfig.REDUCE_CLASS_ATTR, SleepReducer.class.getName());
      finalReduceConf.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
          IntWritable.class.getName());
      finalReduceConf.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
          IntWritable.class.getName());
      finalReduceConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
          NullOutputFormat.class.getName());

      MRHelpers.translateMRConfToTez(finalReduceConf);
    }

    MRHelpers.configureMRApiUsage(mapStageConf);
    if (iReduceStagesCount > 0
        && numIReducer > 0) {
      for (int i = 0; i < iReduceStagesCount; ++i) {
        MRHelpers.configureMRApiUsage(intermediateReduceStageConfs[i]);
      }
    }
    if (numReducer > 0) {
      MRHelpers.configureMRApiUsage(finalReduceConf);
    }

    DataSourceDescriptor dataSource = null;
    if (!generateSplitsInAM && writeSplitsToDFS) {

      LOG.info("Writing splits to DFS");
      dataSource = MRInputHelpers
          .configureMRInputWithLegacySplitGeneration(mapStageConf, remoteStagingDir, true);
    } else {
      dataSource = MRInputLegacy.createConfigBuilder(mapStageConf, SleepInputFormat.class)
          .generateSplitsInAM(generateSplitsInAM).build();
    }

    DAG dag = DAG.create("MRRSleepJob");
    String jarPath = ClassUtil.findContainingJar(getClass());
    if (jarPath == null)  {
        throw new TezUncheckedException("Could not find any jar containing"
            + " MRRSleepJob.class in the classpath");
    }
    Path remoteJarPath = remoteFs.makeQualified(
        new Path(remoteStagingDir, "dag_job.jar"));
    remoteFs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
    FileStatus jarFileStatus = remoteFs.getFileStatus(remoteJarPath);
    
    TokenCache.obtainTokensForNamenodes(this.credentials, new Path[] { remoteJarPath },
        mapStageConf);

    Map<String, LocalResource> commonLocalResources =
        new HashMap<String, LocalResource>();
    LocalResource dagJarLocalRsrc = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromPath(remoteJarPath),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        jarFileStatus.getLen(),
        jarFileStatus.getModificationTime());
    commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);

    List<Vertex> vertices = new ArrayList<Vertex>();

    
    UserPayload mapUserPayload = TezUtils.createUserPayloadFromConf(mapStageConf);
    int numTasks = generateSplitsInAM ? -1 : numMapper;

    Map<String, String> mapEnv = Maps.newHashMap();
    MRHelpers.updateEnvBasedOnMRTaskEnv(mapStageConf, mapEnv, true);
    Map<String, String> reduceEnv = Maps.newHashMap();
    MRHelpers.updateEnvBasedOnMRTaskEnv(mapStageConf, reduceEnv, false);

    Vertex mapVertex = Vertex.create("map", ProcessorDescriptor.create(
        MapProcessor.class.getName()).setUserPayload(mapUserPayload), numTasks,
        MRHelpers.getResourceForMRMapper(mapStageConf));
    mapVertex.addTaskLocalFiles(commonLocalResources)
        .addDataSource("MRInput", dataSource)
        .setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRMapper(mapStageConf)).setTaskEnvironment(mapEnv);
    vertices.add(mapVertex);

    if (iReduceStagesCount > 0
        && numIReducer > 0) {      
      for (int i = 0; i < iReduceStagesCount; ++i) {
        Configuration iconf =
            intermediateReduceStageConfs[i];
        UserPayload iReduceUserPayload = TezUtils.createUserPayloadFromConf(iconf);
        Vertex ivertex = Vertex.create("ireduce" + (i + 1),
            ProcessorDescriptor.create(ReduceProcessor.class.getName()).
                setUserPayload(iReduceUserPayload), numIReducer,
            MRHelpers.getResourceForMRReducer(intermediateReduceStageConfs[i]));
        ivertex.addTaskLocalFiles(commonLocalResources)
            .setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(
                intermediateReduceStageConfs[i])).setTaskEnvironment(reduceEnv);
        vertices.add(ivertex);
      }
    }

    Vertex finalReduceVertex = null;
    if (numReducer > 0) {
      UserPayload reducePayload = TezUtils.createUserPayloadFromConf(finalReduceConf);
      finalReduceVertex = Vertex.create("reduce", ProcessorDescriptor.create(
          ReduceProcessor.class.getName()).setUserPayload(reducePayload), numReducer,
          MRHelpers.getResourceForMRReducer(finalReduceConf));
      finalReduceVertex.addTaskLocalFiles(commonLocalResources)
          .addDataSink("MROutput", MROutputLegacy.createConfigBuilder(
              finalReduceConf, NullOutputFormat.class).build())
          .setTaskLaunchCmdOpts(MRHelpers.getJavaOptsForMRReducer(finalReduceConf))
          .setTaskEnvironment(reduceEnv);
      vertices.add(finalReduceVertex);
    } else {
      // Map only job
      mapVertex.addDataSink("MROutput",
          MROutputLegacy.createConfigBuilder(mapStageConf, NullOutputFormat.class).build());
    }


    Map<String, String> partitionerConf = Maps.newHashMap();
    partitionerConf.put(MRJobConfig.PARTITIONER_CLASS_ATTR, MRRSleepJobPartitioner.class.getName());
    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(IntWritable.class.getName(), IntWritable.class.getName(),
            HashPartitioner.class.getName(), partitionerConf).configureInput().useLegacyInput()
        .done().build();

    for (int i = 0; i < vertices.size(); ++i) {
      dag.addVertex(vertices.get(i));
      if (i != 0) {
        dag.addEdge(
            Edge.create(vertices.get(i - 1), vertices.get(i), edgeConf.createDefaultEdgeProperty()));
      }
    }

    return dag;
  }

  @VisibleForTesting
  public Job createJob(int numMapper, int numReducer, int iReduceStagesCount,
      int numIReducer, long mapSleepTime, int mapSleepCount,
      long reduceSleepTime, int reduceSleepCount,
      long iReduceSleepTime, int iReduceSleepCount)
          throws IOException {
    Configuration conf = getConf();
    conf.setLong(MAP_SLEEP_TIME, mapSleepTime);
    conf.setLong(REDUCE_SLEEP_TIME, reduceSleepTime);
    conf.setLong(IREDUCE_SLEEP_TIME, iReduceSleepTime);
    conf.setInt(MAP_SLEEP_COUNT, mapSleepCount);
    conf.setInt(REDUCE_SLEEP_COUNT, reduceSleepCount);
    conf.setInt(IREDUCE_SLEEP_COUNT, iReduceSleepCount);
    conf.setInt(MRJobConfig.NUM_MAPS, numMapper);
    conf.setInt(IREDUCE_STAGES_COUNT, iReduceStagesCount);
    conf.setInt(IREDUCE_TASKS_COUNT, numIReducer);

    // Configure intermediate reduces
    conf.setInt(
        org.apache.tez.mapreduce.hadoop.MRJobConfig.MRR_INTERMEDIATE_STAGES,
        iReduceStagesCount);
    LOG.info("Running MRR with " + iReduceStagesCount + " IR stages");

    for (int i = 1; i <= iReduceStagesCount; ++i) {
      // Set reducer class for intermediate reduce
      conf.setClass(
          MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(i,
              "mapreduce.job.reduce.class"), ISleepReducer.class, Reducer.class);
      // Set reducer output key class
      conf.setClass(
          MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(i,
              "mapreduce.map.output.key.class"), IntWritable.class, Object.class);
      // Set reducer output value class
      conf.setClass(
          MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(i,
              "mapreduce.map.output.value.class"), IntWritable.class, Object.class);
      conf.setInt(
          MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(i,
              "mapreduce.job.reduces"), numIReducer);
    }

    Job job = Job.getInstance(conf, "sleep");
    job.setNumReduceTasks(numReducer);
    job.setJarByClass(MRRSleepJob.class);
    job.setNumReduceTasks(numReducer);
    job.setMapperClass(SleepMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(SleepReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setInputFormatClass(SleepInputFormat.class);
    job.setPartitionerClass(MRRSleepJobPartitioner.class);
    job.setSpeculativeExecution(false);
    job.setJobName("Sleep job");

    FileInputFormat.addInputPath(job, new Path("ignored"));
    return job;
  }

  public int run(String[] args) throws Exception {

    if(args.length < 1) {
      System.err.println("MRRSleepJob [-m numMapper] [-r numReducer]" +
          " [-ir numIntermediateReducer]" +
          " [-irs numIntermediateReducerStages]" +
          " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]" +
          " [-irt intermediateReduceSleepTime]" +
          " [-recordt recordSleepTime (msec)]" +
          " [-generateSplitsInAM (false)/true]" +
          " [-writeSplitsToDfs (false)/true]");
      ToolRunner.printGenericCommandUsage(System.err);
      return 2;
    }

    int numMapper = 1, numReducer = 1, numIReducer = 1;
    long mapSleepTime = 100, reduceSleepTime = 100, recSleepTime = 100,
        iReduceSleepTime=1;
    int mapSleepCount = 1, reduceSleepCount = 1, iReduceSleepCount = 1;
    int iReduceStagesCount = 1;
    boolean writeSplitsToDfs = false;
    boolean generateSplitsInAM = false;
    boolean splitsOptionFound = false;

    for(int i=0; i < args.length; i++ ) {
      if(args[i].equals("-m")) {
        numMapper = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-r")) {
        numReducer = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-ir")) {
        numIReducer = Integer.parseInt(args[++i]);
      }
      else if(args[i].equals("-mt")) {
        mapSleepTime = Long.parseLong(args[++i]);
      }
      else if(args[i].equals("-rt")) {
        reduceSleepTime = Long.parseLong(args[++i]);
      }
      else if(args[i].equals("-irt")) {
        iReduceSleepTime = Long.parseLong(args[++i]);
      }
      else if(args[i].equals("-irs")) {
        iReduceStagesCount = Integer.parseInt(args[++i]);
      }
      else if (args[i].equals("-recordt")) {
        recSleepTime = Long.parseLong(args[++i]);
      }
      else if (args[i].equals("-generateSplitsInAM")) {
        if (splitsOptionFound) {
          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
        }
        splitsOptionFound = true;
        generateSplitsInAM = Boolean.parseBoolean(args[++i]);
        
      }
      else if (args[i].equals("-writeSplitsToDfs")) {
        if (splitsOptionFound) {
          throw new RuntimeException("Cannot use both -generateSplitsInAm and -writeSplitsToDfs together");
        }
        splitsOptionFound = true;
        writeSplitsToDfs = Boolean.parseBoolean(args[++i]);
      }
    }

    if (numIReducer > 0 && numReducer <= 0) {
      throw new RuntimeException("Cannot have intermediate reduces without"
          + " a final reduce");
    }

    // sleep for *SleepTime duration in Task by recSleepTime per record
    mapSleepCount = (int)Math.ceil(mapSleepTime / ((double)recSleepTime));
    reduceSleepCount = (int)Math.ceil(reduceSleepTime / ((double)recSleepTime));
    iReduceSleepCount = (int)Math.ceil(iReduceSleepTime / ((double)recSleepTime));

    TezConfiguration conf = new TezConfiguration(getConf());
    FileSystem remoteFs = FileSystem.get(conf);

    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
        conf.get(
            TezConfiguration.TEZ_AM_STAGING_DIR,
            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT));
    
    Path remoteStagingDir =
        remoteFs.makeQualified(new Path(conf.get(
            TezConfiguration.TEZ_AM_STAGING_DIR,
            TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT),
            Long.toString(System.currentTimeMillis())));
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir);

    DAG dag = createDAG(remoteFs, conf, remoteStagingDir,
        numMapper, numReducer, iReduceStagesCount, numIReducer,
        mapSleepTime, mapSleepCount, reduceSleepTime, reduceSleepCount,
        iReduceSleepTime, iReduceSleepCount, writeSplitsToDfs, generateSplitsInAM);

    TezClient tezSession = TezClient.create("MRRSleep", conf, false, null, credentials);
    tezSession.start();
    DAGClient dagClient = tezSession.submitDAG(dag);
    dagClient.waitForCompletion();
    tezSession.stop();

    return dagClient.getDAGStatus(null).getState().equals(DAGStatus.State.SUCCEEDED) ? 0 : 1;
  }

}

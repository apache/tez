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
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;

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

    // Configure intermediate reduces
    conf.setInt(MRJobConfig.MRR_INTERMEDIATE_STAGES, 1);

    // Set reducer class for intermediate reduce
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
        "mapreduce.job.reduce.class"), IntSumReducer.class, Reducer.class);
    // Set reducer output key class
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
        "mapreduce.map.output.key.class"), IntWritable.class, Object.class);
    // Set reducer output value class
    conf.setClass(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
        "mapreduce.map.output.value.class"), Text.class, Object.class);
    conf.setInt(MultiStageMRConfigUtil.getPropertyNameForIntermediateStage(1,
        "mapreduce.job.reduces"), 2);

    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "orderedwordcount");
    job.setJarByClass(OrderedWordCount.class);

    // Configure map
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Configure reduce
    job.setReducerClass(MyOrderByNoOpReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    YarnClient yarnClient = new YarnClientImpl();
    yarnClient.init(conf);
    yarnClient.start();

    TezClient tezClient = new TezClient(new TezConfiguration(conf));

    job.submit();
    JobID jobId = job.getJobID();
    ApplicationId appId = TypeConverter.toYarn(jobId).getAppId();

    DAGClient dagClient = tezClient.getDAGClient(appId);
    DAGStatus dagStatus = null;
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

    ExampleDriver.printMRRDAGStatus(dagStatus);
    LOG.info("Application completed. " + "FinalState=" + dagStatus.getState());
    System.exit(dagStatus.getState() == DAGStatus.State.SUCCEEDED ? 0 : 1);
  }

}

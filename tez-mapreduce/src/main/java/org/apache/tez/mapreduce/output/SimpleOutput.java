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
package org.apache.tez.mapreduce.output;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.api.Master;
import org.apache.tez.api.Output;
import org.apache.tez.common.TezTask;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.records.OutputContext;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * {@link SimpleOutput} is an {@link Output} which persists key/value pairs
 * written to it. 
 * 
 * It is compatible with all standard Apache Hadoop MapReduce 
 * {@link OutputFormat} implementations. 
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SimpleOutput implements Output {

  private MRTask task;
  
  boolean useNewApi;
  JobConf jobConf;
  
  org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;
  org.apache.hadoop.mapreduce.OutputFormat newOutputFormat;
  org.apache.hadoop.mapreduce.RecordWriter newRecordWriter;
  
  org.apache.hadoop.mapred.OutputFormat oldOutputFormat;
  org.apache.hadoop.mapred.RecordWriter oldRecordWriter;
  
  private TezCounter outputRecordCounter;
  private TezCounter fileOutputByteCounter; 
  private List<Statistics> fsStats;
  private MRTaskReporter reporter;
  
  @Inject
  public SimpleOutput(
      @Assisted TezTask task
      ) {
  }
  
  public void setTask(MRTask task) {
    this.task = task;
  }
  
  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {

    if (task == null) {
      return;
    }
    
    if (conf instanceof JobConf) {
      jobConf = (JobConf)conf;
    } else {
      jobConf = new JobConf(conf);
    }
    
    useNewApi = jobConf.getUseNewMapper();
    taskAttemptContext = task.getTaskAttemptContext();
    
    outputRecordCounter = task.getOutputRecordsCounter();
    fileOutputByteCounter = task.getFileOutputBytesCounter();

    reporter = task.getMRReporter();
    
    if (useNewApi) {
      try {
        newOutputFormat =
            ReflectionUtils.newInstance(
                taskAttemptContext.getOutputFormatClass(), jobConf);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
      
      List<Statistics> matchedStats = null;
      if (newOutputFormat instanceof 
          org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = 
            MRTask.getFsStatistics(
                org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
                    .getOutputPath(taskAttemptContext), 
                jobConf);
      }
      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes();
      newRecordWriter = 
          newOutputFormat.getRecordWriter(this.taskAttemptContext);
      long bytesOutCurr = getOutputBytes();
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    } else {
      oldOutputFormat = jobConf.getOutputFormat();
      
      List<Statistics> matchedStats = null;
      if (oldOutputFormat instanceof org.apache.hadoop.mapred.FileOutputFormat) {
        matchedStats = 
            MRTask.getFsStatistics(
                org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(
                    jobConf), 
                jobConf);
      }
      fsStats = matchedStats;

      FileSystem fs = FileSystem.get(jobConf);
      String finalName = task.getOutputName();

      long bytesOutPrev = getOutputBytes();
      oldRecordWriter = 
          oldOutputFormat.getRecordWriter(
              fs, jobConf, finalName, reporter);
      long bytesOutCurr = getOutputBytes();
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }
  }
  
  public void write(Object key, Object value) 
      throws IOException, InterruptedException {

    reporter.progress();
    long bytesOutPrev = getOutputBytes();
  
    if (useNewApi) {
      newRecordWriter.write(key, value);
    } else {
      oldRecordWriter.write(key, value);
    }
    
    long bytesOutCurr = getOutputBytes();
    fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    outputRecordCounter.increment(1);

  }

  public void close() throws IOException, InterruptedException {
    reporter.progress();
    long bytesOutPrev = getOutputBytes();
    if (useNewApi) {
      newRecordWriter.close(taskAttemptContext);
    } else {
      oldRecordWriter.close(null);
    }
    long bytesOutCurr = getOutputBytes();
    fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
  }

  public org.apache.hadoop.mapreduce.OutputFormat getNewOutputFormat() {
    return newOutputFormat;
  }
  
  public org.apache.hadoop.mapred.OutputFormat getOldOutputFormat() {
    return oldOutputFormat;
  }
  
  private long getOutputBytes() {
    if (fsStats == null) return 0;
    long bytesWritten = 0;
    for (Statistics stat: fsStats) {
      bytesWritten = bytesWritten + stat.getBytesWritten();
    }
    return bytesWritten;
  }

  @Override
  public OutputContext getOutputContext() {
    return null;
  }

}

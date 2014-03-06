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
import java.text.NumberFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class MROutput implements LogicalOutput {

  private static final Log LOG = LogFactory.getLog(MROutput.class);

  private final NumberFormat taskNumberFormat = NumberFormat.getInstance();
  private final NumberFormat nonTaskNumberFormat = NumberFormat.getInstance();
  
  private TezOutputContext outputContext;
  private JobConf jobConf;
  boolean useNewApi;
  private AtomicBoolean closed = new AtomicBoolean(false);

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.OutputFormat newOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.RecordWriter newRecordWriter;

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.OutputFormat oldOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.RecordWriter oldRecordWriter;

  private TezCounter outputRecordCounter;

  private TaskAttemptContext newApiTaskAttemptContext;
  private org.apache.hadoop.mapred.TaskAttemptContext oldApiTaskAttemptContext;

  private boolean isMapperOutput;

  protected OutputCommitter committer;

  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws IOException, InterruptedException {
    LOG.info("Initializing Simple Output");
    outputContext.requestInitialMemory(0l, null); //mandatory call
    taskNumberFormat.setMinimumIntegerDigits(5);
    taskNumberFormat.setGroupingUsed(false);
    nonTaskNumberFormat.setMinimumIntegerDigits(3);
    nonTaskNumberFormat.setGroupingUsed(false);
    this.outputContext = outputContext;
    Configuration conf = TezUtils.createConfFromUserPayload(
        outputContext.getUserPayload());
    this.jobConf = new JobConf(conf);
    // Add tokens to the jobConf - in case they are accessed within the RW / OF
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());
    this.useNewApi = this.jobConf.getUseNewMapper();
    this.isMapperOutput = jobConf.getBoolean(MRConfig.IS_MAP_PROCESSOR,
        false);
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        outputContext.getDAGAttemptNumber());
    TaskAttemptID taskAttemptId = org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl
        .createMockTaskAttemptID(outputContext, isMapperOutput);
    jobConf.set(JobContext.TASK_ATTEMPT_ID, taskAttemptId.toString());
    jobConf.set(JobContext.TASK_ID, taskAttemptId.getTaskID().toString());
    jobConf.setBoolean(JobContext.TASK_ISMAP, isMapperOutput);
    jobConf.setInt(JobContext.TASK_PARTITION,
      taskAttemptId.getTaskID().getId());
    jobConf.set(JobContext.ID, taskAttemptId.getJobID().toString());
    
    if (useNewApi) {
      // set the output part name to have a unique prefix
      if (jobConf.get("mapreduce.output.basename") == null) {
        jobConf.set("mapreduce.output.basename", getOutputFileNamePrefix());
      }
    }

    outputRecordCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);    

    if (useNewApi) {
      newApiTaskAttemptContext = createTaskAttemptContext(taskAttemptId);
      try {
        newOutputFormat =
            ReflectionUtils.newInstance(
                newApiTaskAttemptContext.getOutputFormatClass(), jobConf);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }

      try {
        newRecordWriter =
            newOutputFormat.getRecordWriter(newApiTaskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while creating record writer", e);
      }
    } else {
      oldApiTaskAttemptContext =
          new org.apache.tez.mapreduce.hadoop.mapred.TaskAttemptContextImpl(
              jobConf, taskAttemptId,
              new MRTaskReporter(outputContext));
      oldOutputFormat = jobConf.getOutputFormat();

      FileSystem fs = FileSystem.get(jobConf);
      String finalName = getOutputName();

      oldRecordWriter =
          oldOutputFormat.getRecordWriter(
              fs, jobConf, finalName, new MRReporter(outputContext));
    }
    initCommitter(jobConf, useNewApi);

    LOG.info("Initialized Simple Output"
        + ", using_new_api: " + useNewApi);
    return null;
  }
  
  @Override
  public void start() {
  }

  public void initCommitter(JobConf job, boolean useNewApi)
      throws IOException, InterruptedException {

    if (useNewApi) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using new api for output committer");
      }

      OutputFormat<?, ?> outputFormat = null;
      try {
        outputFormat = ReflectionUtils.newInstance(
            newApiTaskAttemptContext.getOutputFormatClass(), job);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException("Unknown OutputFormat", cnfe);
      }
      this.committer = outputFormat.getOutputCommitter(
          newApiTaskAttemptContext);
    } else {
      this.committer = job.getOutputCommitter();
    }

    Path outputPath = FileOutputFormat.getOutputPath(job);
    if (outputPath != null) {
      if ((this.committer instanceof FileOutputCommitter)) {
        FileOutputFormat.setWorkOutputPath(job,
            ((FileOutputCommitter) this.committer).getTaskAttemptPath(
                oldApiTaskAttemptContext));
      } else {
        FileOutputFormat.setWorkOutputPath(job, outputPath);
      }
    }
    if (useNewApi) {
      this.committer.setupTask(newApiTaskAttemptContext);
    } else {
      this.committer.setupTask(oldApiTaskAttemptContext);
    }
  }

  public boolean isCommitRequired() throws IOException {
    if (useNewApi) {
      return committer.needsTaskCommit(newApiTaskAttemptContext);
    } else {
      return committer.needsTaskCommit(oldApiTaskAttemptContext);
    }
  }

  private TaskAttemptContext createTaskAttemptContext(TaskAttemptID attemptId) {
    return new TaskAttemptContextImpl(this.jobConf, attemptId, outputContext,
        isMapperOutput, null);
  }

  private String getOutputFileNamePrefix() {
    String prefix = jobConf.get(MRJobConfig.MROUTPUT_FILE_NAME_PREFIX);
    if (prefix == null) {
      prefix = "part-v" + 
          nonTaskNumberFormat.format(outputContext.getTaskVertexIndex()) +  
          "-o" + nonTaskNumberFormat.format(outputContext.getOutputIndex());
    }
    return prefix;
  }

  private String getOutputName() {
    // give a unique prefix to the output name
    return getOutputFileNamePrefix() + 
        "-" + taskNumberFormat.format(outputContext.getTaskIndex());
  }

  @Override
  public KeyValueWriter getWriter() throws IOException {
    return new KeyValueWriter() {
      private final boolean useNewWriter = useNewApi;

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object key, Object value) throws IOException {
        if (useNewWriter) {
          try {
            newRecordWriter.write(key, value);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing next key-value",e);
          }
        } else {
          oldRecordWriter.write(key, value);
        }
        outputRecordCounter.increment(1);
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    // Not expecting any events at the moment.
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    if (closed.getAndSet(true)) {
      return null;
    }

    LOG.info("Closing Simple Output");
    if (useNewApi) {
      try {
        newRecordWriter.close(newApiTaskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing record writer", e);
      }
    } else {
      oldRecordWriter.close(null);
    }
    LOG.info("Closed Simple Output");
    return null;
  }

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    // Nothing to do for now
  }

  /**
   * MROutput expects that a Processor call commit prior to the
   * Processor's completion
   * @throws IOException
   */
  public void commit() throws IOException {
    close();
    if (useNewApi) {
      committer.commitTask(newApiTaskAttemptContext);
    } else {
      committer.commitTask(oldApiTaskAttemptContext);
    }
  }


  /**
   * MROutput expects that a Processor call abort in case of any error
   * ( including an error during commit ) prior to the Processor's completion
   * @throws IOException
   */
  public void abort() throws IOException {
    close();
    if (useNewApi) {
      committer.abortTask(newApiTaskAttemptContext);
    } else {
      committer.abortTask(oldApiTaskAttemptContext);
    }
  }

}

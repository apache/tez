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

package org.apache.tez.mapreduce.committer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;

import java.io.IOException;

/**
 * Implements the {@link OutputCommitter} and provide Map Reduce compatible
 * output commit operations for Map Reduce compatible data sinks. 
 */
@Public
public class MROutputCommitter extends OutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(MROutputCommitter.class);

  private org.apache.hadoop.mapreduce.OutputCommitter committer = null;
  private JobContext jobContext = null;
  private volatile boolean initialized = false;
  private JobConf jobConf = null;
  private boolean newApiCommitter;

  public MROutputCommitter(OutputCommitterContext committerContext) {
    super(committerContext);
  }

  @Override
  public void initialize() throws IOException {
    UserPayload userPayload = getContext().getOutputUserPayload();
    if (!userPayload.hasPayload()) {
      jobConf = new JobConf();
    } else {
      jobConf = new JobConf(
          TezUtils.createConfFromUserPayload(userPayload));
    }
    
    // Read all credentials into the credentials instance stored in JobConf.
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        getContext().getDAGAttemptNumber());
    committer = getOutputCommitter(getContext());
    jobContext = getJobContextFromVertexContext(getContext());
    initialized = true;
  }

  @Override
  public void setupOutput() throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    committer.setupJob(jobContext);
  }

  @Override
  public void commitOutput() throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    committer.commitJob(jobContext);
  }

  @Override
  public void abortOutput(VertexStatus.State finalState) throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    JobStatus.State jobState = getJobStateFromVertexStatusState(finalState);
    committer.abortJob(jobContext, jobState);
  }

  @SuppressWarnings("rawtypes")
  private org.apache.hadoop.mapreduce.OutputCommitter
      getOutputCommitter(OutputCommitterContext context) {

    org.apache.hadoop.mapreduce.OutputCommitter committer = null;
    newApiCommitter = false;
    if (jobConf.getBoolean("mapred.reducer.new-api", false)
        || jobConf.getBoolean("mapred.mapper.new-api", false))  {
      newApiCommitter = true;
    }
    LOG.info("Committer for " + getContext().getVertexName() + ":" + getContext().getOutputName() +
        " using " + (newApiCommitter ? "new" : "old") + "mapred API");

    if (newApiCommitter) {
      TaskAttemptID taskAttemptID = new TaskAttemptID(
          Long.toString(context.getApplicationId().getClusterTimestamp()),
          context.getApplicationId().getId(),
          ((jobConf.getBoolean(MRConfig.IS_MAP_PROCESSOR, false) ?
              TaskType.MAP : TaskType.REDUCE)),
          0, context.getDAGAttemptNumber());

      TaskAttemptContext taskContext = new TaskAttemptContextImpl(jobConf,
          taskAttemptID);
      try {
        OutputFormat outputFormat = ReflectionUtils.newInstance(taskContext
            .getOutputFormatClass(), jobConf);
        committer = outputFormat.getOutputCommitter(taskContext);
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
    } else {
      committer = ReflectionUtils.newInstance(jobConf.getClass(
          "mapred.output.committer.class", FileOutputCommitter.class,
          org.apache.hadoop.mapred.OutputCommitter.class), jobConf);
    }
    LOG.info("OutputCommitter for outputName="
        + context.getOutputName()
        + ", vertexName=" + context.getVertexName()
        + ", outputCommitterClass="
        + committer.getClass().getName());
    return committer;
  }

  // FIXME we are using ApplicationId as DAG id
  private JobContext getJobContextFromVertexContext(OutputCommitterContext context)
      throws IOException {
    JobID jobId = TypeConverter.fromYarn(
        context.getApplicationId());
    return new MRJobContextImpl(jobConf, jobId);
  }

  private JobStatus.State getJobStateFromVertexStatusState(VertexStatus.State state) {
    switch(state) {
      case INITED:
        return JobStatus.State.PREP;
      case RUNNING:
        return JobStatus.State.RUNNING;
      case SUCCEEDED:
        return JobStatus.State.SUCCEEDED;
      case KILLED:
        return JobStatus.State.KILLED;
      case FAILED:
      case ERROR:
        return JobStatus.State.FAILED;
      default:
        throw new TezUncheckedException("Unknown VertexStatus.State: " + state);
    }
  }

  private static class MRJobContextImpl
      extends org.apache.hadoop.mapred.JobContextImpl {

    public MRJobContextImpl(JobConf jobConf, JobID jobId) {
      super(jobConf, jobId);
    }

  }

  @Override
  public boolean isTaskRecoverySupported() {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    return committer.isRecoverySupported();
  }

  @Override
  public void recoverTask(int taskIndex, int attemptId) throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    TaskAttemptID taskAttemptID = new TaskAttemptID(
        Long.toString(getContext().getApplicationId().getClusterTimestamp())
        + String.valueOf(getContext().getVertexIndex()),
        getContext().getApplicationId().getId(),
        ((jobConf.getBoolean(MRConfig.IS_MAP_PROCESSOR, false) ?
            TaskType.MAP : TaskType.REDUCE)),
        taskIndex, attemptId);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(jobConf,
        taskAttemptID);
    committer.recoverTask(taskContext);
  }

}

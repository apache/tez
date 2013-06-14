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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.committer.VertexContext;
import org.apache.tez.dag.api.committer.VertexOutputCommitter;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class MRVertexOutputCommitter extends VertexOutputCommitter {

  private static final Log LOG = LogFactory.getLog(
      MRVertexOutputCommitter.class);

  private OutputCommitter committer;
  private JobContext jobContext;
  private volatile boolean initialized = false;

  public MRVertexOutputCommitter() {
  }

  @SuppressWarnings("rawtypes")
  private OutputCommitter getOutputCommitter(VertexContext context) {
    Configuration conf = context.getConf();

    OutputCommitter committer = null;
    boolean newApiCommitter = false;
    if (conf.getBoolean("mapred.reducer.new-api", false)
        || conf.getBoolean("mapred.mapper.new-api", false))  {
      newApiCommitter = true;
      LOG.info("Using mapred newApiCommitter.");
    }
    
    LOG.info("OutputCommitter set in config for vertex: "
        + context.getVertexId() + " : "
        + conf.get("mapred.output.committer.class"));

    if (newApiCommitter) {
      TezTaskID taskId = TezBuilderUtils.newTaskId(context.getDAGId(),
          context.getVertexId().getId(), 0);
      TezTaskAttemptID attemptID =
          TezBuilderUtils.newTaskAttemptId(taskId, 0);
      TaskAttemptContext taskContext = new TaskAttemptContextImpl(conf,
          TezMRTypeConverter.fromTez(attemptID));
      try {
        OutputFormat outputFormat = ReflectionUtils.newInstance(taskContext
            .getOutputFormatClass(), conf);
        committer = outputFormat.getOutputCommitter(taskContext);
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
    } else {
      committer = ReflectionUtils.newInstance(conf.getClass(
          "mapred.output.committer.class", FileOutputCommitter.class,
          org.apache.hadoop.mapred.OutputCommitter.class), conf);
    }
    LOG.info("OutputCommitter is " + committer.getClass().getName());
    return committer;
  }

  // FIXME we are using ApplicationId as DAG id
  private JobContext getJobContextFromVertexContext(VertexContext context)
      throws IOException {
    // FIXME when we have the vertex level user-land configuration
    // jobConf should be initialized using the user-land level configuration
    // for the vertex in question

    Configuration conf = context.getConf();

    JobConf jobConf = new JobConf(conf);
    JobID jobId = TypeConverter.fromYarn(context.getDAGId().getApplicationId());
    jobConf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    return new MRJobContextImpl(jobConf, jobId);
  }

  private State getJobStateFromVertexStatusState(VertexStatus.State state) {
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

  @Override
  public void init(VertexContext context) throws IOException {
    // TODO VertexContext not the best way to get ApplicationAttemptId. No
    // alternates rightnow.
    context.getConf().setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        context.getApplicationAttemptId().getAttemptId());
    committer = getOutputCommitter(context);
    jobContext = getJobContextFromVertexContext(context);
    initialized = true;
  }

  @Override
  public void setupVertex() throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    committer.setupJob(jobContext);
  }

  @Override
  public void commitVertex() throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    committer.commitJob(jobContext);
  }

  @Override
  public void abortVertex(VertexStatus.State finalState) throws IOException {
    if (!initialized) {
      throw new RuntimeException("Committer not initialized");
    }
    State jobState = getJobStateFromVertexStatusState(finalState);
    committer.abortJob(jobContext, jobState);
  }

  private static class MRJobContextImpl
      extends org.apache.hadoop.mapred.JobContextImpl {

    public MRJobContextImpl(JobConf jobConf, JobID jobId) {
      super(jobConf, jobId);
    }

  }

}

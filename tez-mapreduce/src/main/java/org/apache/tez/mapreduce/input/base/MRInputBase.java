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

package org.apache.tez.mapreduce.input.base;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.InputContext;

import java.io.IOException;
import java.util.List;


@InterfaceAudience.Private
public abstract class MRInputBase extends AbstractLogicalInput {

  protected JobConf jobConf;
  protected TezCounter inputRecordCounter;

  public MRInputBase(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public Reader getReader() throws Exception {
    return null;
  }

  @InterfaceAudience.Private
  protected boolean useNewApi;

  public List<Event> initialize() throws IOException {
    getContext().requestInitialMemory(0l, null); // mandatory call
    MRRuntimeProtos.MRInputUserPayloadProto mrUserPayload =
        MRInputHelpers.parseMRInputPayload(getContext().getUserPayload());
    boolean isGrouped = mrUserPayload.getGroupingEnabled();
    Preconditions.checkArgument(mrUserPayload.hasSplits() == false,
        "Split information not expected in " + this.getClass().getName());
    Configuration conf = TezUtils
        .createConfFromByteString(mrUserPayload.getConfigurationBytes());
    this.jobConf = new JobConf(conf);
    useNewApi = this.jobConf.getUseNewMapper();
    if (isGrouped) {
      if (useNewApi) {
        jobConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR,
            org.apache.hadoop.mapreduce.split.TezGroupedSplitsInputFormat.class.getName());
      } else {
        jobConf.set("mapred.input.format.class",
            org.apache.hadoop.mapred.split.TezGroupedSplitsInputFormat.class.getName());
      }
    }


    // Add tokens to the jobConf - in case they are accessed within the RR / IF
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

    TaskAttemptID taskAttemptId = new TaskAttemptID(
        new TaskID(
            Long.toString(getContext().getApplicationId().getClusterTimestamp()),
            getContext().getApplicationId().getId(), TaskType.MAP,
            getContext().getTaskIndex()),
        getContext().getTaskAttemptNumber());

    jobConf.set(MRJobConfig.TASK_ATTEMPT_ID,
        taskAttemptId.toString());
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        getContext().getDAGAttemptNumber());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_DAG_INDEX, getContext().getDagIdentifier());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_VERTEX_INDEX, getContext().getTaskVertexIndex());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_TASK_INDEX, getContext().getTaskIndex());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_TASK_ATTEMPT_INDEX, getContext().getTaskAttemptNumber());
    jobConf.set(MRInput.TEZ_MAPREDUCE_DAG_NAME, getContext().getDAGName());
    jobConf.set(MRInput.TEZ_MAPREDUCE_VERTEX_NAME, getContext().getTaskVertexName());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_INPUT_INDEX, getContext().getInputIndex());
    jobConf.set(MRInput.TEZ_MAPREDUCE_INPUT_NAME, getContext().getSourceVertexName());
    jobConf.set(MRInput.TEZ_MAPREDUCE_APPLICATION_ID, getContext().getApplicationId().toString());
    jobConf.set(MRInput.TEZ_MAPREDUCE_UNIQUE_IDENTIFIER, getContext().getUniqueIdentifier());
    jobConf.setInt(MRInput.TEZ_MAPREDUCE_DAG_ATTEMPT_NUMBER, getContext().getDAGAttemptNumber());

    this.inputRecordCounter = getContext().getCounters().findCounter(
        TaskCounter.INPUT_RECORDS_PROCESSED);


    return null;
  }
}

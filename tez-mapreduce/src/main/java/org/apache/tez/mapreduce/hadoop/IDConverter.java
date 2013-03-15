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

package org.apache.tez.mapreduce.hadoop;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.tez.records.TezJobID;
import org.apache.tez.records.TezTaskAttemptID;
import org.apache.tez.records.TezTaskID;

public class IDConverter {

  public static JobID toMRJobId(TezJobID jobId) {
    return new JobID(jobId.getJtIdentifier(), jobId.getId());
  }
  
  @SuppressWarnings("deprecation")
  public static TaskID toMRTaskId(TezTaskID taskid) {
    return new TaskID(
        toMRJobId(taskid.getJobID()),        
        taskid.getTaskType().equals(MRTaskType.MAP.toString()), 
        taskid.getId());

  }
  
  public static TaskAttemptID toMRTaskAttemptId(
      TezTaskAttemptID taskAttemptId) {
    return new TaskAttemptID(
        toMRTaskId(taskAttemptId.getTaskID()),
        taskAttemptId.getId());
  }
  
  public static TezJobID fromMRJobId(org.apache.hadoop.mapreduce.JobID jobId) {
    return new TezJobID(jobId.getJtIdentifier(), jobId.getId());
  }

  public static MRTaskType fromMRTaskType(TaskType type) {
    switch (type) {
      case REDUCE:
        return MRTaskType.REDUCE;
      case JOB_SETUP:
        return MRTaskType.JOB_SETUP;
      case JOB_CLEANUP:
        return MRTaskType.JOB_CLEANUP;
      case TASK_CLEANUP:
        return MRTaskType.TASK_CLEANUP;
      case MAP:
        return MRTaskType.MAP;
      default:
        throw new RuntimeException("Unknown TaskType: " + type);
    }
  }

  public static TezTaskID
      fromMRTaskId(org.apache.hadoop.mapreduce.TaskID taskid) {
    return new TezTaskID(
        fromMRJobId(taskid.getJobID()),
        fromMRTaskType(taskid.getTaskType()).toString(),
        taskid.getId());
  }

  public static TezTaskAttemptID fromMRTaskAttemptId(
      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptId) {
    return new TezTaskAttemptID(
        fromMRTaskId(taskAttemptId.getTaskID()),
        taskAttemptId.getId());
  }
  
}

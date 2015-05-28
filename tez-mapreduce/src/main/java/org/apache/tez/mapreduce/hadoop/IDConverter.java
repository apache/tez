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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

public class IDConverter {

  // FIXME hardcoded assumption that one app is one dag
  public static JobID toMRJobId(TezDAGID dagId) {
    return new JobID(
        Long.toString(dagId.getApplicationId().getClusterTimestamp()),
        dagId.getApplicationId().getId());
  }
  
  public static TaskID toMRTaskId(TezTaskID taskid) {
    return new TaskID(
        toMRJobId(taskid.getVertexID().getDAGId()),
        taskid.getVertexID().getId() == 0 ? TaskType.MAP : TaskType.REDUCE,
        taskid.getId());
  }
  
  public static TaskID toMRTaskIdForOutput(TezTaskID taskid) {
    return org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl
        .createMockTaskAttemptIDFromTezTaskId(taskid, (taskid.getVertexID().getId() == 0));
  }

  public static TaskAttemptID toMRTaskAttemptId(
      TezTaskAttemptID taskAttemptId) {
    return new TaskAttemptID(
        toMRTaskId(taskAttemptId.getTaskID()),
        taskAttemptId.getId());
  }
  
  // FIXME hardcoded assumption that one app is one dag
  public static TezDAGID fromMRJobId(
      org.apache.hadoop.mapreduce.JobID jobId) {
    return TezDAGID.getInstance(ApplicationId.newInstance(
        Long.parseLong(jobId.getJtIdentifier()), jobId.getId()), 1);
  }

  // FIXME hack alert converting objects with hard coded id
  public static TezTaskID
      fromMRTaskId(org.apache.hadoop.mapreduce.TaskID taskid) {
    return TezTaskID.getInstance(
        TezVertexID.getInstance(fromMRJobId(taskid.getJobID()),
                (taskid.getTaskType() == TaskType.MAP ? 0 : 1)
            ),
        taskid.getId());
  }

  public static TezTaskAttemptID fromMRTaskAttemptId(
      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptId) {
    return TezTaskAttemptID.getInstance(
        fromMRTaskId(taskAttemptId.getTaskID()),
        taskAttemptId.getId());
  }
  
}

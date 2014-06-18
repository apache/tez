/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.mapreduce.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.mapreduce.common.Utils;

// NOTE: NEWTEZ: This is a copy of org.apache.tez.mapreduce.hadoop.mapred (not mapreduce). mapred likely does not need it's own copy of this class.
// Meant for use by the "mapreduce" API

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl
       extends org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl {

  private final TezCounters tezCounters;
  private final Reporter reporter;
  
  public static org.apache.hadoop.mapred.TaskAttemptID createMockTaskAttemptID(
      long clusterId, int vertexIndex, int appId, int taskIndex, int taskAttemptNumber, boolean isMap) {
    return new org.apache.hadoop.mapred.TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(String.valueOf(clusterId)
            + String.valueOf(vertexIndex), appId,
            isMap ? TaskType.MAP : TaskType.REDUCE, taskIndex),
        taskAttemptNumber);
  }
  
  public static org.apache.hadoop.mapred.TaskAttemptID 
    createMockTaskAttemptIDFromTezTaskAttemptId(TezTaskAttemptID tezTaId, boolean isMap) {
    TezVertexID vId = tezTaId.getTaskID().getVertexID();
    ApplicationId appId = vId.getDAGId().getApplicationId();
    return new org.apache.hadoop.mapred.TaskAttemptID(
        new org.apache.hadoop.mapred.TaskID(String.valueOf(appId.getClusterTimestamp())
            + String.valueOf(vId.getId()), appId.getId(),
            isMap ? TaskType.MAP : TaskType.REDUCE, tezTaId.getTaskID().getId()),
        tezTaId.getId());
  }
  
  public static org.apache.hadoop.mapred.TaskID 
    createMockTaskAttemptIDFromTezTaskId(TezTaskID tezTaId, boolean isMap) {
    TezVertexID vId = tezTaId.getVertexID();
    ApplicationId appId = vId.getDAGId().getApplicationId();
    return new org.apache.hadoop.mapred.TaskID(String.valueOf(appId.getClusterTimestamp())
            + String.valueOf(vId.getId()), appId.getId(),
            isMap ? TaskType.MAP : TaskType.REDUCE, tezTaId.getId());
  }

  // FIXME we need to use DAG Id but we are using App Id
  public TaskAttemptContextImpl(Configuration conf, TezCounters tezCounters, long clusterId,
      int vertexIndex, int appId, int taskIndex, int taskAttemptNumber, boolean isMap,
      Reporter reporter) {
    // TODO NEWTEZ Can the jt Identifier string be taskContext.getUniqueId ?
    this(conf, createMockTaskAttemptID(clusterId, vertexIndex, appId, taskIndex, taskAttemptNumber,
        isMap), tezCounters, reporter);
  }

  //FIXME we need to use DAG Id but we are using App Id
   public TaskAttemptContextImpl(Configuration conf, TaskAttemptID attemptId, 
       TezCounters tezCounters, boolean isMap, Reporter reporter) {
     // TODO NEWTEZ Can the jt Identifier string be taskContext.getUniqueId ?
     this(conf, attemptId, tezCounters, reporter);
   }
 
  public TaskAttemptContextImpl(Configuration conf, TaskAttemptID taId, TezCounters tezCounters, Reporter reporter) {
    super(conf, taId);
    this.tezCounters = tezCounters;
    this.reporter = reporter;
  }

  @Override
  public float getProgress() {
    // TODO NEWTEZ This is broken. Mainly set after all records are processed. Not set for Inputs/Outputs
    return reporter == null ? 0.0f : reporter.getProgress();
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return Utils.getMRCounter(tezCounters.findCounter(counterName));
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return Utils.getMRCounter(tezCounters.findCounter(groupName, counterName));
  }

  /**
   * Report progress.
   */
  @Override
  public void progress() {
    // Nothing to do.
  }

  /**
   * Set the current status of the task to the given string.
   */
  @Override
  public void setStatus(String status) {
    setStatusString(status);
    // Nothing to do until InputContext supports some kind of custom string
    // diagnostics.
  }
}

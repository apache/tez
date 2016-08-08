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

package org.apache.tez.dag.api.client;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos.TaskStateProto;
import org.apache.tez.dag.api.records.DAGProtos.TaskInformationProto;

public class TaskInformationBuilder extends TaskInformation {

  public TaskInformationBuilder() {
    super(TaskInformationProto.newBuilder());
  }

  public void setState(TaskState taskState) {
    getBuilder().setState(getProtoState(taskState));
  }

  public void setId(String id) {
    getBuilder().setId(id);
  }

  public void setScheduledTime(Long scheduledTime) {
    getBuilder().setScheduledTime(scheduledTime);
  }

  public void setStartTime(Long startTime) {
    getBuilder().setStartTime(startTime);
  }

  public void setEndTime(Long endTime) {
    getBuilder().setEndTime(endTime);
  }

  public void setSuccessfulAttemptId(String successfulAttemptId) {
    getBuilder().setSuccessfulAttemptId(successfulAttemptId);
  }

  public void setTaskCounters(TezCounters counters) {
    getBuilder().setTaskCounters(
      DagTypeConverters.convertTezCountersToProto(counters));
  }

  public TaskInformationProto getProto() {
    return getBuilder().build();
  }

  private TaskStateProto getProtoState(TaskState taskState) {
    switch (taskState) {
      case NEW:
        return TaskStateProto.TASK_NEW;
      case SCHEDULED:
        return TaskStateProto.TASK_SCHEDULED;
      case RUNNING:
        return TaskStateProto.TASK_RUNNING;
      case SUCCEEDED:
        return TaskStateProto.TASK_SUCCEEDED;
      case FAILED:
        return TaskStateProto.TASK_FAILED;
      case KILLED:
        return TaskStateProto.TASK_KILLED;
      default:
        throw new TezUncheckedException("Unsupported value for TaskState: " + taskState);
    }
  }

  private TaskInformationProto.Builder getBuilder() {
    return (TaskInformationProto.Builder) this.proxy;
  }
}

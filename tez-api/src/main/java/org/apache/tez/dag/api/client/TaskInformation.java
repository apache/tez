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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;

/**
 * Some information about Tez tasks.
 */
@Public
public class TaskInformation {

  DAGProtos.TaskInformationProtoOrBuilder proxy = null;

  private TezCounters taskCounters = null;
  private AtomicBoolean countersInitialized = new AtomicBoolean(false);

  @Private
  public TaskInformation(DAGProtos.TaskInformationProtoOrBuilder proxy) {
    this.proxy = proxy;
  }

  public TaskState getState() {
    return getState(proxy.getState());
  }

  private TaskState getState(DAGProtos.TaskStateProto stateProto) {
    switch (stateProto) {
      case TASK_NEW:
        return TaskState.NEW;
      case TASK_SCHEDULED:
        return TaskState.SCHEDULED;
      case TASK_RUNNING:
        return TaskState.RUNNING;
      case TASK_SUCCEEDED:
        return TaskState.SUCCEEDED;
      case TASK_FAILED:
        return TaskState.FAILED;
      case TASK_KILLED:
        return TaskState.KILLED;
      default:
        throw new TezUncheckedException(
          "Unsupported value for TaskState: " + stateProto);
    }
  }

  public String getID() {
    return proxy.getId();
  }

  public String getDiagnostics() {
    return proxy.getDiagnostics();
  }

  public Long getScheduledTime() {
    return proxy.getScheduledTime();
  }

  public Long getStartTime() {
    return proxy.getStartTime();
  }

  public Long getEndTime() {
    return proxy.getEndTime();
  }

  public String getSuccessfulAttemptID() {
    return proxy.getSuccessfulAttemptId();
  }

  public TezCounters getTaskCounters() {
    if (countersInitialized.get()) {
      return taskCounters;
    }
    if (proxy.hasTaskCounters()) {
      taskCounters = DagTypeConverters.convertTezCountersFromProto(
        proxy.getTaskCounters());
    }
    countersInitialized.set(true);
    return taskCounters;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    if (obj instanceof TaskInformation) {
      TaskInformation other = (TaskInformation) obj;
      return getState().equals(other.getState())
        && getID().equals(other.getID())
        && getDiagnostics().equals(other.getDiagnostics())
        && getScheduledTime().equals(other.getScheduledTime())
        && getStartTime().equals(other.getStartTime())
        && getEndTime().equals(other.getEndTime())
        && (( getSuccessfulAttemptID() == null && other.getSuccessfulAttemptID() == null)
          || getSuccessfulAttemptID().equals(other.getSuccessfulAttemptID()))
        &&
        ((getTaskCounters() == null && other.getTaskCounters() == null)
          || getTaskCounters().equals(other.getTaskCounters()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 46021;
    int result = prime + getState().hashCode();

    String id = getID();
    String diagnostics = getDiagnostics();
    Long scheduledTime = getScheduledTime();
    Long startTime = getStartTime();
    Long endTime = getEndTime();
    String successfulAttemptId = getSuccessfulAttemptID();
    TezCounters counters = getTaskCounters();

    result = prime * result +
      ((id == null)? 0 : id.hashCode());
    result = prime * result +
      ((diagnostics == null)? 0 : diagnostics.hashCode());
    result = prime * result +
      ((scheduledTime == null)? 0 : scheduledTime.hashCode());
    result = prime * result +
      ((startTime == null)? 0 : startTime.hashCode());
    result = prime * result +
      ((endTime == null)? 0 : endTime.hashCode());
    result = prime * result +
      ((successfulAttemptId == null)? 0 : successfulAttemptId.hashCode());
    result = prime * result +
      ((counters == null)? 0 : counters.hashCode());

    return result;
  }

  @Override
  public String toString() {
    return ("state=" + getState()
      + ", id=" + getID()
      + ", diagnostics=" + getDiagnostics()
      + ", scheduledTime=" + getScheduledTime()
      + ", startTime=" + getStartTime()
      + ", endTime=" + getEndTime()
      + ", successfulAttemptId=" + ( getSuccessfulAttemptID() == null ? "null" : getSuccessfulAttemptID())
      + ", counters="
      + (getTaskCounters() == null ? "null" : getTaskCounters().toString()));
  }
}

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

package org.apache.tez.dag.history.events;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TaskFinishedProto;

public class TaskFinishedEvent implements HistoryEvent {

  private static final Logger LOG = LoggerFactory.getLogger(TaskFinishedEvent.class);

  private TezTaskID taskID;
  private String vertexName;
  private long startTime;
  private long finishTime;
  private TaskState state;
  private TezCounters tezCounters;
  private TezTaskAttemptID successfulAttemptID;
  private String diagnostics;
  private int numFailedAttempts;

  public TaskFinishedEvent(TezTaskID taskID,
      String vertexName, long startTime, long finishTime,
      TezTaskAttemptID successfulAttemptID,
      TaskState state, String diagnostics, TezCounters counters, int failedAttempts) {
    this.vertexName = vertexName;
    this.taskID = taskID;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.state = state;
    this.diagnostics = diagnostics;
    this.tezCounters = counters;
    this.successfulAttemptID = successfulAttemptID;
    this.numFailedAttempts = failedAttempts;
  }

  public TaskFinishedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.TASK_FINISHED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public TaskFinishedProto toProto() {
    TaskFinishedProto.Builder builder = TaskFinishedProto.newBuilder();
    builder.setTaskId(taskID.toString())
        .setState(state.ordinal())
        .setFinishTime(finishTime);
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    if (successfulAttemptID != null) {
      builder.setSuccessfulTaskAttemptId(successfulAttemptID.toString());
    }
    return builder.build();
  }

  public void fromProto(TaskFinishedProto proto) {
    this.taskID = TezTaskID.fromString(proto.getTaskId());
    this.finishTime = proto.getFinishTime();
    this.state = TaskState.values()[proto.getState()];
    if (proto.hasDiagnostics()) {
      this.diagnostics = proto.getDiagnostics();
    }
    if (proto.hasSuccessfulTaskAttemptId()) {
      this.successfulAttemptID =
          TezTaskAttemptID.fromString(proto.getSuccessfulTaskAttemptId());
    }
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    TaskFinishedProto proto = inputStream.readMessage(TaskFinishedProto.PARSER, null);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("vertexName=");
    sb.append(vertexName);
    sb.append(", taskId=");
    sb.append(taskID);
    sb.append(", startTime=");
    sb.append(startTime);
    sb.append(", finishTime=");
    sb.append(finishTime);
    sb.append(", timeTaken=");
    sb.append(finishTime - startTime);
    sb.append(", status=");
    sb.append(state.name());
    sb.append(", successfulAttemptID=");
    sb.append(successfulAttemptID);
    sb.append(", diagnostics=");
    sb.append(diagnostics);
    sb.append(", counters=");
    if (tezCounters == null) {
      sb.append("null");
    } else {
      sb.append("Counters: ");
      sb.append(tezCounters.countCounters());
      for (CounterGroup group : tezCounters) {
        sb.append(", ");
        sb.append(group.getDisplayName());
        for (TezCounter counter : group) {
          sb.append(", ");
          sb.append(counter.getDisplayName()).append("=")
              .append(counter.getValue());
        }
      }
    }
    return sb.toString();
  }

  public TezTaskID getTaskID() {
    return taskID;
  }

  public TaskState getState() {
    return state;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }

  public TezTaskAttemptID getSuccessfulAttemptID() {
    return successfulAttemptID;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public int getNumFailedAttempts() {
    return numFailedAttempts;
  }
}

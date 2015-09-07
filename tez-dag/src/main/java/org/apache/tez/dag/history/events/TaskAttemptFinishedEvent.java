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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl.DataEventDependencyInfo;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DataEventDependencyInfoProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TaskAttemptFinishedProto;

public class TaskAttemptFinishedEvent implements HistoryEvent {

  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptFinishedEvent.class);

  private TezTaskAttemptID taskAttemptId;
  private String vertexName;
  private long startTime;
  private long finishTime;
  private TaskAttemptState state;
  private String diagnostics;
  private TezCounters tezCounters;
  private TaskAttemptTerminationCause error;
  private List<DataEventDependencyInfo> dataEvents;
  
  public TaskAttemptFinishedEvent(TezTaskAttemptID taId,
      String vertexName,
      long startTime,
      long finishTime,
      TaskAttemptState state,
      TaskAttemptTerminationCause error,
      String diagnostics, TezCounters counters, 
      List<DataEventDependencyInfo> dataEvents) {
    this.taskAttemptId = taId;
    this.vertexName = vertexName;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.state = state;
    this.diagnostics = diagnostics;
    this.tezCounters = counters;
    this.error = error;
    this.dataEvents = dataEvents;
  }

  public TaskAttemptFinishedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.TASK_ATTEMPT_FINISHED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }
  
  public List<DataEventDependencyInfo> getDataEvents() {
    return dataEvents;
  }
  
  public TaskAttemptFinishedProto toProto() {
    TaskAttemptFinishedProto.Builder builder =
        TaskAttemptFinishedProto.newBuilder();
    builder.setTaskAttemptId(taskAttemptId.toString())
        .setState(state.ordinal())
        .setFinishTime(finishTime);
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    if (error != null) {
      builder.setErrorEnum(error.name());
    }
    if (tezCounters != null) {
      builder.setCounters(DagTypeConverters.convertTezCountersToProto(tezCounters));
    }
    if (dataEvents != null && !dataEvents.isEmpty()) {
      for (DataEventDependencyInfo info : dataEvents) {
        builder.addDataEvents(DataEventDependencyInfo.toProto(info));
      }
    }
    return builder.build();
  }

  public void fromProto(TaskAttemptFinishedProto proto) {
    this.taskAttemptId = TezTaskAttemptID.fromString(proto.getTaskAttemptId());
    this.finishTime = proto.getFinishTime();
    this.state = TaskAttemptState.values()[proto.getState()];
    if (proto.hasDiagnostics()) {
      this.diagnostics = proto.getDiagnostics();
    }
    if (proto.hasErrorEnum()) {
      this.error = TaskAttemptTerminationCause.valueOf(proto.getErrorEnum());
    }
    if (proto.hasCounters()) {
      this.tezCounters = DagTypeConverters.convertTezCountersFromProto(
        proto.getCounters());
    }
    if (proto.getDataEventsCount() > 0) {
      this.dataEvents = Lists.newArrayListWithCapacity(proto.getDataEventsCount());
      for (DataEventDependencyInfoProto protoEvent : proto.getDataEventsList()) {
        this.dataEvents.add(DataEventDependencyInfo.fromProto(protoEvent));
      }
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    TaskAttemptFinishedProto proto =
        TaskAttemptFinishedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", taskAttemptId=" + taskAttemptId
        + ", startTime=" + startTime
        + ", finishTime=" + finishTime
        + ", timeTaken=" + (finishTime - startTime)
        + ", status=" + state.name()
        + ", errorEnum=" + (error != null ? error.name() : "")
        + ", diagnostics=" + diagnostics
        + ", lastDataEventSourceTA=" + 
              ((dataEvents==null) ? 0:dataEvents.size())
        + ", counters=" + (tezCounters == null ? "null" :
          tezCounters.toString()
            .replaceAll("\\n", ", ").replaceAll("\\s+", " "));
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskAttemptId;
  }

  public TezCounters getCounters() {
    return tezCounters;
  }

  public String getDiagnostics() {
    return diagnostics;
  }
  
  public TaskAttemptTerminationCause getTaskAttemptError() {
    return error;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public TaskAttemptState getState() {
    return state;
  }

  public long getStartTime() {
    return startTime;
  }

}

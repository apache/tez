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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.app.dag.impl.VertexStats;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexFinishStateProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexFinishedProto;

public class VertexFinishedEvent implements HistoryEvent, SummaryEvent {

  private static final Log LOG = LogFactory.getLog(VertexFinishedEvent.class);

  private TezVertexID vertexID;
  private String vertexName;
  private int numTasks;
  private long initRequestedTime;
  private long initedTime;
  private long startRequestedTime;
  private long startTime;
  private long finishTime;
  private VertexState state;
  private String diagnostics;
  private TezCounters tezCounters;
  private boolean fromSummary = false;
  private VertexStats vertexStats;
  private Map<String, Integer> vertexTaskStats;

  public VertexFinishedEvent(TezVertexID vertexId, String vertexName, int numTasks, long initRequestedTime,
                             long initedTime, long startRequestedTime, long startedTime,
                             long finishTime, VertexState state, String diagnostics,
                             TezCounters counters, VertexStats vertexStats,
                             Map<String, Integer> vertexTaskStats) {
    this.vertexName = vertexName;
    this.vertexID = vertexId;
    this.numTasks = numTasks;
    this.initRequestedTime = initRequestedTime;
    this.initedTime = initedTime;
    this.startRequestedTime = startRequestedTime;
    this.startTime = startedTime;
    this.finishTime = finishTime;
    this.state = state;
    this.diagnostics = diagnostics;
    this.tezCounters = counters;
    this.vertexStats = vertexStats;
    this.vertexTaskStats = vertexTaskStats;
  }

  public VertexFinishedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_FINISHED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public VertexFinishedProto toProto() {
    VertexFinishedProto.Builder builder = VertexFinishedProto.newBuilder();
    builder.setVertexName(vertexName)
        .setVertexId(vertexID.toString())
        .setState(state.ordinal())
        .setFinishTime(finishTime);
    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    return builder.build();
  }

  public void fromProto(VertexFinishedProto proto) {
    this.vertexName = proto.getVertexName();
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.finishTime = proto.getFinishTime();
    this.state = VertexState.values()[proto.getState()];
    if (proto.hasDiagnostics())  {
      this.diagnostics = proto.getDiagnostics();
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    VertexFinishedProto proto = VertexFinishedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", vertexId=" + vertexID
        + ", initRequestedTime=" + initRequestedTime
        + ", initedTime=" + initedTime
        + ", startRequestedTime=" + startRequestedTime
        + ", startedTime=" + startTime
        + ", finishTime=" + finishTime
        + ", timeTaken=" + (finishTime - startTime)
        + ", status=" + state.name()
        + ", diagnostics=" + diagnostics
        + ", counters=" + ( tezCounters == null ? "null" :
          tezCounters.toString().replaceAll("\\n", ", ").replaceAll("\\s+", " "))
        + ", vertexStats=" + (vertexStats == null ? "null" : vertexStats.toString())
        + ", vertexTaskStats=" + (vertexTaskStats == null ? "null" : vertexTaskStats.toString());
  }

  public TezVertexID getVertexID() {
    return this.vertexID;
  }

  public VertexState getState() {
    return this.state;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }

  public VertexStats getVertexStats() {
    return vertexStats;
  }

  public String getVertexName() {
    return vertexName;
  }

  public long getStartTime() {
    return startTime;
  }

  public Map<String, Integer> getVertexTaskStats() {
    return vertexTaskStats;
  }

  public int getNumTasks() {
    return numTasks;
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream) throws IOException {
    VertexFinishStateProto finishStateProto =
        VertexFinishStateProto.newBuilder()
            .setState(state.ordinal())
            .setVertexId(vertexID.toString())
            .setNumTasks(numTasks)
            .build();

    SummaryEventProto.Builder builder = RecoveryProtos.SummaryEventProto.newBuilder()
        .setDagId(vertexID.getDAGId().toString())
        .setTimestamp(finishTime)
        .setEventType(getEventType().ordinal())
        .setEventPayload(finishStateProto.toByteString());
    builder.build().writeDelimitedTo(outputStream);

  }

  @Override
  public void fromSummaryProtoStream(SummaryEventProto proto) throws IOException {
    VertexFinishStateProto finishStateProto =
        VertexFinishStateProto.parseFrom(proto.getEventPayload());
    this.vertexID = TezVertexID.fromString(finishStateProto.getVertexId());
    this.state = VertexState.values()[finishStateProto.getState()];
    this.numTasks = finishStateProto.getNumTasks();
    this.finishTime = proto.getTimestamp();
    this.fromSummary = true;
  }

  @Override
  public boolean writeToRecoveryImmediately() {
    return false;
  }

  public boolean isFromSummary() {
    return fromSummary;
  }
}

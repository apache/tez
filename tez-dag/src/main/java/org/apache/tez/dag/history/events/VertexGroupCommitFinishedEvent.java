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

import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexGroupCommitFinishedProto;

public class VertexGroupCommitFinishedEvent implements HistoryEvent, SummaryEvent {

  private TezDAGID dagID;
  private String vertexGroupName;
  private long commitFinishTime;

  public VertexGroupCommitFinishedEvent() {
  }

  public VertexGroupCommitFinishedEvent(TezDAGID dagID,
      String vertexGroupName, long commitFinishTime) {
    this.dagID = dagID;
    this.vertexGroupName = vertexGroupName;
    this.commitFinishTime = commitFinishTime;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_GROUP_COMMIT_FINISHED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return false;
  }

  public VertexGroupCommitFinishedProto toProto() {
    return VertexGroupCommitFinishedProto.newBuilder()
        .setDagId(dagID.toString())
        .setVertexGroupName(vertexGroupName)
        .build();
  }

  public void fromProto(VertexGroupCommitFinishedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.vertexGroupName = proto.getVertexGroupName();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    VertexGroupCommitFinishedProto proto = VertexGroupCommitFinishedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "dagId=" + dagID
        + ", vertexGroup=" + vertexGroupName;
  }

  public String getVertexGroupName() {
    return this.vertexGroupName;
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream) throws IOException {
    SummaryEventProto.Builder builder = RecoveryProtos.SummaryEventProto.newBuilder()
        .setDagId(dagID.toString())
        .setTimestamp(commitFinishTime)
        .setEventType(getEventType().ordinal())
        .setEventPayload(toProto().toByteString());
    builder.build().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromSummaryProtoStream(SummaryEventProto proto) throws IOException {
    VertexGroupCommitFinishedProto vertexGroupCommitFinishedProto =
        VertexGroupCommitFinishedProto.parseFrom(proto.getEventPayload());
    fromProto(vertexGroupCommitFinishedProto);
    this.commitFinishTime = proto.getTimestamp();
  }

  @Override
  public boolean writeToRecoveryImmediately() {
    return false;
  }

  public TezDAGID getDagID() {
    return dagID;
  }

}

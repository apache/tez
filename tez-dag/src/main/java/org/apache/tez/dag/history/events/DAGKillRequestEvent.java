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
import java.io.OutputStream;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGKillRequestProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.SummaryEventProto;
import org.apache.tez.dag.utils.ProtoUtils;

public class DAGKillRequestEvent implements HistoryEvent, SummaryEvent {

  private TezDAGID dagID;
  private long killRequestTime;
  private boolean isSessionStopped;

  public DAGKillRequestEvent() {
  }

  public DAGKillRequestEvent(TezDAGID dagID, long killRequestTime, boolean isSessionStopped) {
    this.dagID = dagID;
    this.killRequestTime = killRequestTime;
    this.isSessionStopped = isSessionStopped;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_KILL_REQUEST;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return false;
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  public DAGKillRequestProto toProto() {
    return DAGKillRequestProto.newBuilder()
        .setDagId(dagID.toString())
        .setKillRequestTime(killRequestTime)
        .setIsSessionStopped(isSessionStopped)
        .build();
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    DAGKillRequestProto proto =
        inputStream.readMessage(DAGKillRequestProto.PARSER, ExtensionRegistry.getEmptyRegistry());
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }
  
  public void fromProto(RecoveryProtos.DAGKillRequestProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.killRequestTime = proto.getKillRequestTime();
    this.isSessionStopped = proto.getIsSessionStopped();
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream)
      throws IOException {
    ProtoUtils.toSummaryEventProto(dagID, killRequestTime,
        HistoryEventType.DAG_KILL_REQUEST, isSessionStopped ? new byte[]{1} : new byte[]{0})
        .writeDelimitedTo(outputStream);
  }

  @Override
  public void fromSummaryProtoStream(SummaryEventProto proto)
      throws IOException {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.killRequestTime = proto.getTimestamp();
    if (proto.getEventPayload().byteAt(0) == 1) {
      this.isSessionStopped = true;
    } else {
      this.isSessionStopped = false;
    }
  }

  @Override
  public boolean writeToRecoveryImmediately() {
    return false;
  }

  public TezDAGID getDagID() {
    return dagID;
  }
  
  public long getKillRequestTime() {
    return killRequestTime;
  }

  public boolean isSessionStopped() {
    return isSessionStopped;
  }
}

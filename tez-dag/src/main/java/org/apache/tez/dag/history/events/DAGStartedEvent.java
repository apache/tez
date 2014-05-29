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
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGStartedProto;

public class DAGStartedEvent implements HistoryEvent {

  private TezDAGID dagID;
  private long startTime;
  private String user;
  private String dagName;

  public DAGStartedEvent() {
  }

  public DAGStartedEvent(TezDAGID dagID, long startTime,
      String user, String dagName) {
    this.dagID = dagID;
    this.startTime = startTime;
    this.user = user;
    this.dagName = dagName;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_STARTED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public DAGStartedProto toProto() {
    return DAGStartedProto.newBuilder()
        .setDagId(dagID.toString())
        .setStartTime(startTime)
        .build();
  }

  public void fromProto(DAGStartedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.startTime = proto.getStartTime();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    DAGStartedProto proto = DAGStartedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "dagID=" + dagID
        + ", startTime=" + startTime;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public TezDAGID getDagID() {
    return dagID;
  }

  public String getUser() {
    return user;
  }

  public String getDagName() {
    return dagName;
  }

}

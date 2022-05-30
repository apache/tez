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
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.DAGIDAware;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGInitializedProto;

public class DAGInitializedEvent implements HistoryEvent, DAGIDAware {

  private TezDAGID dagID;
  private long initTime;
  private String user;
  private String dagName;
  private Map<String, TezVertexID> vertexNameIDMap;

  public DAGInitializedEvent() {
  }

  public DAGInitializedEvent(TezDAGID dagID, long initTime,
      String user, String dagName, Map<String, TezVertexID> vertexNameIDMap) {
    this.dagID = dagID;
    this.initTime = initTime;
    this.user = user;
    this.dagName = dagName;
    this.vertexNameIDMap = vertexNameIDMap;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_INITIALIZED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  @Override
  public String toString() {
    return "dagID=" + dagID
        + ", initTime=" + initTime;
  }

  public RecoveryProtos.DAGInitializedProto toProto() {
    return RecoveryProtos.DAGInitializedProto.newBuilder()
        .setDagId(dagID.toString())
        .setInitTime(initTime)
        .build();
  }

  public void fromProto(RecoveryProtos.DAGInitializedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.initTime = proto.getInitTime();
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    DAGInitializedProto proto =
        inputStream.readMessage(DAGInitializedProto.PARSER, ExtensionRegistry.getEmptyRegistry());
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  public long getInitTime() {
    return this.initTime;
  }

  @Override
  public TezDAGID getDAGID() {
    return dagID;
  }

  public String getUser() {
    return user;
  }

  public String getDagName() {
    return dagName;
  }

  public Map<String, TezVertexID> getVertexNameIDMap() {
    return vertexNameIDMap;
  }

}

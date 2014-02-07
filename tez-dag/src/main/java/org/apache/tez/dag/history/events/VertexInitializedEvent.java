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

import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class VertexInitializedEvent implements HistoryEvent {

  private TezVertexID vertexID;
  private String vertexName;
  private long initRequestedTime;
  private long initedTime;
  private long numTasks;
  private String processorName;

  public VertexInitializedEvent() {
  }

  public VertexInitializedEvent(TezVertexID vertexId,
      String vertexName, long initRequestedTime, long initedTime,
      long numTasks, String processorName) {
    this.vertexName = vertexName;
    this.vertexID = vertexId;
    this.initRequestedTime = initRequestedTime;
    this.initedTime = initedTime;
    this.numTasks = numTasks;
    this.processorName = processorName;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_INITIALIZED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY, vertexID.toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_VERTEX_ID.name());

    // Related entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject vertexEntity = new JSONObject();
    vertexEntity.put(ATSConstants.ENTITY, vertexID.getDAGId().toString());
    vertexEntity.put(ATSConstants.ENTITY_TYPE, EntityTypes.TEZ_DAG_ID.name());
    relatedEntities.put(vertexEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // Events
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, initedTime);
    initEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.VERTEX_INITIALIZED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info
    // TODO fix requested times to be events
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.VERTEX_NAME, vertexName);
    otherInfo.put(ATSConstants.INIT_REQUESTED_TIME, initRequestedTime);
    otherInfo.put(ATSConstants.INIT_TIME, initedTime);
    otherInfo.put(ATSConstants.NUM_TASKS, numTasks);
    otherInfo.put(ATSConstants.PROCESSOR_CLASS_NAME, processorName);
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public RecoveryProtos.VertexInitializedProto toProto() {
    return RecoveryProtos.VertexInitializedProto.newBuilder()
        .setVertexId(vertexID.toString())
        .setVertexName(vertexName)
        .setInitRequestedTime(initRequestedTime)
        .setInitTime(initedTime)
        .setNumTasks(numTasks)
        .build();
  }

  public void fromProto(RecoveryProtos.VertexInitializedProto proto) {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.vertexName = proto.getVertexName();
    this.initRequestedTime = proto.getInitRequestedTime();
    this.initedTime = proto.getInitTime();
    this.numTasks = proto.getNumTasks();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    RecoveryProtos.VertexInitializedProto proto =
        RecoveryProtos.VertexInitializedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", vertexId=" + vertexID
        + ", initRequestedTime=" + initRequestedTime
        + ", initedTime=" + initedTime
        + ", numTasks=" + numTasks
        + ", processorName=" + processorName;
  }

}

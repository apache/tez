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

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGFinishedProto;
import org.apache.tez.dag.utils.ProtoUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DAGFinishedEvent implements HistoryEvent, SummaryEvent {

  private TezDAGID dagID;
  private long startTime;
  private long finishTime;
  private DAGState state;
  private String diagnostics;
  private TezCounters tezCounters;

  public DAGFinishedEvent() {
  }

  public DAGFinishedEvent(TezDAGID dagId, long startTime,
      long finishTime, DAGState state,
      String diagnostics, TezCounters counters) {
    this.dagID = dagId;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.state = state;
    this.diagnostics = diagnostics;
    this.tezCounters = counters;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_FINISHED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        dagID.toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities not needed as should have been done in
    // dag submission event

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject finishEvent = new JSONObject();
    finishEvent.put(ATSConstants.TIMESTAMP, finishTime);
    finishEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_FINISHED.name());
    events.put(finishEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.START_TIME, startTime);
    otherInfo.put(ATSConstants.FINISH_TIME, finishTime);
    otherInfo.put(ATSConstants.TIME_TAKEN, (finishTime - startTime));
    otherInfo.put(ATSConstants.STATUS, state.name());
    otherInfo.put(ATSConstants.DIAGNOSTICS, diagnostics);
    otherInfo.put(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToJSON(this.tezCounters));
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

  public DAGFinishedProto toProto() {
    DAGFinishedProto.Builder builder = DAGFinishedProto.newBuilder();

    builder.setDagId(dagID.toString())
        .setState(state.ordinal())
        .setFinishTime(finishTime);

    if (diagnostics != null) {
      builder.setDiagnostics(diagnostics);
    }
    if (tezCounters != null) {
      builder.setCounters(DagTypeConverters.convertTezCountersToProto(tezCounters));
    }

    return builder.build();
  }

  public void fromProto(DAGFinishedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.finishTime = proto.getFinishTime();
    this.state = DAGState.values()[proto.getState()];
    if (proto.hasDiagnostics()) {
      this.diagnostics = proto.getDiagnostics();
    }
    if (proto.hasCounters()) {
      this.tezCounters = DagTypeConverters.convertTezCountersFromProto(
          proto.getCounters());
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    DAGFinishedProto proto = DAGFinishedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "dagId=" + dagID
        + ", startTime=" + startTime
        + ", finishTime=" + finishTime
        + ", timeTaken=" + (finishTime - startTime)
        + ", status=" + state.name()
        + ", diagnostics=" + diagnostics
        + ", counters=" + ((tezCounters == null) ? "null" :
          (tezCounters.toString()
            .replaceAll("\\n", ", ").replaceAll("\\s+", " ")));
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream) throws IOException {
    ProtoUtils.toSummaryEventProto(dagID, finishTime,
        HistoryEventType.DAG_FINISHED).writeDelimitedTo(outputStream);
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public DAGState getState() {
    return state;
  }

  public TezDAGID getDagID() {
    return dagID;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }

}

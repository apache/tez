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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.SummaryEvent;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DAGSubmittedProto;
import org.apache.tez.dag.utils.ProtoUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

      public class DAGSubmittedEvent implements HistoryEvent, SummaryEvent {

  private TezDAGID dagID;
  private long submitTime;
  private DAGProtos.DAGPlan dagPlan;
  private ApplicationAttemptId applicationAttemptId;

  public DAGSubmittedEvent() {
  }

  public DAGSubmittedEvent(TezDAGID dagID, long submitTime,
      DAGProtos.DAGPlan dagPlan, ApplicationAttemptId applicationAttemptId) {
    this.dagID = dagID;
    this.submitTime = submitTime;
    this.dagPlan = dagPlan;
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_SUBMITTED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        dagID.toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_DAG_ID.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
    JSONObject tezAppEntity = new JSONObject();
    tezAppEntity.put(ATSConstants.ENTITY,
        "tez_" + applicationAttemptId.toString());
    tezAppEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());
    JSONObject appEntity = new JSONObject();
    appEntity.put(ATSConstants.ENTITY,
        applicationAttemptId.getApplicationId().toString());
    appEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ID);
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        applicationAttemptId.toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        ATSConstants.APPLICATION_ATTEMPT_ID);

    relatedEntities.put(tezAppEntity);
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // filters
    JSONObject primaryFilters = new JSONObject();
    primaryFilters.put(ATSConstants.DAG_NAME,
        dagPlan.getName());
    jsonObject.put(ATSConstants.PRIMARY_FILTERS, primaryFilters);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject submitEvent = new JSONObject();
    submitEvent.put(ATSConstants.TIMESTAMP, submitTime);
    submitEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.DAG_SUBMITTED.name());
    events.put(submitEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info such as dag plan
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.DAG_PLAN,
        DAGUtils.generateSimpleJSONPlan(dagPlan));
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

  public DAGSubmittedProto toProto() {
    return DAGSubmittedProto.newBuilder()
        .setDagId(dagID.toString())
        .setApplicationAttemptId(applicationAttemptId.toString())
        .setDagPlan(dagPlan)
        .setSubmitTime(submitTime)
        .build();

  }

  public void fromProto(DAGSubmittedProto proto) {
    this.dagID = TezDAGID.fromString(proto.getDagId());
    this.dagPlan = proto.getDagPlan();
    this.submitTime = proto.getSubmitTime();
    this.applicationAttemptId = ConverterUtils.toApplicationAttemptId(
        proto.getApplicationAttemptId());
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    DAGSubmittedProto proto = DAGSubmittedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "dagID=" + dagID
        + ", submitTime=" + submitTime;
  }

  @Override
  public void toSummaryProtoStream(OutputStream outputStream) throws IOException {
    ProtoUtils.toSummaryEventProto(dagID, submitTime,
        HistoryEventType.DAG_SUBMITTED).writeDelimitedTo(outputStream);
  }

  public String getDAGName() {
    if (dagPlan != null && dagPlan.hasName()) {
      return dagPlan.getName();
    }
    return null;
  }

  public DAGProtos.DAGPlan getDAGPlan() {
    return this.dagPlan;
  }

  public TezDAGID getDagID() {
    return dagID;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public long getSubmitTime() {
    return submitTime;
  }
}

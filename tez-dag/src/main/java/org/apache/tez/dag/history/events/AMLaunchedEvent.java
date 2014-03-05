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
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.recovery.records.RecoveryProtos.AMLaunchedProto;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AMLaunchedEvent implements HistoryEvent {

  private ApplicationAttemptId applicationAttemptId;
  private long launchTime;
  private long appSubmitTime;

  public AMLaunchedEvent() {
  }

  public AMLaunchedEvent(ApplicationAttemptId appAttemptId,
      long launchTime, long appSubmitTime) {
    this.applicationAttemptId = appAttemptId;
    this.launchTime = launchTime;
    this.appSubmitTime = appSubmitTime;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.AM_LAUNCHED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + applicationAttemptId.toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    // Related Entities
    JSONArray relatedEntities = new JSONArray();
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
    relatedEntities.put(appEntity);
    relatedEntities.put(appAttemptEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject initEvent = new JSONObject();
    initEvent.put(ATSConstants.TIMESTAMP, launchTime);
    initEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.AM_LAUNCHED.name());
    events.put(initEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // Other info to tag with Tez AM
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.APP_SUBMIT_TIME, appSubmitTime);
    jsonObject.put(ATSConstants.OTHER_INFO, otherInfo);

    return jsonObject;
  }

  @Override
  public boolean isRecoveryEvent() {
    return false;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  @Override
  public String toString() {
    return "appAttemptId=" + applicationAttemptId
        + ", appSubmitTime=" + appSubmitTime
        + ", launchTime=" + launchTime;
  }

  public AMLaunchedProto toProto() {
    return AMLaunchedProto.newBuilder()
        .setApplicationAttemptId(this.applicationAttemptId.toString())
        .setAppSubmitTime(appSubmitTime)
        .setLaunchTime(launchTime)
        .build();
  }

  public void fromProto(AMLaunchedProto proto) {
    this.applicationAttemptId =
        ConverterUtils.toApplicationAttemptId(proto.getApplicationAttemptId());
    this.launchTime = proto.getLaunchTime();
    this.appSubmitTime = proto.getAppSubmitTime();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    AMLaunchedProto proto = AMLaunchedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public long getAppSubmitTime() {
    return appSubmitTime;
  }

}

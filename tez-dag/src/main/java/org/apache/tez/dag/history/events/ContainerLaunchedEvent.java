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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.recovery.records.RecoveryProtos.ContainerLaunchedProto;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ContainerLaunchedEvent implements HistoryEvent {

  private ContainerId containerId;
  private long launchTime;
  private ApplicationAttemptId applicationAttemptId;

  public ContainerLaunchedEvent() {
  }

  public ContainerLaunchedEvent(ContainerId containerId,
      long launchTime,
      ApplicationAttemptId applicationAttemptId) {
    this.containerId = containerId;
    this.launchTime = launchTime;
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.CONTAINER_LAUNCHED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(ATSConstants.ENTITY,
        "tez_" + containerId.toString());
    jsonObject.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_CONTAINER_ID.name());

    JSONArray relatedEntities = new JSONArray();
    JSONObject appAttemptEntity = new JSONObject();
    appAttemptEntity.put(ATSConstants.ENTITY,
        applicationAttemptId.toString());
    appAttemptEntity.put(ATSConstants.ENTITY_TYPE,
        EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    JSONObject containerEntity = new JSONObject();
    containerEntity.put(ATSConstants.ENTITY, containerId.toString());
    containerEntity.put(ATSConstants.ENTITY_TYPE, ATSConstants.CONTAINER_ID);

    relatedEntities.put(appAttemptEntity);
    relatedEntities.put(containerEntity);
    jsonObject.put(ATSConstants.RELATED_ENTITIES, relatedEntities);

    // TODO decide whether this goes into different events,
    // event info or other info.
    JSONArray events = new JSONArray();
    JSONObject launchEvent = new JSONObject();
    launchEvent.put(ATSConstants.TIMESTAMP, launchTime);
    launchEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.CONTAINER_LAUNCHED.name());
    events.put(launchEvent);
    jsonObject.put(ATSConstants.EVENTS, events);

    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

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

  public ContainerLaunchedProto toProto() {
    return ContainerLaunchedProto.newBuilder()
        .setApplicationAttemptId(applicationAttemptId.toString())
        .setContainerId(containerId.toString())
        .setLaunchTime(launchTime)
        .build();
  }

  public void fromProto(ContainerLaunchedProto proto) {
    this.containerId = ConverterUtils.toContainerId(proto.getContainerId());
    launchTime = proto.getLaunchTime();
    this.applicationAttemptId = ConverterUtils.toApplicationAttemptId(
        proto.getApplicationAttemptId());
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    ContainerLaunchedProto proto =
        ContainerLaunchedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "containerId=" + containerId
        + ", launchTime=" + launchTime;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public long getLaunchTime() {
    return launchTime;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

}

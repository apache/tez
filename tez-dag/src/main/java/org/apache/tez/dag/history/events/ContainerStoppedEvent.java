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
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.ats.EntityTypes;
import org.apache.tez.dag.history.utils.ATSConstants;
import org.apache.tez.dag.recovery.records.RecoveryProtos.ContainerStoppedProto;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ContainerStoppedEvent implements HistoryEvent {

  private ContainerId containerId;
  private long stopTime;
  private int exitStatus;
  private ApplicationAttemptId applicationAttemptId;

  public ContainerStoppedEvent() {
  }
  
  public ContainerStoppedEvent(ContainerId containerId,
      long stopTime,
      int exitStatus,
      ApplicationAttemptId applicationAttemptId) {
    this.containerId = containerId;
    this.stopTime = stopTime;
    this.exitStatus = exitStatus;
    this.applicationAttemptId = applicationAttemptId;
  }
  
  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.CONTAINER_STOPPED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    // structure is identical to ContainerLaunchedEvent
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
    JSONObject stopEvent = new JSONObject();
    stopEvent.put(ATSConstants.TIMESTAMP, stopTime);
    stopEvent.put(ATSConstants.EVENT_TYPE,
        HistoryEventType.CONTAINER_STOPPED.name());
    events.put(stopEvent);
    jsonObject.put(ATSConstants.EVENTS, events);
    
    // TODO add other container info here? or assume AHS will have this?
    // TODO container logs?

    // Other info
    JSONObject otherInfo = new JSONObject();
    otherInfo.put(ATSConstants.EXIT_STATUS, exitStatus);
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

  public ContainerStoppedProto toProto() {
    return ContainerStoppedProto.newBuilder()
        .setApplicationAttemptId(applicationAttemptId.toString())
        .setContainerId(containerId.toString())
        .setStopTime(stopTime)
        .setExitStatus(exitStatus)
        .build();
  }

  public void fromProto(ContainerStoppedProto proto) {
    this.containerId = ConverterUtils.toContainerId(proto.getContainerId());
    stopTime = proto.getStopTime();
    exitStatus = proto.getExitStatus();
    this.applicationAttemptId = ConverterUtils.toApplicationAttemptId(
        proto.getApplicationAttemptId());
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    ContainerStoppedProto proto =
        ContainerStoppedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "containerId=" + containerId
        + ", stoppedTime=" + stopTime 
        + ", exitStatus=" + exitStatus;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public long getStoppedTime() {
    return stopTime;
  }
  
  public int getExitStatus() {
    return exitStatus;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

}

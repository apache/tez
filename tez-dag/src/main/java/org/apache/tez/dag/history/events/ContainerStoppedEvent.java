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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.recovery.records.RecoveryProtos.ContainerStoppedProto;

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
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
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

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
import org.apache.tez.dag.recovery.records.RecoveryProtos.ContainerLaunchedProto;

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
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
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

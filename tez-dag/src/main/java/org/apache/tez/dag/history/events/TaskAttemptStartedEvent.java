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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TaskAttemptStartedProto;

public class TaskAttemptStartedEvent implements HistoryEvent {

  private TezTaskAttemptID taskAttemptId;
  private String inProgressLogsUrl;
  private String completedLogsUrl;
  private String vertexName;
  private long launchTime;
  private ContainerId containerId;
  private NodeId nodeId;
  private String nodeHttpAddress;
  private TezTaskAttemptID creationCausalTA;
  private long creationTime;
  private long allocationTime;

  public TaskAttemptStartedEvent(TezTaskAttemptID taId,
      String vertexName, long launchTime,
      ContainerId containerId, NodeId nodeId,
      String inProgressLogsUrl, String completedLogsUrl,
      String nodeHttpAddress, long creationTime, TezTaskAttemptID creationCausalTA, long allocationTime) {
    this.taskAttemptId = taId;
    this.vertexName = vertexName;
    this.launchTime = launchTime;
    this.containerId = containerId;
    this.nodeId = nodeId;
    this.inProgressLogsUrl = inProgressLogsUrl;
    this.completedLogsUrl = completedLogsUrl;
    this.nodeHttpAddress = nodeHttpAddress;
    this.creationTime = creationTime;
    this.creationCausalTA = creationCausalTA;
    this.allocationTime = allocationTime;
  }

  public TaskAttemptStartedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.TASK_ATTEMPT_STARTED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public TaskAttemptStartedProto toProto() {
    TaskAttemptStartedProto.Builder builder = TaskAttemptStartedProto.newBuilder();
    builder.setTaskAttemptId(taskAttemptId.toString())
        .setStartTime(launchTime)
        .setContainerId(containerId.toString())
        .setNodeId(nodeId.toString())
        .setCreationTime(creationTime)
        .setAllocationTime(allocationTime);
    if (creationCausalTA != null) {
      builder.setCreationCausalTA(creationCausalTA.toString());
    }
    return builder.build();
  }

  public void fromProto(TaskAttemptStartedProto proto) {
    this.taskAttemptId = TezTaskAttemptID.fromString(proto.getTaskAttemptId());
    this.launchTime = proto.getStartTime();
    this.containerId = ConverterUtils.toContainerId(proto.getContainerId());
    this.nodeId = ConverterUtils.toNodeId(proto.getNodeId());
    this.creationTime = proto.getCreationTime();
    this.allocationTime = proto.getAllocationTime();
    if (proto.hasCreationCausalTA()) {
      this.creationCausalTA = TezTaskAttemptID.fromString(proto.getCreationCausalTA());
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    TaskAttemptStartedProto proto = TaskAttemptStartedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", taskAttemptId=" + taskAttemptId
        + ", creationTime=" + creationTime
        + ", allocationTime=" + allocationTime
        + ", startTime=" + launchTime
        + ", containerId=" + containerId
        + ", nodeId=" + nodeId
        + ", inProgressLogs=" + inProgressLogsUrl
        + ", completedLogs=" + completedLogsUrl;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return this.taskAttemptId;
  }

  public long getStartTime() {
    return launchTime;
  }
  
  public long getCreationTime() {
    return creationTime;
  }
  
  public long getAllocationTime() {
    return allocationTime;
  }
  
  public TezTaskAttemptID getCreationCausalTA() {
    return creationCausalTA;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public String getInProgressLogsUrl() {
    return inProgressLogsUrl;
  }

  public String getCompletedLogsUrl() {
    return completedLogsUrl;
  }

  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }

}

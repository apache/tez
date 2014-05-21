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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.recovery.records.RecoveryProtos.AMStartedProto;

public class AMStartedEvent implements HistoryEvent {

  private ApplicationAttemptId applicationAttemptId;
  private long startTime;
  private String user;

  public AMStartedEvent() {
  }

  public AMStartedEvent(ApplicationAttemptId appAttemptId,
      long startTime, String user) {
    this.applicationAttemptId = appAttemptId;
    this.startTime = startTime;
    this.user = user;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.AM_STARTED;
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
    return "appAttemptId=" + applicationAttemptId
        + ", startTime=" + startTime;
  }

  public AMStartedProto toProto() {
    return AMStartedProto.newBuilder()
        .setApplicationAttemptId(this.applicationAttemptId.toString())
        .setStartTime(startTime)
        .build();
  }

  public void fromProto(AMStartedProto proto) {
    this.applicationAttemptId =
        ConverterUtils.toApplicationAttemptId(proto.getApplicationAttemptId());
    this.startTime = proto.getStartTime();
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    AMStartedProto proto = AMStartedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getUser() {
    return user;
  }

}

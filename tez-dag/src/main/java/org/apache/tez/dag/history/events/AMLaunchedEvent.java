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
import org.apache.tez.dag.recovery.records.RecoveryProtos.AMLaunchedProto;

public class AMLaunchedEvent implements HistoryEvent {

  private ApplicationAttemptId applicationAttemptId;
  private long launchTime;
  private long appSubmitTime;
  private String user;

  public AMLaunchedEvent() {
  }

  public AMLaunchedEvent(ApplicationAttemptId appAttemptId,
      long launchTime, long appSubmitTime, String user) {
    this.applicationAttemptId = appAttemptId;
    this.launchTime = launchTime;
    this.appSubmitTime = appSubmitTime;
    this.user = user;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.AM_LAUNCHED;
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

  public String getUser() {
    return user;
  }

}

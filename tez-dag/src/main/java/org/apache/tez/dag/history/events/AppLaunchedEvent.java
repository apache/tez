/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.events;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.VersionInfo;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;

public class AppLaunchedEvent implements HistoryEvent {

  private ApplicationId applicationId;
  private long launchTime;
  private long appSubmitTime;
  private String user;
  private Configuration conf;
  private VersionInfo version;

  public AppLaunchedEvent() {
  }

  public AppLaunchedEvent(ApplicationId appId,
                          long launchTime, long appSubmitTime, String user,
                          Configuration conf, VersionInfo version) {
    this.applicationId = appId;
    this.launchTime = launchTime;
    this.appSubmitTime = appSubmitTime;
    this.user = user;
    this.conf = conf;
    this.version = version;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.APP_LAUNCHED;
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
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException("Not a recovery event");
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    throw new UnsupportedOperationException("Not a recovery event");
  }

  @Override
  public String toString() {
    return "applicationId=" + applicationId
        + ", appSubmitTime=" + appSubmitTime
        + ", launchTime=" + launchTime;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
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

  public Configuration getConf() {
    return conf;
  }

  public VersionInfo getVersion() {
    return version;
  }
}

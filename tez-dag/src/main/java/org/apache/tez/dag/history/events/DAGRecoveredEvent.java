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
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;

public class DAGRecoveredEvent implements HistoryEvent {

  private final ApplicationAttemptId applicationAttemptId;
  private final TezDAGID dagID;
  private final long recoveredTime;
  private final DAGState recoveredDagState;
  private final String recoveryFailureReason;
  private final String dagName;
  private final String user;

  private boolean historyLoggingEnabled = true;

  public DAGRecoveredEvent(ApplicationAttemptId applicationAttemptId,
      TezDAGID dagId, String dagName, String user,
      long recoveredTime, DAGState recoveredState,
      String recoveryFailureReason) {
    this.applicationAttemptId = applicationAttemptId;
    this.dagID = dagId;
    this.dagName = dagName;
    this.user = user;
    this.recoveredTime = recoveredTime;
    this.recoveredDagState = recoveredState;
    this.recoveryFailureReason = recoveryFailureReason;
  }

  public DAGRecoveredEvent(ApplicationAttemptId applicationAttemptId,
      TezDAGID dagId, String dagName, String user, long recoveredTime) {
    this(applicationAttemptId, dagId, dagName, user, recoveredTime, null, null);
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_RECOVERED;
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
  public void toProtoStream(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException("Invalid operation for eventType "
        + getEventType().name());
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    throw new UnsupportedOperationException("Invalid operation for eventType "
        + getEventType().name());
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public TezDAGID getDagID() {
    return dagID;
  }

  public long getRecoveredTime() {
    return recoveredTime;
  }

  public DAGState getRecoveredDagState() {
    return recoveredDagState;
  }

  public String getRecoveryFailureReason() {
    return recoveryFailureReason;
  }

  public String getDagName() {
    return dagName;
  }

  public String getUser() {
    return user;
  }

  public boolean isHistoryLoggingEnabled() {
    return historyLoggingEnabled;
  }

  public void setHistoryLoggingEnabled(boolean historyLoggingEnabled) {
    this.historyLoggingEnabled = historyLoggingEnabled;
  }

  @Override
  public String toString() {
    return "applicationAttemptId="
        + (applicationAttemptId != null ? applicationAttemptId.toString() : "null")
        + ", dagId=" + (dagID != null ? dagID.toString() : "null")
        + ", recoveredTime=" + recoveredTime
        + ", recoveredState=" + (recoveredDagState != null ? recoveredDagState.name() : "null" )
        + ", recoveryFailureReason=" + recoveryFailureReason;
  }

}
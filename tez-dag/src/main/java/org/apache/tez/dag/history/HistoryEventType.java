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

package org.apache.tez.dag.history;

import org.apache.tez.dag.api.HistoryLogLevel;
import org.apache.tez.dag.api.TezUncheckedException;

public enum HistoryEventType {
  APP_LAUNCHED(HistoryLogLevel.AM),
  AM_LAUNCHED(HistoryLogLevel.AM),
  AM_STARTED(HistoryLogLevel.AM),
  DAG_SUBMITTED(HistoryLogLevel.DAG),
  DAG_INITIALIZED(HistoryLogLevel.DAG),
  DAG_STARTED(HistoryLogLevel.DAG),
  DAG_FINISHED(HistoryLogLevel.DAG),
  DAG_KILL_REQUEST(HistoryLogLevel.DAG),
  VERTEX_INITIALIZED(HistoryLogLevel.VERTEX),
  VERTEX_STARTED(HistoryLogLevel.VERTEX),
  VERTEX_CONFIGURE_DONE(HistoryLogLevel.VERTEX),
  VERTEX_FINISHED(HistoryLogLevel.VERTEX),
  TASK_STARTED(HistoryLogLevel.TASK),
  TASK_FINISHED(HistoryLogLevel.TASK),
  TASK_ATTEMPT_STARTED(HistoryLogLevel.TASK_ATTEMPT),
  TASK_ATTEMPT_FINISHED(HistoryLogLevel.TASK_ATTEMPT),
  CONTAINER_LAUNCHED(HistoryLogLevel.ALL),
  CONTAINER_STOPPED(HistoryLogLevel.ALL),
  DAG_COMMIT_STARTED(HistoryLogLevel.DAG),
  VERTEX_COMMIT_STARTED(HistoryLogLevel.VERTEX),
  VERTEX_GROUP_COMMIT_STARTED(HistoryLogLevel.VERTEX),
  VERTEX_GROUP_COMMIT_FINISHED(HistoryLogLevel.VERTEX),
  DAG_RECOVERED(HistoryLogLevel.DAG);

  private final HistoryLogLevel historyLogLevel;

  private HistoryEventType(HistoryLogLevel historyLogLevel) {
    this.historyLogLevel = historyLogLevel;
  }

  public static boolean isDAGSpecificEvent(HistoryEventType historyEventType) {
    switch (historyEventType) {
      case APP_LAUNCHED:
      case AM_LAUNCHED:
      case AM_STARTED:
      case CONTAINER_LAUNCHED:
      case CONTAINER_STOPPED:
        return false;
      case DAG_SUBMITTED:
      case DAG_INITIALIZED:
      case DAG_STARTED:
      case DAG_FINISHED:
      case DAG_KILL_REQUEST:
      case VERTEX_INITIALIZED:
      case VERTEX_STARTED:
      case VERTEX_CONFIGURE_DONE:
      case VERTEX_FINISHED:
      case TASK_STARTED:
      case TASK_FINISHED:
      case TASK_ATTEMPT_STARTED:
      case TASK_ATTEMPT_FINISHED:
      case DAG_COMMIT_STARTED:
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_RECOVERED:
        return true;
      default:
        throw new TezUncheckedException("Unhandled history event type: " + historyEventType.name());
    }
  }

  public HistoryLogLevel getHistoryLogLevel() {
    return historyLogLevel;
  }

}

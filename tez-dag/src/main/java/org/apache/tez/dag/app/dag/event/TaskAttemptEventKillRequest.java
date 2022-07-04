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
package org.apache.tez.dag.app.dag.event;

import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskAttemptEventKillRequest extends TaskAttemptEvent
    implements DiagnosableEvent, TaskAttemptEventTerminationCauseEvent, RecoveryEvent {

  private final String message;
  private final TaskAttemptTerminationCause errorCause;
  private boolean fromRecovery = false;

  public TaskAttemptEventKillRequest(TezTaskAttemptID id, String message, TaskAttemptTerminationCause err) {
    super(id, TaskAttemptEventType.TA_KILL_REQUEST);
    this.message = message;
    this.errorCause = err;
  }

  public TaskAttemptEventKillRequest(TezTaskAttemptID id, String message, TaskAttemptTerminationCause err,
                                     boolean fromRecovery) {
    this(id, message, err);
    this.fromRecovery = fromRecovery;
  }

  @Override
  public String getDiagnosticInfo() {
    return message;
  }

  @Override
  public TaskAttemptTerminationCause getTerminationCause() {
    return errorCause;
  }

  @Override
  public boolean isFromRecovery() {
    return fromRecovery;
  }
}

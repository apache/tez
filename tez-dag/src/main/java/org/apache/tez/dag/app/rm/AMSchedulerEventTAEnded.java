/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class AMSchedulerEventTAEnded extends AMSchedulerEvent {

  private final TaskAttempt attempt;
  private final ContainerId containerId;
  private final TaskAttemptState state;
  private final TaskAttemptEndReason taskAttemptEndReason;
  private final String diagnostics;

  public AMSchedulerEventTAEnded(TaskAttempt attempt, ContainerId containerId,
      TaskAttemptState state, TaskAttemptEndReason taskAttemptEndReason, String diagnostics, int schedulerId) {
    super(AMSchedulerEventType.S_TA_ENDED, schedulerId);
    this.attempt = attempt;
    this.containerId = containerId;
    this.state = state;
    this.taskAttemptEndReason = taskAttemptEndReason;
    this.diagnostics = diagnostics;
  }

  public TezTaskAttemptID getAttemptID() {
    return this.attempt.getID();
  }

  public TaskAttempt getAttempt() {
    return this.attempt;
  }

  public TaskAttemptState getState() {
    return this.state;
  }

  public ContainerId getUsedContainerId() {
    return this.containerId;
  }

  public TaskAttemptEndReason getTaskAttemptEndReason() {
    return taskAttemptEndReason;
  }

  public String getDiagnostics() {
    return diagnostics;
  }
}

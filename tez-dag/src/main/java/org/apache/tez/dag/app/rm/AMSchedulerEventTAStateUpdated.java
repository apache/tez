/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app.rm;

import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.serviceplugins.api.TaskScheduler.SchedulerTaskState;

public class AMSchedulerEventTAStateUpdated extends AMSchedulerEvent {

  private final TaskAttempt taskAttempt;
  private final SchedulerTaskState state;

  public AMSchedulerEventTAStateUpdated(TaskAttempt taskAttempt, SchedulerTaskState state,
                                        int schedulerId) {
    super(AMSchedulerEventType.S_TA_STATE_UPDATED, schedulerId);
    this.taskAttempt = taskAttempt;
    this.state = state;
  }

  public TaskAttempt getTaskAttempt() {
    return taskAttempt;
  }

  public SchedulerTaskState getState() {
    return state;
  }
}
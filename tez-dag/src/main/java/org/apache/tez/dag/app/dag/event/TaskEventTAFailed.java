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

package org.apache.tez.dag.app.dag.event;

import java.util.Objects;

import org.apache.tez.common.TezAbstractEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.TaskFailureType;

@SuppressWarnings("rawtypes")
public class TaskEventTAFailed extends TaskEventTAUpdate {

  private final TezAbstractEvent causalEvent;
  private final TaskFailureType taskFailureType;

  public TaskEventTAFailed(TezTaskAttemptID id, TaskFailureType taskFailureType, TezAbstractEvent causalEvent) {
    super(id, TaskEventType.T_ATTEMPT_FAILED);
    Objects.requireNonNull(taskFailureType, "FailureType must be specified for a failed attempt");
    this.taskFailureType = taskFailureType;
    this.causalEvent = causalEvent;
  }

  public TezAbstractEvent getCausalEvent() {
    return causalEvent;
  }

  public TaskFailureType getTaskFailureType() {
    return taskFailureType;
  }
}

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

package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMSchedulerEventTAEnded extends AMSchedulerEvent {

  private final TaskAttemptId attemptId;
  private final ContainerId containerId;
  private TaskAttemptState state;

  public AMSchedulerEventTAEnded(TaskAttemptId attemptId,
      ContainerId containerId, TaskAttemptState state) {
    super(AMSchedulerEventType.S_TA_ENDED);
    this.attemptId = attemptId;
    this.containerId = containerId;
    this.state = state;
  }

  public TaskAttemptId getAttemptID() {
    return this.attemptId;
  }

  public ContainerId getUsedContainerId() {
    return this.containerId;
  }

  public TaskAttemptState getState() {
    return this.state;
  }
}

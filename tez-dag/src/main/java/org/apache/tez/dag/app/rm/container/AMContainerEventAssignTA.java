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

package org.apache.tez.dag.app.rm.container;

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class AMContainerEventAssignTA extends AMContainerEvent {

  private final TezTaskAttemptID attemptId;
  // TODO Maybe have tht TAL pull the remoteTask from the TaskAttempt itself ?
  private final TaskSpec remoteTaskSpec;
  private final Map<String, LocalResource> taskLocalResources;
  private final Credentials credentials;
  private final int priority;

  public AMContainerEventAssignTA(ContainerId containerId, TezTaskAttemptID attemptId,
      Object remoteTaskSpec, Map<String, LocalResource> taskLocalResources, Credentials credentials,
      int priority) {
    super(containerId, AMContainerEventType.C_ASSIGN_TA);
    this.attemptId = attemptId;
    this.remoteTaskSpec = (TaskSpec) remoteTaskSpec;
    this.taskLocalResources = taskLocalResources;
    this.credentials = credentials;
    this.priority = priority;
  }

  public TaskSpec getRemoteTaskSpec() {
    return this.remoteTaskSpec;
  }
  
  public Map<String, LocalResource> getRemoteTaskLocalResources() {
    return this.taskLocalResources;
  }

  public TezTaskAttemptID getTaskAttemptId() {
    return this.attemptId;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }

  public int getPriority() {
    return priority;
  }
}

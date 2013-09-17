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

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskAttemptEventStartedRemotely extends TaskAttemptEvent {

  private final ContainerId containerId;
  // TODO Can appAcls be handled elsewhere ?
  private final Map<ApplicationAccessType, String> applicationACLs;

  public TaskAttemptEventStartedRemotely(TezTaskAttemptID id, ContainerId containerId,
      Map<ApplicationAccessType, String> appAcls) {
    super(id, TaskAttemptEventType.TA_STARTED_REMOTELY);
    this.containerId = containerId;
    this.applicationACLs = appAcls;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public Map<ApplicationAccessType, String> getApplicationACLs() {
    return applicationACLs;
  }

}

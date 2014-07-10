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

package org.apache.tez.dag.app.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerEventCompleted extends AMContainerEvent {

  private final int exitStatus;
  private final String diagnostics;

  public AMContainerEventCompleted(ContainerId containerId, 
      int exitStatus, String diagnostics) {
    super(containerId, AMContainerEventType.C_COMPLETED);
    this.exitStatus = exitStatus;
    this.diagnostics = diagnostics;
  }

  public boolean isPreempted() {
    return (exitStatus == ContainerExitStatus.PREEMPTED);
  }
  
  public boolean isDiskFailed() {
    return (exitStatus == ContainerExitStatus.DISKS_FAILED);
  }
  
  public String getDiagnostics() {
    return diagnostics;
  }
  
  public int getContainerExitStatus() {
    return exitStatus;
  }

}

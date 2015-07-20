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
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;

public class AMContainerEventCompleted extends AMContainerEvent {

  private final int exitStatus;
  private final String diagnostics;
  private final TaskAttemptTerminationCause errCause;

  public AMContainerEventCompleted(ContainerId containerId, 
      int exitStatus, String diagnostics, TaskAttemptTerminationCause errCause) {
    super(containerId, AMContainerEventType.C_COMPLETED);
    this.exitStatus = exitStatus;
    this.diagnostics = diagnostics;
    this.errCause = errCause;
  }

  public boolean isPreempted() {
    return (exitStatus == ContainerExitStatus.PREEMPTED || 
        errCause == TaskAttemptTerminationCause.INTERNAL_PREEMPTION);
  }
  
  public boolean isDiskFailed() {
    return (exitStatus == ContainerExitStatus.DISKS_FAILED);
  }
  
  public boolean isSystemAction() {
    return isPreempted() || isDiskFailed();
  }
  
  public String getDiagnostics() {
    return diagnostics;
  }
  
  public int getContainerExitStatus() {
    return exitStatus;
  }
  
  public TaskAttemptTerminationCause getTerminationCause() {
    return errCause;
  }

  public ContainerEndReason getContainerEndReason() {
    if (errCause != null) {
      switch (errCause) {
        case INTERNAL_PREEMPTION:
          return ContainerEndReason.INTERNAL_PREEMPTION;
        case EXTERNAL_PREEMPTION:
          return ContainerEndReason.EXTERNAL_PREEMPTION;
        case FRAMEWORK_ERROR:
          return ContainerEndReason.FRAMEWORK_ERROR;
        case APPLICATION_ERROR:
          return ContainerEndReason.APPLICATION_ERROR;
        case CONTAINER_LAUNCH_FAILED:
          return ContainerEndReason.LAUNCH_FAILED;
        case NODE_FAILED:
          return ContainerEndReason.NODE_FAILED;
        case CONTAINER_EXITED:
          return ContainerEndReason.COMPLETED;
        case UNKNOWN_ERROR:
        case TERMINATED_BY_CLIENT:
        case TERMINATED_AT_SHUTDOWN:
        case TERMINATED_INEFFECTIVE_SPECULATION:
        case TERMINATED_EFFECTIVE_SPECULATION:
        case TERMINATED_ORPHANED:
        case INPUT_READ_ERROR:
        case OUTPUT_WRITE_ERROR:
        case OUTPUT_LOST:
        case TASK_HEARTBEAT_ERROR:
        case CONTAINER_STOPPED:
        case NODE_DISK_ERROR:
        case COMMUNICATION_ERROR:
        case SERVICE_BUSY:
        case INTERRUPTED_BY_SYSTEM:
        case INTERRUPTED_BY_USER:
        default:
          return ContainerEndReason.OTHER;
      }
    } else {
      return ContainerEndReason.OTHER;
    }
  }
}

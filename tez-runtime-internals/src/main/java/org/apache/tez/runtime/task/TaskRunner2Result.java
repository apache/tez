/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.task;

import org.apache.tez.runtime.api.TaskFailureType;

public class TaskRunner2Result {
  final EndReason endReason;
  final TaskFailureType taskFailureType;
  final Throwable error;
  final boolean containerShutdownRequested;

  public TaskRunner2Result(EndReason endReason, TaskFailureType taskFailureType,
                           Throwable error, boolean containerShutdownRequested) {
    this.endReason = endReason;
    this.error = error;
    this.containerShutdownRequested = containerShutdownRequested;
    this.taskFailureType = taskFailureType;
  }

  public EndReason getEndReason() {
    return endReason;
  }

  public TaskFailureType getTaskFailureType() {
    return taskFailureType;
  }

  public Throwable getError() {
    return error;
  }

  public boolean isContainerShutdownRequested() {
    return containerShutdownRequested;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TaskRunner2Result{");
    sb.append("endReason=").append(endReason);
    sb.append(", containerShutdownRequested=").append(containerShutdownRequested);
    if (endReason != EndReason.SUCCESS) {
      sb.append(", failureType=").append(taskFailureType);
      sb.append(", error=").append(error);
    }
    sb.append("}");
    return sb.toString();
  }
}

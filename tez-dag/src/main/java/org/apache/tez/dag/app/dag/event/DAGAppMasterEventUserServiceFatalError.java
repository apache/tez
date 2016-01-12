/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.EnumSet;

import com.google.common.base.Preconditions;

public class DAGAppMasterEventUserServiceFatalError extends DAGAppMasterEvent implements DiagnosableEvent {

  private final Throwable error;
  private final String diagnostics;

  public DAGAppMasterEventUserServiceFatalError(DAGAppMasterEventType type,
                                                String diagnostics, Throwable t) {
    super(type);
    Preconditions.checkArgument(
        EnumSet.of(DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
            DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR,
            DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR).contains(type),
        "Event created with incorrect type: " + type);
    this.error = t;
    this.diagnostics = diagnostics;
  }

  public Throwable getError() {
    return error;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagnostics;
  }
}

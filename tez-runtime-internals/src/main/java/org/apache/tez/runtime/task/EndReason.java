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

public enum EndReason {
  SUCCESS(false),
  CONTAINER_STOP_REQUESTED(false),
  KILL_REQUESTED(true), // External kill request
  TASK_KILL_REQUEST(false), // Kill request originating from the task itself (self-kill)
  COMMUNICATION_FAILURE(false),
  TASK_ERROR(false);

  private final boolean isActionable;

  EndReason(boolean isActionable) {
    this.isActionable = isActionable;
  }
}

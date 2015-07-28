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

package org.apache.tez.serviceplugins.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tez.dag.api.NamedEntityDescriptor;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TaskCommunicatorDescriptor extends NamedEntityDescriptor<TaskCommunicatorDescriptor> {


  private TaskCommunicatorDescriptor(String taskCommName, String taskCommClassname) {
    super(taskCommName, taskCommClassname);
  }

  public static TaskCommunicatorDescriptor create(String taskCommName, String taskCommClassname) {
    return new TaskCommunicatorDescriptor(taskCommName, taskCommClassname);
  }
}

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

package org.apache.tez.dag.app;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.ServicePluginError;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezTaskAttemptID;
/**
 * This class listens for changes to the state of a Task.
 */
public interface TaskCommunicatorManagerInterface {

  void registerRunningContainer(ContainerId containerId, int taskCommId);

  void registerTaskAttempt(AMContainerTask amContainerTask, ContainerId containerId, int taskCommId);
  
  void unregisterRunningContainer(ContainerId containerId, int taskCommId, ContainerEndReason endReason, String diagnostics);
  
  void unregisterTaskAttempt(TezTaskAttemptID attemptID, int taskCommId, TaskAttemptEndReason endReason, String diagnostics);

  void dagComplete(DAG dag);

  void dagSubmitted();

  TaskCommunicatorWrapper getTaskCommunicator(int taskCommIndex);

  void reportError(int taskCommIndex, ServicePluginError servicePluginError, String diagnostics, DagInfo dagName);
}

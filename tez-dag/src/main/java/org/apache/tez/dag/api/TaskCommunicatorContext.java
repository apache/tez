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

package org.apache.tez.dag.api;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.records.TezTaskAttemptID;


// Do not make calls into this from within a held lock.

// TODO TEZ-2003 Move this into the tez-api module
public interface TaskCommunicatorContext {

  // TODO TEZ-2003 Add signalling back into this to indicate errors - e.g. Container unregsitered, task no longer running, etc.

  // TODO TEZ-2003 Maybe add book-keeping as a helper library, instead of each impl tracking container to task etc.

  ApplicationAttemptId getApplicationAttemptId();
  Credentials getCredentials();

  // TODO TEZ-2003 Move to vertex, taskIndex, version
  boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException;

  // TODO TEZ-2003 Split the heartbeat API to a liveness check and a status update
  TaskHeartbeatResponse heartbeat(TaskHeartbeatRequest request) throws IOException, TezException;

  boolean isKnownContainer(ContainerId containerId);

  void taskAlive(TezTaskAttemptID taskAttemptId);

  void containerAlive(ContainerId containerId);

  // TODO TEZ-2003 Move to vertex, taskIndex, version. Rename to taskAttempt*
  void taskStartedRemotely(TezTaskAttemptID taskAttemptId, ContainerId containerId);

  // TODO TEZ-2003 Move to vertex, taskIndex, version. Rename to taskAttempt*
  void taskKilled(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason, @Nullable String diagnostics);

  // TODO TEZ-2003 Move to vertex, taskIndex, version. Rename to taskAttempt*
  void taskFailed(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason, @Nullable String diagnostics);

  /**
   * Register to get notifications on updates to the specified vertex. Notifications will be sent
   * via {@link org.apache.tez.runtime.api.InputInitializer#onVertexStateUpdated(org.apache.tez.dag.api.event.VertexStateUpdate)} </p>
   *
   * This method can only be invoked once. Duplicate invocations will result in an error.
   *
   * @param vertexName the vertex name for which notifications are required.
   * @param stateSet   the set of states for which notifications are required. null implies all
   */
  void registerForVertexStateUpdates(String vertexName, @Nullable Set<VertexState> stateSet);
  // TODO TEZ-2003 API. Should a method exist for task succeeded.

  // TODO Eventually Add methods to report availability stats to the scheduler.

  /**
   * Get the name of the currently executing dag
   * @return the name of the currently executing dag
   */
  String getCurretnDagName();

  /**
   * Get the name of the Input vertices for the specified vertex.
   * Root Inputs are not returned.
   * @param vertexName the vertex for which source vertex names will be returned
   * @return an Iterable containing the list of input vertices for the specified vertex
   */
  Iterable<String> getInputVertexNames(String vertexName);

  /**
   * Get the total number of tasks in the given vertex
   * @param vertexName
   * @return total number of tasks in this vertex
   */
  int getVertexTotalTaskCount(String vertexName);

  /**
   * Get the number of completed tasks for a given vertex
   * @param vertexName the vertex name
   * @return the number of completed tasks for the vertex
   */
  int getVertexCompletedTaskCount(String vertexName);

  /**
   * Get the number of running tasks for a given vertex
   * @param vertexName the vertex name
   * @return the number of running tasks for the vertex
   */
  int getVertexRunningTaskCount(String vertexName);

  /**
   * Get the start time for the first attempt of the specified task
   * @param vertexName the vertex to which the task belongs
   * @param taskIndex the index of the task
   * @return the start time for the first attempt of the task
   */
  long getFirstAttemptStartTime(String vertexName, int taskIndex);

  /**
   * Get the start time for the currently executing DAG
   * @return time when the current dag started executing
   */
  long getDagStartTime();
}

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

package org.apache.tez.dag.api.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.tez.dag.api.TezException;

/**
 * Class for monitoring the <code>DAG</code> running in a Tez DAG
 * Application Master.
 */
@Public
public abstract class DAGClient implements Closeable {

  /**
   * Gets DAG execution context for use with logging
   * @return summary of DAG execution
   */
  public abstract String getExecutionContext();

  @Private
  /**
   * Get the YARN ApplicationReport for the app running the DAG. For performance
   * reasons this may be stale copy and should be used to access static info. It
   * may be null.
   * @return <code>ApplicationReport</code> or null
   */
  protected abstract ApplicationReport getApplicationReportInternal();

  /**
   * Get the status of the specified DAG
   * @param statusOptions Optionally, retrieve additional information based on
   *                      specified options. To retrieve basic information, this can be null
   */
  public abstract DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions)
      throws IOException, TezException;

  /**
   * Get the status of the specified DAG when it reaches a final state, or the timeout expires.
   *
   * @param statusOptions Optionally, retrieve additional information based on
   *                      specified options. To retrieve basic information, this can be null
   * @param timeout RPC call timeout. Value -1 waits for infinite and returns when
   *                DAG reaches final state
   * @return DAG Status
   * @throws IOException
   * @throws TezException
   */
  public abstract DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions,
      long timeout)
      throws IOException, TezException;

  /**
   * Get basic DAG information details such as name / id / vertex ID -> name mapping
   * @return DAG information
   * @throws IOException
   * @throws TezException
   */
  public abstract DAGInformation getDAGInformation() throws IOException, TezException;

  /**
   * Retrieve some details for a given task such as state / task counters etc.
   * @param vertexID ID of the vertex to which the task belongs
   * @param taskID ID of the task whose details need to be retrieved
   * @return Task information
   * @throws IOException
   * @throws TezException
   */
  public abstract TaskInformation getTaskInformation(String vertexID, String taskID) throws IOException, TezException;

  /**
   * Retrieve a list of task information objects. This API can be called multiple times to paginate
   * over task information for a set of tasks at a time.
   * @param vertexID ID of the vertex to which the tasks belong
   * @param startTaskID if null, we retrieve Task information details from the first task (ordered by TaskID).
   * If not null, we retrieve task information for tasks starting with this task.
   * @param limit Number of task information objects to retrieve
   * @return List of Task information objects.
   * @throws IOException
   * @throws TezException
   */
  public abstract List<TaskInformation> getTaskInformation(String vertexID, @Nullable String startTaskID, int limit) throws IOException, TezException;

  /**
   * Get the status of a Vertex of a DAG
   * @param statusOptions Optionally, retrieve additional information based on
   *                      specified options
   */
  public abstract VertexStatus getVertexStatus(String vertexName,
      Set<StatusGetOpts> statusOptions)
    throws IOException, TezException;

  /**
   * Kill a running DAG
   *
   */
  public abstract void tryKillDAG() throws IOException, TezException;

  /**
   * Wait for DAG to complete without printing any vertex statuses
   * 
   * @return Final DAG Status
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException 
   */
  public abstract DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException;

  /**
   * Wait for DAG to complete and periodically print *all* vertices' status.
   * 
   * @param statusGetOpts
   *          : status get options. For example, to get counter pass
   *          <code>EnumSet.of(StatusGetOpts.GET_COUNTERS)</code>
   * @return Final DAG Status
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException 
   */
  public abstract DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<StatusGetOpts> statusGetOpts)
      throws IOException, TezException, InterruptedException;
}

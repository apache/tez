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
   * Get the status of a Vertex of a DAG
   * @param statusOptions Optionally, retrieve additional information based on
   *                      specified options
   */
  public abstract VertexStatus getVertexStatus(String vertexName,
      Set<StatusGetOpts> statusOptions)
    throws IOException, TezException;

  /**
   * Get the dag identifier for the currently executing dag. This is a string
   * which represents this dag
   * @return the dag identifier
   */
  public abstract String getDagIdentifierString();

  /**
   * Get the session identifier for the session in which this dag is running
   * @return the session identifier
   */
  public abstract String getSessionIdentifierString();

  /**
   * Kill a running DAG
   *
   */
  public abstract void tryKillDAG() throws IOException, TezException;

  /**
   * Wait forever for DAG to complete without printing any vertex statuses
   * 
   * @return Final DAG Status
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException 
   */
  public abstract DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException;

  /**
   * Wait for DAG to complete without printing any vertex statuses
   *
   * @param timeMs Maximum wait duration
   * @return Final DAG Status, or null on timeout or if DAG is no longer running
   * @throws IOException
   * @throws TezException
   * @throws InterruptedException
   */
  public DAGStatus waitForCompletion(long timeMs) throws IOException, TezException, InterruptedException {
    // Make non-abstract to avoid compat issues in Hive.
    throw new UnsupportedOperationException();
  }

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

  /**
   * Returns the Tez AM's web ui address if any.
   *
   * @return The http web UI address
   * @throws IOException
   * @throws TezException
   */
  public abstract String getWebUIAddress() throws IOException, TezException;

}

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

package org.apache.tez.dag.app.dag.speculation.legacy;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;

/**
 * Estimate the runtime for tasks of a given vertex.
 * 
 */
public interface TaskRuntimeEstimator {
  public void enrollAttempt(TezTaskAttemptID id, long timestamp);

  public long attemptEnrolledTime(TezTaskAttemptID attemptID);

  public void updateAttempt(TezTaskAttemptID taId, TaskAttemptState reportedState, long timestamp);

  public void contextualize(Configuration conf, Vertex vertex);

  /**
   *
   * Find a maximum reasonable execution wallclock time.  Includes the time
   * already elapsed.
   *
   * Find a maximum reasonable execution time.  Includes the time
   * already elapsed.  If the projected total execution time for this task
   * ever exceeds its reasonable execution time, we may speculate it.
   *
   * @param id the {@link TezTaskID} of the task we are asking about
   * @return the task's maximum reasonable runtime, or MAX_VALUE if
   *         we don't have enough information to rule out any runtime,
   *         however long.
   *
   */
  public long thresholdRuntime(TezTaskID id);

  /**
   *
   * Estimate a task attempt's total runtime.  Includes the time already
   * elapsed.
   *
   * @param id the {@link TezTaskAttemptID} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long estimatedRuntime(TezTaskAttemptID id);

  /**
   *
   * Estimates how long a new attempt on this task will take if we start
   *  one now
   *
   * @return our best estimate of a new attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long newAttemptEstimatedRuntime();

  /**
   *
   * Computes the width of the error band of our estimate of the task
   *  runtime as returned by {@link #estimatedRuntime(TezTaskAttemptID)}
   *
   * @param id the {@link TezTaskAttemptID} of the attempt we are asking about
   * @return our best estimate of the attempt's runtime, or {@code -1} if
   *         we don't have enough information yet to produce an estimate.
   *
   */
  public long runtimeEstimateVariance(TezTaskAttemptID id);
}

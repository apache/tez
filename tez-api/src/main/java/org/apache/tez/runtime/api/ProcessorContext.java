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

package org.apache.tez.runtime.api;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.common.ProgressHelper;

/**
 * Context handle for the Processor to initialize itself.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface ProcessorContext extends TaskContext {

  /**
   * validate that progress is the valid range.
   * @param progress
   * @return the processed value of the progress that is guaranteed to be within
   *          the valid range.
   */
  static float preProcessProgress(float progress) {
    return ProgressHelper.processProgress(progress);
  }

  /**
   * Set the overall progress of this Task Attempt.
   * This automatically results in invocation of {@link ProcessorContext#notifyProgress()} 
   * and so invoking that separately is not required.
   * @param progress Progress in the range from [0.0 - 1.0f]
   */
  default void setProgress(float progress) {
    setProgressInternally(preProcessProgress(progress));
  }

  /**
   * The actual implementation of the taskAttempt progress.
   * All implementations needs to override this method
   * @param progress
   */
  void setProgressInternally(float progress);

  /**
   * Check whether this attempt can commit its output
   * @return true if commit allowed
   * @throws IOException
   */
  public boolean canCommit() throws IOException;

  /**
   * Blocking call which returns when any of the specified Inputs is ready for
   * consumption.
   * 
   * There can be multiple parallel invocations of this function - where each
   * invocation blocks on the Inputs that it specifies.
   * 
   * If multiple Inputs are ready, any one of them may be returned by this
   * method - including an Input which may have been returned in a previous
   * call. If invoking this method multiple times, it's recommended to remove
   * previously completed Inputs from the invocation list.
   * 
   * @param inputs
   *          the list of Inputs to monitor
   * @return the Input which is ready for consumption
   * @throws InterruptedException
   */
  public Input waitForAnyInputReady(Collection<Input> inputs) throws InterruptedException;

  /**
   * Blocking call which returns when any of the specified Inputs is ready for
   * consumption.
   *
   * There can be multiple parallel invocations of this function - where each
   * invocation blocks on the Inputs that it specifies.
   *
   * If multiple Inputs are ready, any one of them may be returned by this
   * method - including an Input which may have been returned in a previous
   * call. If invoking this method multiple times, it's recommended to remove
   * previously completed Inputs from the invocation list.
   *
   * @param inputs
   *          the list of Inputs to monitor
   * @param timeoutMillis
   *          timeout to return in milliseconds. If this value is negative,
   *          this function will wait forever until all inputs get ready
   *          or interrupted.
   * @return the Input which is ready for consumption. return null when timeout occurs.
   * @throws InterruptedException
   */
  public Input waitForAnyInputReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException;

  /**
   * Blocking call which returns only after all of the specified Inputs are
   * ready for consumption.
   * 
   * There can be multiple parallel invocations of this function - where each
   * invocation blocks on the Inputs that it specifies.
   * 
   * @param inputs
   *          the list of Inputs to monitor
   * @throws InterruptedException
   */
  public void waitForAllInputsReady(Collection<Input> inputs) throws InterruptedException;

  /**
   * Blocking call which returns only after all of the specified Inputs are
   * ready for consumption with timeout.
   *
   * There can be multiple parallel invocations of this function - where each
   * invocation blocks on the Inputs that it specifies.
   *
   * @param inputs
   *          the list of Inputs to monitor
   * @param timeoutMillis
   *          timeout to return in milliseconds. If this value is negative,
   *          this function will wait forever until all inputs get ready
   *          or interrupted.
   * @return Return true if all inputs are ready. Otherwise, return false.
   * @throws InterruptedException
   */
  public boolean waitForAllInputsReady(Collection<Input> inputs, long timeoutMillis) throws InterruptedException;
}

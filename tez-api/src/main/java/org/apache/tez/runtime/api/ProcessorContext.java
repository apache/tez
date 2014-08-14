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

/**
 * Context handle for the Processor to initialize itself.
 * This interface is not supposed to be implemented by users
 */
@Public
public interface ProcessorContext extends TaskContext {

  /**
   * Set the overall progress of this Task Attempt
   * @param progress Progress in the range from [0.0 - 1.0f]
   */
  public void setProgress(float progress);

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
}

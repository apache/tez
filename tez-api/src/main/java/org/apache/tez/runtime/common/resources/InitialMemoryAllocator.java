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

package org.apache.tez.runtime.common.resources;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configurable;



/**
 * Used to balance memory requests before a task starts executing.
 */
@Private
public interface InitialMemoryAllocator extends Configurable {

  /**
   * @param availableForAllocation
   *          memory available for allocation
   * @param numTotalInputs
   *          number of inputs for the task
   * @param numTotalOutputs
   *          number of outputs for the tasks
   * @param requests
   *          Iterable view of requests received
   * @return list of allocations, one per request. This must be ordered in the
   *         same order of the requests.
   */
  public abstract Iterable<Long> assignMemory(long availableForAllocation, int numTotalInputs,
      int numTotalOutputs, Iterable<InitialMemoryRequestContext> requests);

}
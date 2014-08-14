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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.UserPayload;

/**
 * Context for {@link MergedLogicalInput}
 * This interface is not supposed to be implemented by users
 */
@Public
public interface MergedInputContext {

  /**
   * Get the user payload for this input
   * @return {@link UserPayload}
   */
  public UserPayload getUserPayload();
  
  /**
   * Inform the framework that the specific Input is ready for consumption.
   * 
   * This method can be invoked multiple times.
   */
  public void inputIsReady();
  
  /**
   * Get the work directories for the Input
   * @return an array of work dirs
   */
  public String[] getWorkDirs();
}

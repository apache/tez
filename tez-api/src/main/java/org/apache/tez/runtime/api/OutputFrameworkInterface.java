/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.api;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Represents the Tez framework part of an {@link org.apache.tez.runtime.api.Output}.
 * <p/>
 *
 * This interface has methods which are used by the Tez framework to control the Output.
 * <p/>
 *
 * During initialization, Outputs must specify an initial memory requirement via
 * {@link OutputContext}.requestInitialMemory
 * <p/>
 * 
 * Users are expected to derive from {@link AbstractLogicalOutput}
 *
 */
@Public
public interface OutputFrameworkInterface {

  /**
   * Initializes the <code>Output</code>
   *
   * @return list of events that were generated during initialization
   * @throws Exception
   *           if an error occurs
   */
  public List<Event> initialize() throws Exception;

  /**
   * Handles user and system generated {@link Event}s, which typically carry
   * information such as a downstream vertex being ready to consume input.
   *
   * @param outputEvents
   *          the list of {@link Event}s
   */
  public void handleEvents(List<Event> outputEvents);

  /**
   * Closes the <code>Output</code>
   *
   * @return list of events that were generated during close
   * @throws Exception
   *           if an error occurs
   */
  public List<Event> close() throws Exception;
}

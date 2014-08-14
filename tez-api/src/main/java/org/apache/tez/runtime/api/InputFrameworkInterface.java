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
 * Represents an input through which a TezProcessor receives data on an edge.
 * <p/>
 *
 * This interface has methods which are used by the Tez framework to control the Input.
 * <p/>
 *
 * During initialization, Inputs must specify an initial memory requirement via
 * {@link InputContext}.requestInitialMemory
 * <p/>
 *
 *
 * Inputs must also inform the framework once they are ready to be consumed.
 * This typically means that the Processor will not block when reading from the
 * corresponding Input. This is done via {@link InputContext}.inputIsReady.
 * Inputs choose the policy on when they are ready.
 * 
 * Input implementations are expected to derive from {@link AbstractLogicalInput}
 */
@Public
public interface InputFrameworkInterface {
  /**
   * Initializes the <code>Input</code>.
   *
   * @return list of events that were generated during initialization
   * @throws Exception
   *           if an error occurs
   */
  public List<Event> initialize() throws Exception;

  /**
   * Handles user and system generated {@link Event}s, which typically carry
   * information such as an output being available on the previous vertex.
   *
   * @param inputEvents
   *          the list of {@link Event}s
   */
  public void handleEvents(List<Event> inputEvents) throws Exception;



  /**
   * Closes the <code>Input</code>
   *
   * @return list of events that were generated during close
   * @throws Exception
   *           if an error occurs
   */
  public List<Event> close() throws Exception;
}

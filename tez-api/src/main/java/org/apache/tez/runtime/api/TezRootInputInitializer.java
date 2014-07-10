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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tez.runtime.api.events.RootInputInitializerEvent;

/**
 * <code>TezRootInputInitializer</code>s are used to initialize root vertices
 * within the AM. They can be used to distribute data across the tasks for the
 * vertex, determine the number of tasks at runtime, update the Input payload
 * etc.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface TezRootInputInitializer {

  /**
   * Run the root input initializer. This is the main method where initialization takes place. If an
   * Initializer is written to accept events, a notification mechanism should be setup, with the
   * heavy lifting of processing the event being done via this method. The moment this method
   * returns a list of events, RootInputInitialization is considered to be complete.
   *
   * @param inputVertexContext initializer context which can be used to access the payload, vertex
   *                           properties, etc
   * @return a list of events which are eventually routed to a {@link org.apache.tez.dag.api.VertexManagerPlugin}
   * for routing
   * @throws Exception
   */
  List<Event> initialize(TezRootInputInitializerContext inputVertexContext)
      throws Exception;

  /**
   * Handle events meant for the specific Initializer. This is a notification mechanism to inform
   * the Initializer about events received. Extensive event processing should not be performed via
   * this method call. Instead this should just be used as a notification method to the main
   * initialization via the initialize method.
   *
   * @param events list of events
   * @throws Exception
   */
  void handleInputInitializerEvent(List<RootInputInitializerEvent> events) throws Exception;
  
}

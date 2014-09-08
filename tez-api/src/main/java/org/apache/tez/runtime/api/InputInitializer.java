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

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

/**
 * <code>InputInitializer</code>s are typically used to initialize vertices
 * connected to data sources. They run in the App Master and can be used to
 * distribute data across the tasks for the vertex, determine the number of
 * tasks at runtime, update the Input payload etc.
 */
@Unstable
@Public
public abstract class InputInitializer {

  private final InputInitializerContext initializerContext;

  /**
   * Constructor an instance of the InputInitializer. Classes extending this to create a
   * InputInitializer, must provide the same constructor so that Tez can create an instance of
   * the class at runtime.
   *
   * @param initializerContext initializer context which can be used to access the payload, vertex
   *                           properties, etc
   */
  public InputInitializer(InputInitializerContext initializerContext) {
    this.initializerContext = initializerContext;
  }

  /**
   * Run the initializer. This is the main method where
   * initialization takes place. If an Initializer is written to accept events,
   * a notification mechanism should be setup, with the heavy lifting of
   * processing the event being done via this method. The moment this method
   * returns a list of events, input initialization is considered to be
   * complete.
   * 
   * @return a list of events which are eventually routed to a
   *         {@link org.apache.tez.dag.api.VertexManagerPlugin} for routing
   * @throws Exception
   */
  public abstract List<Event> initialize()
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
  public abstract void handleInputInitializerEvent(List<InputInitializerEvent> events)
      throws Exception;

  /**
   * Return ahe {@link org.apache.tez.runtime.api.InputInitializerContext}
   * for this specific instance of the Initializer.
   * 
   * @return the {@link org.apache.tez.runtime.api.InputInitializerContext}
   *         for the initializer
   */
  public final InputInitializerContext getContext() {
    return this.initializerContext;
  }

  /**
   * Receive notifications on vertex state changes.
   * <p/>
   * State changes will be received based on the registration via {@link
   * org.apache.tez.runtime.api.InputInitializerContext#registerForVertexStateUpdates(String,
   * java.util.Set)}. Notifications will be received for all registered state changes, and not just
   * for the latest state update. They will be in order in which the state change occurred.
   *
   * @param stateUpdate an event indicating the name of the vertex, and it's updated state.
   *                    Additional information may be available for specific events, Look at the
   *                    type hierarchy for {@link org.apache.tez.dag.api.event.VertexStateUpdate}
   */
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
  }
}

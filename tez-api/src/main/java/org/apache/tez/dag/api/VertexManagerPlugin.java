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

package org.apache.tez.dag.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

/**
 * Interface to plugin user logic into the VertexManager to implement runtime 
 * scheduling optimizations and graph reconfiguration.
 * The plugin will be notified of interesting events in the vertex execution life
 * cycle and can respond to them by via the context object
 */
@Unstable
@Public
public abstract class VertexManagerPlugin {

  private final VertexManagerPluginContext context;

  /**
   * Crete an instance of the VertexManagerPlugin. Classes extending this to
   * create a VertexManagerPlugin, must provide the same constructor so that Tez
   * can create an instance of the class at runtime.
   * 
   * @param context
   *          vertex manager plugin context which can be used to access the
   *          payload, vertex properties, etc
   */
  public VertexManagerPlugin(VertexManagerPluginContext context) {
    this.context = context;
  }

  /**
   * Initialize the plugin. Called when the vertex is initializing. This happens 
   * after all source vertices and inputs have initialized
   * @throws Exception
   */
  public abstract void initialize() throws Exception;

  /**
   * Notification that the vertex is ready to start running tasks
   * @param completions Source vertices and all their tasks that have already completed
   * @throws Exception
   */
  public abstract void onVertexStarted(Map<String, List<Integer>> completions) throws Exception;

  /**
   * Notification of a source vertex completion.
   * @param srcVertexName
   * @param taskId Index of the task that completed
   * @throws Exception
   */
  public abstract void onSourceTaskCompleted(String srcVertexName, Integer taskId) throws Exception;

  /**
   * Notification of an event directly sent to this vertex manager
   * @param vmEvent
   * @throws Exception
   */
  public abstract void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws Exception;

  /**
   * Notification that the inputs of this vertex have initialized
   * @param inputName
   * @param inputDescriptor
   * @param events
   * @throws Exception
   */
  public abstract void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) throws Exception;

  /**
   * Return ahe {@link org.apache.tez.dag.api.VertexManagerPluginContext} for this specific instance of
   * the vertex manager.
   *
   * @return the {@link org.apache.tez.dag.api.VertexManagerPluginContext} for the input
   */
  public final VertexManagerPluginContext getContext() {
    return this.context;
  }
  
  /**
   * Receive notifications on vertex state changes.
   * <p/>
   * State changes will be received based on the registration via
   * {@link VertexManagerPluginContext#registerForVertexStateUpdates(String, java.util.Set)}
   * . Notifications will be received for all registered state changes, and not
   * just for the latest state update. They will be in order in which the state
   * change occurred.
   * </p><br>This method may be invoked concurrently with {@link #onVertexStarted(Map)} etc. and 
   * multi-threading/concurrency implications must be considered.
   *  
   * @param stateUpdate
   *          an event indicating the name of the vertex, and it's updated
   *          state. Additional information may be available for specific
   *          events, Look at the type hierarchy for
   *          {@link org.apache.tez.dag.api.event.VertexStateUpdate}
   */
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception {
  }  
 }

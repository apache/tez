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

import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

/**
 * Interface to plugin user logic into the VertexManager to implement runtime 
 * scheduling optimizations and graph reconfiguration.
 * The plugin will be notified of interesting events in the vertex execution life
 * cycle and can respond to them by via the context object
 */
public abstract class VertexManagerPlugin {
  /**
   * Initialize the plugin. Called when the vertex is initializing. This happens 
   * after all source vertices and inputs have initialized
   * @param context
   */
  public abstract void initialize(VertexManagerPluginContext context);

  /**
   * Notification that the vertex is ready to start running tasks
   * @param completions Source vertices and all their tasks that have already completed
   */
  public abstract void onVertexStarted(Map<String, List<Integer>> completions);

  /**
   * Notification of a source vertex completion.
   * @param srcVertexName
   * @param taskId Index of the task that completed
   */
  public abstract void onSourceTaskCompleted(String srcVertexName, Integer taskId);

  /**
   * Notification of an event directly sent to this vertex manager
   * @param vmEvent
   */
  public abstract void onVertexManagerEventReceived(VertexManagerEvent vmEvent);

  /**
   * Notification that the inputs of this vertex have initialized
   * @param inputName
   * @param inputDescriptor
   * @param events
   */
  public abstract void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events);
 }

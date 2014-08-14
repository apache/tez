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
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

/**
 * This interface defines the routing of the event between tasks of producer and 
 * consumer vertices. The routing is bi-directional. Users can customize the 
 * routing by providing an implementation of this interface.
 */
@Public
@Unstable
public abstract class EdgeManagerPlugin {

  private final EdgeManagerPluginContext context;

  /**
   * Create an instance of the EdgeManagerPlugin. Classes extending this to
   * create a EdgeManagerPlugin, must provide the same constructor so that Tez
   * can create an instance of the class at runtime.
   * 
   * @param context
   *          the context within which this EdgeManagerPlugin will run. Includes
   *          information like configuration which the user may have specified
   *          while setting up the edge.
   */
  public EdgeManagerPlugin(EdgeManagerPluginContext context) {
    this.context = context;
  }

  /**
   * Initializes the EdgeManagerPlugin. This method is called in the following
   * circumstances </p> 1. when initializing an EdgeManagerPlugin for the first time.
   * </p> 2. When an EdgeManagerPlugin is replaced at runtime. At this point, an
   * EdgeManagerPlugin instance is created and setup by the user. The initialize
   * method will be called with the original {@link EdgeManagerPluginContext} when the
   * EdgeManagerPlugin is replaced.
   *
   */
  public abstract void initialize();
  
  /**
   * Get the number of physical inputs on the destination task
   * @param destinationTaskIndex Index of destination task for which number of 
   * inputs is needed
   * @return Number of physical inputs on the destination task
   */
  public abstract int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex);

  /**
   * Get the number of physical outputs on the source task
   * @param sourceTaskIndex Index of the source task for which number of outputs 
   * is needed
   * @return Number of physical outputs on the source task
   */
  public abstract int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex);
  
  /**
   * Return the routing information to inform consumers about the source task
   * output that is now available. The return map has the routing information.
   * The event will be routed to every destination task index in the key of the
   * map. Every physical input in the value for that task key will receive the
   * input.
   * 
   * @param event
   *          Data movement event that contains the output information
   * @param sourceTaskIndex
   *          Source task that produced the event
   * @param sourceOutputIndex
   *          Index of the physical output on the source task that produced the
   *          event
   * @param destinationTaskAndInputIndices
   *          Map via which the routing information is returned
   */
  public abstract void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices);
  
  /**
   * Return the routing information to inform consumers about the failure of a
   * source task whose outputs have been potentially lost. The return map has
   * the routing information. The failure notification event will be sent to
   * every task index in the key of the map. Every physical input in the value
   * for that task key will receive the failure notification. This method will
   * be called once for every source task failure and information for all
   * affected destinations must be provided in that invocation.
   * 
   * @param sourceTaskIndex
   *          Source task
   * @param destinationTaskAndInputIndices
   *          Map via which the routing information is returned
   */
  public abstract void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices);

  /**
   * Get the number of destination tasks that consume data from the source task
   * @param sourceTaskIndex Source task index
   */
  public abstract int getNumDestinationConsumerTasks(int sourceTaskIndex);
  
  /**
   * Return the source task index to which to send the input error event
   * 
   * @param event
   *          Input read error event. Has more information about the error
   * @param destinationTaskIndex
   *          Destination task that reported the error
   * @param destinationFailedInputIndex
   *          Index of the physical input on the destination task that reported 
   *          the error
   * @return Index of the source task that created the unavailable input
   */
  public abstract int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex);

  /**
   * Return ahe {@link org.apache.tez.dag.api.EdgeManagerPluginContext} for this specific instance of
   * the vertex manager.
   *
   * @return the {@link org.apache.tez.dag.api.EdgeManagerPluginContext} for the input
   */
  public EdgeManagerPluginContext getContext() {
    return this.context;
  }
}

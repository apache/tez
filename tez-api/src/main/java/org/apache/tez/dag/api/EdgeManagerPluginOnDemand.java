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

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

/**
 * This interface defines the routing of the event between tasks of producer and 
 * consumer vertices. The routing is bi-directional. Users can customize the 
 * routing by providing an implementation of this interface.
 */
@Public
@Unstable
public abstract class EdgeManagerPluginOnDemand extends EdgeManagerPlugin {

  /**
   * Class to provide routing metadata for {@link Event}s to be routed between
   * producer and consumer tasks. The routing data enabled the system to send 
   * the event from the producer task output to the consumer task input
   */
  public static class EventRouteMetadata {
    private final int numEvents;
    private final int[] targetIndices;
    private final int[] sourceIndices;
    
    /**
     * Create an {@link EventRouteMetadata} that will create numEvents copies of
     * the {@link Event} to be routed. Use this to create
     * {@link EventRouteMetadata} for {@link DataMovementEvent}s or
     * {@link InputFailedEvent}s where the target input indices must be
     * specified to route those events. Typically numEvents would be 1 for these
     * events.
     * 
     * @param numEvents
     *          Number of copies of the event to be routed
     * @param targetIndices
     *          Target input indices. The array length must match the number of
     *          events specified when creating the {@link EventRouteMetadata}
     *          object
     * @return {@link EventRouteMetadata}
     */
    public static EventRouteMetadata create(int numEvents, int[] targetIndices) {
      return new EventRouteMetadata(numEvents, targetIndices, null);
    }
    
    /**
     * Create an {@link EventRouteMetadata} that will create numEvents copies of
     * the {@link Event} to be routed. Use this to create
     * {@link EventRouteMetadata} for {@link CompositeDataMovementEvent} where
     * the target input indices and source output indices must be specified to
     * route those events. Typically numEvents would be 1 for these events.
     * 
     * @param numEvents
     *          Number of copies of the event to be routed
     * @param targetIndices
     *          Target input indices. The array length must match the number of
     *          events specified when creating the {@link EventRouteMetadata}
     *          object
     * @param sourceIndices
     *          Source output indices. The array length must match the number of
     *          events specified when creating the {@link EventRouteMetadata}
     *          object
     * @return {@link EventRouteMetadata}
     */
    public static EventRouteMetadata create(int numEvents, int[] targetIndices, int[] sourceIndices) {
      return new EventRouteMetadata(numEvents, targetIndices, sourceIndices);
    }

    private EventRouteMetadata(int numEvents, int[] targetIndices, int[] sourceIndices) {
      this.numEvents = numEvents;
      this.targetIndices = targetIndices;
      this.sourceIndices = sourceIndices;
    }

    /**
     * Get the number of copies of the event to be routed
     * @return Number of copies
     */
    public int getNumEvents() {
      return numEvents;
    }

    /**
     * Get the target input indices
     * @return Target input indices
     */
    public @Nullable int[] getTargetIndices() {
      return targetIndices;
    }

    /**
     * Get the source output indices
     * @return Source output indices
     */
    public @Nullable int[] getSourceIndices() {
      return sourceIndices;
    }
  }

  /**
   * Create an instance of the {@link EdgeManagerPluginOnDemand}. Classes
   * extending this to create a {@link EdgeManagerPluginOnDemand}, must provide
   * the same constructor so that Tez can create an instance of the class at
   * runtime.
   * 
   * @param context
   *          the context within which this {@link EdgeManagerPluginOnDemand}
   *          will run. Includes information like configuration which the user
   *          may have specified while setting up the edge.
   */
  public EdgeManagerPluginOnDemand(EdgeManagerPluginContext context) {
    super(context);
  }

  /**
   * Initializes the EdgeManagerPlugin. This method is called in the following
   * circumstances </p> 1. when initializing an EdgeManagerPlugin for the first time.
   * </p> 2. When an EdgeManagerPlugin is replaced at runtime. At this point, an
   * EdgeManagerPlugin instance is created and setup by the user. The initialize
   * method will be called with the original {@link EdgeManagerPluginContext} when the
   * EdgeManagerPlugin is replaced.
   * @throws Exception
   */
  public abstract void initialize() throws Exception;
  
  /**
   * This method will be invoked just before routing of events will begin. The
   * plugin can use this opportunity to make any runtime initialization's that
   * depend on the actual state of the DAG or vertices.
   */
  public abstract void prepareForRouting() throws Exception;

  /**
   * Get the number of physical inputs on the destination task
   * @param destinationTaskIndex Index of destination task for which number of 
   * inputs is needed
   * @return Number of physical inputs on the destination task
   * @throws Exception
   */
  public abstract int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) throws Exception;

  /**
   * Get the number of physical outputs on the source task
   * @param sourceTaskIndex Index of the source task for which number of outputs 
   * is needed
   * @return Number of physical outputs on the source task
   * @throws Exception
   */
  public abstract int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) throws Exception;
  
  /**
   * Get the number of destination tasks that consume data from the source task
   * @param sourceTaskIndex Source task index
   * @throws Exception
   */
  public abstract int getNumDestinationConsumerTasks(int sourceTaskIndex) throws Exception;
  
  /**
   * Return the source task index to which to send the input error event
   * 
   * @param destinationTaskIndex
   *          Destination task that reported the error
   * @param destinationFailedInputIndex
   *          Index of the physical input on the destination task that reported 
   *          the error
   * @return Index of the source task that created the unavailable input
   * @throws Exception
   */
  public abstract int routeInputErrorEventToSource(int destinationTaskIndex,
      int destinationFailedInputIndex) throws Exception;
  
  /**
   * The method provides the {@link EventRouteMetadata} to route a
   * {@link DataMovementEvent} produced by the given source task to the given
   * destination task. The returned {@link EventRouteMetadata} should have the
   * target input indices set to enable the routing. If the routing metadata is
   * common across different events then the plugin can cache and reuse the same
   * object.
   * 
   * @param sourceTaskIndex
   *          The index of the task in the source vertex of this edge that
   *          produced a {@link DataMovementEvent}
   * @param sourceOutputIndex
   *          Index of the physical output on the source task that produced the
   *          event
   * @param destinationTaskIndex
   * @return {@link EventRouteMetadata} with target indices set. Maybe null if
   *         the given destination task does not read input from the given
   *         source task.
   * @throws Exception
   */
  public abstract @Nullable EventRouteMetadata routeDataMovementEventToDestination(int sourceTaskIndex,
      int sourceOutputIndex, int destinationTaskIndex) throws Exception;

  /**
   * The method provides the {@link EventRouteMetadata} to route a
   * {@link CompositeDataMovementEvent} produced by the given source task to the
   * given destination task. The returned {@link EventRouteMetadata} should have
   * the target input indices and source output indices set to enable the
   * routing. If the routing metadata is common across different events then the
   * plugin can cache and reuse the same object.
   * 
   * @param sourceTaskIndex
   *          The index of the task in the source vertex of this edge that
   *          produced a {@link CompositeDataMovementEvent}
   * @param destinationTaskIndex
   *          The index of the task in the destination vertex of this edge
   * @return {@link EventRouteMetadata} with source and target indices set. This
   *         may be null if the destination task does not read data from the
   *         source task.
   * @throws Exception
   */
  public abstract @Nullable EventRouteMetadata routeCompositeDataMovementEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception;

  /**
   * The method provides the {@link EventRouteMetadata} to route an
   * {@link InputFailedEvent} produced by the given source task to the given
   * destination task. The returned {@link EventRouteMetadata} should have the
   * target input indices set to enable the routing. If the routing metadata is
   * common across different events then the plugin can cache and reuse the same
   * object.
   * 
   * @param sourceTaskIndex
   *          The index of the failed task in the source vertex of this edge.
   * @param destinationTaskIndex
   *          The index of a task in the destination vertex of this edge.
   * @return {@link EventRouteMetadata} with target indices set. Maybe null if
   *         the given destination task does not read input from the given
   *         source task.
   * @throws Exception
   */
  public abstract @Nullable EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception;
  
  /**
   * Return the {@link org.apache.tez.dag.api.EdgeManagerPluginContext} for this specific instance of
   * the vertex manager.
   *
   * @return the {@link org.apache.tez.dag.api.EdgeManagerPluginContext} for the input
   */
  public EdgeManagerPluginContext getContext() {
    return super.getContext();
  }

  // Empty implementations of EdgeManagerPlugin interfaces that are not needed
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
   * @throws Exception
   */
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) throws Exception {}

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
   * @throws Exception
   */
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) throws Exception {}

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
   * @throws Exception
   */
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex) throws Exception { 
    return routeInputErrorEventToSource(destinationTaskIndex, destinationFailedInputIndex);
  }

}

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

package org.apache.tez.test;

import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class EdgeManagerForTest extends EdgeManagerPluginOnDemand {

  private UserPayload userPayload;

  public EdgeManagerForTest(EdgeManagerPluginContext context) {
    super(context);
  }

  public EdgeManagerPluginContext getEdgeManagerContext() {
    return getContext();
  }

  public UserPayload getUserPayload() {
    return userPayload;
  }

  // Overridden methods
  @Override
  public void initialize() {
    this.userPayload = getContext().getUserPayload();
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
    return 0;
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
    return 0;
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event, int sourceTaskIndex,
      int sourceOutputIndex, Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return 0;
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event, int destinationTaskIndex, int destinationFailedInputIndex) {
    return 0;
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) { 
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int sourceTaskIndex,
      int sourceOutputIndex, int destinationTaskIndex) throws Exception {
    return null;
  }

  @Override
  public EventRouteMetadata routeCompositeDataMovementEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex)
      throws Exception {
    return null;
  }

  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception {
    return null;
  }

  @Override
  public void prepareForRouting() throws Exception {
  }

  @Override
  public int routeInputErrorEventToSource(int destinationTaskIndex, int destinationFailedInputIndex)
      throws Exception {
    return 0;
  }
  
  // End of overridden methods

}
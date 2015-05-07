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

package org.apache.tez.dag.app.dag.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class BroadcastEdgeManager extends EdgeManagerPluginOnDemand {

  EventRouteMetadata[] commonRouteMeta;

  public BroadcastEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() {
  }
  
  @Override
  public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
    return getContext().getSourceVertexNumTasks();
  }
  
  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
    return 1;
  }
  
  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex, 
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    List<Integer> inputIndices = 
        Collections.unmodifiableList(Collections.singletonList(sourceTaskIndex));
    // for each task make the i-th source task as the i-th physical input
    for (int i=0; i<getContext().getDestinationVertexNumTasks(); ++i) {
      destinationTaskAndInputIndices.put(i, inputIndices);
    }
  }
  
  @Override
  public void prepareForRouting() throws Exception {
    int numSourceTasks = getContext().getSourceVertexNumTasks();
    commonRouteMeta = new EventRouteMetadata[numSourceTasks];
    for (int i=0; i<numSourceTasks; ++i) {
      commonRouteMeta[i] = EventRouteMetadata.create(1, new int[]{i}, new int[]{0});
    }
  }
  
  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(
      int sourceTaskIndex, int sourceOutputIndex, int destinationTaskIndex)
      throws Exception {
    return commonRouteMeta[sourceTaskIndex];
  }
  
  @Override
  public EventRouteMetadata routeCompositeDataMovementEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex)
      throws Exception {
    return commonRouteMeta[sourceTaskIndex];
  }

  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception {
    return commonRouteMeta[sourceTaskIndex];
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    List<Integer> inputIndices = 
        Collections.unmodifiableList(Collections.singletonList(sourceTaskIndex));
    // for each task make the i-th source task as the i-th physical input
    for (int i=0; i<getContext().getDestinationVertexNumTasks(); ++i) {
      destinationTaskAndInputIndices.put(i, inputIndices);
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destinationTaskIndex, int destinationFailedInputIndex)
      throws Exception {
    return destinationFailedInputIndex;
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex) {
    return destinationFailedInputIndex;
  }
  
  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return getContext().getDestinationVertexNumTasks();
  }

}

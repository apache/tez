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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import com.google.common.base.Preconditions;

public class OneToOneEdgeManager extends EdgeManagerPluginOnDemand {

  List<Integer> destinationInputIndices = 
      Collections.unmodifiableList(Collections.singletonList(0));
  AtomicBoolean stateChecked = new AtomicBoolean(false);
 
  final EventRouteMetadata commonRouteMeta = 
      EventRouteMetadata.create(1, new int[]{0}, new int[]{0});

  public OneToOneEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() {
    // Nothing to do.
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
    return 1;
  }
  
  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
    return 1;
  }
  
  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex, 
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    checkState();
    destinationTaskAndInputIndices.put(sourceTaskIndex, destinationInputIndices);
  }
  
  @Override
  public void prepareForRouting() throws Exception {
    checkState();
  }
  
  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(
      int sourceTaskIndex, int sourceOutputIndex, int destinationTaskIndex)
      throws Exception {
    if (sourceTaskIndex == destinationTaskIndex) {
      return commonRouteMeta;
    }
    return null;
  }
  
  @Override
  public @Nullable EventRouteMetadata routeCompositeDataMovementEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex)
      throws Exception {
    if (sourceTaskIndex == destinationTaskIndex) {
      return commonRouteMeta;
    }
    return null;
  }

  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception {
    return commonRouteMeta;
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    destinationTaskAndInputIndices.put(sourceTaskIndex, destinationInputIndices);
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex) {
    return destinationTaskIndex;
  }
  
  @Override
  public int routeInputErrorEventToSource(int destinationTaskIndex, int destinationFailedInputIndex) {
    return destinationTaskIndex;
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return 1;
  }
  
  private void checkState() {
    if (stateChecked.get()) {
      return;
    }
    // by the time routing is initiated all task counts must be determined and stable
    Preconditions.checkState(getContext().getSourceVertexNumTasks() == getContext()
        .getDestinationVertexNumTasks(), "1-1 source and destination task counts must match."
        + " Destination: " + getContext().getDestinationVertexName() + " tasks: "
        + getContext().getDestinationVertexNumTasks() + " Source: "
        + getContext().getSourceVertexName() + " tasks: " + getContext().getSourceVertexNumTasks());
    stateChecked.set(true);
  }

}

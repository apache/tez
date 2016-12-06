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

package org.apache.tez.dag.library.vertexmanager;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.UserPayload;

import javax.annotation.Nullable;

import java.util.HashMap;

/**
 * Edge manager for fair routing. Each destination task has its
 * DestinationTaskInputsProperty used to decide how to do event routing
 * between source and destination.
 */
public class FairShuffleEdgeManager extends EdgeManagerPluginOnDemand {

  private FairEdgeConfiguration conf = null;
  // The key in the mapping is the destination task index.
  // The value in the mapping is DestinationTaskInputsProperty of the
  // destination task.
  private HashMap<Integer, DestinationTaskInputsProperty> mapping;

  // used by the framework at runtime. initialize is the real initializer at runtime
  public FairShuffleEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskIndex) {
    return mapping.get(destTaskIndex).getNumOfPhysicalInputs();
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
    return conf.getNumBuckets();
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    int numTasks = 0;
    for(DestinationTaskInputsProperty entry: mapping.values()) {
      if (entry.isSourceTaskInRange(sourceTaskIndex)) {
        numTasks++;
      }
    }
    return numTasks;
  }

  // called at runtime to initialize the custom edge.
  @Override
  public void initialize() {
    UserPayload userPayload = getContext().getUserPayload();
    if (userPayload == null || userPayload.getPayload() == null ||
        userPayload.getPayload().limit() == 0) {
      throw new RuntimeException("Could not initialize FairShuffleEdgeManager"
          + " from provided user payload");
    }
    try {
      conf = FairEdgeConfiguration.fromUserPayload(userPayload);
      mapping = conf.getRoutingTable();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not initialize FairShuffleEdgeManager"
          + " from provided user payload", e);
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destinationTaskIndex,
      int destinationFailedInputIndex) {
    return mapping.get(destinationTaskIndex).getSourceTaskIndex(
        destinationFailedInputIndex);
  }

  @Override
  public void prepareForRouting() throws Exception {
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(
      int sourceTaskIndex, int sourceOutputIndex, int destTaskIndex)
      throws Exception {
    DestinationTaskInputsProperty property = mapping.get(destTaskIndex);
    int targetIndex = property.getPhysicalInputIndex(sourceTaskIndex,
        sourceOutputIndex);
    if (targetIndex != -1) {
      return EventRouteMetadata.create(1, new int[]{targetIndex});
    } else {
      return null;
    }
  }

  // Create an array of "count" consecutive integers with starting
  // value equal to "startValue".
  private int[] getRange(int startValue, int count) {
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = startValue + i;
    }
    return values;
  }

  @Override
  public @Nullable CompositeEventRouteMetadata
      routeCompositeDataMovementEventToDestination(int sourceTaskIndex,
      int destinationTaskIndex) {
    DestinationTaskInputsProperty property = mapping.get(destinationTaskIndex);
    int firstPhysicalInputIndex =
        property.getFirstPhysicalInputIndex(sourceTaskIndex);
    if (firstPhysicalInputIndex >= 0) {
      return CompositeEventRouteMetadata.create(property.getNumOfPartitions(),
          firstPhysicalInputIndex,
          property.getFirstPartitionId());
    } else {
      return null;
    }
  }

  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception {
    DestinationTaskInputsProperty property = mapping.get(destinationTaskIndex);
    int firstPhysicalInputIndex =
        property.getFirstPhysicalInputIndex(sourceTaskIndex);
    if (firstPhysicalInputIndex >= 0) {
      return EventRouteMetadata.create(property.getNumOfPartitions(),
          getRange(firstPhysicalInputIndex, property.getNumOfPartitions()));
    } else {
      return null;
    }
  }
}


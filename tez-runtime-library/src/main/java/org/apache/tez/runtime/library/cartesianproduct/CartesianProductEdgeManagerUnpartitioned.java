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
package org.apache.tez.runtime.library.cartesianproduct;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.CompositeEventRouteMetadata;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.runtime.library.utils.Grouper;

import javax.annotation.Nullable;


class CartesianProductEdgeManagerUnpartitioned extends CartesianProductEdgeManagerReal {
  private int positionId;
  private int[] numGroups;
  private int numDestinationConsumerTasks;
  private Grouper grouper = new Grouper();

  public CartesianProductEdgeManagerUnpartitioned(EdgeManagerPluginContext context) {
    super(context);
  }

  public void initialize(CartesianProductEdgeManagerConfig config) {
    positionId = config.getSourceVertices().indexOf(getContext().getSourceVertexName());
    this.numGroups = config.getNumGroups();

    if (numGroups != null && numGroups[positionId] != 0) {
      grouper.init(config.getNumTasks()[positionId], numGroups[positionId]);
      numDestinationConsumerTasks = 1;
      for (int numGroup : numGroups) {
        numDestinationConsumerTasks *= numGroup;
      }
      numDestinationConsumerTasks /= numGroups[positionId];
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    return failedInputId + grouper.getFirstTaskInGroup(
      CartesianProductCombination.fromTaskId(numGroups, destTaskId).getCombination().get(positionId));
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId, int srcOutputId,
                                                                int destTaskId) throws Exception {
    int groupId =
      CartesianProductCombination.fromTaskId(numGroups, destTaskId).getCombination().get(positionId);
    if (grouper.isInGroup(srcTaskId, groupId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(groupId);
      return EventRouteMetadata.create(1, new int[] {idx});
    }
    return null;
  }

  @Nullable
  @Override
  public CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                                  int destTaskId)
    throws Exception {
    int groupId =
      CartesianProductCombination.fromTaskId(numGroups, destTaskId).getCombination().get(positionId);
    if (grouper.isInGroup(srcTaskId, groupId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(groupId);
      return CompositeEventRouteMetadata.create(1, idx, 0);
    }
    return null;
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int groupId =
      CartesianProductCombination.fromTaskId(numGroups, destTaskId).getCombination().get(positionId);
    if (grouper.isInGroup(srcTaskId, groupId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(groupId);
      return EventRouteMetadata.create(1, new int[] {idx});
    }
    return null;
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    int groupId =
      CartesianProductCombination.fromTaskId(numGroups, destTaskId).getCombination().get(positionId);
    return grouper.getNumTasksInGroup(groupId);
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int srcTaskId) {
    return 1;
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return numDestinationConsumerTasks;
  }
}
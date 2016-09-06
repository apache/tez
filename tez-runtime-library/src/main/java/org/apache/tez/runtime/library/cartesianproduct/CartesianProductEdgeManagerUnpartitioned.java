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
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;

import javax.annotation.Nullable;
import java.util.Arrays;

import static org.apache.tez.dag.api.EdgeManagerPluginOnDemand.*;

class CartesianProductEdgeManagerUnpartitioned extends CartesianProductEdgeManagerReal {
  private int positionId;
  private int[] numTasks;
  private int numDestinationConsumerTasks;

  public CartesianProductEdgeManagerUnpartitioned(EdgeManagerPluginContext context) {
    super(context);
  }

  public void initialize(CartesianProductEdgeManagerConfig config) {
    positionId = config.getSourceVertices().indexOf(getContext().getSourceVertexName());
    this.numTasks = config.getNumTasks();

    if (numTasks != null && numTasks[positionId] != 0) {
      numDestinationConsumerTasks = 1;
      for (int numTask : numTasks) {
        numDestinationConsumerTasks *= numTask;
      }
      numDestinationConsumerTasks /= numTasks[positionId];
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    return
      CartesianProductCombination.fromTaskId(numTasks, destTaskId).getCombination().get(positionId);
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId, int srcOutputId,
                                                                int destTaskId) throws Exception {
    int index = CartesianProductCombination.fromTaskId(numTasks, destTaskId)
      .getCombination().get(positionId);
    return index == srcTaskId ? EventRouteMetadata.create(1, new int[]{0}) : null;
  }

  @Nullable
  @Override
  public EventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int index = CartesianProductCombination.fromTaskId(numTasks, destTaskId)
        .getCombination().get(positionId);
    return index == srcTaskId ? EventRouteMetadata.create(1, new int[]{0}, new int[]{0}) : null;
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int index = CartesianProductCombination.fromTaskId(numTasks, destTaskId)
      .getCombination().get(positionId);
    return index == srcTaskId ? EventRouteMetadata.create(1, new int[]{0}) : null;
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    return 1;
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
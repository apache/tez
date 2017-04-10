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
  private int numChunk;
  private int chunkIdOffset;
  private int[] numChunkPerSrc;
  private int numDestinationConsumerTasks;
  private Grouper grouper = new Grouper();

  public CartesianProductEdgeManagerUnpartitioned(EdgeManagerPluginContext context) {
    super(context);
  }

  public void initialize(CartesianProductEdgeManagerConfig config) {
    String groupName = getContext().getVertexGroupName();
    String srcName = groupName != null ? groupName : getContext().getSourceVertexName();
    this.positionId = config.getSourceVertices().indexOf(srcName);
    this.numChunkPerSrc = config.numChunksPerSrc;
    this.numChunk = config.numChunk;
    this.chunkIdOffset = config.chunkIdOffset;

    if (numChunk != 0) {
      grouper.init(getContext().getSourceVertexNumTasks(), numChunk);
      numDestinationConsumerTasks = 1;
      for (int numGroup : numChunkPerSrc) {
        numDestinationConsumerTasks *= numGroup;
      }
      numDestinationConsumerTasks /= numChunkPerSrc[positionId];
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    return failedInputId + grouper.getFirstTaskInGroup(
      CartesianProductCombination.fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionId)
        - chunkIdOffset);
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId, int srcOutputId,
                                                                int destTaskId) throws Exception {
    int chunkId =
      CartesianProductCombination.fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionId)
        - chunkIdOffset;
    if (0 <= chunkId && chunkId < numChunk && grouper.isInGroup(srcTaskId, chunkId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(chunkId);
      return EventRouteMetadata.create(1, new int[] {idx});
    }
    return null;
  }

  @Nullable
  @Override
  public CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                                  int destTaskId)
    throws Exception {
    int chunkId =
      CartesianProductCombination.fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionId)
        - chunkIdOffset;
    if (0 <= chunkId && chunkId < numChunk && grouper.isInGroup(srcTaskId, chunkId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(chunkId);
      return CompositeEventRouteMetadata.create(1, idx, 0);
    }
    return null;
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int chunkId =
      CartesianProductCombination.fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionId)
        - chunkIdOffset;
    if (0 <= chunkId && chunkId < numChunk && grouper.isInGroup(srcTaskId, chunkId)) {
      int idx = srcTaskId - grouper.getFirstTaskInGroup(chunkId);
      return EventRouteMetadata.create(1, new int[] {idx});
    }
    return null;
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    int chunkId =
      CartesianProductCombination.fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionId)
        - chunkIdOffset;
    return 0 <= chunkId && chunkId < numChunk ? grouper.getNumTasksInGroup(chunkId) : 0;
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
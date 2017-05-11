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

import com.google.common.primitives.Ints;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.CompositeEventRouteMetadata;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.runtime.library.utils.Grouper;

import javax.annotation.Nullable;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductCombination.fromTaskId;
import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.*;


class FairCartesianProductEdgeManager extends CartesianProductEdgeManagerReal {
  private int numPartition;
  // position of current source in all cartesian product sources
  private int positionInSrc;
  // #chunk of each cartesian product source
  private int[] numChunkPerSrc;
  // #task of each vertex in vertex group that contains src vertex
  private int[] numTaskPerSrcVertexInGroup;
  // position of src vertex in vertex group
  private int positionInGroup;
  // # destination tasks that consume same chunk
  private int numDestConsumerPerChunk;
  private Grouper grouper = new Grouper();
  private Grouper grouperForComputeOffset = new Grouper();

  public FairCartesianProductEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductConfigProto config) {
    String groupName = getContext().getVertexGroupName();
    String srcName = groupName != null ? groupName : getContext().getSourceVertexName();
    this.positionInSrc = config.getSourcesList().indexOf(srcName);

    if (config.hasNumPartitionsForFairCase()) {
      this.numPartition = config.getNumPartitionsForFairCase();
    } else {
      this.numPartition = (int) Math.pow(config.getMaxParallelism(), 1.0 / config.getSourcesCount());
    }

    if (config.getNumChunksCount() > 0) {
      // initialize after reconfiguration
      this.numChunkPerSrc = Ints.toArray(config.getNumChunksList());
      grouper.init(getContext().getSourceVertexNumTasks() * numPartition,
        numChunkPerSrc[positionInSrc]);
      this.numTaskPerSrcVertexInGroup = Ints.toArray(config.getNumTaskPerVertexInGroupList());
      this.positionInGroup = config.getPositionInGroup();

      numDestConsumerPerChunk = 1;
      for (int numChunk : numChunkPerSrc) {
        numDestConsumerPerChunk *= numChunk;
      }
      numDestConsumerPerChunk /= numChunkPerSrc[positionInSrc];
    }
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    int chunkId = fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionInSrc);
    int itemId = failedInputId - getItemIdOffset(chunkId) + grouper.getFirstItemInGroup(chunkId);
    return itemId / numPartition;
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId, int srcOutputId,
                                                                int destTaskId) throws Exception {
    int itemId = srcTaskId * numPartition + srcOutputId;
    int chunkId = fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionInSrc);
    if (grouper.isInGroup(itemId, chunkId)) {
      int idx = itemId - grouper.getFirstItemInGroup(chunkId) + getItemIdOffset(chunkId);
      return EventRouteMetadata.create(1, new int[] {idx});
    }
    return null;
  }

  @Nullable
  @Override
  public CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                                  int destTaskId)
    throws Exception {
    int chunkId = fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionInSrc);
    int firstItemInChunk = grouper.getFirstItemInGroup(chunkId);
    int lastItemInChunk = grouper.getLastItemInGroup(chunkId);
    int firstItemInSrcTask =  srcTaskId * numPartition;
    int lastItemInSrcTask = firstItemInSrcTask + numPartition - 1;
    if (!(lastItemInChunk < firstItemInSrcTask || firstItemInChunk > lastItemInSrcTask)) {
      int firstItem = Math.max(firstItemInChunk, firstItemInSrcTask);
      int lastItem = Math.min(lastItemInChunk, lastItemInSrcTask);
      return CompositeEventRouteMetadata.create(lastItem - firstItem + 1,
        firstItem - firstItemInChunk + getItemIdOffset(chunkId), firstItem - firstItemInSrcTask);
    }
    return null;
  }

  /**
   * #item from vertices before source vertex in the same vertex group
   * @param chunkId
   * @return
   */
  private int getItemIdOffset(int chunkId) {
    int offset = 0;
    for (int i = 0; i < positionInGroup; i++) {
      grouperForComputeOffset.init(numTaskPerSrcVertexInGroup[i] * numPartition,
        numChunkPerSrc[positionInSrc]);
      offset += grouperForComputeOffset.getNumItemsInGroup(chunkId);
    }
    return offset;
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int chunkId = fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionInSrc);
    int firstItemInChunk = grouper.getFirstItemInGroup(chunkId);
    int lastItemInChunk = grouper.getLastItemInGroup(chunkId);
    int firstItemInSrcTask = srcTaskId * numPartition;
    int lastItemInSrcTask = firstItemInSrcTask + numPartition - 1;
    if (!(lastItemInChunk < firstItemInSrcTask || firstItemInChunk > lastItemInSrcTask)) {
      int firstItem = Math.max(firstItemInChunk, firstItemInSrcTask);
      int lastItem = Math.min(lastItemInChunk, lastItemInSrcTask);
      int[] targetIndices = new int[lastItem - firstItem + 1];
      for (int i = firstItem; i <= lastItem; i++) {
        targetIndices[i - firstItem] = i - firstItemInChunk + getItemIdOffset(chunkId);
      }
      return EventRouteMetadata.create(targetIndices.length, targetIndices);
    }
    return null;
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    int chunkId = fromTaskId(numChunkPerSrc, destTaskId).getCombination().get(positionInSrc);
    if (0 <= chunkId && chunkId < numChunkPerSrc[positionInSrc]) {
      return grouper.getNumItemsInGroup(chunkId);
    }
    return 0;
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int srcTaskId) {
    return numPartition;
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    int numChunk = grouper.getGroupId(sourceTaskIndex * numPartition + numPartition - 1)
      - grouper.getGroupId(sourceTaskIndex * numPartition) + 1;
    return numDestConsumerPerChunk * numChunk;
  }
}
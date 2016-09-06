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
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.dag.api.UserPayload;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CartesianProductEdgeManagerPartitioned extends CartesianProductEdgeManagerReal {
  private int positionId;
  private CartesianProductFilter filter;
  private int[] taskIdMapping;
  private CartesianProductEdgeManagerConfig config;
  private int[] numPartitions;

  public CartesianProductEdgeManagerPartitioned(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductEdgeManagerConfig config) throws Exception {
    this.config = config;
    this.numPartitions = Ints.toArray(config.getNumPartitions());
    positionId = config.getSourceVertices().indexOf(getContext().getSourceVertexName());
    CartesianProductFilterDescriptor filterDescriptor = config.getFilterDescriptor();
    if (filterDescriptor != null) {
      filter = ReflectionUtils.createClazzInstance(filterDescriptor.getClassName(),
        new Class[] {UserPayload.class}, new UserPayload[] {filterDescriptor.getUserPayload()});
    }
    generateTaskIdMapping();
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    return failedInputId;
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId, int srcOutputId,
                                                                int destTaskId) throws Exception {
    int partition = CartesianProductCombination.fromTaskId(numPartitions,
      getIdealTaskId(destTaskId)).getCombination().get(positionId);
    return srcOutputId != partition ? null :
      EventRouteMetadata.create(1, new int[]{srcTaskId});
  }

  @Nullable
  @Override
  public EventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    int partition = CartesianProductCombination.fromTaskId(numPartitions,
      getIdealTaskId(destTaskId)).getCombination().get(positionId);
    return EventRouteMetadata.create(1, new int[]{srcTaskId}, new int[]{partition});
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    return EventRouteMetadata.create(1, new int[]{srcTaskId});
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    return getContext().getSourceVertexNumTasks();
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int srcTaskId) {
    return numPartitions[positionId];
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return getContext().getDestinationVertexNumTasks();
  }

  private void generateTaskIdMapping() {
    List<Integer> idealTaskId = new ArrayList<>();
    Map<String, Integer> vertexPartitionMap = new HashMap<>();
    CartesianProductCombination combination =
      new CartesianProductCombination(numPartitions);
    combination.firstTask();
    List<String> sourceVertices = config.getSourceVertices();
    do {
      for (int i = 0; i < sourceVertices.size(); i++) {
        vertexPartitionMap.put(sourceVertices.get(i), combination.getCombination().get(i));
      }
      if (filter == null || filter.isValidCombination(vertexPartitionMap)) {
        idealTaskId.add(combination.getTaskId());
      }
    } while (combination.nextTask());
    this.taskIdMapping = Ints.toArray(idealTaskId);
  }

  private int getIdealTaskId(int realTaskId) {
    return taskIdMapping[realTaskId];
  }
}
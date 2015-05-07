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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ScatterGatherEdgeManager extends EdgeManagerPluginOnDemand {
  
  private AtomicReference<ArrayList<EventRouteMetadata>> commonRouteMeta = 
      new AtomicReference<ArrayList<EventRouteMetadata>>();
  private Object commonRouteMetaLock = new Object();
  private int[][] sourceIndices;
  private int[][] targetIndices;

  public ScatterGatherEdgeManager(EdgeManagerPluginContext context) {
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
    int physicalOutputs = getContext().getDestinationVertexNumTasks();
    Preconditions.checkArgument(physicalOutputs >= 0,
        "ScatteGather edge manager must have destination vertex task parallelism specified");
    return physicalOutputs;
  }

  private ArrayList<EventRouteMetadata> getOrCreateCommonRouteMeta() {
    ArrayList<EventRouteMetadata> metaData = commonRouteMeta.get();
    if (metaData == null) {
      synchronized (commonRouteMetaLock) {
        metaData = commonRouteMeta.get();
        if (metaData == null) {
          int numSourceTasks = getContext().getSourceVertexNumTasks();
          ArrayList<EventRouteMetadata> localEventMeta = Lists
              .newArrayListWithCapacity(numSourceTasks);
          for (int i=0; i<numSourceTasks; ++i) {
            localEventMeta.add(EventRouteMetadata.create(1, new int[]{i}, new int[]{0}));
          }
          Preconditions.checkState(commonRouteMeta.compareAndSet(null, localEventMeta));
          metaData = commonRouteMeta.get();
        }
      }
    }
    return metaData;
  }

  private void createIndices() {
    // source indices derive from num dest tasks (==partitions)
    int numTargetTasks = getContext().getDestinationVertexNumTasks();
    sourceIndices = new int[numTargetTasks][];
    for (int i=0; i<numTargetTasks; ++i) {
      sourceIndices[i] = new int[]{i};
    }
    // target indices derive from num src tasks
    int numSourceTasks = getContext().getSourceVertexNumTasks();
    targetIndices = new int[numSourceTasks][];
    for (int i=0; i<numSourceTasks; ++i) {
      targetIndices[i] = new int[]{i};
    }
  }
  
  @Override
  public void prepareForRouting() throws Exception {
    createIndices();    
  }

  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(
      int sourceTaskIndex, int sourceOutputIndex, int destinationTaskIndex) throws Exception {
    if (sourceOutputIndex == destinationTaskIndex) {
      return getOrCreateCommonRouteMeta().get(sourceTaskIndex);
    }
    return null;
  }
  
  @Override
  public @Nullable EventRouteMetadata routeCompositeDataMovementEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex)
      throws Exception {
    return EventRouteMetadata.create(1, targetIndices[sourceTaskIndex], 
        sourceIndices[destinationTaskIndex]);
  }

  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, int destinationTaskIndex) throws Exception {
    return getOrCreateCommonRouteMeta().get(sourceTaskIndex);
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int sourceOutputIndex, Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    // the i-th source output goes to the i-th destination task
    // the n-th source task becomes the n-th physical input on the task
    destinationTaskAndInputIndices.put(sourceOutputIndex, Collections.singletonList(sourceTaskIndex));
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
    for (int i=0; i<getContext().getDestinationVertexNumTasks(); ++i) {
      destinationTaskAndInputIndices.put(i, Collections.singletonList(sourceTaskIndex));
    }
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex, int destinationFailedInputIndex) {
    return destinationFailedInputIndex;
  }

  @Override
  public int routeInputErrorEventToSource(int destinationTaskIndex, int destinationFailedInputIndex) {
    return destinationFailedInputIndex;
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return getContext().getDestinationVertexNumTasks();
  }

}

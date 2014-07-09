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

import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class OneToOneEdgeManager extends EdgeManager {

  List<Integer> destinationInputIndices = 
      Collections.unmodifiableList(Collections.singletonList(0)); 

  @Override
  public void initialize(EdgeManagerContext edgeManagerContext) {
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
    destinationTaskAndInputIndices.put(sourceTaskIndex, destinationInputIndices);
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
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return 1;
  }

}

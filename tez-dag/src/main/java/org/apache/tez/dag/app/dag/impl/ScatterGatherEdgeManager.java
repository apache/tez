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

import java.util.List;

import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;

public class ScatterGatherEdgeManager implements EdgeManager {

  @Override
  public void initialize(EdgeManagerContext edgeManagerContext) {
    // Nothing to do.
  }

  @Override
  public int getNumDestinationTaskInputs(int numSourceTasks,
      int destinationTaskIndex) {
    return numSourceTasks;
  }
  
  @Override
  public int getNumSourceTaskOutputs(int numDestinationTasks, int sourceTaskIndex) {
    return numDestinationTasks;
  }

  @Override
  public void routeEventToDestinationTasks(DataMovementEvent event,
      int sourceTaskIndex, int numDestinationTasks, List<Integer> taskIndices) {
    int destinationTaskIndex = event.getSourceIndex();
    event.setTargetIndex(sourceTaskIndex);
    taskIndices.add(new Integer(destinationTaskIndex));
  }

  @Override
  public void routeEventToDestinationTasks(InputFailedEvent event,
      int sourceTaskIndex, int numDestinationTasks, List<Integer> taskIndices) {
    event.setTargetIndex(sourceTaskIndex);
    addAllDestinationTaskIndices(numDestinationTasks, taskIndices);
  }

  @Override
  public int routeEventToSourceTasks(int destinationTaskIndex,
      InputReadErrorEvent event) {
    return event.getIndex();
  }

  @Override
  public int getDestinationConsumerTaskNumber(int sourceTaskIndex, int numDestTasks) {
    return numDestTasks;
  }
  
  void addAllDestinationTaskIndices(int numDestinationTasks, List<Integer> taskIndices) {
    for(int i=0; i<numDestinationTasks; ++i) {
      taskIndices.add(new Integer(i));
    }    
  }

}

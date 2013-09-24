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

import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class BroadcastEdgeManager extends EdgeManager {

  @Override
  public int getNumDestinationTaskInputs(Vertex sourceVertex,
      int destinationTaskIndex) {
    return sourceVertex.getTotalTasks();
  }
  
  @Override
  public int getNumSourceTaskOutputs(Vertex destinationVertex,
      int sourceTaskIndex) {
    return 1;
  }
  
  @Override
  public void routeEventToDestinationTasks(DataMovementEvent event,
      int sourceTaskIndex, int numDestinationTasks, List<Integer> taskIndices) {
    event.setTargetIndex(sourceTaskIndex);
    addAllDestinationTaskIndices(numDestinationTasks, taskIndices);
  }
  
  @Override
  public void routeEventToDestinationTasks(InputFailedEvent event,
      int sourceTaskIndex, int numDestinationTasks , List<Integer> taskIndices) {
    event.setTargetIndex(sourceTaskIndex);
    addAllDestinationTaskIndices(numDestinationTasks, taskIndices);    
  }
  
  @Override
  public int routeEventToSourceTasks(int destinationTaskIndex,
      InputReadErrorEvent event) {
    return destinationTaskIndex;
  }
  
  void addAllDestinationTaskIndices(int numDestinationTasks, List<Integer> taskIndeces) {
    for(int i=0; i<numDestinationTasks; ++i) {
      taskIndeces.add(new Integer(i));
    }    
  }

}

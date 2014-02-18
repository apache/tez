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
import java.util.Map;

import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

public class NullEdgeManager implements EdgeManager {

  public NullEdgeManager() {
  }

  @Override
  public void initialize(EdgeManagerContext edgeManagerContext) {
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int numSourceTasks, int destinationTaskIndex) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int numDestinationTasks, int sourceTaskIndex) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

  @Override
  public void routeDataMovementEventToDestination(DataMovementEvent event, int sourceTaskIndex,
      int numDestinationTasks, Map<Integer, List<Integer>> inputIndicesToTaskIndices) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex, int numDestinationTasks) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event, int destinationTaskIndex) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      int numDestinationTasks,
      Map<Integer, List<Integer>> inputIndicesToTaskIndices) {
    throw new UnsupportedOperationException(
        "Cannot route events. EdgeManager should have been replaced at runtime");
  }

}
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

package org.apache.tez.dag.library.edgemanager;

import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import java.util.List;
import java.util.Map;

/**
 * A dummy edge manager used in scenarios where application will depend on
 * the direct connection between containers/tasks to handle all data communications,
 * including both routing and actual data transfers.
 */

public class SilentEdgeManager extends EdgeManagerPlugin {

  /**
   * Create an instance of the EdgeManagerPlugin. Classes extending this to
   * create a EdgeManagerPlugin, must provide the same constructor so that Tez
   * can create an instance of the class at runtime.
   *
   * @param context the context within which this EdgeManagerPlugin will run. Includes
   *                information like configuration which the user may have specified
   *                while setting up the edge.
   */
  public SilentEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {

  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) throws Exception {
    return 0;
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) throws Exception {
    return 0;
  }

  @Override
  public void routeDataMovementEventToDestination(
      DataMovementEvent event, int sourceTaskIndex, int sourceOutputIndex,
      Map<Integer, List<Integer>> destinationTaskAndInputIndices) throws Exception {
    throw new UnsupportedOperationException(
        "routeDataMovementEventToDestination not supported for SilentEdgeManager");
  }

  @Override
  public void routeInputSourceTaskFailedEventToDestination(
      int sourceTaskIndex, Map<Integer, List<Integer>> destinationTaskAndInputIndices) throws Exception {
    throw new UnsupportedOperationException(
        "routeInputSourceTaskFailedEventToDestination not supported for SilentEdgeManager");
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) throws Exception {
    return 0;
  }

  @Override
  public int routeInputErrorEventToSource(InputReadErrorEvent event, int destinationTaskIndex, int destinationFailedInputIndex) throws Exception {
    return 0;
  }
}

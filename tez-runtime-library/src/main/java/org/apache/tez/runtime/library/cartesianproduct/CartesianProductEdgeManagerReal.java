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
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.CompositeEventRouteMetadata;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

/**
 * base class of cartesian product edge manager implementation
 */
abstract class CartesianProductEdgeManagerReal {
  private final EdgeManagerPluginContext context;

  public CartesianProductEdgeManagerReal(EdgeManagerPluginContext context) {
    this.context = context;
  }

  public EdgeManagerPluginContext getContext() {
    return this.context;
  }

  public abstract void initialize(CartesianProductConfigProto config) throws Exception;

  public void prepareForRouting() throws Exception {}

  public abstract int routeInputErrorEventToSource(int destTaskId, int failedInputId)
    throws Exception;

  public abstract EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId,
                                                                         int srcOutputId,
                                                                         int destTaskId)
    throws Exception;

  public abstract CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                                           int destTaskId)
    throws Exception;

  public abstract EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                                  int destTaskId)
    throws Exception;

  public abstract int getNumDestinationTaskPhysicalInputs(int destTaskId);

  public abstract int getNumSourceTaskPhysicalOutputs(int srcTaskId);

  public abstract int getNumDestinationConsumerTasks(int sourceTaskIndex);
}

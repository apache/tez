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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.TezException;

import javax.annotation.Nullable;

/**
 * This EM wrap a real edge manager implementation object. It choose whether it's partitioned or
 * unpartitioned implementation according to the config. All method invocations are actually
 * redirected to real implementation.
 */
public class CartesianProductEdgeManager extends EdgeManagerPluginOnDemand {
  private CartesianProductEdgeManagerReal edgeManagerReal;

  public CartesianProductEdgeManager(EdgeManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    Preconditions.checkArgument(getContext().getUserPayload() != null);
    CartesianProductEdgeManagerConfig config = CartesianProductEdgeManagerConfig.fromUserPayload(
      getContext().getUserPayload());
    // no need to check config because config comes from VM and is already checked by VM
    edgeManagerReal = config.getIsPartitioned()
      ? new CartesianProductEdgeManagerPartitioned(getContext())
      : new CartesianProductEdgeManagerUnpartitioned(getContext());
    edgeManagerReal.initialize(config);
  }

  @VisibleForTesting
  protected CartesianProductEdgeManagerReal getEdgeManagerReal() {
    return this.edgeManagerReal;
  }

  @Override
  public void prepareForRouting() throws Exception {
    edgeManagerReal.prepareForRouting();
  }

  @Override
  public int routeInputErrorEventToSource(int destTaskId, int failedInputId) throws Exception {
    return edgeManagerReal.routeInputErrorEventToSource(destTaskId, failedInputId);
  }

  @Nullable
  @Override
  public EventRouteMetadata routeDataMovementEventToDestination(int srcTaskId,
                                                                int srcOutputId,
                                                                int destTaskId)
    throws Exception {
    return edgeManagerReal.routeDataMovementEventToDestination(srcTaskId, srcOutputId, destTaskId);
  }

  @Nullable
  @Override
  public EventRouteMetadata routeCompositeDataMovementEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    return edgeManagerReal.routeCompositeDataMovementEventToDestination(srcTaskId, destTaskId);
  }

  @Nullable
  @Override
  public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(int srcTaskId,
                                                                         int destTaskId)
    throws Exception {
    return edgeManagerReal.routeInputSourceTaskFailedEventToDestination(srcTaskId, destTaskId);
  }

  @Override
  public int getNumDestinationTaskPhysicalInputs(int destTaskId) {
    return edgeManagerReal.getNumDestinationTaskPhysicalInputs(destTaskId);
  }

  @Override
  public int getNumSourceTaskPhysicalOutputs(int srcTaskId) {
    return edgeManagerReal.getNumSourceTaskPhysicalOutputs(srcTaskId);
  }

  @Override
  public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
    return edgeManagerReal.getNumDestinationConsumerTasks(sourceTaskIndex);
  }
}
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
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This VM wrap a real vertex manager implementation object. It choose whether it's partitioned or
 * unpartitioned implementation according to the config. All method invocations are actually
 * redirected to real implementation.
 */
public class CartesianProductVertexManager extends VertexManagerPlugin {
  public static final String TEZ_CAERESIAN_PRODUCT_SLOW_START_MIN_FRACTION =
    "tez.cartesian-product.min-src-fraction";
  public static final float TEZ_CAERESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT = 0.25f;
  public static final String TEZ_CAERESIAN_PRODUCT_SLOW_START_MAX_FRACTION =
    "tez.cartesian-product.min-src-fraction";
  public static final float TEZ_CAERESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT = 0.75f;

  private CartesianProductVertexManagerReal vertexManagerReal = null;

  public CartesianProductVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    CartesianProductVertexManagerConfig config =
      CartesianProductVertexManagerConfig.fromUserPayload(getContext().getUserPayload());
    // check whether DAG and config are is consistent
    Map<String, EdgeProperty> edgePropertyMap = getContext().getInputVertexEdgeProperties();
    Set<String> sourceVerticesDAG = edgePropertyMap.keySet();
    Set<String> sourceVerticesConfig = new HashSet<>();
    sourceVerticesConfig.addAll(config.getSourceVertices());

    for (Map.Entry<String, EdgeProperty> entry : edgePropertyMap.entrySet()) {
      if (entry.getValue().getEdgeManagerDescriptor().getClassName()
        .equals(CartesianProductEdgeManager.class.getName())) {
        Preconditions.checkArgument(sourceVerticesDAG.contains(entry.getKey()),
          entry.getKey() + " has CartesianProductEdgeManager but isn't in " +
            "CartesianProductVertexManagerConfig");
      } else {
        Preconditions.checkArgument(!sourceVerticesDAG.contains(entry.getKey()),
          entry.getKey() + " has no CartesianProductEdgeManager but is in " +
            "CartesianProductVertexManagerConfig");
      }
    }

    for (String vertex : sourceVerticesConfig) {
      Preconditions.checkArgument(sourceVerticesDAG.contains(vertex),
        vertex + " is in CartesianProductVertexManagerConfig but not a source vertex in DAG");
      Preconditions.checkArgument(
        edgePropertyMap.get(vertex).getEdgeManagerDescriptor().getClassName()
          .equals(CartesianProductEdgeManager.class.getName()),
        vertex + " is in CartesianProductVertexManagerConfig and a source vertex, but has no " +
          "CartesianProductEdgeManager");
    }

    vertexManagerReal = config.getIsPartitioned()
      ? new CartesianProductVertexManagerPartitioned(getContext())
      : new CartesianProductVertexManagerUnpartitioned(getContext());
    vertexManagerReal.initialize(config);
  }

  @VisibleForTesting
  protected CartesianProductVertexManagerReal getVertexManagerReal() {
    return this.vertexManagerReal;
  }

  /**
   * no op currently, will be used for locality based optimization in future
   * @param vmEvent
   * @throws Exception
   */
  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws Exception {

    vertexManagerReal.onVertexManagerEventReceived(vmEvent);
  }

  /**
   * Currently direct input to cartesian product vertex is not supported
   * @param inputName
   * @param inputDescriptor
   * @param events
   * @throws Exception
   */
  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
                                      List<Event> events) throws Exception {
    throw new TezException("Direct input to cartesian product vertex is not supported yet");
  }

  @Override
  public void onVertexStarted(List<TaskAttemptIdentifier> completions) throws Exception {
    vertexManagerReal.onVertexStarted(completions);
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception{
    vertexManagerReal.onVertexStateUpdated(stateUpdate);
  }

  @Override
  public void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws Exception {
    vertexManagerReal.onSourceTaskCompleted(attempt);
  }
}
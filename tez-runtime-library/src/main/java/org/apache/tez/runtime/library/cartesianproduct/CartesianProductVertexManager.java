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
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.BROADCAST;
import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;

/**
 * This VM wrap a real vertex manager implementation object. It choose whether it's partitioned or
 * unpartitioned implementation according to the config. All method invocations are actually
 * redirected to real implementation.
 *
 * Predefined parallelism isn't allowed for cartesian product vertex. Parallellism has to be
 * determined by vertex manager.
 */
public class CartesianProductVertexManager extends VertexManagerPlugin {
  /**
   * Begin scheduling task when the fraction of finished cartesian product source tasks reaches
   * this value
   */
  public static final String TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION =
    "tez.cartesian-product.min-src-fraction";
  public static final float TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT = 0.25f;

  /**
   * Schedule all tasks when the fraction of finished cartesian product source tasks reach this value
   */
  public static final String TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION =
    "tez.cartesian-product.max-src-fraction";
  public static final float TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT = 0.75f;

  /**
   * Enables automatic grouping. It groups source tasks of each cartesian product source vertex
   * so that every group generates similar output size. And parallelism can be reduced because
   * destination tasks handle combinations of per group output instead of per task output. This is
   * only available for unpartitioned case for now, and it's useful for scenarios where there are
   * many source tasks generate small outputs.
   */
  public static final String TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING =
    "tez.cartesian-product.enable-auto-grouping";
  public static final boolean TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING_DEFAULT = true;

  /**
   * The number of output bytes we want from each group.
   */
  public static final String TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP =
    "tez.cartesian-product.desired-input-per-src";
  public static final long TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP_DEFAULT = 32 * 1024 * 1024;

  private CartesianProductVertexManagerReal vertexManagerReal = null;

  public CartesianProductVertexManager(VertexManagerPluginContext context) {
    super(context);
    Preconditions.checkArgument(context.getVertexNumTasks(context.getVertexName()) == -1,
      "Vertex with CartesianProductVertexManager cannot use pre-defined parallelism");
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
      String vertex = entry.getKey();
      EdgeProperty edgeProperty = entry.getValue();
      EdgeManagerPluginDescriptor empDescriptor = edgeProperty.getEdgeManagerDescriptor();
      if (empDescriptor != null
        && empDescriptor.getClassName().equals(CartesianProductEdgeManager.class.getName())) {
        Preconditions.checkArgument(sourceVerticesConfig.contains(vertex),
          vertex + " has CartesianProductEdgeManager but isn't in " +
            "CartesianProductVertexManagerConfig");
      } else {
        Preconditions.checkArgument(!sourceVerticesConfig.contains(vertex),
          vertex + " has no CartesianProductEdgeManager but is in " +
            "CartesianProductVertexManagerConfig");
      }

      if (edgeProperty.getDataMovementType() == CUSTOM) {
        Preconditions.checkArgument(sourceVerticesConfig.contains(vertex),
          "Only broadcast and cartesian product edges are allowed in cartesian product vertex");
      } else {
        Preconditions.checkArgument(edgeProperty.getDataMovementType() == BROADCAST,
          "Only broadcast and cartesian product edges are allowed in cartesian product vertex");
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
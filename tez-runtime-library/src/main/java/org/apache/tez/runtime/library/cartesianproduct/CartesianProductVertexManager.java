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
import com.google.protobuf.ByteString;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.BROADCAST;
import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;
import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.*;

/**
 * This VM wrap a real vertex manager implementation object. It choose whether it's partitioned or
 * fair implementation according to the config. All method invocations are actually
 * redirected to real implementation.
 *
 * Predefined parallelism isn't allowed for cartesian product vertex. Parallellism has to be
 * determined by vertex manager.
 *
 * If a vertex use this vertex, its input edges must be either cartesian product edge or broadcast
 * edge.
 *
 * Sources can be either vertices or vertex groups (only in fair cartesian product).
 *
 * Slow start only works in partitioned case.
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
   * Num partitions as int value, for fair cartesian product only.
   * Set this if auto determined num partition is not large enough
   */
  public static final String TEZ_CARTESIAN_PRODUCT_NUM_PARTITIONS =
    "tez.cartesian-product.num-partitions";

  /**
   * Whether to disable grouping in fair cartesian product
   * If this is set to true, it's best to set "tez.cartesian-product.num-partitions" to 1 to avoid
   * unnecessary overhead caused by multiple partitions.
   */
  public static final String TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING =
    "tez.cartesian-product.disable-grouping";
  public static final boolean TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING_DEFAULT = true;

  /**
   * If every source vertex has this percents of tasks completed and generate some output,
   * we can begin auto grouping.
   *
   * Positive float value, max 1.
   * If not set, auto grouping will begin once every source vertex generate enough output
   */
  public static final String TEZ_CARTESIAN_PRODUCT_GROUPING_FRACTION =
    "tez.cartesian-product.grouping-fraction";

  /**
   * Max parallelism, for fair cartesian product only.
   * This is used to avoid get too many tasks. The value must be positive.
   */
  public static final String TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM =
    "tez.cartesian-product.max-parallelism";
  public static final int TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM_DEFAULT = 1000;

  /**
   * Min cartesian product operations per worker, for fair cartesian product only.
   * This is used to avoid a task gets too small workload. The value must be positive.
   */
  public static final String TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER =
    "tez.cartesian-product.min-ops-per-worker";
  public static final long TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER_DEFAULT = 1000000;

  private CartesianProductVertexManagerReal vertexManagerReal = null;

  public CartesianProductVertexManager(VertexManagerPluginContext context) {
    super(context);
    Preconditions.checkArgument(context.getVertexNumTasks(context.getVertexName()) == -1,
      "Vertex with CartesianProductVertexManager cannot use pre-defined parallelism");
  }

  @Override
  public void initialize() throws Exception {
    CartesianProductConfigProto config = CartesianProductConfigProto.parseFrom(
      ByteString.copyFrom(getContext().getUserPayload().getPayload()));
    // check whether DAG and config are is consistent
    Map<String, EdgeProperty> edgePropertyMap = getContext().getInputVertexEdgeProperties();
    Set<String> sourceVerticesDAG = edgePropertyMap.keySet();
    Set<String> sourceVerticesConfig = new HashSet<>(config.getSourcesList());

    Map<String, List<String>> vertexGroups = getContext().getInputVertexGroups();
    Map<String, String> vertexToGroup = new HashMap<>();
    for (Map.Entry<String, List<String>> group : vertexGroups.entrySet()) {
      for (String vertex : group.getValue()) {
        vertexToGroup.put(vertex, group.getKey());
      }
    }

    for (Map.Entry<String, EdgeProperty> entry : edgePropertyMap.entrySet()) {
      String vertex = entry.getKey();
      String group = vertexToGroup.get(vertex);
      EdgeProperty edgeProperty = entry.getValue();
      EdgeManagerPluginDescriptor empDescriptor = edgeProperty.getEdgeManagerDescriptor();
      if (empDescriptor != null
        && empDescriptor.getClassName().equals(CartesianProductEdgeManager.class.getName())) {
        Preconditions.checkArgument(
          sourceVerticesConfig.contains(vertex) || sourceVerticesConfig.contains(group),
          vertex + " has CartesianProductEdgeManager but isn't in " +
            "CartesianProductVertexManagerConfig");
      } else {
        Preconditions.checkArgument(
          !sourceVerticesConfig.contains(vertex) && !sourceVerticesConfig.contains(group),
          vertex + " has no CartesianProductEdgeManager but is in " +
            "CartesianProductVertexManagerConfig");
      }

      if (edgeProperty.getDataMovementType() == CUSTOM) {
        Preconditions.checkArgument(
          sourceVerticesConfig.contains(vertex) || sourceVerticesConfig.contains(group),
          "Only broadcast and cartesian product edges are allowed in cartesian product vertex");
      } else {
        Preconditions.checkArgument(edgeProperty.getDataMovementType() == BROADCAST,
          "Only broadcast and cartesian product edges are allowed in cartesian product vertex");
      }
    }

    for (String src : sourceVerticesConfig) {
      List<String> vertices =
        vertexGroups.containsKey(src) ? vertexGroups.get(src) : Collections.singletonList(src);
      for (String v : vertices) {
        Preconditions.checkArgument(
          sourceVerticesDAG.contains(v),
          v + " is in CartesianProductVertexManagerConfig but not a source vertex in DAG");
        Preconditions.checkArgument(
          edgePropertyMap.get(v).getEdgeManagerDescriptor().getClassName()
            .equals(CartesianProductEdgeManager.class.getName()),
          v + " is in CartesianProductVertexManagerConfig and a source vertex, but has no " +
            "CartesianProductEdgeManager");
      }
    }

    vertexManagerReal = config.getIsPartitioned()
      ? new CartesianProductVertexManagerPartitioned(getContext())
      : new FairCartesianProductVertexManager(getContext());
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
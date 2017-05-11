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
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

/**
 * <class>CartesianProductConfig</class> is used to configure both
 * <class>CartesianProductVertexManager</class> and <class>CartesianProductEdgeManager</class>.
 * User need to specify the vertices and number of partitions of each vertices' output at least.
 * In partitioned case, filter should be specified here also(via
 * <class>CartesianProductFilterDescriptor</class>. User may also configure min/max fractions used
 * in slow start.
 */
@Evolving
public class CartesianProductConfig {
  private final boolean isPartitioned;
  private final String[] sources;
 // numPartition[i] means how many partitions sourceVertices[i] will generate
 // (not used in fair cartesian product)
  private final int[] numPartitions;
  private final CartesianProductFilterDescriptor filterDescriptor;

  /**
   * create config for fair cartesian product
   * @param sources list of names of source vertices or vertex groups
   */
  public CartesianProductConfig(List<String> sources) {
    Preconditions.checkArgument(sources != null, "source list cannot be null");
    Preconditions.checkArgument(sources.size() > 1,
      "there must be more than 1 source " + "67, currently only " + sources.size());

    this.isPartitioned = false;
    this.sources = sources.toArray(new String[sources.size()]);
    this.numPartitions = null;
    this.filterDescriptor = null;
  }

  /**
   * create config for partitioned case without filter
   * @param vertexPartitionMap the map from vertex name to its number of partitions
   */
  public CartesianProductConfig(Map<String, Integer> vertexPartitionMap) {
    this(vertexPartitionMap, null);
  }

  /**
   * create config for partitioned case with filter
   * @param vertexPartitionMap the map from vertex name to its number of partitions
   * @param filterDescriptor
   */
  public CartesianProductConfig(Map<String, Integer> vertexPartitionMap,
                                CartesianProductFilterDescriptor filterDescriptor) {
    Preconditions.checkArgument(vertexPartitionMap != null, "vertex-partition map cannot be null");
    Preconditions.checkArgument(vertexPartitionMap.size() > 1,
      "there must be more than 1 source vertices, currently only " + vertexPartitionMap.size());

    this.isPartitioned = true;
    this.numPartitions = new int[vertexPartitionMap.size()];
    this.sources = new String[vertexPartitionMap.size()];
    this.filterDescriptor = filterDescriptor;

    int i = 0;
    for (Map.Entry<String, Integer> entry : vertexPartitionMap.entrySet()) {
      this.sources[i] = entry.getKey();
      this.numPartitions[i] = entry.getValue();
      i++;
    }

    checkNumPartitions();
  }

  /**
   * create config for partitioned case, with specified source vertices order
   * @param numPartitions
   * @param sources
   * @param filterDescriptor
   */
  @VisibleForTesting
  protected CartesianProductConfig(int[] numPartitions, String[] sources,
                                   CartesianProductFilterDescriptor filterDescriptor) {
    Preconditions.checkArgument(numPartitions != null, "partitions count array can't be null");
    Preconditions.checkArgument(sources != null, "source array can't be null");
    Preconditions.checkArgument(numPartitions.length == sources.length,
      "partitions count array(length: " + numPartitions.length + ") and source array " +
        "(length: " + sources.length + ") cannot have different length");
    Preconditions.checkArgument(sources.length > 1,
      "there must be more than 1 source " + ", currently only " + sources.length);

    this.isPartitioned = true;
    this.numPartitions = numPartitions;
    this.sources = sources;
    this.filterDescriptor = filterDescriptor;

    checkNumPartitions();
  }

  /**
   * create config for both cases, used by subclass
   */
  protected CartesianProductConfig(boolean isPartitioned, int[] numPartitions,
                                   String[] sources,
                                   CartesianProductFilterDescriptor filterDescriptor) {
    this.isPartitioned = isPartitioned;
    this.numPartitions = numPartitions;
    this.sources = sources;
    this.filterDescriptor = filterDescriptor;
  }

  @VisibleForTesting
  protected void checkNumPartitions() {
    if (isPartitioned) {
      boolean isUnpartitioned = true;
      for (int i = 0; i < numPartitions.length; i++) {
        Preconditions.checkArgument(this.numPartitions[i] > 0,
          "Vertex " + sources[i] + "has negative (" + numPartitions[i] + ") partitions");
        isUnpartitioned = isUnpartitioned && numPartitions[i] == 1;
      }
      Preconditions.checkArgument(!isUnpartitioned,
        "every source has 1 partition in a partitioned case");
    } else {
      Preconditions.checkArgument(this.numPartitions == null,
        "partition counts should be null in fair cartesian product");
    }
  }

  /**
   * @return the array of source vertices (or source vertex group) names
   */
  public List<String> getSourceVertices() {
    return Collections.unmodifiableList(Arrays.asList(sources));
  }

  /**
   * @return the array of number of partitions, the order is same as result of
   *         <method>getSourceVertices</method>
   */
  public List<Integer> getNumPartitions() {
    if (this.numPartitions == null) {
      return null;
    }
    return Collections.unmodifiableList(Ints.asList(this.numPartitions));
  }

  public boolean getIsPartitioned() {
    return isPartitioned;
  }

  public CartesianProductFilterDescriptor getFilterDescriptor() {
    return this.filterDescriptor;
  }

  public UserPayload toUserPayload(TezConfiguration conf) throws IOException {
    return UserPayload.create(ByteBuffer.wrap(toProto(conf).toByteArray()));
  }

  protected CartesianProductConfigProto toProto(TezConfiguration conf) {
    CartesianProductConfigProto.Builder builder =
      CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(this.isPartitioned)
      .addAllSources(Arrays.asList(sources));

    if (isPartitioned) {
      builder.addAllNumPartitions(Ints.asList(numPartitions));
      if (filterDescriptor != null) {
        builder.setFilterClassName(filterDescriptor.getClassName());
        UserPayload filterUesrPayload = filterDescriptor.getUserPayload();
        if (filterUesrPayload != null) {
          builder.setFilterUserPayload(ByteString.copyFrom(filterUesrPayload.getPayload()));
        }
      }
    }

    if (conf != null) {
      builder.setMinFraction(conf.getFloat(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT));
      builder.setMaxFraction(conf.getFloat(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT));
      builder.setMaxParallelism(conf.getInt(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM_DEFAULT));
      builder.setMinOpsPerWorker(conf.getLong(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER_DEFAULT));
      builder.setEnableGrouping(conf.getBoolean(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING_DEFAULT));
      if (conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_GROUPING_FRACTION) != null) {
        builder.setGroupingFraction(Float.parseFloat(
          conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_GROUPING_FRACTION)));
        Preconditions.checkArgument(0 < builder.getGroupingFraction() &&
          builder.getGroupingFraction() <= 1, "grouping fraction should be larger than 0 and less" +
          " or equal to 1, current value: " + builder.getGroupingFraction());
      }
      if (conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_NUM_PARTITIONS) != null) {
        builder.setNumPartitionsForFairCase(Integer.parseInt(
          conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_NUM_PARTITIONS)));
        Preconditions.checkArgument(builder.getNumPartitionsForFairCase() > 0,
          "Number of partitions for fair cartesian product should be positive integer");
      }
    }
    Preconditions.checkArgument(builder.getMinFraction() <= builder.getMaxFraction(),
      "min fraction(" + builder.getMinFraction() + ") should be less than max fraction(" +
        builder.getMaxFraction() + ") in cartesian product slow start");
    Preconditions.checkArgument(builder.getMaxParallelism() > 0,
      "max parallelism must be positive, currently is " + builder.getMaxParallelism());
    Preconditions.checkArgument(builder.getMinOpsPerWorker() > 0,
      "Min ops per worker must be positive, currently is " + builder.getMinOpsPerWorker());

    return builder.build();
  }

  protected static CartesianProductConfigProto userPayloadToProto(UserPayload payload)
    throws InvalidProtocolBufferException {
    Preconditions.checkArgument(payload != null, "UserPayload is null");
    Preconditions.checkArgument(payload.getPayload() != null, "UserPayload carreis null payload");
    return
      CartesianProductConfigProto.parseFrom(ByteString.copyFrom(payload.getPayload()));
  }

  protected static CartesianProductConfig fromUserPayload(UserPayload payload)
    throws InvalidProtocolBufferException {
    return fromProto(userPayloadToProto(payload));
  }

  protected static CartesianProductConfig fromProto(
    CartesianProductConfigProto proto) {
    if (!proto.getIsPartitioned()) {
      return new CartesianProductConfig(proto.getSourcesList());
    } else {
      String[] sourceVertices = new String[proto.getSourcesList().size()];
      proto.getSourcesList().toArray(sourceVertices);
      CartesianProductFilterDescriptor filterDescriptor = null;
      if (proto.hasFilterClassName()) {
        filterDescriptor = new CartesianProductFilterDescriptor(proto.getFilterClassName());
        if (proto.hasFilterUserPayload()) {
          filterDescriptor.setUserPayload(
            UserPayload.create(ByteBuffer.wrap(proto.getFilterUserPayload().toByteArray())));
        }
      }
      return new CartesianProductConfig(Ints.toArray(proto.getNumPartitionsList()),
        sourceVertices, filterDescriptor);
    }
  }
}

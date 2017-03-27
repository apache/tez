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
  private final String[] sourceVertices;
  private final int[] numPartitions;
  private final CartesianProductFilterDescriptor filterDescriptor;

  /**
   * create config for unpartitioned case
   * @param sourceVertices list of source vertices names
   */
  public CartesianProductConfig(List<String> sourceVertices) {
    Preconditions.checkArgument(sourceVertices != null, "source vertices list cannot be null");
    Preconditions.checkArgument(sourceVertices.size() > 1,
      "there must be more than 1 source " + "vertices, currently only " + sourceVertices.size());

    this.isPartitioned = false;
    this.sourceVertices = sourceVertices.toArray(new String[sourceVertices.size()]);
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
      "there must be more than 1 source " + "vertices, currently only " + vertexPartitionMap.size());

    this.isPartitioned = true;
    this.numPartitions = new int[vertexPartitionMap.size()];
    this.sourceVertices = new String[vertexPartitionMap.size()];
    this.filterDescriptor = filterDescriptor;

    int i = 0;
    for (Map.Entry<String, Integer> entry : vertexPartitionMap.entrySet()) {
      this.sourceVertices[i] = entry.getKey();
      this.numPartitions[i] = entry.getValue();
      i++;
    }

    checkNumPartitions();
  }

  /**
   * create config for partitioned case, with specified source vertices order
   * @param numPartitions
   * @param sourceVertices
   * @param filterDescriptor
   */
  @VisibleForTesting
  protected CartesianProductConfig(int[] numPartitions, String[] sourceVertices,
                                   CartesianProductFilterDescriptor filterDescriptor) {
    Preconditions.checkArgument(numPartitions != null, "partitions count array can't be null");
    Preconditions.checkArgument(sourceVertices != null, "source vertices array can't be null");
    Preconditions.checkArgument(numPartitions.length == sourceVertices.length,
      "partitions count array(length: " + numPartitions.length + ") and source vertices array " +
        "(length: " + sourceVertices.length + ") cannot have different length");
    Preconditions.checkArgument(sourceVertices.length > 1,
      "there must be more than 1 source " + "vertices, currently only " + sourceVertices.length);

    this.isPartitioned = true;
    this.numPartitions = numPartitions;
    this.sourceVertices = sourceVertices;
    this.filterDescriptor = filterDescriptor;

    checkNumPartitions();
  }

  /**
   * create config for both cases, used by subclass
   */
  protected CartesianProductConfig(boolean isPartitioned, int[] numPartitions,
                                   String[] sourceVertices,
                                   CartesianProductFilterDescriptor filterDescriptor) {
    this.isPartitioned = isPartitioned;
    this.numPartitions = numPartitions;
    this.sourceVertices = sourceVertices;
    this.filterDescriptor = filterDescriptor;
  }

  @VisibleForTesting
  protected void checkNumPartitions() {
    if (isPartitioned) {
      boolean isUnpartitioned = true;
      for (int i = 0; i < numPartitions.length; i++) {
        Preconditions.checkArgument(this.numPartitions[i] > 0,
          "Vertex " + sourceVertices[i] + "has negative (" + numPartitions[i] + ") partitions");
        isUnpartitioned = isUnpartitioned && numPartitions[i] == 1;
      }
      Preconditions.checkArgument(!isUnpartitioned,
        "every source vertex has 1 partition in a partitioned case");
    } else {
      Preconditions.checkArgument(this.numPartitions == null,
        "partition counts should be null in unpartitioned case");
    }
  }

  /**
   * @return the array of source vertices names
   */
  public List<String> getSourceVertices() {
    return Collections.unmodifiableList(Arrays.asList(sourceVertices));
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
      .addAllSourceVertices(Arrays.asList(sourceVertices));

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

    builder.setMinFraction(
      CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT);
    builder.setMaxFraction(
      CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT);

    if (conf != null) {
      builder.setMinFraction(conf.getFloat(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT));
      builder.setMaxFraction(conf.getFloat(
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION,
        CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT));
      String enableAutoGrouping =
        conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING);
      if (enableAutoGrouping != null) {
        builder.setEnableAutoGrouping(Boolean.parseBoolean(enableAutoGrouping));
      }
      String desiredBytesPerGroup =
        conf.get(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP);
      if (desiredBytesPerGroup != null) {
        builder.setDesiredBytesPerGroup(Long.parseLong(desiredBytesPerGroup));
      }
    }
    Preconditions.checkArgument(builder.getMinFraction() <= builder.getMaxFraction(),
      "min fraction(" + builder.getMinFraction() + ") should be less than max fraction(" +
        builder.getMaxFraction() + ") in cartesian product slow start");

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
      return new CartesianProductConfig(proto.getSourceVerticesList());
    } else {
      String[] sourceVertices = new String[proto.getSourceVerticesList().size()];
      proto.getSourceVerticesList().toArray(sourceVertices);
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

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

import com.google.common.primitives.Ints;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class TestCartesianProductConfig {
  private TezConfiguration conf;

  @Before
  public void setup() {
    conf = new TezConfiguration();
  }

  @Test(timeout = 5000)
  public void testSerializationPartitioned() throws IOException {
    Map<String, Integer> vertexPartitionMap = new HashMap<>();
    vertexPartitionMap.put("v1", 2);
    vertexPartitionMap.put("v2", 3);
    vertexPartitionMap.put("v3", 4);
    String filterClassName = "filter";
    byte[] bytes = new byte[10];
    (new Random()).nextBytes(bytes);
    CartesianProductFilterDescriptor filterDescriptor =
      new CartesianProductFilterDescriptor(filterClassName)
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(bytes)));
    CartesianProductConfig config =
      new CartesianProductConfig(vertexPartitionMap, filterDescriptor);
    UserPayload payload = config.toUserPayload(conf);
    CartesianProductConfig parsedConfig = CartesianProductConfig.fromUserPayload(payload);
    assertConfigEquals(config, parsedConfig);
  }

  @Test(timeout = 5000)
  public void testSerializationFair() throws Exception {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v1");
    sourceVertices.add("v2");
    sourceVertices.add("v3");
    CartesianProductConfig config =
      new CartesianProductConfig(sourceVertices);
    UserPayload payload = config.toUserPayload(conf);
    CartesianProductConfig parsedConfig = CartesianProductConfig.fromUserPayload(payload);
    assertConfigEquals(config, parsedConfig);

    // fair cartesian product config should have null in numPartitions fields
    try {
      config = new CartesianProductConfig(false, new int[]{}, new String[]{"v0","v1"},null);
      config.checkNumPartitions();
    } catch (Exception e) {
      return;
    }
    throw new Exception();
  }

  private void assertConfigEquals(CartesianProductConfig config1, CartesianProductConfig config2) {
    assertArrayEquals(config1.getSourceVertices().toArray(new String[0]),
      config2.getSourceVertices().toArray(new String[0]));
    if (config1.getNumPartitions() == null) {
      assertNull(config2.getNumPartitions());
    } else {
      assertArrayEquals(Ints.toArray(config1.getNumPartitions()),
        Ints.toArray(config2.getNumPartitions()));
    }
    CartesianProductFilterDescriptor descriptor1 = config1.getFilterDescriptor();
    CartesianProductFilterDescriptor descriptor2 = config1.getFilterDescriptor();

    if (descriptor1 != null && descriptor2 != null) {
      assertEquals(descriptor1.getClassName(), descriptor2.getClassName());
      UserPayload payload1 = descriptor1.getUserPayload();
      UserPayload payload2 = descriptor2.getUserPayload();
      if (payload1 != null && payload2 != null) {
        assertEquals(0, payload1.getPayload().compareTo(payload2.getPayload()));
      }
    } else {
      assertNull(descriptor1);
      assertNull(descriptor2);
    }
  }

  @Test(timeout = 5000)
  public void testFairCartesianProductConfig() {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v0");
    sourceVertices.add("v1");
    CartesianProductConfig config = new CartesianProductConfig(sourceVertices);

    // conf not set
    CartesianProductConfigProto proto = config.toProto(conf);
    assertEquals(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM_DEFAULT,
      proto.getMaxParallelism());
    assertEquals(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER_DEFAULT,
      proto.getMinOpsPerWorker());
    assertEquals(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING_DEFAULT,
      proto.getEnableGrouping());
    assertFalse(proto.hasNumPartitionsForFairCase());
    assertFalse(proto.hasGroupingFraction());

    // conf set
    conf.setInt(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM, 1000);
    conf.setLong(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER, 1000000);
    conf.setBoolean(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING, false);
    conf.setFloat(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_GROUPING_FRACTION, 0.75f);
    conf.setInt(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_NUM_PARTITIONS, 25);
    proto = config.toProto(conf);
    assertEquals(1000, proto.getMaxParallelism());
    assertEquals(1000000, proto.getMinOpsPerWorker());
    assertFalse(proto.getEnableGrouping());
    assertEquals(0.75f, proto.getGroupingFraction(), 0.01);
    assertEquals(25, proto.getNumPartitionsForFairCase());
  }
}
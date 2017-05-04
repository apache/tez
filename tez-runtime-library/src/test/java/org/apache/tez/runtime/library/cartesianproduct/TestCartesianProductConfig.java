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
import static org.junit.Assert.assertTrue;

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
  public void testSerializationUnpartitioned() throws Exception {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v1");
    sourceVertices.add("v2");
    sourceVertices.add("v3");
    CartesianProductConfig config =
      new CartesianProductConfig(sourceVertices);
    UserPayload payload = config.toUserPayload(conf);
    CartesianProductConfig parsedConfig = CartesianProductConfig.fromUserPayload(payload);
    assertConfigEquals(config, parsedConfig);

    // unpartitioned config should have null in numPartitions fields
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
  public void testAutoGroupingConfig() {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v0");
    sourceVertices.add("v1");
    CartesianProductConfig config = new CartesianProductConfig(sourceVertices);

    // auto grouping conf not set
    CartesianProductConfigProto proto = config.toProto(conf);
    assertFalse(proto.hasEnableAutoGrouping());
    assertFalse(proto.hasDesiredBytesPerChunk());

    // auto groupinig conf not set
    conf.setBoolean(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING, true);
    conf.setLong(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP, 1000);
    proto = config.toProto(conf);
    assertTrue(proto.hasEnableAutoGrouping());
    assertTrue(proto.hasDesiredBytesPerChunk());
    assertEquals(true, proto.getEnableAutoGrouping());
    assertEquals(1000, proto.getDesiredBytesPerChunk());
  }
}
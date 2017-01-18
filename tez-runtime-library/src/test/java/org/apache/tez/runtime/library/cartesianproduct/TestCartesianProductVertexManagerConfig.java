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

import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestCartesianProductVertexManagerConfig {
  @Test(timeout = 5000)
  public void testAutoGroupingConfig() throws IOException {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v0");
    sourceVertices.add("v1");
    CartesianProductConfig config = new CartesianProductConfig(sourceVertices);
    TezConfiguration conf = new TezConfiguration();

    // auto group not set in proto
    CartesianProductVertexManagerConfig vmConf =
      CartesianProductVertexManagerConfig.fromUserPayload(config.toUserPayload(conf));
    assertEquals(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING_DEFAULT,
      vmConf.isEnableAutoGrouping());
    assertEquals(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP_DEFAULT,
      vmConf.getDesiredBytesPerGroup());

    // auto group set in proto
    conf.setBoolean(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_AUTO_GROUPING, true);
    conf.setLong(CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_DESIRED_BYTES_PER_GROUP, 1000);
    vmConf = CartesianProductVertexManagerConfig.fromUserPayload(config.toUserPayload(conf));
    assertEquals(true, vmConf.isEnableAutoGrouping());
    assertEquals(1000, vmConf.getDesiredBytesPerGroup());
  }
}

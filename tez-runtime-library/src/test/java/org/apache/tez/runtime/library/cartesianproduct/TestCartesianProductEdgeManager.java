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
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.UserPayload;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCartesianProductEdgeManager {
  @Test(timeout = 5000)
  public void testInitialize() throws Exception {
    EdgeManagerPluginContext context = mock(EdgeManagerPluginContext.class);
    when(context.getSourceVertexName()).thenReturn("v0");
    CartesianProductEdgeManager edgeManager = new CartesianProductEdgeManager(context);

    // partitioned case
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(true)
      .addAllSources(Arrays.asList("v0", "v1"))
      .addAllNumPartitions(Ints.asList(2,3))
      .setMaxParallelism(100).setMinOpsPerWorker(1);
    UserPayload payload = UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray()));
    when(context.getUserPayload()).thenReturn(payload);
    edgeManager.initialize();
    assertTrue(edgeManager.getEdgeManagerReal()
      instanceof CartesianProductEdgeManagerPartitioned);

    // unpartitioned case
    builder.clear();
    builder.setIsPartitioned(false)
      .addAllSources(Arrays.asList("v0", "v1"))
      .addAllNumChunks(Ints.asList(2,3))
      .setMaxParallelism(100).setMinOpsPerWorker(1);
    payload = UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray()));
    when(context.getUserPayload()).thenReturn(payload);
    when(context.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize();
    assertTrue(edgeManager.getEdgeManagerReal()
      instanceof FairCartesianProductEdgeManager);
  }
}

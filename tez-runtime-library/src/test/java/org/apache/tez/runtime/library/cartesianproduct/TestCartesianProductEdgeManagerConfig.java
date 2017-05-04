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
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestCartesianProductEdgeManagerConfig {
  @Test(timeout = 5000)
  public void testUnpartitionedAutoGroupingConfig() throws IOException {
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v0");
    sourceVertices.add("v1");
    int[] numChunkPerSrc = new int[] {2, 3};
    int numGroup = 3, chunkIdOffset = 0;

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addAllNumChunks(Ints.asList(numChunkPerSrc))
      .addAllSources(sourceVertices).setNumChunk(numGroup).setChunkIdOffset(chunkIdOffset);
    UserPayload payload = UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray()));

    CartesianProductEdgeManagerConfig config =
      CartesianProductEdgeManagerConfig.fromUserPayload(payload);
    assertArrayEquals(numChunkPerSrc, config.numChunksPerSrc);
    assertEquals(numGroup, config.numChunk);
    assertEquals(chunkIdOffset, config.chunkIdOffset);
  }
}

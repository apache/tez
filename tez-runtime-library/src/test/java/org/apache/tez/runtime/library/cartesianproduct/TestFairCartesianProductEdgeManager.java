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

import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.CompositeEventRouteMetadata;
import org.junit.Before;
import org.junit.Test;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFairCartesianProductEdgeManager {
  private EdgeManagerPluginContext mockContext;
  private FairCartesianProductEdgeManager edgeManager;

  @Before
  public void setup() {
    mockContext = mock(EdgeManagerPluginContext.class);
    edgeManager = new FairCartesianProductEdgeManager(mockContext);
  }

  static class TestData {
    int srcId, destId, inputId;
    Object expected;

    public TestData(int srcId, int destId, int inputId, Object expected) {
      this.srcId = srcId;
      this.destId = destId;
      this.inputId = inputId;
      this.expected = expected;
    }
  }

  private TestData dataForRouting(int srcId, int destId, Object expected) {
    return new TestData(srcId, destId, -1, expected);
  }

  private TestData dataForInputError(int destId, int inputId, Object expected) {
    return new TestData(-1, destId, inputId, expected);
  }

  private TestData dataForSrc(int srcId, Object expected) {
    return new TestData(srcId, -1, -1, expected);
  }

  private TestData dataForDest(int destId, Object expected) {
    return new TestData(-1, destId, -1, expected);
  }

  private void testEdgeManager(CartesianProductConfigProto conf, String vName, int numTask,
                               String groupName, TestData cDMEInvalid, TestData cDMEValid,
                               TestData srcFailInvalid, TestData srcFailValid,
                               TestData inputError, TestData numDestInput,
                               TestData numSrcOutputTest, TestData numConsumerTest)
    throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn(vName);
    when(mockContext.getSourceVertexNumTasks()).thenReturn(numTask);
    when(mockContext.getVertexGroupName()).thenReturn(groupName);
    edgeManager.initialize(conf);

    CompositeEventRouteMetadata cDME;

    if (cDMEInvalid != null) {
      cDME = edgeManager.routeCompositeDataMovementEventToDestination(cDMEInvalid.srcId,
        cDMEInvalid.destId);
      assertNull(cDME);
    }

    cDME = edgeManager.routeCompositeDataMovementEventToDestination(cDMEValid.srcId,
      cDMEValid.destId);
    assertNotNull(cDME);
    CompositeEventRouteMetadata expectedCDME = (CompositeEventRouteMetadata)(cDMEValid.expected);
    assertEquals(expectedCDME.getCount(), cDME.getCount());
    assertEquals(expectedCDME.getTarget(), cDME.getTarget());
    assertEquals(expectedCDME.getSource(), cDME.getSource());

    EventRouteMetadata dme;
    if (srcFailInvalid != null) {
      dme = edgeManager.routeInputSourceTaskFailedEventToDestination(srcFailInvalid.srcId,
        srcFailInvalid.destId);
      assertNull(dme);
    }

    dme = edgeManager.routeInputSourceTaskFailedEventToDestination(srcFailValid.srcId,
      srcFailValid.destId);
    assertNotNull(dme);
    EventRouteMetadata expectedDME = (EventRouteMetadata)(srcFailValid.expected);
    assertEquals(expectedDME.getNumEvents(), dme.getNumEvents());
    assertArrayEquals(expectedDME.getTargetIndices(), dme.getTargetIndices());

    assertEquals(inputError.expected,
      edgeManager.routeInputErrorEventToSource(inputError.destId, inputError.inputId));

    assertEquals(numDestInput.expected,
      edgeManager.getNumDestinationTaskPhysicalInputs(numDestInput.destId));
    assertEquals(numSrcOutputTest.expected,
      edgeManager.getNumSourceTaskPhysicalOutputs(numSrcOutputTest.srcId));
    assertEquals(numConsumerTest.expected,
      edgeManager.getNumDestinationConsumerTasks(numConsumerTest.srcId));
  }

  /**
   * Vertex v0 has 2 tasks, 2 chunks
   * Vertex v1 has 30 tasks, 3 chunks
   */
  @Test(timeout = 5000)
  public void testTwoWayAllVertex() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1")
      .addNumChunks(2).addNumChunks(3).setMaxParallelism(10).setNumPartitionsForFairCase(10);
    CartesianProductConfigProto config = builder.build();
    testEdgeManager(config, "v0", 2, null, dataForRouting(1, 1, null),
      dataForRouting(1, 3, CompositeEventRouteMetadata.create(10, 0, 0)),
      dataForRouting(1, 1, null),
      dataForRouting(1, 3, EventRouteMetadata.create(10, new int[]{0,1,2,3,4,5,6,7,8,9})),
      dataForInputError(1, 0, 0), dataForDest(1, 10), dataForSrc(1, 10), dataForSrc(1, 3));
    testEdgeManager(config, "v1", 30, null, dataForRouting(1, 2, null),
      dataForRouting(1, 0, CompositeEventRouteMetadata.create(10, 10, 0)),
      dataForRouting(1, 2, null),
      dataForRouting(1, 0, EventRouteMetadata.create(10, new int[]{10,11,12,13,14,15,16,17,18,19})),
      dataForInputError(1,0,10), dataForDest(1, 100), dataForSrc(1, 10), dataForSrc(1, 2));
  }

  /**
   * Vertex v0 has 2 tasks, 2 chunks
   * Vertex v1 has 30 tasks, 3 chunks
   * Vertex v2 has 1 tasks, 4 chunks
   */
  @Test(timeout = 5000)
  public void testThreeWayAllVertex() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1").addSources("v2")
      .addNumChunks(2).addNumChunks(3).addNumChunks(4)
      .setMaxParallelism(12).setNumPartitionsForFairCase(12);
    CartesianProductConfigProto config = builder.build();
    testEdgeManager(config, "v0", 2, null, dataForRouting(1, 1, null),
      dataForRouting(1, 12, CompositeEventRouteMetadata.create(12, 0, 0)),
      dataForRouting(1, 1, null),
      dataForRouting(1, 12, EventRouteMetadata.create(12, new int[]{0,1,2,3,4,5,6,7,8,9,10,11})),
      dataForInputError(1, 0, 0), dataForDest(1, 12), dataForSrc(1, 12), dataForSrc(1, 12));
    testEdgeManager(config, "v1", 30, null, dataForRouting(1, 4, null),
      dataForRouting(1, 13, CompositeEventRouteMetadata.create(12, 12, 0)),
      dataForRouting(1, 4, null),
      dataForRouting(1, 13,
        EventRouteMetadata.create(12, new int[]{12,13,14,15,16,17,18,19,20,21,22,23})),
      dataForInputError(1, 0, 0), dataForDest(1, 120), dataForSrc(1, 12), dataForSrc(1, 8));
    testEdgeManager(config, "v2", 1, null,
      null, dataForRouting(0, 13, CompositeEventRouteMetadata.create(3, 0, 3)),
      null, dataForRouting(0, 13, EventRouteMetadata.create(3, new int[]{0,1,2})),
      dataForInputError(1, 0, 0), dataForDest(1, 3), dataForSrc(0, 12), dataForSrc(0, 24));
  }

  /**
   * v0 with group g0 {v1, v2}
   * Vertex v0 has 2 chunks
   * Vertex v1 has 10 tasks
   * Vertex v2 has 20 tasks
   * Group g0 has 3 chunks
   */
  @Test(timeout = 5000)
  public void testTwoWayVertexWithVertexGroup() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("g0")
      .addNumChunks(2).addNumChunks(3).setPositionInGroup(10).setNumPartitionsForFairCase(10)
      .addNumTaskPerVertexInGroup(10).addNumTaskPerVertexInGroup(20).setPositionInGroup(0);
    testEdgeManager(builder.build(), "v1", 10, "g0", dataForRouting(0, 4, null),
      dataForRouting(0, 3, CompositeEventRouteMetadata.create(10, 0, 0)),
      dataForRouting(0, 4, null),
      dataForRouting(0, 3, EventRouteMetadata.create(10, new int[]{0,1,2,3,4,5,6,7,8,9})),
      dataForInputError(3, 0, 0), dataForDest(2, 34), dataForSrc(0, 10), dataForSrc(0, 2));
    builder.setPositionInGroup(1);
    testEdgeManager(builder.build(), "v2", 20, "g0", dataForRouting(1, 1, null),
      dataForRouting(6, 1, CompositeEventRouteMetadata.create(4, 33, 6)),
      dataForRouting(1, 1, null),
      dataForRouting(6, 1, EventRouteMetadata.create(4, new int[]{33,34,35,36})),
      dataForInputError(1, 33, 6), dataForDest(0, 66), dataForSrc(1, 10), dataForSrc(6, 4));
  }

  /**
   * group g0 {v1, v2} with group g1 {v3, v4}
   *
   * Vertex v0 has 2 tasks
   * Vertex v1 has 4 tasks
   * Group g0 has 2 chunks
   * Group g1 has 3 chunks
   */
  @Test(timeout = 5000)
  public void testTwoWayAllVertexGroup() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("g0").addSources("g1")
      .addNumChunks(2).addNumChunks(3).setMaxParallelism(10).setNumPartitionsForFairCase(10)
      .addNumTaskPerVertexInGroup(2).addNumTaskPerVertexInGroup(5).setPositionInGroup(0);
    testEdgeManager(builder.build(), "v0", 2, "g0", dataForRouting(1, 1, null),
      dataForRouting(0, 1, CompositeEventRouteMetadata.create(10, 0, 0)),
      dataForRouting(1, 1, null),
      dataForRouting(0, 1, EventRouteMetadata.create(10, new int[]{0,1,2,3,4,5,6,7,8,9})),
      dataForInputError(1, 0, 0), dataForDest(1, 10), dataForSrc(1, 10), dataForSrc(1, 3));
    builder.setPositionInGroup(1);
    testEdgeManager(builder.build(), "v1", 5, "g0", dataForRouting(3, 1, null),
      dataForRouting(1, 1, CompositeEventRouteMetadata.create(10, 20, 0)),
      dataForRouting(3, 1, null),
      dataForRouting(1, 1, EventRouteMetadata.create(10, new int[]{20,21,22,23,24,25,26,27,28,29})),
      dataForInputError(1, 15, 0), dataForDest(1, 25), dataForSrc(1, 10), dataForSrc(1, 3));
  }

  @Test(timeout = 5000)
  public void testNumPartition() throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("source");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(10);
    when(mockContext.getVertexGroupName()).thenReturn(null);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(false).addSources("v0").addSources("v1").setMaxParallelism(100);

    edgeManager.initialize(builder.build());
    assertEquals(10, edgeManager.getNumSourceTaskPhysicalOutputs(0));

    builder.setNumPartitionsForFairCase(20);
    edgeManager = new FairCartesianProductEdgeManager(mockContext);
    edgeManager.initialize(builder.build());
    assertEquals(20, edgeManager.getNumSourceTaskPhysicalOutputs(0));
  }
}
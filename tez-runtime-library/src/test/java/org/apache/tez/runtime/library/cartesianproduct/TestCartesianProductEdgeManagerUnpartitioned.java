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

import static org.apache.tez.runtime.library.cartesianproduct.TestCartesianProductEdgeManagerUnpartitioned.TestData.dataForDest;
import static org.apache.tez.runtime.library.cartesianproduct.TestCartesianProductEdgeManagerUnpartitioned.TestData.dataForInputError;
import static org.apache.tez.runtime.library.cartesianproduct.TestCartesianProductEdgeManagerUnpartitioned.TestData.dataForRouting;
import static org.apache.tez.runtime.library.cartesianproduct.TestCartesianProductEdgeManagerUnpartitioned.TestData.dataForSrc;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCartesianProductEdgeManagerUnpartitioned {
  private EdgeManagerPluginContext mockContext;
  private CartesianProductEdgeManagerUnpartitioned edgeManager;

  @Before
  public void setup() {
    mockContext = mock(EdgeManagerPluginContext.class);
    edgeManager = new CartesianProductEdgeManagerUnpartitioned(mockContext);
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

    public static TestData dataForRouting(int srcId, int destId, Object expected) {
      return new TestData(srcId, destId, -1, expected);
    }

    public static TestData dataForInputError(int destId, int inputId, Object expected) {
      return new TestData(-1, destId, inputId, expected);
    }

    public static TestData dataForSrc(int srcId, Object expected) {
      return new TestData(srcId, -1, -1, expected);
    }

    public static TestData dataForDest(int destId, Object expected) {
      return new TestData(-1, destId, -1, expected);
    }
  }

  private void testEdgeManager(CartesianProductEdgeManagerConfig conf, String vName, int numTask,
                               String groupName, TestData cDMEInvalid, TestData cDMEValid,
                               TestData srcFailInvalid, TestData srcFailValid,
                               TestData inputError, TestData numDestInput,
                               TestData numSrcOutputTest, TestData numConsumerTest)
    throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn(vName);
    when(mockContext.getSourceVertexNumTasks()).thenReturn(numTask);
    when(mockContext.getVertexGroupName()).thenReturn(groupName);
    edgeManager.initialize(conf);

    CompositeEventRouteMetadata cDME =
      edgeManager.routeCompositeDataMovementEventToDestination(cDMEInvalid.srcId,
        cDMEInvalid.destId);
    assertNull(cDME);

    cDME = edgeManager.routeCompositeDataMovementEventToDestination(cDMEValid.srcId,
      cDMEValid.destId);
    assertNotNull(cDME);
    CompositeEventRouteMetadata expectedCDME = (CompositeEventRouteMetadata)(cDMEValid.expected);
    assertEquals(expectedCDME.getCount(), cDME.getCount());
    assertEquals(expectedCDME.getTarget(), cDME.getTarget());
    assertEquals(expectedCDME.getSource(), cDME.getSource());

    EventRouteMetadata dme =
      edgeManager.routeInputSourceTaskFailedEventToDestination(srcFailInvalid.srcId,
        srcFailInvalid.destId);
    assertNull(dme);

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
   * Vertex v0 has 2 tasks
   * Vertex v1 has 3 tasks
   */
  @Test(timeout = 5000)
  public void testTwoWayAllVertex() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1"}, null,
      new int[]{2,3}, 2, 0, null), "v0", 2, null,
      dataForRouting(1, 1, null), dataForRouting(1, 3, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 1, null), dataForRouting(1, 3, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 3));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1"}, null,
        new int[]{2,3}, 3, 0, null), "v1", 3, null,
      dataForRouting(1, 2, null), dataForRouting(1, 1, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 2, null), dataForRouting(1, 1, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1,0,1), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 2));
  }

  /**
   * Vertex v0 has 2 tasks
   * Vertex v1 has 3 tasks
   * Vertex v2 has 4 tasks
   */
  @Test(timeout = 5000)
  public void testThreeWayAllVertex() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1","v2"},
      null, new int[]{2,3,4}, 2, 0, null), "v0", 2, null,
      dataForRouting(1, 1, null), dataForRouting(1, 12, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 1, null), dataForRouting(1, 12, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 12));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1","v2"},
        null, new int[]{2,3,4}, 3, 0, null), "v1", 3, null,
      dataForRouting(1, 1, null), dataForRouting(1, 16, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 1, null), dataForRouting(1, 16, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 8));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1","v2"},
        null, new int[]{2,3,4}, 4, 0, null), "v2", 4, null,
      dataForRouting(1, 0, null), dataForRouting(1, 13, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 0, null), dataForRouting(1, 13, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 1), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 6));
  }

  @Test(timeout = 5000)
  public void testZeroSrcTask() {
    CartesianProductEdgeManagerConfig emConfig =
      new CartesianProductEdgeManagerConfig(false, new String[]{"v0", "v1"}, null,
        new int[]{2,0}, 0,0, null);
    testZeroSrcTaskV0(emConfig);
    testZeroSrcTaskV1(emConfig);
  }

  private void testZeroSrcTaskV0(CartesianProductEdgeManagerConfig config) {
    when(mockContext.getSourceVertexName()).thenReturn("v0");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    assertEquals(0, edgeManager.getNumDestinationConsumerTasks(0));
    assertEquals(0, edgeManager.getNumDestinationConsumerTasks(1));
  }

  private void testZeroSrcTaskV1(CartesianProductEdgeManagerConfig config) {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(0);
    edgeManager.initialize(config);
  }

  /**
   * Vertex v0 has 10 tasks 2 groups
   * Vertex v1 has 30 tasks 3 group
   */
  @Test(timeout = 5000)
  public void testTwoWayAllVertexAutoGrouping() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1"},
        null, new int[]{2,3}, 2, 0, null), "v0", 10, null,
      dataForRouting(6, 1, null), dataForRouting(1, 0, CompositeEventRouteMetadata.create(1, 1, 0)),
      dataForRouting(6, 1, null), dataForRouting(1, 0, EventRouteMetadata.create(1, new int[]{1})),
      dataForInputError(1, 1, 1), dataForDest(1, 5), dataForSrc(1, 1), dataForSrc(1, 3));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1"},
        null, new int[]{2,3}, 3, 0, null), "v1", 30, null,
      dataForRouting(6, 1, null), dataForRouting(11, 1, CompositeEventRouteMetadata.create(1, 1, 0)),
      dataForRouting(6, 1, null), dataForRouting(11, 1, EventRouteMetadata.create(1, new int[]{1})),
      dataForInputError(1, 1, 11), dataForDest(1, 10), dataForSrc(1, 1), dataForSrc(1, 2));
  }

  /**
   * v0 with group g0 {v1, v2}
   * Vertex v0 has 2 tasks
   * Vertex v1 has 1 tasks
   * Vertex v2 has 2 tasks
   */
  @Test(timeout = 5000)
  public void testTwoWayVertexWithVertexGroup() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 2, 0, null), "v0", 2, null,
      dataForRouting(1, 1, null), dataForRouting(1, 3, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 1, null), dataForRouting(1, 3, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 3));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 1, 0, null), "v1", 1, "g0",
      dataForRouting(0, 1, null), dataForRouting(0, 3, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(0, 1, null), dataForRouting(0, 3, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(3, 0, 0), dataForDest(0, 1), dataForSrc(0, 1), dataForSrc(0, 2));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 2, 1, null), "v2", 2, "g0",
      dataForRouting(1, 1, null), dataForRouting(0, 1, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 1, null), dataForRouting(0, 1, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 2));
  }

  /**
   * group g0 {v1, v2} with group g1 {v3, v4}
   *
   * Vertex v0 has 1 tasks
   * Vertex v1 has 2 tasks
   * Vertex v2 has 3 tasks
   * Vertex v3 has 4 tasks
   */
  @Test(timeout = 5000)
  public void testTwoWayAllVertexGroup() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"g0","g1"},
        null, new int[]{3,7}, 1, 0, null), "v0", 1, "g0",
      dataForRouting(0, 7, null), dataForRouting(0, 1, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(0, 7, null), dataForRouting(0, 1, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 0), dataForDest(1, 1), dataForSrc(0, 1), dataForSrc(0, 7));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"g0","g1"},
        null, new int[]{3,7}, 2, 1, null), "v1", 2, "g0",
      dataForRouting(0, 1, null), dataForRouting(1, 15, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(0, 1, null), dataForRouting(1, 15, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(7, 0, 0), dataForDest(7, 1), dataForSrc(1, 1), dataForSrc(1, 7));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"g0","g1"},
        null, new int[]{3,7}, 3, 0, null), "v2", 3, "g1",
      dataForRouting(1, 0, null), dataForRouting(1, 1, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(1, 0, null), dataForRouting(1, 1, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(1, 0, 1), dataForDest(1, 1), dataForSrc(1, 1), dataForSrc(1, 3));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"g0","g1"},
        null, new int[]{3,7}, 4, 3, null), "v3", 4, "g1",
      dataForRouting(0, 1, null), dataForRouting(1, 4, CompositeEventRouteMetadata.create(1, 0, 0)),
      dataForRouting(0, 1, null), dataForRouting(1, 4, EventRouteMetadata.create(1, new int[]{0})),
      dataForInputError(4, 0, 1), dataForDest(4, 1), dataForSrc(1, 1), dataForSrc(1, 3));
  }


  /**
   * v0 with group g0 {v1, v2}
   * Vertex v0 has 10 tasks, 2 groups
   * Vertex v1 has 10 tasks, 1 group
   * Vertex v2 has 20 tasks, 2 groups
   */
  @Test(timeout = 5000)
  public void testTwoWayWithVertexGroupAutoGrouping() throws Exception {
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 2, 0, null), "v0", 10, null,
      dataForRouting(0, 4, null), dataForRouting(2, 1, CompositeEventRouteMetadata.create(1, 2, 0)),
      dataForRouting(0, 4, null), dataForRouting(2, 1, EventRouteMetadata.create(1, new int[]{2})),
      dataForInputError(1, 3, 3), dataForDest(1, 5), dataForSrc(1, 1), dataForSrc(1, 3));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 1, 0, null), "v1", 10, "g0",
      dataForRouting(1, 1, null), dataForRouting(2, 3, CompositeEventRouteMetadata.create(1, 2, 0)),
      dataForRouting(1, 1, null), dataForRouting(2, 3, EventRouteMetadata.create(1, new int[]{2})),
      dataForInputError(3, 1, 1), dataForDest(0, 10), dataForSrc(1, 1), dataForSrc(1, 2));
    testEdgeManager(new CartesianProductEdgeManagerConfig(false, new String[]{"v0","g0"},
        null, new int[]{2,3}, 2, 1, null), "v2", 20, "g0",
      dataForRouting(11, 1, null), dataForRouting(12, 2, CompositeEventRouteMetadata.create(1, 2, 0)),
      dataForRouting(11, 1, null), dataForRouting(12, 2, EventRouteMetadata.create(1, new int[]{2})),
      dataForInputError(2, 2, 12), dataForDest(1, 10), dataForSrc(1, 1), dataForSrc(1, 2));
  }
}
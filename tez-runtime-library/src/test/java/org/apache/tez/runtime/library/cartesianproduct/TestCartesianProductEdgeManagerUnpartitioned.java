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
import org.junit.Before;
import org.junit.Test;

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

  /**
   * Vertex v0 has 2 tasks
   * Vertex v1 has 3 tasks
   */
  @Test(timeout = 5000)
  public void testTwoWay() throws Exception {
    CartesianProductEdgeManagerConfig emConfig =
      new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1"}, null, new int[]{2,3}, null);
    testTwoWayV0(emConfig);
    testTwoWayV1(emConfig);
  }

  private void testTwoWayV0(CartesianProductEdgeManagerConfig config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v0");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    EventRouteMetadata routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 3);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 3);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    assertEquals(0, edgeManager.routeInputErrorEventToSource(1, 0));

    assertEquals(1, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(1, edgeManager.getNumSourceTaskPhysicalOutputs(1));
    assertEquals(3, edgeManager.getNumDestinationConsumerTasks(1));
  }

  private void testTwoWayV1(CartesianProductEdgeManagerConfig config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    edgeManager.initialize(config);

    EventRouteMetadata routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 2);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 2);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    assertEquals(1, edgeManager.routeInputErrorEventToSource(1, 0));

    assertEquals(1, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(1, edgeManager.getNumSourceTaskPhysicalOutputs(1));
    assertEquals(2, edgeManager.getNumDestinationConsumerTasks(1));
  }

  /**
   * Vertex v0 has 2 tasks
   * Vertex v1 has 3 tasks
   * Vertex v2 has 4 tasks
   */
  @Test(timeout = 5000)
  public void testThreeWay() throws Exception {
    CartesianProductEdgeManagerConfig emConfig =
      new CartesianProductEdgeManagerConfig(false, new String[]{"v0","v1","v2"}, null, new int[]{2,3,4}, null);
    testThreeWayV0(emConfig);
    testThreeWayV1(emConfig);
    testThreeWayV2(emConfig);
  }

  private void testThreeWayV0(CartesianProductEdgeManagerConfig config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v0");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    EventRouteMetadata routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 12);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 12);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    assertEquals(0, edgeManager.routeInputErrorEventToSource(1, 0));

    assertEquals(1, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(1, edgeManager.getNumSourceTaskPhysicalOutputs(1));
    assertEquals(12, edgeManager.getNumDestinationConsumerTasks(1));
  }

  private void testThreeWayV1(CartesianProductEdgeManagerConfig config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    edgeManager.initialize(config);

    EventRouteMetadata routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 16);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 16);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    assertEquals(0, edgeManager.routeInputErrorEventToSource(1, 0));

    assertEquals(1, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(1, edgeManager.getNumSourceTaskPhysicalOutputs(1));
    assertEquals(8, edgeManager.getNumDestinationConsumerTasks(1));
  }

  private void testThreeWayV2(CartesianProductEdgeManagerConfig config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v2");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(4);
    edgeManager.initialize(config);

    EventRouteMetadata routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 0);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 13);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 0);
    assertNull(routingData);

    routingData = edgeManager.routeCompositeDataMovementEventToDestination(1, 13);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{0}, routingData.getTargetIndices());
    assertArrayEquals(new int[]{0}, routingData.getSourceIndices());

    assertEquals(1, edgeManager.routeInputErrorEventToSource(1, 0));

    assertEquals(1, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(1, edgeManager.getNumSourceTaskPhysicalOutputs(1));
    assertEquals(6, edgeManager.getNumDestinationConsumerTasks(1));
  }

  @Test(timeout = 5000)
  public void testZeroSrcTask() {
    CartesianProductEdgeManagerConfig emConfig =
      new CartesianProductEdgeManagerConfig(false, new String[]{"v0", "v1"}, null, new int[]{2, 0}, null);
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
}

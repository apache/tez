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

import com.google.protobuf.ByteString;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.EventRouteMetadata;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand.CompositeEventRouteMetadata;
import org.apache.tez.dag.api.UserPayload;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCartesianProductEdgeManagerPartitioned {
  private EdgeManagerPluginContext mockContext;
  private CartesianProductEdgeManagerPartitioned edgeManager;

  @Before
  public void setup() {
    mockContext = mock(EdgeManagerPluginContext.class);
    edgeManager = new CartesianProductEdgeManagerPartitioned(mockContext);
  }

  /**
   * Vertex v0 has 2 tasks which generate 3 partitions
   * Vertex v1 has 3 tasks which generate 4 partitions
   */
  @Test(timeout = 5000)
  public void testTwoWay() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(true).addSources("v0").addSources("v1")
      .addNumPartitions(3).addNumPartitions(4);
    when(mockContext.getDestinationVertexNumTasks()).thenReturn(12);
    CartesianProductConfigProto config = builder.build();
    testTwoWayV0(config);
    testTwoWayV1(config);
  }

  private void testTwoWayV0(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v0");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(0, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeDataMovementEventToDestination(1,0,1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    routingData = edgeManager.routeDataMovementEventToDestination(1,1,1);
    assertNull(routingData);

    routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(1, edgeManager.routeInputErrorEventToSource(1, 1));

    assertEquals(12, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(2, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(3, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  private void testTwoWayV1(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(1, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(2, edgeManager.routeInputErrorEventToSource(1, 2));

    assertEquals(12, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(3, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  public static class TestFilter extends CartesianProductFilter {
    char op;

    public TestFilter(UserPayload payload) {
      super(payload);
      op = payload.getPayload().getChar();
    }

    @Override
    public boolean isValidCombination(Map<String, Integer> vertexPartitionMap) {
      switch (op) {
        case '>':
          return vertexPartitionMap.get("v0") > vertexPartitionMap.get("v1");
        case '<':
          return vertexPartitionMap.get("v0") < vertexPartitionMap.get("v1");
        default:
          return true;
      }
    }
  }

  /**
   * Vertex v0 has 2 tasks which generate 3 partitions
   * Vertex v1 has 3 tasks which generate 4 partitions
   */
  @Test(timeout = 5000)
  public void testTwoWayWithFilter() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    ByteBuffer buffer = ByteBuffer.allocate(2).putChar('>');
    buffer.flip();
    builder.setIsPartitioned(true).addSources("v0").addSources("v1")
      .addNumPartitions(3).addNumPartitions(4).setFilterClassName(TestFilter.class.getName())
      .setFilterUserPayload(ByteString.copyFrom(buffer));
    CartesianProductConfigProto config = builder.build();
    when(mockContext.getDestinationVertexNumTasks()).thenReturn(3);
    testTwoWayV0WithFilter(config);
    testTwoWayV1WithFilter(config);
  }

  private void testTwoWayV0WithFilter(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v0");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(2, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(1, edgeManager.routeInputErrorEventToSource(1, 1));

    assertEquals(3, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(2, edgeManager.getNumDestinationTaskPhysicalInputs(1));
    assertEquals(3, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  private void testTwoWayV1WithFilter(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(0, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(2, edgeManager.routeInputErrorEventToSource(1, 2));

    assertEquals(3, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(3, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  /**
   * Vertex v0 has 2 tasks which generate 4 partitions
   * Vertex v1 has 3 tasks which generate 3 partitions
   * Vertex v2 has 4 tasks which generate 2 partitions
   */
  @Test(timeout = 5000)
  public void testThreeWay() throws Exception {
    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    builder.setIsPartitioned(true).addSources("v0").addSources("v1").addSources("v2")
      .addNumPartitions(4).addNumPartitions(3).addNumPartitions(2);
    CartesianProductConfigProto config = builder.build();

    when(mockContext.getDestinationVertexNumTasks()).thenReturn(24);
    testThreeWayV0(config);
    testThreeWayV1(config);
    testThreeWayV2(config);
  }

  private void testThreeWayV0(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v0");

    when(mockContext.getSourceVertexNumTasks()).thenReturn(2);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(0, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(1, edgeManager.routeInputErrorEventToSource(1, 1));

    assertEquals(24, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(2, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(4, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  private void testThreeWayV1(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v1");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(3);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(0, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(2, edgeManager.routeInputErrorEventToSource(1, 2));

    assertEquals(24, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(3, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(3, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }

  private void testThreeWayV2(CartesianProductConfigProto config) throws Exception {
    when(mockContext.getSourceVertexName()).thenReturn("v2");
    when(mockContext.getSourceVertexNumTasks()).thenReturn(4);
    edgeManager.initialize(config);

    CompositeEventRouteMetadata compositeRoutingData =
      edgeManager.routeCompositeDataMovementEventToDestination(1, 1);
    assertNotNull(compositeRoutingData);
    assertEquals(1, compositeRoutingData.getCount());
    assertEquals(1, compositeRoutingData.getSource());
    assertEquals(1, compositeRoutingData.getTarget());

    EventRouteMetadata routingData = edgeManager.routeInputSourceTaskFailedEventToDestination(1, 1);
    assertNotNull(routingData);
    assertEquals(1, routingData.getNumEvents());
    assertArrayEquals(new int[]{1}, routingData.getTargetIndices());

    assertEquals(2, edgeManager.routeInputErrorEventToSource(1, 2));

    assertEquals(24, edgeManager.getNumDestinationConsumerTasks(1));
    assertEquals(4, edgeManager.getNumDestinationTaskPhysicalInputs(10));
    assertEquals(2, edgeManager.getNumSourceTaskPhysicalOutputs(2));
  }
}

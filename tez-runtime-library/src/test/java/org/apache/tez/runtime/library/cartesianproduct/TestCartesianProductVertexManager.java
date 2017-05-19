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

import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.BROADCAST;
import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCartesianProductVertexManager {
  private CartesianProductVertexManager vertexManager;
  private VertexManagerPluginContext context;
  private String vertexName = "cp";
  private TezConfiguration conf;
  private CartesianProductConfig config;
  private Map<String, EdgeProperty> edgePropertyMap;
  private EdgeProperty cpEdge = EdgeProperty.create(EdgeManagerPluginDescriptor.create(
    CartesianProductEdgeManager.class.getName()), null, null, null, null);
  private EdgeProperty customEdge = EdgeProperty.create(EdgeManagerPluginDescriptor.create(
    "OTHER_EDGE"), null, null, null, null);
  private EdgeProperty broadcastEdge =
    EdgeProperty.create(DataMovementType.BROADCAST, null, null, null, null);

  @Before
  public void setup() {
    context = mock(VertexManagerPluginContext.class);
    conf = new TezConfiguration();
    edgePropertyMap = new HashMap<>();
    edgePropertyMap.put("v0", cpEdge);
    edgePropertyMap.put("v1", cpEdge);
    when(context.getVertexName()).thenReturn(vertexName);
    when(context.getVertexNumTasks(vertexName)).thenReturn(-1);
    when(context.getInputVertexEdgeProperties()).thenReturn(edgePropertyMap);
    when(context.getUserPayload()).thenAnswer(new Answer<UserPayload>() {
      @Override
      public UserPayload answer(InvocationOnMock invocation) throws Throwable {
        return config.toUserPayload(conf);
      }
    });
    vertexManager = new CartesianProductVertexManager(context);
  }

  @Test(timeout = 5000)
  public void testRejectPredefinedParallelism() throws Exception {
    when(context.getVertexNumTasks(vertexName)).thenReturn(10);
    try {
      vertexManager = new CartesianProductVertexManager(context);
      assertTrue(false);
    } catch (Exception ignored){}
  }

  @Test(timeout = 5000)
  public void testChooseRealVertexManager() throws Exception {
    // partitioned case
    config = new CartesianProductConfig(new int[]{2, 3}, new String[]{"v0", "v1"}, null);
    vertexManager.initialize();
    assertTrue(vertexManager.getVertexManagerReal()
      instanceof CartesianProductVertexManagerPartitioned);

    // unpartitioned case
    List<String> sourceVertices = new ArrayList<>();
    sourceVertices.add("v0");
    sourceVertices.add("v1");
    config = new CartesianProductConfig(sourceVertices);
    vertexManager.initialize();
    assertTrue(vertexManager.getVertexManagerReal()
      instanceof FairCartesianProductVertexManager);
  }

  @Test(timeout = 5000)
  public void testCheckDAGConfigConsistent() throws Exception {
    // positive case
    edgePropertyMap.put("v2", broadcastEdge);
    config = new CartesianProductConfig(new int[]{2, 3}, new String[]{"v0", "v1"}, null);
    vertexManager.initialize();

    // cartesian product edge in dag but not in config
    edgePropertyMap.put("v2", cpEdge);
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}

    // non-cartesian-product edge in dag but in config
    edgePropertyMap.put("v2", broadcastEdge);
    config = new CartesianProductConfig(new int[]{2, 3, 4}, new String[]{"v0", "v1", "v2"}, null);
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}

    edgePropertyMap.put("v2", customEdge);
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}

    // edge in config but not in dag
    edgePropertyMap.remove("v2");
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}
  }

  @Test(timeout = 5000)
  public void testCheckDAGConfigConsistentWithVertexGroup() throws Exception {
    // positive case
    edgePropertyMap.put("v2", cpEdge);
    config = new CartesianProductConfig(new int[]{2, 3}, new String[]{"v0", "g0"}, null);
    Map<String, List<String>> vertexGroups = new HashMap<>();
    vertexGroups.put("g0", Arrays.asList("v1", "v2"));
    when(context.getInputVertexGroups()).thenReturn(vertexGroups);
    vertexManager.initialize();

    // vertex group is in cartesian product config, but one member doesn't have cp edge
    edgePropertyMap.put("v2", broadcastEdge);
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}
  }

  @Test(timeout = 5000)
  public void testOtherEdgeType() throws Exception {
    // forbid other custom edge
    edgePropertyMap.put("v2", customEdge);
    config = new CartesianProductConfig(new int[]{2, 3}, new String[]{"v0", "v1"}, null);
    try {
      vertexManager.initialize();
      assertTrue(false);
    } catch (Exception ignored) {}

    // broadcast edge should be allowed and other non-custom edge shouldn't be allowed
    for (DataMovementType type : DataMovementType.values()) {
      if (type == CUSTOM) {
        continue;
      }
      edgePropertyMap.put("v2", EdgeProperty.create(type, null, null, null, null));
      try {
        vertexManager.initialize();
        assertTrue(type == BROADCAST);
      } catch (Exception e) {
        assertTrue(type != BROADCAST);
      }
    }
  }
}

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
package org.apache.tez.dag.api;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestEdge {
  Vertex v1, v2;
  EdgeProperty edgeProperty;
  Set<Edge> set;

  @Before
  public void setup() {
    v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"));
    v2 = Vertex.create("v2", ProcessorDescriptor.create("Processor"));
    edgeProperty = EdgeProperty.create(EdgeProperty.DataMovementType.SCATTER_GATHER,
      EdgeProperty.DataSourceType.PERSISTED, EdgeProperty.SchedulingType.CONCURRENT,
      OutputDescriptor.create("output"), InputDescriptor.create("input"));
    set = new HashSet<>();
  }

  @Test(timeout = 5000)
  public void testHashAndEqualsUnnamed() {
    // edges without name but everything else same are equal and have same hash
    Edge e1 = Edge.create(v1, v2, edgeProperty);
    Edge e2 = Edge.create(v1, v2, edgeProperty);
    assertEquals(e1, e2);
    set.add(e1);
    assertTrue(set.contains(e2));
  }

  @Test(timeout = 5000)
  public void testHashAndEqualsNamed() {
    // edges with everything same including name are equal and have same hash
    Edge e1 = Edge.create(v1, v2, edgeProperty, "e1");
    Edge e2 = Edge.create(v1, v2, edgeProperty, "e1");
    assertEquals(e1, e2);
    set.add(e1);
    assertTrue(set.contains(e2));
  }

  @Test(timeout = 5000)
  public void testHashAndEqualsDifferentName() {
    // edges with different name but everything else same are not equal and have different hash
    Edge e1 = Edge.create(v1, v2, edgeProperty, "e1");
    Edge e2 = Edge.create(v1, v2, edgeProperty, "e2");
    assertNotEquals(e1, e2);
    set.add(e1);
    assertFalse(set.contains(e2));
  }
}

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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.junit.Assert;
import org.junit.Test;

public class TestDAGVerify {

  private final String dummyProcessorClassName = TestDAGVerify.class.getName();
  private final String dummyInputClassName = TestDAGVerify.class.getName();
  private final String dummyOutputClassName = TestDAGVerify.class.getName();
  private final int dummyTaskCount = 2;
  private final Resource dummyTaskResource = Resource.newInstance(1, 1);

  //    v1
  //    |
  //    v2
  @Test
  public void testVerify1() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  //    v1 <----
  //      |     ^
  //       v2   ^
  //      |  |  ^
  //    v3    v4
  @Test
  public void testCycle1() {
    IllegalStateException ex=null;
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e3 = new Edge(v2, v4,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e4 = new Edge(v4, v1,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.addEdge(e3);
    dag.addEdge(e4);
    try{
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith("DAG contains a cycle"));
  }

  //     v1
  //      |
  //    -> v2
  //    ^  | |
  //    v3    v4
  @Test
  public void testCycle2() {
    IllegalStateException ex=null;
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e3 = new Edge(v2, v4,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e4 = new Edge(v3, v2,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.addEdge(e3);
    dag.addEdge(e4);
    try{
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith("DAG contains a cycle"));
  }

  @Test
  public void repeatedVertexName() {
    IllegalStateException ex=null;
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v1repeat = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v1repeat);
    try {
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith("DAG contains multiple vertices with name"));
  }

  //  v1  v2
  //   |  |
  //    v3
  @Test
  public void BinaryInputDisallowed() {
    IllegalStateException ex=null;
    try {
      Vertex v1 = new Vertex("v1",
          new ProcessorDescriptor("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v2 = new Vertex("v2",
          new ProcessorDescriptor("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v3 = new Vertex("v3",
          new ProcessorDescriptor("ReduceProcessor"),
          dummyTaskCount, dummyTaskResource);
      Edge e1 = new Edge(v1, v3,
          new EdgeProperty(ConnectionPattern.ONE_TO_ONE, SourceType.STABLE,
              new OutputDescriptor("dummy output class"),
              new InputDescriptor("dummy input class")));
      Edge e2 = new Edge(v2, v3,
          new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
              new OutputDescriptor("dummy output class"),
              new InputDescriptor("dummy input class")));
      DAG dag = new DAG("testDag");
      dag.addVertex(v1);
      dag.addVertex(v2);
      dag.addVertex(v3);
      dag.addEdge(e1);
      dag.addEdge(e2);
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith(
        "Unsupported connection pattern on edge"));
  }

  //  v1  v2
  //   |  |
  //    v3
  @Test
  public void BinaryInputAllowed() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("ReduceProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v3,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
  }

  //   v1
  //  |  |
  //  v2  v3
  @Test
  public void BinaryOutput() {
    IllegalStateException ex=null;
    try {
      Vertex v1 = new Vertex("v1",
          new ProcessorDescriptor("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v2 = new Vertex("v2",
          new ProcessorDescriptor("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v3 = new Vertex("v3",
          new ProcessorDescriptor("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Edge e1 = new Edge(v1, v2,
          new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
              new OutputDescriptor("dummy output class"),
              new InputDescriptor("dummy input class")));
      Edge e2 = new Edge(v1, v2,
          new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE,
              new OutputDescriptor("dummy output class"),
              new InputDescriptor("dummy input class")));
      DAG dag = new DAG("testDag");
      dag.addVertex(v1);
      dag.addVertex(v2);
      dag.addVertex(v3);
      dag.addEdge(e1);
      dag.addEdge(e2);
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith("Vertex has outDegree>1"));
  }

  @Test
  public void testDagWithNoVertices() {
    IllegalStateException ex=null;
    try {
      DAG dag = new DAG("testDag");
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage()
        .startsWith("Invalid dag containing 0 vertices"));
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidVertexConstruction() {
    try {
      Vertex v1 = new Vertex("v1",
          new ProcessorDescriptor("MapProcessor"),
          0, dummyTaskResource);
      Assert.fail("Expected exception for 0 parallelism");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Parallelism cannot be 0"));
    }
    try {
      Vertex v1 = new Vertex("v1",
          new ProcessorDescriptor("MapProcessor"),
          1, null);
      Assert.fail("Expected exception for 0 parallelism");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Resource cannot be null"));
    }
  }

}

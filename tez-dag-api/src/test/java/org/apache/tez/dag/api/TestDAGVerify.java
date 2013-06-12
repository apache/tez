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

import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.junit.Assert;
import org.junit.Test;

public class TestDAGVerify {
  
  private final String dummyProcessorClassName = TestDAGVerify.class.getName();
  private final String dummyInputClassName = TestDAGVerify.class.getName();
  private final String dummyOutputClassName = TestDAGVerify.class.getName();
  private final int dummyTaskCount = 2;
  
  //    v1
  //    |  
  //    v2
  @Test
  public void testVerify1() {
    Vertex v1 = new Vertex("v1", new ProcessorDescriptor(dummyProcessorClassName, null), dummyTaskCount);
    Vertex v2 = new Vertex("v2", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Edge e1 = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor(dummyOutputClassName, null), new InputDescriptor(dummyInputClassName, null)));
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
    Vertex v1 = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v2 = new Vertex("v2", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v3 = new Vertex("v3", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v4 = new Vertex("v4", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Edge e1 = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e2 = new Edge(v2, v3, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e3 = new Edge(v2, v4, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e4 = new Edge(v4, v1, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
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
    Vertex v1 = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v2 = new Vertex("v2", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v3 = new Vertex("v3", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v4 = new Vertex("v4", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Edge e1 = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e2 = new Edge(v2, v3, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e3 = new Edge(v2, v4, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
    Edge e4 = new Edge(v3, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
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
    Vertex v1 = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
    Vertex v1repeat = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
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
  public void BinaryInput() {
    IllegalStateException ex=null;
    try {
      Vertex v1 = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Vertex v2 = new Vertex("v2", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Vertex v3 = new Vertex("v3", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Edge e1 = new Edge(v1, v3, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
      Edge e2 = new Edge(v2, v3, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
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
    Assert.assertTrue(ex.getMessage().startsWith("Vertex has inDegree>1"));
  }
  
  //   v1  
  //  |  |     
  //  v2  v3 
  @Test
  public void BinaryOutput() {
    IllegalStateException ex=null;
    try {
      Vertex v1 = new Vertex("v1", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Vertex v2 = new Vertex("v2", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Vertex v3 = new Vertex("v3", new ProcessorDescriptor("org.apache.tez.mapreduce.processor.reduce.MapProcessor", null), dummyTaskCount);
      Edge e1 = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
      Edge e2 = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE, SourceType.STABLE, new OutputDescriptor("dummy output class", null), new InputDescriptor("dummy input class", null)));
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
}

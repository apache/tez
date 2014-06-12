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
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
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
  @Test(timeout = 5000)
  public void testVerifyScatterGather() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(timeout = 5000)
  public void testVerifyCustomEdge() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(new EdgeManagerDescriptor("emClass"),
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(timeout = 5000)
  public void testVerifyOneToOne() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(timeout = 5000)
  // v1 (known) -> v2 (-1) -> v3 (-1)
  public void testVerifyOneToOneInferParallelism() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
    Assert.assertEquals(dummyTaskCount, v2.getParallelism());
    Assert.assertEquals(dummyTaskCount, v3.getParallelism());
  }
  
  @Test(timeout = 5000)
  // v1 (known) -> v2 (-1) -> v3 (-1)
  // The test checks resiliency to ordering of the vertices/edges
  public void testVerifyOneToOneInferParallelismReverseOrder() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v3);
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e2);
    dag.addEdge(e1);
    dag.verify();
    Assert.assertEquals(dummyTaskCount, v2.getParallelism());
    Assert.assertEquals(dummyTaskCount, v3.getParallelism());
  }
  
  @Test(timeout = 5000)
  public void testVerifyOneToOneNoInferParallelism() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
    Assert.assertEquals(-1, v2.getParallelism());
  }
  
  @Test(timeout = 5000)
  // v1 (-1) -> v2 (known) -> v3 (-1)
  public void testVerifyOneToOneIncorrectParallelism1() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = new Edge(v1, v3,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    dag.addEdge(e2);
    try {
      dag.verify();
      Assert.assertTrue(false);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "1-1 Edge. Destination vertex parallelism must match source vertex"));
    }
  }

  @Test(timeout = 5000)
  // v1 (-1) -> v3 (-1), v2 (known) -> v3 (-1)
  // order of edges should not matter
  public void testVerifyOneToOneIncorrectParallelism2() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor(dummyProcessorClassName),
        -1, dummyTaskResource);
    Edge e1 = new Edge(v1, v4,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    Edge e2 = new Edge(v2, v4,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    Edge e3 = new Edge(v3, v4,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.addEdge(e3);
    try {
      dag.verify();
      Assert.assertTrue(false);
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains(
          "1-1 Edge. Destination vertex parallelism must match source vertex"));
    }
  }
  
  @Test(timeout = 5000)
  public void testVerifyBroadcast() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.BROADCAST, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(expected = IllegalStateException.class, timeout = 5000)  
  public void testVerify3() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.EPHEMERAL, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor(dummyOutputClassName),
            new InputDescriptor(dummyInputClassName)));
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(expected = IllegalStateException.class, timeout = 5000)  
  public void testVerify4() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.EPHEMERAL, SchedulingType.CONCURRENT, 
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
  @Test(timeout = 5000)
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
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e3 = new Edge(v2, v4,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e4 = new Edge(v4, v1,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
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
  @Test(timeout = 5000)
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
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e3 = new Edge(v2, v4,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e4 = new Edge(v3, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
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

  @Test(timeout = 5000)
  public void repeatedVertexName() {
    IllegalStateException ex=null;
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v1repeat = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    try {
      DAG dag = new DAG("testDag");
      dag.addVertex(v1);
      dag.addVertex(v1repeat);
      dag.verify();
    }
    catch (IllegalStateException e){
      ex = e;
    }
    Assert.assertNotNull(ex);
    System.out.println(ex.getMessage());
    Assert.assertTrue(ex.getMessage().startsWith("Vertex v1 already defined"));
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testInputAndInputVertexNameCollision() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v2.addInput("v1", new InputDescriptor(), null);
    
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testOutputAndOutputVertexNameCollision() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addOutput("v2", new OutputDescriptor());
    
    Edge e1 = new Edge(v1, v2,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testOutputAndVertexNameCollision() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addOutput("v2", new OutputDescriptor());
    
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testInputAndVertexNameCollision() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addInput("v2", new InputDescriptor(), null);
    
    DAG dag = new DAG("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.verify();
  }

  //  v1  v2
  //   |  |
  //    v3
  @Test(timeout = 5000)
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
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")));
    Edge e2 = new Edge(v2, v3,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
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
  
  @Test(timeout = 5000)
  public void testVertexGroupWithMultipleOutputEdges() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    
    DAG dag = new DAG("testDag");
    VertexGroup uv12 = dag.createVertexGroup("uv12", v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addOutput("uvOut", outDesc, null);
    
    GroupInputEdge e1 = new GroupInputEdge(uv12, v3,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));
    
    GroupInputEdge e2 = new GroupInputEdge(uv12, v4,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
    
    Assert.assertEquals(2, v1.getOutputVertices().size());
    Assert.assertEquals(2, v2.getOutputVertices().size());
    Assert.assertTrue(v1.getOutputVertices().contains(v3));
    Assert.assertTrue(v1.getOutputVertices().contains(v4));
    Assert.assertTrue(v2.getOutputVertices().contains(v3));
    Assert.assertTrue(v2.getOutputVertices().contains(v4));
  }
  
  @Test(timeout = 5000)
  public void testVertexGroup() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v5 = new Vertex("v5",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    
    DAG dag = new DAG("testDag");
    String groupName1 = "uv12";
    VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addOutput("uvOut", outDesc, null);
    
    String groupName2 = "uv23";
    VertexGroup uv23 = dag.createVertexGroup(groupName2, v2, v3);
    
    GroupInputEdge e1 = new GroupInputEdge(uv12, v4,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));
    GroupInputEdge e2 = new GroupInputEdge(uv23, v5,
        new EdgeProperty(DataMovementType.SCATTER_GATHER, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));
    
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addVertex(v5);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();

    // for the first Group v1 and v2 should get connected to v4 and also have 1 output
    // for the second Group v2 and v3 should get connected to v5
    // the Group place holders should disappear
    Assert.assertNull(dag.getVertex(uv12.getGroupName()));
    Assert.assertNull(dag.getVertex(uv23.getGroupName()));
    Assert.assertFalse(dag.edges.contains(e1));
    Assert.assertFalse(dag.edges.contains(e2));
    Assert.assertEquals(1, v1.getOutputs().size());
    Assert.assertEquals(1, v2.getOutputs().size());
    Assert.assertEquals(outDesc, v1.getOutputs().get(0).getDescriptor());
    Assert.assertEquals(outDesc, v2.getOutputs().get(0).getDescriptor());
    Assert.assertEquals(1, v1.getOutputVertices().size());
    Assert.assertEquals(1, v3.getOutputVertices().size());
    Assert.assertEquals(2, v2.getOutputVertices().size());
    Assert.assertTrue(v1.getOutputVertices().contains(v4));
    Assert.assertTrue(v3.getOutputVertices().contains(v5));
    Assert.assertTrue(v2.getOutputVertices().contains(v4));
    Assert.assertTrue(v2.getOutputVertices().contains(v5));
    Assert.assertEquals(2, v4.getInputVertices().size());
    Assert.assertTrue(v4.getInputVertices().contains(v1));
    Assert.assertTrue(v4.getInputVertices().contains(v2));
    Assert.assertEquals(2, v5.getInputVertices().size());
    Assert.assertTrue(v5.getInputVertices().contains(v2));
    Assert.assertTrue(v5.getInputVertices().contains(v3));
    Assert.assertEquals(1, v4.getGroupInputs().size());
    Assert.assertTrue(v4.getGroupInputs().containsKey(groupName1));
    Assert.assertEquals(1, v5.getGroupInputs().size());
    Assert.assertTrue(v5.getGroupInputs().containsKey(groupName2));
    Assert.assertEquals(2, dag.vertexGroups.size());
  }
  
  @Test(timeout = 5000)
  public void testVertexGroupOneToOne() {
    Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = new Vertex("v2",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = new Vertex("v3",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = new Vertex("v4",
        new ProcessorDescriptor("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v5 = new Vertex("v5",
        new ProcessorDescriptor("Processor"),
        -1, dummyTaskResource);
    
    DAG dag = new DAG("testDag");
    String groupName1 = "uv12";
    VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addOutput("uvOut", outDesc, null);
    
    String groupName2 = "uv23";
    VertexGroup uv23 = dag.createVertexGroup(groupName2, v2, v3);
    
    GroupInputEdge e1 = new GroupInputEdge(uv12, v4,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));
    GroupInputEdge e2 = new GroupInputEdge(uv23, v5,
        new EdgeProperty(DataMovementType.ONE_TO_ONE, 
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            new OutputDescriptor("dummy output class"),
            new InputDescriptor("dummy input class")),
            new InputDescriptor("dummy input class"));
    
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addVertex(v5);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
    Assert.assertEquals(dummyTaskCount, v5.getParallelism());
  }

  //   v1
  //  |  |
  //  v2  v3
  @Test(timeout = 5000)
  public void BinaryOutput() {
    IllegalStateException ex = null;
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
          new EdgeProperty(DataMovementType.SCATTER_GATHER, 
              DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
              new OutputDescriptor("dummy output class"),
              new InputDescriptor("dummy input class")));
      Edge e2 = new Edge(v1, v2,
          new EdgeProperty(DataMovementType.SCATTER_GATHER, 
              DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, 
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
    Assert.assertNull(ex);
  }

  @Test(timeout = 5000)
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
  @Test(timeout = 5000)
  public void testInvalidVertexConstruction() {
    {
      Vertex v1 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        0, dummyTaskResource);
      Vertex v2 = new Vertex("v1",
        new ProcessorDescriptor("MapProcessor"),
        -1, dummyTaskResource);
    }
    try {
      Vertex v1 = new Vertex("v1",
          new ProcessorDescriptor("MapProcessor"),
          -2, dummyTaskResource);
      Assert.fail("Expected exception for 0 parallelism");
    } catch (IllegalArgumentException e) {
      Assert
          .assertTrue(e
              .getMessage()
              .startsWith(
                  "Parallelism should be -1 if determined by the AM, otherwise should be >= 0"));
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

  @Test(timeout = 5000)
  public void testMultipleRootInputsAllowed() {
    DAG dag = new DAG("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1")
        .setUserPayload("processor1Bytes".getBytes());
    Vertex v1 = new Vertex("v1", pd1, 10, Resource.newInstance(1024, 1));
    VertexManagerPluginDescriptor vertexManagerPluginDescriptor = new VertexManagerPluginDescriptor(
        "TestVertexManager");
    v1.setVertexManagerPlugin(vertexManagerPluginDescriptor);

    InputDescriptor inputDescriptor1 = new InputDescriptor("input1").setUserPayload("inputBytes"
        .getBytes());
    InputDescriptor inputDescriptor2 = new InputDescriptor("input2").setUserPayload("inputBytes"
        .getBytes());
    v1.addInput("input1", inputDescriptor1, null);
    v1.addInput("input2", inputDescriptor2, null);

    dag.addVertex(v1);

    dag.createDag(new TezConfiguration());
  }

}

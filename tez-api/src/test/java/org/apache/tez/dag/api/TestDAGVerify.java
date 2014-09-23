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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.common.security.DAGAccessControls;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(timeout = 5000)
  public void testVerifyCustomEdge() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(EdgeManagerPluginDescriptor.create("emClass"),
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(timeout = 5000)
  public void testVerifyOneToOne() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(timeout = 5000)
  // v1 (known) -> v2 (-1) -> v3 (-1)
  public void testVerifyOneToOneInferParallelism() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
    Assert.assertEquals(-1, v2.getParallelism());
  }
  
  @Test(timeout = 5000)
  // v1 (-1) -> v2 (known) -> v3 (-1)
  public void testVerifyOneToOneIncorrectParallelism1() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("MapProcessor"),
        -1, dummyTaskResource);
    Edge e1 = Edge.create(v1, v3,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create(dummyProcessorClassName),
        -1, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create(dummyProcessorClassName),
        -1, dummyTaskResource);
    Edge e1 = Edge.create(v1, v4,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    Edge e2 = Edge.create(v2, v4,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    Edge e3 = Edge.create(v3, v4,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.BROADCAST,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(expected = IllegalStateException.class, timeout = 5000)  
  public void testVerify3() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.EPHEMERAL, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }

  @Test(expected = IllegalStateException.class, timeout = 5000)  
  public void testVerify4() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.EPHEMERAL, SchedulingType.CONCURRENT,
            OutputDescriptor.create(dummyOutputClassName),
            InputDescriptor.create(dummyInputClassName)));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e3 = Edge.create(v2, v4,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e4 = Edge.create(v4, v1,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e3 = Edge.create(v2, v4,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e4 = Edge.create(v3, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v1repeat = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    try {
      DAG dag = DAG.create("testDag");
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v2.addDataSource("v1", DataSourceDescriptor.create(null, null, null));
    
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testOutputAndOutputVertexNameCollision() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addDataSink("v2", DataSinkDescriptor.create(null, null, null));
    
    Edge e1 = Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(e1);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testOutputAndVertexNameCollision() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addDataSink("v2", DataSinkDescriptor.create(null, null, null));
    
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.verify();
  }
  
  @Test(expected = IllegalStateException.class, timeout = 5000)
  public void testInputAndVertexNameCollision() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    
    v1.addDataSource("v2", DataSourceDescriptor.create(null, null, null));
    
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.verify();
  }

  //  v1  v2
  //   |  |
  //    v3
  @Test(timeout = 5000)
  public void BinaryInputAllowed() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("MapProcessor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("ReduceProcessor"),
        dummyTaskCount, dummyTaskResource);
    Edge e1 = Edge.create(v1, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    Edge e2 = Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")));
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
  }
  
  @Test(timeout = 5000)
  public void testVertexGroupWithMultipleOutputEdges() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    
    DAG dag = DAG.create("testDag");
    VertexGroup uv12 = dag.createVertexGroup("uv12", v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, null, null));
    
    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));
    
    GroupInputEdge e2 = GroupInputEdge.create(uv12, v4,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addEdge(e1);
    dag.addEdge(e2);
    dag.verify();
    for (int i = 0; i< 10;++i){
      dag.verify();  // should be OK when called multiple times
    }
    
    Assert.assertEquals(2, v1.getOutputVertices().size());
    Assert.assertEquals(2, v2.getOutputVertices().size());
    Assert.assertTrue(v1.getOutputVertices().contains(v3));
    Assert.assertTrue(v1.getOutputVertices().contains(v4));
    Assert.assertTrue(v2.getOutputVertices().contains(v3));
    Assert.assertTrue(v2.getOutputVertices().contains(v4));
  }
  
  @Test(timeout = 5000)
  public void testVertexGroup() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v5 = Vertex.create("v5",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    
    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, null, null));
    
    String groupName2 = "uv23";
    VertexGroup uv23 = dag.createVertexGroup(groupName2, v2, v3);
    
    GroupInputEdge e1 = GroupInputEdge.create(uv12, v4,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));
    GroupInputEdge e2 = GroupInputEdge.create(uv23, v5,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));
    
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addVertex(v5);
    dag.addEdge(e1);
    dag.addEdge(e2);
    for (int i = 0; i< 10;++i){
      dag.verify(); // should be OK when called multiple times
    }
    
    // for the first Group v1 and v2 should get connected to v4 and also have 1 output
    // for the second Group v2 and v3 should get connected to v5
    // the Group place holders should disappear
    Assert.assertNull(dag.getVertex(uv12.getGroupName()));
    Assert.assertNull(dag.getVertex(uv23.getGroupName()));
    Assert.assertFalse(dag.edges.contains(e1));
    Assert.assertFalse(dag.edges.contains(e2));
    Assert.assertEquals(1, v1.getOutputs().size());
    Assert.assertEquals(1, v2.getOutputs().size());
    Assert.assertEquals(outDesc, v1.getOutputs().get(0).getIODescriptor());
    Assert.assertEquals(outDesc, v2.getOutputs().get(0).getIODescriptor());
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
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v4 = Vertex.create("v4",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v5 = Vertex.create("v5",
        ProcessorDescriptor.create("Processor"),
        -1, dummyTaskResource);
    
    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);
    OutputDescriptor outDesc = new OutputDescriptor();
    uv12.addDataSink("uvOut", DataSinkDescriptor.create(outDesc, null, null));
    
    String groupName2 = "uv23";
    VertexGroup uv23 = dag.createVertexGroup(groupName2, v2, v3);
    
    GroupInputEdge e1 = GroupInputEdge.create(uv12, v4,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));
    GroupInputEdge e2 = GroupInputEdge.create(uv23, v5,
        EdgeProperty.create(DataMovementType.ONE_TO_ONE,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));
    
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addVertex(v4);
    dag.addVertex(v5);
    dag.addEdge(e1);
    dag.addEdge(e2);
    for (int i = 0; i< 10;++i){
      dag.verify();  // should be OK when called multiple times
    }
    
    Assert.assertEquals(dummyTaskCount, v5.getParallelism());
  }

  //   v1
  //  |  |
  //  v2  v3
  @Test(timeout = 5000)
  public void BinaryOutput() {
    IllegalStateException ex = null;
    try {
      Vertex v1 = Vertex.create("v1",
          ProcessorDescriptor.create("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v2 = Vertex.create("v2",
          ProcessorDescriptor.create("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Vertex v3 = Vertex.create("v3",
          ProcessorDescriptor.create("MapProcessor"),
          dummyTaskCount, dummyTaskResource);
      Edge e1 = Edge.create(v1, v2,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
              OutputDescriptor.create("dummy output class"),
              InputDescriptor.create("dummy input class")));
      Edge e2 = Edge.create(v1, v2,
          EdgeProperty.create(DataMovementType.SCATTER_GATHER,
              DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
              OutputDescriptor.create("dummy output class"),
              InputDescriptor.create("dummy input class")));
      DAG dag = DAG.create("testDag");
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
      DAG dag = DAG.create("testDag");
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
      Vertex v1 = Vertex.create("v1",
          ProcessorDescriptor.create("MapProcessor"),
          0, dummyTaskResource);
      Vertex v2 = Vertex.create("v1",
          ProcessorDescriptor.create("MapProcessor"),
          -1, dummyTaskResource);
    }
    try {
      Vertex v1 = Vertex.create("v1",
          ProcessorDescriptor.create("MapProcessor"),
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
      Vertex v1 = Vertex.create("v1",
          ProcessorDescriptor.create("MapProcessor"),
          1, null);
      Assert.fail("Expected exception for 0 parallelism");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Resource cannot be null"));
    }
  }

  @Test(timeout = 5000)
  public void testMultipleRootInputsAllowed() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    VertexManagerPluginDescriptor vertexManagerPluginDescriptor =
        VertexManagerPluginDescriptor.create(
            "TestVertexManager");
    v1.setVertexManagerPlugin(vertexManagerPluginDescriptor);

    InputDescriptor inputDescriptor1 = InputDescriptor.create("input1")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    InputDescriptor inputDescriptor2 = InputDescriptor.create("input2")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    v1.addDataSource("input1", DataSourceDescriptor.create(inputDescriptor1, null, null));
    v1.addDataSource("input2", DataSourceDescriptor.create(inputDescriptor2, null, null));

    dag.addVertex(v1);

    dag.createDag(new TezConfiguration(), null, null, null, true);
  }
  
  
  @Test(timeout = 5000)
  public void testDAGCreateDataInference() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create(dummyProcessorClassName));
    Map<String, LocalResource> lrs1 = Maps.newHashMap();
    String lrName1 = "LR1";
    lrs1.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    Map<String, LocalResource> lrs2 = Maps.newHashMap();
    String lrName2 = "LR2";
    lrs2.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test1"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    
    Set<String> hosts = Sets.newHashSet();
    hosts.add("h1");
    hosts.add("h2");
    List<TaskLocationHint> taskLocationHints = Lists.newLinkedList();
    taskLocationHints.add(TaskLocationHint.createTaskLocationHint(hosts, null));
    taskLocationHints.add(TaskLocationHint.createTaskLocationHint(hosts, null));
    VertexLocationHint vLoc = VertexLocationHint.create(taskLocationHints);
    DataSourceDescriptor ds = DataSourceDescriptor.create(InputDescriptor.create("I.class"), 
        null, dummyTaskCount, null, vLoc, lrs2);
    v1.addDataSource("i1", ds);
        
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addTaskLocalFiles(lrs1);
    DAGPlan dagPlan = dag.createDag(new TezConfiguration(), null, null, null, true);
    Assert.assertEquals(lrName1, dagPlan.getLocalResource(0).getName());
    VertexPlan vPlan = dagPlan.getVertex(0);
    PlanTaskConfiguration taskPlan = vPlan.getTaskConfig();
    Assert.assertEquals(dummyTaskCount, taskPlan.getNumTasks());
    Assert.assertEquals(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT, taskPlan.getMemoryMb());
    Assert.assertEquals(lrName2, taskPlan.getLocalResource(0).getName());
    Assert.assertEquals(dummyTaskCount, vPlan.getTaskLocationHintCount());
  }

  @Test(timeout = 5000)
  public void testInferredFilesFail() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create(dummyProcessorClassName),
        dummyTaskCount, dummyTaskResource);
    Map<String, LocalResource> lrs = Maps.newHashMap();
    String lrName1 = "LR1";
    lrs.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    v1.addTaskLocalFiles(lrs);
    try {
      v1.addTaskLocalFiles(lrs);
      Assert.fail();
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Attempting to add duplicate resource"));
    }

    DataSourceDescriptor ds = DataSourceDescriptor.create(InputDescriptor.create("I.class"), 
        null, -1, null, null, lrs);
    v1.addDataSource("i1", ds);
    
    DAG dag = DAG.create("testDag");
    dag.addVertex(v1);
    dag.addTaskLocalFiles(lrs);
    try {
      dag.addTaskLocalFiles(lrs);
      Assert.fail();
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Attempting to add duplicate resource"));
    }
    try {
      // data source will add duplicate common files to vertex
      dag.createDag(new TezConfiguration(), null, null, null, true);
      Assert.fail();
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Attempting to add duplicate resource"));
    }
  }
  
  @Test(timeout = 5000)
  public void testDAGAccessControls() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    dag.addVertex(v1);

    DAGAccessControls dagAccessControls = new DAGAccessControls();
    dagAccessControls.setUsersWithViewACLs(Arrays.asList("u1"))
        .setUsersWithModifyACLs(Arrays.asList("*"))
        .setGroupsWithViewACLs(Arrays.asList("g1"))
        .setGroupsWithModifyACLs(Arrays.asList("g2"));
    dag.setAccessControls(dagAccessControls);

    Configuration conf = new Configuration(false);
    DAGPlan dagPlan = dag.createDag(conf, null, null, null, true);
    Assert.assertNull(conf.get(TezConstants.TEZ_DAG_VIEW_ACLS));
    Assert.assertNull(conf.get(TezConstants.TEZ_DAG_MODIFY_ACLS));

    ConfigurationProto confProto = dagPlan.getDagKeyValues();
    boolean foundViewAcls = false;
    boolean foundModifyAcls = false;

    for (PlanKeyValuePair pair : confProto.getConfKeyValuesList()) {
      if (pair.getKey().equals(TezConstants.TEZ_DAG_VIEW_ACLS)) {
        foundViewAcls = true;
        Assert.assertEquals("u1 g1", pair.getValue());
      } else if (pair.getKey().equals(TezConstants.TEZ_DAG_MODIFY_ACLS)) {
        foundModifyAcls = true;
        Assert.assertEquals("*", pair.getValue());
      }
    }
    Assert.assertTrue(foundViewAcls);
    Assert.assertTrue(foundModifyAcls);
  }

}

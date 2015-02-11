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

public class TestDAG {

  private final int dummyTaskCount = 2;
  private final Resource dummyTaskResource = Resource.newInstance(1, 1);

  @Test(timeout = 5000)
  public void testDuplicatedVertices() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    DAG dag = DAG.create("testDAG");
    dag.addVertex(v1);
    try {
      dag.addVertex(v2);
      Assert.fail("should fail it due to duplicated vertices");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("Vertex v1 already defined!", e.getMessage());
    }
  }

  @Test(timeout = 5000)
  public void testDuplicatedEdges() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    Edge edge1 = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.CONCURRENT, OutputDescriptor.create("output"),
        InputDescriptor.create("input")));
    Edge edge2 = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.CONCURRENT, OutputDescriptor.create("output"),
        InputDescriptor.create("input")));

    DAG dag = DAG.create("testDAG");
    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addEdge(edge1);
    try {
      dag.addEdge(edge2);
      Assert.fail("should fail it due to duplicated edges");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage().contains("already defined!"));
    }
  }

  @Test(timeout = 5000)
  public void testDuplicatedVertexGroup() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("testDAG");
    dag.createVertexGroup("group_1", v1,v2);
    try {
      dag.createVertexGroup("group_1", v1,v2);
      Assert.fail("should fail it due to duplicated VertexGroups");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("VertexGroup group_1 already defined!", e.getMessage());
    }
  }

  @Test(timeout = 5000)
  public void testDuplicatedGroupInputEdge() {
    Vertex v1 = Vertex.create("v1",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3",
        ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("testDag");
    String groupName1 = "uv12";
    VertexGroup uv12 = dag.createVertexGroup(groupName1, v1, v2);

    GroupInputEdge e1 = GroupInputEdge.create(uv12, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));

    GroupInputEdge e2 = GroupInputEdge.create(uv12, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
            OutputDescriptor.create("dummy output class"),
            InputDescriptor.create("dummy input class")),
        InputDescriptor.create("dummy input class"));

    dag.addVertex(v1);
    dag.addVertex(v2);
    dag.addVertex(v3);
    dag.addEdge(e1);
    try {
      dag.addEdge(e2);
      Assert.fail("should fail it due to duplicated GroupInputEdge");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e.getMessage().contains("already defined"));
    }
  }

  @Test(timeout = 5000)
  public void testDAGConf() {
    DAG dag = DAG.create("dag1");
    // it's OK to set custom configuration
    dag.setConf("unknown_conf", "value");

    // set invalid AM level configuration
    try {
      dag.setConf(TezConfiguration.TEZ_AM_SESSION_MODE, true+"");
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals("tez.am.mode.session is set at the scope of DAG,"
          + " but it is only valid in the scope of AM",
          e.getMessage());
    }
    // set valid DAG level configuration
    dag.setConf(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false + "");
    // set valid Vertex level configuration
    dag.setConf(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 3 + "");
  }

  @Test(timeout = 5000)
  public void testVertexConf() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("dummyProcessor"));
    // it's OK to set custom property
    v1.setConf("unknown_conf", "value");

    // set invalid AM level configuration
    try {
      v1.setConf(TezConfiguration.TEZ_AM_SESSION_MODE, true+"");
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals("tez.am.mode.session is set at the scope of VERTEX,"
          + " but it is only valid in the scope of AM",
          e.getMessage());
    }

    // set invalid DAG level configuration
    try {
      v1.setConf(TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS, false + "");
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals("tez.am.commit-all-outputs-on-dag-success is set at the scope of VERTEX,"
          + " but it is only valid in the scope of DAG",
          e.getMessage());
    }
    // set valid Vertex level configuration
    v1.setConf(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 3 + "");
  }
}

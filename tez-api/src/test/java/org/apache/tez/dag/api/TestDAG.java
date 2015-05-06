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
import org.junit.Assert;
import org.junit.Test;

public class TestDAG {

  private final int dummyTaskCount = 2;
  private final Resource dummyTaskResource = Resource.newInstance(1, 1);

  @Test(timeout = 5000)
  public void testDuplicatedVertexGroup() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v2 = Vertex.create("v2", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);
    Vertex v3 = Vertex.create("v3", ProcessorDescriptor.create("Processor"),
        dummyTaskCount, dummyTaskResource);

    DAG dag = DAG.create("testDAG");
    dag.createVertexGroup("group_1", v1,v2);

    try {
      dag.createVertexGroup("group_1", v2, v3);
      Assert.fail("should fail it due to duplicated VertexGroups");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals("VertexGroup group_1 already defined!", e.getMessage());
    }
    // it is possible to create vertex group with same member but different group name 
    dag.createVertexGroup("group_2", v1, v2);

  }

  @Test(timeout = 5000)
  public void testDuplicatedInput() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("dummyProcessor"));
    DataSourceDescriptor dataSource =
        DataSourceDescriptor.create(InputDescriptor.create("dummyInput"), null, null);
    try {
      v1.addDataSource(null, dataSource);
      Assert.fail("Should fail due to invalid inputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("InputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSource("", dataSource);
      Assert.fail("Should fail due to invalid inputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("InputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSource(" ", dataSource);
      Assert.fail("Should fail due to invalid inputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("InputName should not be null, empty or white space only,"));
    }

    v1.addDataSource("input_1", dataSource);
    try {
      v1.addDataSource("input_1",
          DataSourceDescriptor.create(InputDescriptor.create("dummyInput"), null, null));
      Assert.fail("Should fail due to duplicated input");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Duplicated input:input_1, vertexName=v1", e.getMessage());
    }
  }

  @Test(timeout = 5000)
  public void testDuplicatedOutput_1() {
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("dummyProcessor"));
    DataSinkDescriptor dataSink =
        DataSinkDescriptor.create(OutputDescriptor.create("dummyOutput"), null, null);
    try {
      v1.addDataSink(null, dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSink("", dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSink(" ", dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }

    v1.addDataSink("output_1",
        DataSinkDescriptor.create(OutputDescriptor.create("dummyOutput"), null, null));
    try {
      v1.addDataSink("output_1",
          DataSinkDescriptor.create(OutputDescriptor.create("dummyOutput"), null, null));
      Assert.fail("Should fail due to duplicated output");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Duplicated output:output_1, vertexName=v1", e.getMessage());
    }
  }

  @Test(timeout = 5000)
  public void testDuplicatedOutput_2() {
    DAG dag = DAG.create("dag1");
    Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create("dummyProcessor"));
    DataSinkDescriptor dataSink =
        DataSinkDescriptor.create(OutputDescriptor.create("dummyOutput"), null, null);
    try {
      v1.addDataSink(null, dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSink("", dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }
    try {
      v1.addDataSink(" ", dataSink);
      Assert.fail("Should fail due to invalid outputName");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage()
          .contains("OutputName should not be null, empty or white space only,"));
    }

    v1.addDataSink("output_1", dataSink);
    Vertex v2 = Vertex.create("v1", ProcessorDescriptor.create("dummyProcessor"));
    VertexGroup vGroup = dag.createVertexGroup("group_1", v1,v2);
    try {
      vGroup.addDataSink("output_1",
          DataSinkDescriptor.create(OutputDescriptor.create("dummyOutput"), null, null));
      Assert.fail("Should fail due to duplicated output");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Duplicated output:output_1, vertexName=v1", e.getMessage());
    }
  }
}

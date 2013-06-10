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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// based on TestDAGLocationHint
public class TestDAGPlan {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(); //TODO: doesn't seem to be deleting this folder automatically as expected.

  @Test
  public void testBasicJobPlanSerde() throws IOException {

    DAGPlan job = DAGPlan.newBuilder()
       .setName("test")
       .addVertex(
           VertexPlan.newBuilder()
             .setName("vertex1")
             .setType(PlanVertexType.NORMAL)
             .addTaskLocationHint(PlanTaskLocationHint.newBuilder().addHost("machineName").addRack("rack1").build())
             .setTaskConfig(
                 PlanTaskConfiguration.newBuilder()
                   .setNumTasks(2)
                   .setVirtualCores(4)
                   .setMemoryMb(1024)
                   .setJavaOpts("")
                   .setTaskModule("x.y")
                   .build())
             .build())
        .build();
   File file = tempFolder.newFile("jobPlan");
   FileOutputStream outStream = null;
   try {
     outStream = new FileOutputStream(file);
     job.writeTo(outStream); 
   }
   finally {
     if(outStream != null){
       outStream.close();  
     }
   }

   DAGPlan inJob;
   FileInputStream inputStream;
   try {
     inputStream = new FileInputStream(file);
     inJob = DAGPlan.newBuilder().mergeFrom(inputStream).build();
   }
   finally {
     outStream.close();  
   }

   Assert.assertEquals(job, inJob);
  }
  
  @Test
  public void testUserPayloadSerde() {
    DAG dag = new DAG().setName("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1",
        ByteBuffer.wrap("processor1Bytes".getBytes()));
    ProcessorDescriptor pd2 = new ProcessorDescriptor("processor2",
        ByteBuffer.wrap("processor2Bytes".getBytes()));
    Vertex v1 = new Vertex("v1", pd1, 10);
    Vertex v2 = new Vertex("v2", pd2, 1);
    v1.setJavaOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalResources(new HashMap<String, LocalResource>())
        .setTaskResource(Resource.newInstance(1024, 1));
    v2.setJavaOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalResources(new HashMap<String, LocalResource>())
        .setTaskResource(Resource.newInstance(1024, 1));

    InputDescriptor inputDescriptor = new InputDescriptor("input",
        ByteBuffer.wrap("inputBytes".getBytes()));
    OutputDescriptor outputDescriptor = new OutputDescriptor("output",
        ByteBuffer.wrap("outputBytes".getBytes()));
    Edge edge = new Edge(v1, v2, new EdgeProperty(ConnectionPattern.BIPARTITE,
        SourceType.STABLE, inputDescriptor, outputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag();

    assertEquals(2, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    EdgePlan edgeProto = dagProto.getEdge(0);

    assertEquals("processor1Bytes", new String(v1Proto.getProcessorDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("processor1", v1Proto.getProcessorDescriptor().getClassName());

    assertEquals("processor2Bytes", new String(v2Proto.getProcessorDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v2Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getInputDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getInputDescriptor().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getOutputDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getOutputDescriptor().getClassName());

    Map<String, EdgeProperty> edgePropertyMap = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList());
    assertEquals(1, edgePropertyMap.size());
    EdgeProperty edgeProperty = edgePropertyMap.values().iterator().next();

    byte[] ib = new byte[edgeProperty.getInputDescriptor().getUserPayload()
        .capacity()];
    edgeProperty.getInputDescriptor().getUserPayload().get(ib);
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getInputDescriptor().getClassName());

    byte[] ob = new byte[edgeProperty.getOutputDescriptor().getUserPayload()
        .capacity()];
    edgeProperty.getOutputDescriptor().getUserPayload().get(ob);
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getOutputDescriptor().getClassName());
  }
}

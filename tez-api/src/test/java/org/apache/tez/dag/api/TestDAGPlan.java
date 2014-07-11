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
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
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

  @Test(timeout = 5000)
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

  @Test(timeout = 5000)
  public void testEdgeManagerSerde() {
    DAG dag = new DAG("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1")
        .setUserPayload("processor1Bytes".getBytes());
    ProcessorDescriptor pd2 = new ProcessorDescriptor("processor2")
        .setUserPayload("processor2Bytes".getBytes());
    Vertex v1 = new Vertex("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = new Vertex("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = new InputDescriptor("input").setUserPayload("inputBytes"
        .getBytes());
    OutputDescriptor outputDescriptor = new OutputDescriptor("output").setUserPayload("outputBytes"
        .getBytes());
    Edge edge = new Edge(v1, v2, new EdgeProperty(
        new EdgeManagerDescriptor("emClass").setUserPayload("emPayload".getBytes()),
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration());

    EdgeProperty edgeProperty = DagTypeConverters.createEdgePropertyMapFromDAGPlan(dagProto
        .getEdgeList().get(0));

    EdgeManagerDescriptor emDesc = edgeProperty.getEdgeManagerDescriptor();
    Assert.assertNotNull(emDesc);
    Assert.assertEquals("emClass", emDesc.getClassName());
    Assert.assertTrue(Arrays.equals("emPayload".getBytes(), emDesc.getUserPayload()));
  }

  @Test(timeout = 5000)
  public void testUserPayloadSerde() {
    DAG dag = new DAG("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1").
        setUserPayload("processor1Bytes".getBytes());
    ProcessorDescriptor pd2 = new ProcessorDescriptor("processor2").
        setUserPayload("processor2Bytes".getBytes());
    Vertex v1 = new Vertex("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = new Vertex("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = new InputDescriptor("input").
        setUserPayload("inputBytes".getBytes());
    OutputDescriptor outputDescriptor = new OutputDescriptor("output").
        setUserPayload("outputBytes".getBytes());
    Edge edge = new Edge(v1, v2, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration());

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

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test(timeout = 5000)
  public void userVertexOrderingIsMaintained() {
    DAG dag = new DAG("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1").
        setUserPayload("processor1Bytes".getBytes());
    ProcessorDescriptor pd2 = new ProcessorDescriptor("processor2").
        setUserPayload("processor2Bytes".getBytes());
    ProcessorDescriptor pd3 = new ProcessorDescriptor("processor3").
        setUserPayload("processor3Bytes".getBytes());
    Vertex v1 = new Vertex("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = new Vertex("v2", pd2, 1, Resource.newInstance(1024, 1));
    Vertex v3 = new Vertex("v3", pd3, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());
    v3.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = new InputDescriptor("input").
        setUserPayload("inputBytes".getBytes());
    OutputDescriptor outputDescriptor = new OutputDescriptor("output").
        setUserPayload("outputBytes".getBytes());
    Edge edge = new Edge(v1, v2, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge).addVertex(v3);

    DAGPlan dagProto = dag.createDag(new TezConfiguration());

    assertEquals(3, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    VertexPlan v3Proto = dagProto.getVertex(2);
    EdgePlan edgeProto = dagProto.getEdge(0);

    assertEquals("processor1Bytes", new String(v1Proto.getProcessorDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("processor1", v1Proto.getProcessorDescriptor().getClassName());

    assertEquals("processor2Bytes", new String(v2Proto.getProcessorDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v2Proto.getProcessorDescriptor().getClassName());

    assertEquals("processor3Bytes", new String(v3Proto.getProcessorDescriptor()
        .getUserPayload().toByteArray()));
    assertEquals("processor3", v3Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test (timeout=5000)
  public void testCredentialsSerde() {
    DAG dag = new DAG("testDag");
    ProcessorDescriptor pd1 = new ProcessorDescriptor("processor1").
        setUserPayload("processor1Bytes".getBytes());
    ProcessorDescriptor pd2 = new ProcessorDescriptor("processor2").
        setUserPayload("processor2Bytes".getBytes());
    Vertex v1 = new Vertex("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = new Vertex("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .setTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = new InputDescriptor("input").
        setUserPayload("inputBytes".getBytes());
    OutputDescriptor outputDescriptor = new OutputDescriptor("output").
        setUserPayload("outputBytes".getBytes());
    Edge edge = new Edge(v1, v2, new EdgeProperty(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    Credentials dagCredentials = new Credentials();
    Token<TokenIdentifier> token1 = new Token<TokenIdentifier>();
    Token<TokenIdentifier> token2 = new Token<TokenIdentifier>();
    dagCredentials.addToken(new Text("Token1"), token1);
    dagCredentials.addToken(new Text("Token2"), token2);
    
    dag.setCredentials(dagCredentials);

    DAGPlan dagProto = dag.createDag(new TezConfiguration());

    assertTrue(dagProto.hasCredentialsBinary());
    
    Credentials fetchedCredentials = DagTypeConverters.convertByteStringToCredentials(dagProto
        .getCredentialsBinary());
    
    assertEquals(2, fetchedCredentials.numberOfTokens());
    assertNotNull(fetchedCredentials.getToken(new Text("Token1")));
    assertNotNull(fetchedCredentials.getToken(new Text("Token2")));
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api;

import java.nio.ByteBuffer;

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
import org.apache.tez.common.JavaOptsChecker;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.EdgePlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskConfiguration;
import org.apache.tez.dag.api.records.DAGProtos.PlanTaskLocationHint;
import org.apache.tez.dag.api.records.DAGProtos.PlanVertexType;
import org.apache.tez.dag.api.records.DAGProtos.VertexExecutionContextProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
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
    } finally {
      if (outStream != null) {
        outStream.close();
      }
    }

    DAGPlan inJob;
    FileInputStream inputStream;
    try {
      inputStream = new FileInputStream(file);
      inJob = DAGPlan.newBuilder().mergeFrom(inputStream).build();
    } finally {
      outStream.close();
    }

    Assert.assertEquals(job, inJob);
  }

  @Test(timeout = 5000)
  public void testEdgeManagerSerde() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    ProcessorDescriptor pd2 = ProcessorDescriptor.create("processor2")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("processor2Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = Vertex.create("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = InputDescriptor.create("input")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        EdgeManagerPluginDescriptor.create("emClass").setUserPayload(
            UserPayload.create(ByteBuffer.wrap("emPayload".getBytes()))),
        DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    EdgeProperty edgeProperty = DagTypeConverters.createEdgePropertyMapFromDAGPlan(dagProto
        .getEdgeList().get(0));

    EdgeManagerPluginDescriptor emDesc = edgeProperty.getEdgeManagerDescriptor();
    Assert.assertNotNull(emDesc);
    Assert.assertEquals("emClass", emDesc.getClassName());
    Assert.assertTrue(
        Arrays.equals("emPayload".getBytes(), emDesc.getUserPayload().deepCopyAsArray()));
  }

  @Test(timeout = 5000)
  public void testUserPayloadSerde() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    ProcessorDescriptor pd2 = ProcessorDescriptor.create("processor2").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor2Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = Vertex.create("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertEquals(2, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    EdgePlan edgeProto = dagProto.getEdge(0);

    assertEquals("processor1Bytes", new String(v1Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor1", v1Proto.getProcessorDescriptor().getClassName());

    assertEquals("processor2Bytes", new String(v2Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v2Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload().deepCopyAsArray();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload().deepCopyAsArray();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test(timeout = 5000)
  public void userVertexOrderingIsMaintained() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    ProcessorDescriptor pd2 = ProcessorDescriptor.create("processor2").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor2Bytes".getBytes())));
    ProcessorDescriptor pd3 = ProcessorDescriptor.create("processor3").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor3Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = Vertex.create("v2", pd2, 1, Resource.newInstance(1024, 1));
    Vertex v3 = Vertex.create("v3", pd3, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v3.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge).addVertex(v3);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertEquals(3, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    VertexPlan v1Proto = dagProto.getVertex(0);
    VertexPlan v2Proto = dagProto.getVertex(1);
    VertexPlan v3Proto = dagProto.getVertex(2);
    EdgePlan edgeProto = dagProto.getEdge(0);

    // either v1 or v2 will be on top based on topological order 
    String v1ProtoPayload = new String(v1Proto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    String v2ProtoPayload = new String(v2Proto.getProcessorDescriptor().getTezUserPayload().getUserPayload().toByteArray());
    assertTrue(v1ProtoPayload.equals("processor1Bytes") || v1ProtoPayload.equals("processor3Bytes"));
    assertTrue(v2ProtoPayload.equals("processor1Bytes") || v2ProtoPayload.equals("processor3Bytes"));
    assertTrue(v1Proto.getProcessorDescriptor().getClassName().equals("processor1") ||
        v1Proto.getProcessorDescriptor().getClassName().equals("processor3"));
    assertTrue(v2Proto.getProcessorDescriptor().getClassName().equals("processor1") ||
        v2Proto.getProcessorDescriptor().getClassName().equals("processor3"));

    assertEquals("processor2Bytes", new String(v3Proto.getProcessorDescriptor().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("processor2", v3Proto.getProcessorDescriptor().getClassName());

    assertEquals("inputBytes", new String(edgeProto.getEdgeDestination().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("input", edgeProto.getEdgeDestination().getClassName());

    assertEquals("outputBytes", new String(edgeProto.getEdgeSource().getTezUserPayload()
        .getUserPayload().toByteArray()));
    assertEquals("output", edgeProto.getEdgeSource().getClassName());

    EdgeProperty edgeProperty = DagTypeConverters
        .createEdgePropertyMapFromDAGPlan(dagProto.getEdgeList().get(0));

    byte[] ib = edgeProperty.getEdgeDestination().getUserPayload().deepCopyAsArray();
    assertEquals("inputBytes", new String(ib));
    assertEquals("input", edgeProperty.getEdgeDestination().getClassName());

    byte[] ob = edgeProperty.getEdgeSource().getUserPayload().deepCopyAsArray();
    assertEquals("outputBytes", new String(ob));
    assertEquals("output", edgeProperty.getEdgeSource().getClassName());
  }

  @Test(timeout = 5000)
  public void testCredentialsSerde() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    ProcessorDescriptor pd2 = ProcessorDescriptor.create("processor2").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor2Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    Vertex v2 = Vertex.create("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);

    Credentials dagCredentials = new Credentials();
    Token<TokenIdentifier> token1 = new Token<TokenIdentifier>();
    Token<TokenIdentifier> token2 = new Token<TokenIdentifier>();
    dagCredentials.addToken(new Text("Token1"), token1);
    dagCredentials.addToken(new Text("Token2"), token2);

    dag.setCredentials(dagCredentials);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true);

    assertTrue(dagProto.hasCredentialsBinary());

    Credentials fetchedCredentials = DagTypeConverters.convertByteStringToCredentials(dagProto
        .getCredentialsBinary());

    assertEquals(2, fetchedCredentials.numberOfTokens());
    assertNotNull(fetchedCredentials.getToken(new Text("Token1")));
    assertNotNull(fetchedCredentials.getToken(new Text("Token2")));
  }

  @Test(timeout = 5000)
  public void testInvalidExecContext_1() {
    DAG dag = DAG.create("dag1");
    dag.setExecutionContext(VertexExecutionContext.createExecuteInAm(true));
    Vertex v1 = Vertex.create("testvertex", ProcessorDescriptor.create("processor1"), 1);
    dag.addVertex(v1);

    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("AM execution"));
    }

    dag.setExecutionContext(VertexExecutionContext.createExecuteInContainers(true));

    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("container execution"));
    }
  }

  @Test(timeout = 5000)
  public void testInvalidExecContext_2() {

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor
        .create(false,
            new TaskSchedulerDescriptor[]{TaskSchedulerDescriptor.create("plugin", null)},
            new ContainerLauncherDescriptor[]{ContainerLauncherDescriptor.create("plugin", null)},
            new TaskCommunicatorDescriptor[]{TaskCommunicatorDescriptor.create("plugin", null)});

    VertexExecutionContext validExecContext = VertexExecutionContext.create("plugin", "plugin",
        "plugin");
    VertexExecutionContext invalidExecContext1 =
        VertexExecutionContext.create("invalidplugin", "plugin", "plugin");
    VertexExecutionContext invalidExecContext2 =
        VertexExecutionContext.create("plugin", "invalidplugin", "plugin");
    VertexExecutionContext invalidExecContext3 =
        VertexExecutionContext.create("plugin", "plugin", "invalidplugin");

    DAG dag = DAG.create("dag1");
    dag.setExecutionContext(VertexExecutionContext.createExecuteInContainers(true));
    Vertex v1 = Vertex.create("testvertex", ProcessorDescriptor.create("processor1"), 1);
    dag.addVertex(v1);

    // Should succeed. Default context is containers.
    dag.createDag(new TezConfiguration(false), null, null, null, true,
        servicePluginsDescriptor, null);

    // Set execute in AM should fail
    v1.setExecutionContext(VertexExecutionContext.createExecuteInAm(true));
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("AM execution"));
    }

    // Valid context
    v1.setExecutionContext(validExecContext);
    dag.createDag(new TezConfiguration(false), null, null, null, true,
        servicePluginsDescriptor, null);

    // Invalid task scheduler
    v1.setExecutionContext(invalidExecContext1);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("task scheduler"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }

    // Invalid ContainerLauncher
    v1.setExecutionContext(invalidExecContext2);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("container launcher"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }

    // Invalid task comm
    v1.setExecutionContext(invalidExecContext3);
    try {
      dag.createDag(new TezConfiguration(false), null, null, null, true,
          servicePluginsDescriptor, null);
      fail("Expecting dag create to fail due to invalid ServicePluginDescriptor");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("testvertex"));
      assertTrue(e.getMessage().contains("task communicator"));
      assertTrue(e.getMessage().contains("invalidplugin"));
    }
  }

  @Test(timeout = 5000)
  public void testServiceDescriptorPropagation() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    ProcessorDescriptor pd2 = ProcessorDescriptor.create("processor2").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("processor2Bytes".getBytes())));

    VertexExecutionContext defaultExecutionContext =
        VertexExecutionContext.create("plugin", "plugin", "plugin");
    VertexExecutionContext v1Context = VertexExecutionContext.createExecuteInAm(true);

    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor
        .create(true, new TaskSchedulerDescriptor[]{TaskSchedulerDescriptor.create("plugin", null)},
            new ContainerLauncherDescriptor[]{ContainerLauncherDescriptor.create("plugin", null)},
            new TaskCommunicatorDescriptor[]{TaskCommunicatorDescriptor.create("plugin", null)});

    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1))
        .setExecutionContext(v1Context);
    Vertex v2 = Vertex.create("v2", pd2, 1, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());
    v2.setTaskLaunchCmdOpts("").setTaskEnvironment(new HashMap<String, String>())
        .addTaskLocalFiles(new HashMap<String, LocalResource>());

    InputDescriptor inputDescriptor = InputDescriptor.create("input").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("inputBytes".getBytes())));
    OutputDescriptor outputDescriptor = OutputDescriptor.create("output").
        setUserPayload(UserPayload.create(ByteBuffer.wrap("outputBytes".getBytes())));
    Edge edge = Edge.create(v1, v2, EdgeProperty.create(
        DataMovementType.SCATTER_GATHER, DataSourceType.PERSISTED,
        SchedulingType.SEQUENTIAL, outputDescriptor, inputDescriptor));

    dag.addVertex(v1).addVertex(v2).addEdge(edge);
    dag.setExecutionContext(defaultExecutionContext);

    DAGPlan dagProto = dag.createDag(new TezConfiguration(), null, null, null, true,
        servicePluginsDescriptor, null);

    assertEquals(2, dagProto.getVertexCount());
    assertEquals(1, dagProto.getEdgeCount());

    assertTrue(dagProto.hasDefaultExecutionContext());
    VertexExecutionContextProto defaultContextProto = dagProto.getDefaultExecutionContext();
    assertFalse(defaultContextProto.getExecuteInContainers());
    assertFalse(defaultContextProto.getExecuteInAm());
    assertEquals("plugin", defaultContextProto.getTaskSchedulerName());
    assertEquals("plugin", defaultContextProto.getContainerLauncherName());
    assertEquals("plugin", defaultContextProto.getTaskCommName());

    VertexPlan v1Proto = dagProto.getVertex(0);
    assertTrue(v1Proto.hasExecutionContext());
    VertexExecutionContextProto v1ContextProto = v1Proto.getExecutionContext();
    assertFalse(v1ContextProto.getExecuteInContainers());
    assertTrue(v1ContextProto.getExecuteInAm());
    assertFalse(v1ContextProto.hasTaskSchedulerName());
    assertFalse(v1ContextProto.hasContainerLauncherName());
    assertFalse(v1ContextProto.hasTaskCommName());

    VertexPlan v2Proto = dagProto.getVertex(1);
    assertFalse(v2Proto.hasExecutionContext());
  }

  @Test(timeout = 5000)
  public void testInvalidJavaOpts() {
    DAG dag = DAG.create("testDag");
    ProcessorDescriptor pd1 = ProcessorDescriptor.create("processor1")
        .setUserPayload(UserPayload.create(ByteBuffer.wrap("processor1Bytes".getBytes())));
    Vertex v1 = Vertex.create("v1", pd1, 10, Resource.newInstance(1024, 1));
    v1.setTaskLaunchCmdOpts(" -XX:+UseG1GC ");

    dag.addVertex(v1);

    TezConfiguration conf = new TezConfiguration(false);
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:+UseParallelGC ");
    try {
      dag.createDag(conf, null, null, null, true, null, new JavaOptsChecker());
      fail("Expected dag creation to fail for invalid java opts");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid/conflicting GC options"));
    }

    // Should not fail as java opts valid
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:-UseParallelGC ");
    dag.createDag(conf, null, null, null, true, null, new JavaOptsChecker());

    // Should not fail as no checker enabled
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, "  -XX:+UseParallelGC ");
    dag.createDag(conf, null, null, null, true, null, null);
  }
}

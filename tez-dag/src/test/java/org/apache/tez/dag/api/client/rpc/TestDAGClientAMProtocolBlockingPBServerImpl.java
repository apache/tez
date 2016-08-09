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

package org.apache.tez.dag.api.client.rpc;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGInformationBuilder;
import org.apache.tez.dag.api.client.TaskInformation;
import org.apache.tez.dag.api.client.TaskInformationBuilder;
import org.apache.tez.dag.api.client.TaskState;
import org.apache.tez.dag.api.client.VertexInformation;
import org.apache.tez.dag.api.client.VertexInformationBuilder;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGInformationRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGInformationResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationListRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetTaskInformationListResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

public class TestDAGClientAMProtocolBlockingPBServerImpl {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"));

  @Captor
  private ArgumentCaptor<Map<String, LocalResource>> localResourcesCaptor;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test(timeout = 5000)
  @SuppressWarnings("unchecked")
  public void testSubmitDagInSessionWithLargeDagPlan() throws Exception {
    int maxIPCMsgSize = 1024;
    String dagPlanName = "dagplan-name";
    File requestFile = tmpFolder.newFile("request-file");
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, maxIPCMsgSize);

    byte[] randomBytes = new byte[2*maxIPCMsgSize];
    (new Random()).nextBytes(randomBytes);
    UserPayload payload = UserPayload.create(ByteBuffer.wrap(randomBytes));
    Vertex vertex = Vertex.create("V", ProcessorDescriptor.create("P").setUserPayload(payload), 1);
    DAGPlan dagPlan = DAG.create(dagPlanName).addVertex(vertex).createDag(conf, null, null, null, false);

    String lrName = "localResource";
    String scheme = "file";
    String host = "localhost";
    int port = 80;
    String path = "/test";
    URL lrURL = URL.newInstance(scheme, host, port, path);
    LocalResource localResource = LocalResource.newInstance(lrURL, LocalResourceType.FILE,
        LocalResourceVisibility.PUBLIC, 1, 1);
    Map<String, LocalResource> localResources = new HashMap<>();
    localResources.put(lrName, localResource);

    SubmitDAGRequestProto.Builder requestBuilder = SubmitDAGRequestProto.newBuilder().setDAGPlan(dagPlan)
        .setAdditionalAmResources(DagTypeConverters.convertFromLocalResources(localResources));
    try (FileOutputStream fileOutputStream = new FileOutputStream(requestFile)) {
      requestBuilder.build().writeTo(fileOutputStream);
    }

    DAGClientHandler dagClientHandler = mock(DAGClientHandler.class);
    ACLManager aclManager = mock(ACLManager.class);
    DAGClientAMProtocolBlockingPBServerImpl serverImpl = spy(new DAGClientAMProtocolBlockingPBServerImpl(
        dagClientHandler, FileSystem.get(conf)));
    when(dagClientHandler.getACLManager()).thenReturn(aclManager);
    when(dagClientHandler.submitDAG((DAGPlan)any(), (Map<String, LocalResource>)any())).thenReturn("dag-id");
    when(aclManager.checkAMModifyAccess((UserGroupInformation) any())).thenReturn(true);

    requestBuilder.clear().setSerializedRequestPath(requestFile.getAbsolutePath());
    serverImpl.submitDAG(null, requestBuilder.build());

    ArgumentCaptor<DAGPlan> dagPlanCaptor = ArgumentCaptor.forClass(DAGPlan.class);
    verify(dagClientHandler).submitDAG(dagPlanCaptor.capture(), localResourcesCaptor.capture());
    dagPlan = dagPlanCaptor.getValue();
    localResources = localResourcesCaptor.getValue();

    assertEquals(dagPlan.getName(), dagPlanName);
    assertEquals(dagPlan.getVertexCount(), 1);
    assertTrue(dagPlan.getSerializedSize() > maxIPCMsgSize);
    assertArrayEquals(randomBytes, dagPlan.getVertex(0).getProcessorDescriptor().getTezUserPayload().getUserPayload().
        toByteArray());
    assertEquals(localResources.size(), 1);
    assertTrue(localResources.containsKey(lrName));
    localResource = localResources.get(lrName);
    assertEquals(localResource.getType(), LocalResourceType.FILE);
    assertEquals(localResource.getVisibility(), LocalResourceVisibility.PUBLIC);
    lrURL = localResource.getResource();
    assertEquals(lrURL.getScheme(), scheme);
    assertEquals(lrURL.getHost(), host);
    assertEquals(lrURL.getPort(), port);
    assertEquals(lrURL.getFile(), path);
  }

  @Test(timeout = 5000)
  public void testGetDAGInformation() throws Exception {
    TezConfiguration conf = new TezConfiguration();

    String dagId = "dag_9999_0001_1";

    GetDAGInformationRequestProto.Builder requestBuilder =
      GetDAGInformationRequestProto.newBuilder().setDagId(dagId);

    VertexInformationBuilder vertexInformationBuilder = createTestVertex("vertex_100_0001_1_02", "test vertex");
    DAGInformationBuilder testDAGInfo = createTestDAG(dagId, "my test dag", vertexInformationBuilder);
    GetDAGInformationResponseProto response =
      GetDAGInformationResponseProto.newBuilder()
        .setDagInformation(testDAGInfo.getProto())
        .build();

    DAGClientHandler dagClientHandler = mock(DAGClientHandler.class);
    ACLManager aclManager = mock(ACLManager.class);
    DAGClientAMProtocolBlockingPBServerImpl serverImpl = spy(new DAGClientAMProtocolBlockingPBServerImpl(
      dagClientHandler, FileSystem.get(conf)));
    when(dagClientHandler.getACLManager(dagId)).thenReturn(aclManager);
    when(dagClientHandler.getDAGInformation(dagId)).thenReturn(testDAGInfo);
    when(aclManager.checkDAGViewAccess((UserGroupInformation) any())).thenReturn(true);

    GetDAGInformationResponseProto actualResponse = serverImpl.getDAGInformation(null, requestBuilder.build());

    assertEquals(response, actualResponse);
  }

  @Test(timeout = 5000)
  public void testGetTaskInformation() throws Exception {
    TezConfiguration conf = new TezConfiguration();

    String dagId = "dag_9999_0001_1";
    String vertexId = "vertex_100_0001_1_02";
    String taskId = "task_100_0001_1_02_000000";

    GetTaskInformationRequestProto.Builder requestBuilder =
      GetTaskInformationRequestProto.newBuilder()
      .setDagId(dagId)
      .setVertexId(vertexId)
      .setTaskId(taskId);

    TaskInformationBuilder task = createTestTask(taskId, "test diagnostics", TaskState.RUNNING, 123, 456);

    GetTaskInformationResponseProto response =
      GetTaskInformationResponseProto.newBuilder()
        .setTaskInformation(task.getProto())
        .build();

    DAGClientHandler dagClientHandler = mock(DAGClientHandler.class);
    ACLManager aclManager = mock(ACLManager.class);
    DAGClientAMProtocolBlockingPBServerImpl serverImpl = spy(new DAGClientAMProtocolBlockingPBServerImpl(
      dagClientHandler, FileSystem.get(conf)));
    when(dagClientHandler.getACLManager(dagId)).thenReturn(aclManager);
    when(dagClientHandler.getTaskInformation(dagId, vertexId, taskId)).thenReturn(task);
    when(aclManager.checkDAGViewAccess((UserGroupInformation) any())).thenReturn(true);

    GetTaskInformationResponseProto actualResponse = serverImpl.getTaskInformation(null, requestBuilder.build());

    assertEquals(response, actualResponse);
  }

  @Test(timeout = 5000)
  public void testGetTaskInformationList() throws Exception {
    TezConfiguration conf = new TezConfiguration();

    String dagId = "dag_9999_0001_1";
    String vertexId = "vertex_100_0001_1_02";
    String taskId1 = "task_100_0001_1_01_000000";
    String taskId2 = "task_100_0001_1_02_000000";
    int limit = 1;

    GetTaskInformationListRequestProto.Builder requestBuilder =
      GetTaskInformationListRequestProto.newBuilder()
        .setDagId(dagId)
        .setVertexId(vertexId)
        .setLimit(limit);

    TaskInformationBuilder task1 = createTestTask(taskId1, "test diagnostics", TaskState.RUNNING, 123, 456);
    TaskInformationBuilder task2 = createTestTask(taskId2, "test diagnostics", TaskState.RUNNING, 123, 456);

    GetTaskInformationListResponseProto response =
      GetTaskInformationListResponseProto.newBuilder()
        .addTaskInformation(task1.getProto())
        .build();

    DAGClientHandler dagClientHandler = mock(DAGClientHandler.class);
    ACLManager aclManager = mock(ACLManager.class);
    DAGClientAMProtocolBlockingPBServerImpl serverImpl = spy(new DAGClientAMProtocolBlockingPBServerImpl(
      dagClientHandler, FileSystem.get(conf)));
    when(dagClientHandler.getACLManager(dagId)).thenReturn(aclManager);
    when(dagClientHandler.getTaskInformationList(dagId, vertexId, null, limit)).thenReturn(Lists.<TaskInformation>newArrayList(task1));
    when(aclManager.checkDAGViewAccess((UserGroupInformation) any())).thenReturn(true);

    // first try call with no startTaskId
    GetTaskInformationListResponseProto actualResponse = serverImpl.getTaskInformationList(null, requestBuilder.build());
    assertEquals(response, actualResponse);

    // now follow up with a Task id call
    requestBuilder.clear();
    String startTaskId = actualResponse.getTaskInformation(0).getId();
    requestBuilder.setDagId(dagId).setVertexId(vertexId).setStartTaskId(startTaskId).setLimit(limit);
    response = GetTaskInformationListResponseProto.newBuilder()
        .addTaskInformation(task2.getProto())
        .build();
    when(dagClientHandler.getTaskInformationList(dagId, vertexId, startTaskId, limit)).thenReturn(Lists.<TaskInformation>newArrayList(task2));

    actualResponse = serverImpl.getTaskInformationList(null, requestBuilder.build());
    assertEquals(response, actualResponse);
  }

  private DAGInformationBuilder createTestDAG(String dagId, String dagName, VertexInformation ... vertices) {
    DAGInformationBuilder dagInformationBuilder = new DAGInformationBuilder();
    List<VertexInformation> vertexInformationList = Lists.newArrayList(vertices);
    dagInformationBuilder.setDagId(dagId);
    dagInformationBuilder.setName(dagName);
    dagInformationBuilder.setVertexInformationList(vertexInformationList);

    return dagInformationBuilder;
  }

  private VertexInformationBuilder createTestVertex(String vertexId, String vertexName, TaskInformation ... tasks) {
    VertexInformationBuilder vertexInformationBuilder = new VertexInformationBuilder();
    vertexInformationBuilder.setId(vertexId);
    vertexInformationBuilder.setName(vertexName);

    if (tasks != null && tasks.length > 0) {
      vertexInformationBuilder.setTaskInformationList(Lists.newArrayList(tasks));
    }
    return vertexInformationBuilder;
  }

  private TaskInformationBuilder createTestTask(String taskId, String diagnostics, TaskState state, long startTime, long endTime) {
    TaskInformationBuilder taskInformationBuilder = new TaskInformationBuilder();
    taskInformationBuilder.setId(taskId);
    taskInformationBuilder.setDiagnostics(diagnostics);
    taskInformationBuilder.setState(state);
    taskInformationBuilder.setStartTime(startTime);
    taskInformationBuilder.setScheduledTime(startTime);
    taskInformationBuilder.setEndTime(endTime);

    return taskInformationBuilder;
  }
}

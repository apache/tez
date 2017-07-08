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
import java.util.Map;
import java.util.Random;

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

  @Test(timeout = 100000)
  @SuppressWarnings("unchecked")
  public void testSubmitDagInSessionWithLargeDagPlan() throws Exception {
    int maxIPCMsgSize = 1024;
    String dagPlanName = "dagplan-name";
    File requestFile = tmpFolder.newFile("request-file");
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, maxIPCMsgSize);

    // Check with 70 MB (64 MB is CodedInputStream's default limit in earlier versions of protobuf)
    byte[] randomBytes = new byte[70 << 20];
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
}

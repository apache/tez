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

package org.apache.tez.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Maps;
import com.google.protobuf.RpcController;

public class TestTezClient {

  class TezClientForTest extends TezClient {
    TezYarnClient mockTezYarnClient;
    DAGClientAMProtocolBlockingPB sessionAmProxy;
    YarnClient mockYarnClient;
    ApplicationId mockAppId;

    public TezClientForTest(String name, TezConfiguration tezConf,
        @Nullable Map<String, LocalResource> localResources,
        @Nullable Credentials credentials) {
      super(name, tezConf, localResources, credentials);
    }
    
    @Override
    protected FrameworkClient createFrameworkClient() {
      return mockTezYarnClient;
    }
    
    @Override
    protected DAGClientAMProtocolBlockingPB getSessionAMProxy(ApplicationId appId) 
        throws TezException, IOException {
      return sessionAmProxy;
    }
  }
  
  TezClientForTest configure() throws YarnException, IOException {
    return configure(new HashMap<String, LocalResource>(), true);
  }
  
  TezClientForTest configure(Map<String, LocalResource> lrs, boolean isSession) throws YarnException, IOException {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);
    TezClientForTest client = new TezClientForTest("test", conf, lrs, null);

    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    YarnClient yarnClient = mock(YarnClient.class, RETURNS_DEEP_STUBS);
    when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId1);

    DAGClientAMProtocolBlockingPB sessionAmProxy = mock(DAGClientAMProtocolBlockingPB.class, RETURNS_DEEP_STUBS);

    client.sessionAmProxy = sessionAmProxy;
    client.mockTezYarnClient = new TezYarnClient(yarnClient);
    client.mockYarnClient = yarnClient;
    client.mockAppId = appId1;
    
    return client;    
  }
  
  @Test (timeout = 5000)
  public void testTezclientApp() throws Exception {
    testTezClient(false);
  }
  
  @Test (timeout = 5000)
  public void testTezclientSession() throws Exception {
    testTezClient(true);
  }
  
  public void testTezClient(boolean isSession) throws Exception {
    Map<String, LocalResource> lrs = Maps.newHashMap();
    String lrName1 = "LR1";
    lrs.put(lrName1, LocalResource.newInstance(URL.newInstance("file:///", "localhost", 0, "test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    
    TezClientForTest client = configure(lrs, isSession);
    
    ArgumentCaptor<ApplicationSubmissionContext> captor = ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
    client.start();
    verify(client.mockYarnClient, times(1)).init((Configuration)any());
    verify(client.mockYarnClient, times(1)).start();
    if (isSession) {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(3, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    } else {
      verify(client.mockYarnClient, times(0)).submitApplication(captor.capture());
    }
    
    String mockLR1Name = "LR1";
    Map<String, LocalResource> lrDAG = Collections.singletonMap(mockLR1Name, LocalResource
        .newInstance(URL.newInstance("file:///", "localhost", 0, "test"), LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
        Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex).addTaskLocalFiles(lrDAG);
    DAGClient dagClient = client.submitDAG(dag);
    
    // verify that both DAG and TezClient localResources are added to the vertex
    Map<String, LocalResource> vertexLR = vertex.getTaskLocalFiles();
    Assert.assertTrue(vertexLR.containsKey(mockLR1Name));
    
    Assert.assertTrue(dagClient.getExecutionContext().contains(client.mockAppId.toString()));
    
    if (isSession) {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      verify(client.sessionAmProxy, times(1)).submitDAG((RpcController)any(), (SubmitDAGRequestProto) any());
    } else {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(4, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_PLAN_BINARY_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    }
    
    // add resources
    String lrName2 = "LR2";
    lrs.clear();
    lrs.put(lrName2, LocalResource.newInstance(URL.newInstance("file:///", "localhost", 0, "test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    client.addAppMasterLocalFiles(lrs);
    
    ApplicationId appId2 = ApplicationId.newInstance(0, 2);
    when(client.mockYarnClient.createApplication().getNewApplicationResponse().getApplicationId())
        .thenReturn(appId2);
    
    dag = DAG.create("DAG").addVertex(
        Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1, Resource.newInstance(1, 1)));
    dagClient = client.submitDAG(dag);
    
    if (isSession) {
      // same app master
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      Assert.assertTrue(dagClient.getExecutionContext().contains(client.mockAppId.toString()));
      // additional resource is sent
      ArgumentCaptor<SubmitDAGRequestProto> captor1 = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
      verify(client.sessionAmProxy, times(2)).submitDAG((RpcController)any(), captor1.capture());
      SubmitDAGRequestProto proto = captor1.getValue();
      Assert.assertEquals(1, proto.getAdditionalAmResources().getLocalResourcesCount());
      Assert.assertEquals(lrName2, proto.getAdditionalAmResources().getLocalResources(0).getName());
    } else {
      // new app master
      Assert.assertTrue(dagClient.getExecutionContext().contains(appId2.toString()));
      verify(client.mockYarnClient, times(2)).submitApplication(captor.capture());
      // additional resource is added
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(5, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_PLAN_BINARY_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName2));
    }
    
    client.stop();
    if (isSession) {
      verify(client.sessionAmProxy, times(1)).shutdownSession((RpcController) any(),
          (ShutdownSessionRequestProto) any());
    }
    verify(client.mockYarnClient, times(1)).stop();
  }
  
  public void testPreWarm() throws Exception {
    TezClientForTest client = configure();
    client.start();
    PreWarmVertex vertex = PreWarmVertex.create("PreWarm", 1, Resource.newInstance(1, 1));
    client.preWarm(vertex);
    
    ArgumentCaptor<SubmitDAGRequestProto> captor1 = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
    verify(client.sessionAmProxy, times(1)).submitDAG((RpcController)any(), (SubmitDAGRequestProto) any());
    SubmitDAGRequestProto proto = captor1.getValue();
    Assert.assertTrue(proto.getDAGPlan().getName().startsWith(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX));

    client.stop();
  }
  
  @Test (timeout = 10000)
  public void testMultipleSubmissions() throws Exception {
    testMultipleSubmissionsJob(false);
    testMultipleSubmissionsJob(true);
  }
  
  public void testMultipleSubmissionsJob(boolean isSession) throws Exception {
    TezClientForTest client1 = configure(new HashMap<String, LocalResource>(), isSession);
    client1.start();
    
    String mockLR1Name = "LR1";
    Map<String, LocalResource> lrDAG = Collections.singletonMap(mockLR1Name, LocalResource
        .newInstance(URL.newInstance("file:///", "localhost", 0, "test"), LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
        Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex).addTaskLocalFiles(lrDAG);
    // the dag resource will be added to the vertex once
    client1.submitDAG(dag);
    
    TezClientForTest client2 = configure();
    client2.start();
    
    // verify resubmission of same dag to new client (simulates submission error resulting in the
    // creation of a new client and resubmission of the DAG)
    // TEZ-1563 dag resource will not be added again to the vertex because its cached
    // so resubmission works fine
    client2.submitDAG(dag);
    
    client1.stop();
    client2.stop();
  }
  
  @Test(timeout = 5000)
  public void testWaitTillReady_Interrupt() throws Exception {
    final TezClientForTest client = configure();
    client.start();

    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.NEW);
    final AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          client.waitTillReady();
        } catch (Exception e) {
          exceptionReference.set(e);
        }
      };
    };
    thread.start();
    thread.join(250);
    thread.interrupt();
    thread.join();
    Assert.assertThat(exceptionReference.get(),CoreMatchers. instanceOf(InterruptedException.class));
    client.stop();
  }

}

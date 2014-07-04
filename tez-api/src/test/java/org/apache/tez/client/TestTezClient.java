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
import java.util.Map;

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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Maps;
import com.google.protobuf.RpcController;



public class TestTezClient {

  class TezClientForTest extends TezClient {
    YarnClient mockYarnClient;
    DAGClientAMProtocolBlockingPB sessionAmProxy;

    public TezClientForTest(String name, TezConfiguration tezConf,
        @Nullable Map<String, LocalResource> localResources,
        @Nullable Credentials credentials) {
      super(name, tezConf, localResources, credentials);
    }
    
    @Override
    protected YarnClient createYarnClient() {
      return mockYarnClient;
    }
    
    @Override
    protected DAGClientAMProtocolBlockingPB getSessionAMProxy(ApplicationId appId) 
        throws TezException, IOException {
      return sessionAmProxy;
    }
  }
  
  @Test
  public void testTezclientApp() throws Exception {
    testTezClient(false);
  }
  
  @Test
  public void testTezclientSession() throws Exception {
    testTezClient(true);
  }
  
  public void testTezClient(boolean isSession) throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);

    Map<String, LocalResource> lrs = Maps.newHashMap();
    String lrName1 = "LR1";
    lrs.put(lrName1, LocalResource.newInstance(URL.newInstance("file:///", "localhost", 0, "test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    
    TezClientForTest client = new TezClientForTest("test", conf, lrs, null);
    
    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    YarnClient yarnClient = mock(YarnClient.class, RETURNS_DEEP_STUBS);
    when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId1);
    ArgumentCaptor<ApplicationSubmissionContext> captor = ArgumentCaptor.forClass(ApplicationSubmissionContext.class);

    DAGClientAMProtocolBlockingPB sessionAmProxy = mock(DAGClientAMProtocolBlockingPB.class, RETURNS_DEEP_STUBS);
    
    client.sessionAmProxy = sessionAmProxy;
    client.mockYarnClient = yarnClient;
    
    client.start();
    verify(yarnClient, times(1)).init((Configuration)any());
    verify(yarnClient, times(1)).start();
    if (isSession) {
      verify(yarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(3, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    } else {
      verify(yarnClient, times(0)).submitApplication(captor.capture());
    }
    
    DAG dag = new DAG("DAG").addVertex(
        new Vertex("Vertex", new ProcessorDescriptor("P"), 1, Resource.newInstance(1, 1)));
    DAGClient dagClient = client.submitDAG(dag);
    
    Assert.assertEquals(appId1, dagClient.getApplicationId());
    
    if (isSession) {
      verify(yarnClient, times(1)).submitApplication(captor.capture());
      verify(sessionAmProxy, times(1)).submitDAG((RpcController)any(), (SubmitDAGRequestProto) any());
    } else {
      verify(yarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(4, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_PB_PLAN_BINARY_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    }
    
    // add resources
    String lrName2 = "LR2";
    lrs.clear();
    lrs.put(lrName2, LocalResource.newInstance(URL.newInstance("file:///", "localhost", 0, "test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    client.addAppMasterLocalResources(lrs);
    
    ApplicationId appId2 = ApplicationId.newInstance(0, 2);
    when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId2);
    
    dag = new DAG("DAG").addVertex(
        new Vertex("Vertex", new ProcessorDescriptor("P"), 1, Resource.newInstance(1, 1)));
    dagClient = client.submitDAG(dag);
    
    if (isSession) {
      // same app master
      verify(yarnClient, times(1)).submitApplication(captor.capture());
      Assert.assertEquals(appId1, dagClient.getApplicationId());
      // additional resource is sent
      ArgumentCaptor<SubmitDAGRequestProto> captor1 = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
      verify(sessionAmProxy, times(2)).submitDAG((RpcController)any(), captor1.capture());
      SubmitDAGRequestProto proto = captor1.getValue();
      Assert.assertEquals(1, proto.getAdditionalAmResources().getLocalResourcesCount());
      Assert.assertEquals(lrName2, proto.getAdditionalAmResources().getLocalResources(0).getName());
    } else {
      // new app master
      Assert.assertEquals(appId2, dagClient.getApplicationId());
      verify(yarnClient, times(2)).submitApplication(captor.capture());
      // additional resource is added
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(5, context.getAMContainerSpec().getLocalResources().size());
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_PB_BINARY_CONF_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConfiguration.TEZ_PB_PLAN_BINARY_NAME));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
      Assert.assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName2));
    }
    
    
    
    client.stop();
    if (isSession) {
      verify(sessionAmProxy, times(1)).shutdownSession((RpcController) any(), (ShutdownSessionRequestProto)any());
    }
    verify(yarnClient, times(1)).stop();
  }
}

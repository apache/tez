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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.counters.LimitExceededException;
import org.apache.tez.common.counters.Limits;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.SessionNotReady;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConfigurationConstants;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetAMStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.ShutdownSessionRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TezAppMasterStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusStateProto;
import org.apache.tez.dag.api.records.DAGProtos.ProgressProto;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.Maps;
import com.google.protobuf.RpcController;

public class TestTezClient {
  static final long HARD_KILL_TIMEOUT = 1500L;

  class TezClientForTest extends TezClient {
    TezYarnClient mockTezYarnClient;
    DAGClientAMProtocolBlockingPB sessionAmProxy;
    YarnClient mockYarnClient;
    ApplicationId mockAppId;
    boolean callRealGetSessionAMProxy;
    Long prewarmTimeoutMs;

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
    protected DAGClientAMProtocolBlockingPB getAMProxy(ApplicationId appId)
        throws TezException, IOException {
      if (!callRealGetSessionAMProxy) {
        return sessionAmProxy;
      }
      return super.getAMProxy(appId);
    }

    public void setPrewarmTimeoutMs(Long prewarmTimeoutMs) {
      this.prewarmTimeoutMs = prewarmTimeoutMs;
    }

    @Override
    protected long getPrewarmWaitTimeMs() {
      return prewarmTimeoutMs == null ? super.getPrewarmWaitTimeMs() : prewarmTimeoutMs;
    }
  }
  
  TezClientForTest configureAndCreateTezClient() throws YarnException, IOException, ServiceException {
    return configureAndCreateTezClient(null);
  }

  TezClientForTest configureAndCreateTezClient(TezConfiguration conf) throws YarnException, ServiceException,
      IOException {
    return configureAndCreateTezClient(new HashMap<String, LocalResource>(), true, conf);
  }
  
  TezClientForTest configureAndCreateTezClient(Map<String, LocalResource> lrs, boolean isSession,
                                               TezConfiguration conf) throws YarnException, IOException, ServiceException {
    if (conf == null) {
      conf = new TezConfiguration();
    }
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    conf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, isSession);
    TezClientForTest client = new TezClientForTest("test", conf, lrs, null);

    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    YarnClient yarnClient = mock(YarnClient.class, RETURNS_DEEP_STUBS);
    when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId1);
    when(yarnClient.getApplicationReport(appId1).getYarnApplicationState()).thenReturn(YarnApplicationState.NEW);
    when(yarnClient.submitApplication(any(ApplicationSubmissionContext.class))).thenReturn(appId1);

    DAGClientAMProtocolBlockingPB sessionAmProxy = mock(DAGClientAMProtocolBlockingPB.class, RETURNS_DEEP_STUBS);
    when(sessionAmProxy.getAMStatus(any(RpcController.class), any(GetAMStatusRequestProto.class)))
        .thenReturn(GetAMStatusResponseProto.newBuilder().setStatus(TezAppMasterStatusProto.RUNNING).build());

    client.sessionAmProxy = sessionAmProxy;
    client.mockTezYarnClient = new TezYarnClient(yarnClient);
    client.mockYarnClient = yarnClient;
    client.mockAppId = appId1;
    
    return client;    
  }
  
  @Test (timeout = 5000)
  public void testTezclientApp() throws Exception {
    testTezClient(false, true);
  }
  
  @Test (timeout = 5000)
  public void testTezclientSession() throws Exception {
    testTezClient(true, true);
  }

  @Test (timeout = 5000)
  public void testTezClientSessionLargeDAGPlan() throws Exception {
    // request size is within threshold of being serialized
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 10, 10, false);
    // DAGPlan exceeds the threshold but is still less than max IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 6*1024*1024, 10, true);
    // DAGPlan exceeds max IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 15*1024*1024, 10, true);
    // amResources exceeds the threshold but is still less than max IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 10, 6*1024*1024, true);
    // amResources exceeds max IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 10, 15*1024*1024, true);
    // DAGPlan and amResources together exceed threshold but less than IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 3*1024*1024, 3*1024*1024, true);
    // DAGPlan and amResources all exceed max IPC size
    _testTezClientSessionLargeDAGPlan(10*1024*1024, 15*1024*1024, 15*1024*1024, true);
  }

  private void _testTezClientSessionLargeDAGPlan(int maxIPCMsgSize, int payloadSize, int amResourceSize,
                                               boolean shouldSerialize) throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, maxIPCMsgSize);
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, "target/"+this.getClass().getName());
    TezClientForTest client = configureAndCreateTezClient(null, true, conf);

    Map<String, LocalResource> localResourceMap = new HashMap<>();
    byte[] bytes = new byte[amResourceSize];
    Arrays.fill(bytes, (byte)1);
    String lrName = new String(bytes);
    localResourceMap.put(lrName, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));

    ProcessorDescriptor processorDescriptor = ProcessorDescriptor.create("P");
    processorDescriptor.setUserPayload(UserPayload.create(ByteBuffer.allocate(payloadSize)));
    Vertex vertex = Vertex.create("Vertex", processorDescriptor, 1, Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex);

    client.start();
    client.addAppMasterLocalFiles(localResourceMap);
    client.submitDAG(dag);
    client.stop();

    ArgumentCaptor<SubmitDAGRequestProto> captor = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
    verify(client.sessionAmProxy).submitDAG((RpcController)any(), captor.capture());
    SubmitDAGRequestProto request = captor.getValue();

    if (shouldSerialize) {
      /* we need manually delete the serialized dagplan since staging path here won't be destroyed */
      Path dagPlanPath = new Path(request.getSerializedRequestPath());
      FileSystem fs = FileSystem.getLocal(conf);
      fs.deleteOnExit(dagPlanPath);
      fs.delete(dagPlanPath, false);

      assertTrue(request.hasSerializedRequestPath());
      assertFalse(request.hasDAGPlan());
      assertFalse(request.hasAdditionalAmResources());
    } else {
      assertFalse(request.hasSerializedRequestPath());
      assertTrue(request.hasDAGPlan());
      assertTrue(request.hasAdditionalAmResources());
    }
  }

  @Test (timeout = 5000)
  public void testGetClient() throws Exception {
    /* BEGIN first TezClient usage without calling stop() */
    TezClientForTest client = testTezClient(true, false);
    /* END first TezClient usage without calling stop() */

    /* BEGIN reuse of AM from new TezClient */
    ArgumentCaptor<ApplicationSubmissionContext> captor = ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
            .thenReturn(YarnApplicationState.RUNNING);

    //Reuse existing appId from first TezClient
    ApplicationId existingAppId = client.mockAppId;
    TezClientForTest client2 = configureAndCreateTezClient(null, true,
            client.amConfig.getTezConfiguration());
    String mockLR1Name = "LR1";
    Map<String, LocalResource> lrDAG = Collections.singletonMap(mockLR1Name, LocalResource
            .newInstance(URL.newInstance("file", "localhost", 0, "/test1"), LocalResourceType.FILE,
                    LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
            Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex).addTaskLocalFiles(lrDAG);

    //Bind TezClient to existing app and submit a dag
    DAGClient dagClient = client2.getClient(existingAppId).submitDAG(dag);

    assertTrue(dagClient.getExecutionContext().contains(existingAppId.toString()));
    assertEquals(dagClient.getSessionIdentifierString(), existingAppId.toString());

    // Validate request for new AM is not submitted to RM */
    verify(client2.mockYarnClient, times(0)).submitApplication(captor.capture());

    // Validate dag submission from second TezClient as normal */
    verify(client2.sessionAmProxy, times(1)).submitDAG((RpcController)any(), (SubmitDAGRequestProto) any());

    // Validate stop from new TezClient as normal */
    client2.stop();
    verify(client2.sessionAmProxy, times(1)).shutdownSession((RpcController) any(),
            (ShutdownSessionRequestProto) any());
    verify(client2.mockYarnClient, times(1)).stop();
    /* END reuse of AM from new TezClient */
  }
  
  public TezClientForTest testTezClient(boolean isSession, boolean shouldStop) throws Exception {
    Map<String, LocalResource> lrs = Maps.newHashMap();
    String lrName1 = "LR1";
    lrs.put(lrName1, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    
    TezClientForTest client = configureAndCreateTezClient(lrs, isSession, null);

    ArgumentCaptor<ApplicationSubmissionContext> captor = ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
    .thenReturn(YarnApplicationState.RUNNING);
    client.start();
    verify(client.mockYarnClient, times(1)).init((Configuration)any());
    verify(client.mockYarnClient, times(1)).start();
    if (isSession) {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(3, context.getAMContainerSpec().getLocalResources().size());
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    } else {
      verify(client.mockYarnClient, times(0)).submitApplication(captor.capture());
    }
    
    String mockLR1Name = "LR1";
    Map<String, LocalResource> lrDAG = Collections.singletonMap(mockLR1Name, LocalResource
        .newInstance(URL.newInstance("file", "localhost", 0, "/test1"), LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
        Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex).addTaskLocalFiles(lrDAG);
    DAGClient dagClient = client.submitDAG(dag);
        
    assertTrue(dagClient.getExecutionContext().contains(client.mockAppId.toString()));
    assertEquals(dagClient.getSessionIdentifierString(), client.mockAppId.toString());

    if (isSession) {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      verify(client.sessionAmProxy, times(1)).submitDAG((RpcController)any(), (SubmitDAGRequestProto) any());
    } else {
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(4, context.getAMContainerSpec().getLocalResources().size());
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_PLAN_BINARY_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
    }
    
    // add resources
    String lrName2 = "LR2";
    lrs.clear();
    lrs.put(lrName2, LocalResource.newInstance(URL.newInstance("file", "localhost", 0, "/test2"),
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 1, 1));
    client.addAppMasterLocalFiles(lrs);
    
    ApplicationId appId2 = ApplicationId.newInstance(0, 2);
    when(client.mockYarnClient.createApplication().getNewApplicationResponse().getApplicationId())
        .thenReturn(appId2);
    
    when(client.mockYarnClient.getApplicationReport(appId2).getYarnApplicationState())
    .thenReturn(YarnApplicationState.RUNNING);
    dag = DAG.create("DAG").addVertex(
        Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1, Resource.newInstance(1, 1)));
    dagClient = client.submitDAG(dag);
    
    if (isSession) {
      // same app master
      verify(client.mockYarnClient, times(1)).submitApplication(captor.capture());
      assertTrue(dagClient.getExecutionContext().contains(client.mockAppId.toString()));
      assertEquals(dagClient.getSessionIdentifierString(), client.mockAppId.toString());
      // additional resource is sent
      ArgumentCaptor<SubmitDAGRequestProto> captor1 = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
      verify(client.sessionAmProxy, times(2)).submitDAG((RpcController)any(), captor1.capture());
      SubmitDAGRequestProto proto = captor1.getValue();
      Assert.assertEquals(1, proto.getAdditionalAmResources().getLocalResourcesCount());
      Assert.assertEquals(lrName2, proto.getAdditionalAmResources().getLocalResources(0).getName());
    } else {
      // new app master
      assertTrue(dagClient.getExecutionContext().contains(appId2.toString()));
      assertEquals(dagClient.getSessionIdentifierString(), appId2.toString());
      verify(client.mockYarnClient, times(2)).submitApplication(captor.capture());
      // additional resource is added
      ApplicationSubmissionContext context = captor.getValue();
      Assert.assertEquals(5, context.getAMContainerSpec().getLocalResources().size());
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_BINARY_CONF_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          TezConstants.TEZ_PB_PLAN_BINARY_NAME));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName1));
      assertTrue(context.getAMContainerSpec().getLocalResources().containsKey(
          lrName2));
    }

    if(shouldStop) {
      client.stop();
      if (isSession) {
        verify(client.sessionAmProxy, times(1)).shutdownSession((RpcController) any(),
                (ShutdownSessionRequestProto) any());
      }
      verify(client.mockYarnClient, times(1)).stop();
    }
    return client;
  }

  @Test (timeout=5000)
  public void testPreWarm() throws Exception {
    TezClientForTest client = configureAndCreateTezClient();
    client.start();

    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.RUNNING);
    
    when(
        client.sessionAmProxy.getAMStatus((RpcController) any(), (GetAMStatusRequestProto) any()))
        .thenReturn(GetAMStatusResponseProto.newBuilder().setStatus(TezAppMasterStatusProto.READY).build());

    PreWarmVertex vertex = PreWarmVertex.create("PreWarm", 1, Resource.newInstance(1, 1));
    client.preWarm(vertex);
    
    ArgumentCaptor<SubmitDAGRequestProto> captor1 = ArgumentCaptor.forClass(SubmitDAGRequestProto.class);
    verify(client.sessionAmProxy, times(1)).submitDAG((RpcController)any(), captor1.capture());
    SubmitDAGRequestProto proto = captor1.getValue();
    assertTrue(proto.getDAGPlan().getName().startsWith(TezConstants.TEZ_PREWARM_DAG_NAME_PREFIX));

    setClientToReportStoppedDags(client);
    client.stop();
  }


  @Test (timeout=5000)
  public void testPreWarmCloseStuck() throws Exception {
    TezClientForTest client = configureAndCreateTezClient();
    client.setPrewarmTimeoutMs(10L); // Don't wait too long.
    client.start();

    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.RUNNING);
    when(client.sessionAmProxy.getAMStatus((RpcController) any(), (GetAMStatusRequestProto) any()))
        .thenReturn(GetAMStatusResponseProto.newBuilder().setStatus(TezAppMasterStatusProto.READY).build());

    PreWarmVertex vertex = PreWarmVertex.create("PreWarm", 1, Resource.newInstance(1, 1));
    client.preWarm(vertex);
    // Keep prewarm in "running" state. Client should give up waiting; if it doesn't, the test will time out.
    client.stop();
  }


  private void setClientToReportStoppedDags(TezClientForTest client) throws Exception {
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
      .thenReturn(YarnApplicationState.FINISHED);
    when(client.sessionAmProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(DAGStatusProto.newBuilder()
          .addDiagnostics("Diagnostics_0").setState(DAGStatusStateProto.DAG_SUCCEEDED)
          .setDAGProgress(ProgressProto.newBuilder()
                  .setFailedTaskCount(0).setKilledTaskCount(0).setRunningTaskCount(0)
                  .setSucceededTaskCount(1).setTotalTaskCount(1).build()).build()).build());
  }

  @Test (timeout=30000)
  public void testPreWarmWithTimeout() throws Exception {
    long startTime = 0 , endTime = 0;
    TezClientForTest client = configureAndCreateTezClient();
    final TezClientForTest spyClient = spy(client);
    doCallRealMethod().when(spyClient).start();
    doCallRealMethod().when(spyClient).stop();
    spyClient.start();

    when(
        spyClient.mockYarnClient.getApplicationReport(
            spyClient.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.RUNNING);
    when(
        spyClient.sessionAmProxy.getAMStatus((RpcController) any(),
            (GetAMStatusRequestProto) any()))
        .thenReturn(
            GetAMStatusResponseProto.newBuilder().setStatus(
                TezAppMasterStatusProto.INITIALIZING).build());
    PreWarmVertex vertex =
        PreWarmVertex.create("PreWarm", 1, Resource.newInstance(1, 1));
    int timeout = 5000;
    try {
      startTime = Time.monotonicNow();
      spyClient.preWarm(vertex, timeout, TimeUnit.MILLISECONDS);
      fail("PreWarm should have encountered an Exception!");
    } catch (SessionNotReady te) {
      endTime = Time.monotonicNow();
      assertTrue("Time taken is not as expected",
          (endTime - startTime) > timeout);
      verify(spyClient, times(0)).submitDAG(any(DAG.class));
      Assert.assertTrue("Unexpected Exception message",
          te.getMessage().contains("Tez AM not ready"));

    }

    when(
        spyClient.sessionAmProxy.getAMStatus((RpcController) any(),
            (GetAMStatusRequestProto) any()))
        .thenReturn(
            GetAMStatusResponseProto.newBuilder().setStatus(
                TezAppMasterStatusProto.READY).build());
    try {
      startTime = Time.monotonicNow();
      spyClient.preWarm(vertex, timeout, TimeUnit.MILLISECONDS);
      endTime = Time.monotonicNow();
      assertTrue("Time taken is not as expected",
          (endTime - startTime) <= timeout);
      verify(spyClient, times(1)).submitDAG(any(DAG.class));
    } catch (TezException te) {
      fail("PreWarm should have succeeded!");
    }
    Thread amStateThread = new Thread() {
      @Override
      public void run() {
          CountDownLatch latch = new CountDownLatch(1);
          try {
            when(
                spyClient.sessionAmProxy.getAMStatus((RpcController) any(),
                    (GetAMStatusRequestProto) any()))
                .thenReturn(
                    GetAMStatusResponseProto.newBuilder().setStatus(
                        TezAppMasterStatusProto.INITIALIZING).build());
            latch.await(1000, TimeUnit.MILLISECONDS);
            when(
                spyClient.sessionAmProxy.getAMStatus((RpcController) any(),
                    (GetAMStatusRequestProto) any()))
                .thenReturn(
                    GetAMStatusResponseProto.newBuilder().setStatus(
                        TezAppMasterStatusProto.READY).build());
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ServiceException e) {
            e.printStackTrace();
          }
        }
      };
    amStateThread.start();
    startTime = Time.monotonicNow();
    spyClient.preWarm(vertex, timeout, TimeUnit.MILLISECONDS);
    endTime = Time.monotonicNow();
    assertTrue("Time taken is not as expected",
        (endTime - startTime) <= timeout);
    verify(spyClient, times(2)).submitDAG(any(DAG.class));
    setClientToReportStoppedDags(client);
    spyClient.stop();
    client.stop();
  }

  @Test (timeout = 10000)
  public void testMultipleSubmissions() throws Exception {
    testMultipleSubmissionsJob(false);
    testMultipleSubmissionsJob(true);
  }
  
  public void testMultipleSubmissionsJob(boolean isSession) throws Exception {
    TezClientForTest client1 = configureAndCreateTezClient(new HashMap<String, LocalResource>(),
        isSession, null);
    when(client1.mockYarnClient.getApplicationReport(client1.mockAppId).getYarnApplicationState())
    .thenReturn(YarnApplicationState.RUNNING);
    client1.start();
    
    String mockLR1Name = "LR1";
    Map<String, LocalResource> lrDAG = Collections.singletonMap(mockLR1Name, LocalResource
        .newInstance(URL.newInstance("file", "localhost", 0, "/test"), LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    String mockLR2Name = "LR2";
    Map<String, LocalResource> lrVertex = Collections.singletonMap(mockLR2Name, LocalResource
        .newInstance(URL.newInstance("file", "localhost", 0, "/test1"), LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
        Resource.newInstance(1, 1)).addTaskLocalFiles(lrVertex);
    DAG dag = DAG.create("DAG").addVertex(vertex).addTaskLocalFiles(lrDAG);

    // the dag resource will be added to the vertex once
    client1.submitDAG(dag);
    
    TezClientForTest client2 = configureAndCreateTezClient();
    when(client2.mockYarnClient.getApplicationReport(client2.mockAppId).getYarnApplicationState())
    .thenReturn(YarnApplicationState.RUNNING);
    client2.start();
    
    // verify resubmission of same dag to new client (simulates submission error resulting in the
    // creation of a new client and resubmission of the DAG)
    client2.submitDAG(dag);
    
    client1.stop();
    client2.stop();
  }
  
  @Test(timeout = 5000)
  public void testWaitTillReady_Interrupt() throws Exception {
    final TezClientForTest client = configureAndCreateTezClient();
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
    Assert.assertThat(exceptionReference.get(), CoreMatchers.instanceOf(InterruptedException.class));
    client.stop();
  }
  
  @Test(timeout = 5000)
  public void testWaitTillReadyAppFailed() throws Exception {
    final TezClientForTest client = configureAndCreateTezClient();
    client.start();
    String msg = "Application Test Failed";
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.NEW).thenReturn(YarnApplicationState.FAILED);
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getDiagnostics()).thenReturn(
        msg);
    try {
      client.waitTillReady();
      fail();
    } catch (SessionNotRunning e) {
      assertTrue(e.getMessage().contains(msg));
    }
    client.stop();
  }
  
  @Test(timeout = 5000)
  public void testWaitTillReadyAppFailedNoDiagnostics() throws Exception {
    final TezClientForTest client = configureAndCreateTezClient();
    client.start();
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.NEW).thenReturn(YarnApplicationState.FAILED);
    try {
      client.waitTillReady();
      fail();
    } catch (SessionNotRunning e) {
      assertTrue(e.getMessage().contains(TezClient.NO_CLUSTER_DIAGNOSTICS_MSG));
    }
    client.stop();
  }
  
  @Test(timeout = 5000)
  public void testSubmitDAGAppFailed() throws Exception {
    final TezClientForTest client = configureAndCreateTezClient();
    client.start();
    
    client.callRealGetSessionAMProxy = true;
    String msg = "Application Test Failed";
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.KILLED);
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getDiagnostics()).thenReturn(
        msg);

    Vertex vertex = Vertex.create("Vertex", ProcessorDescriptor.create("P"), 1,
        Resource.newInstance(1, 1));
    DAG dag = DAG.create("DAG").addVertex(vertex);
    
    try {
      client.submitDAG(dag);
      fail();
    } catch (SessionNotRunning e) {
      assertTrue(e.getMessage().contains(msg));
    }
    client.stop();
  }

  @Test(timeout = 5000)
  public void testTezClientCounterLimits() throws YarnException, IOException, ServiceException {
    Limits.reset();
    int defaultCounterLimit = TezConfiguration.TEZ_COUNTERS_MAX_DEFAULT;

    int newCounterLimit = defaultCounterLimit + 500;

    TezConfiguration conf = new TezConfiguration();
    conf.setInt(TezConfiguration.TEZ_COUNTERS_MAX, newCounterLimit);

    configureAndCreateTezClient(conf);

    TezCounters counters = new TezCounters();
    for (int i = 0 ; i < newCounterLimit ; i++) {
      counters.findCounter("GroupName", "TestCounter" + i).setValue(i);
    }

    try {
      counters.findCounter("GroupName", "TestCounterFail").setValue(1);
      fail("Expecting a LimitExceedException - too many counters");
    } catch (LimitExceededException e) {
    }
  }

  @Test(timeout = 5000)
  public void testClientBuilder() {
    TezConfiguration tezConfWitSession = new TezConfiguration();
    tezConfWitSession.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);

    TezConfiguration tezConfNoSession = new TezConfiguration();
    tezConfNoSession.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);

    AMConfiguration amConf;
    TezClient tezClient;
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localResourceMap = new HashMap<>();
    localResourceMap.put("testResource", mock(LocalResource.class));
    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor.create(true);

    // Session mode via conf
    tezClient = TezClient.newBuilder("client", tezConfWitSession).build();
    assertTrue(tezClient.isSession);
    assertNull(tezClient.servicePluginsDescriptor);
    assertNotNull(tezClient.apiVersionInfo);
    amConf = tezClient.amConfig;
    assertNotNull(amConf);
    assertEquals(0, amConf.getAMLocalResources().size());
    assertNull(amConf.getCredentials());
    assertTrue(
        amConf.getTezConfiguration().getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false));

    // Non-Session mode via conf
    tezClient = TezClient.newBuilder("client", tezConfNoSession).build();
    assertFalse(tezClient.isSession);
    assertNull(tezClient.servicePluginsDescriptor);
    assertNotNull(tezClient.apiVersionInfo);
    amConf = tezClient.amConfig;
    assertNotNull(amConf);
    assertEquals(0, amConf.getAMLocalResources().size());
    assertNull(amConf.getCredentials());
    assertFalse(
        amConf.getTezConfiguration().getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true));

    // no-session via config. API explicit session.
    tezClient = TezClient.newBuilder("client", tezConfNoSession).setIsSession(true).build();
    assertTrue(tezClient.isSession);
    assertNull(tezClient.servicePluginsDescriptor);
    assertNotNull(tezClient.apiVersionInfo);
    amConf = tezClient.amConfig;
    assertNotNull(amConf);
    assertEquals(0, amConf.getAMLocalResources().size());
    assertNull(amConf.getCredentials());
    assertTrue(
        amConf.getTezConfiguration().getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false));

    // Plugins, credentials, local resources
    tezClient = TezClient.newBuilder("client", tezConfWitSession).setCredentials(credentials)
        .setLocalResources(localResourceMap).setServicePluginDescriptor(servicePluginsDescriptor)
        .build();
    assertTrue(tezClient.isSession);
    assertEquals(servicePluginsDescriptor, tezClient.servicePluginsDescriptor);
    assertNotNull(tezClient.apiVersionInfo);
    amConf = tezClient.amConfig;
    assertNotNull(amConf);
    assertEquals(1, amConf.getAMLocalResources().size());
    assertEquals(localResourceMap, amConf.getAMLocalResources());
    assertEquals(credentials, amConf.getCredentials());
    assertTrue(
        amConf.getTezConfiguration().getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false));
  }

  public static class InvalidChecker {
    // No-op class
  }

  @Test(timeout = 5000)
  public void testInvalidJavaOptsChecker1() throws YarnException, IOException, ServiceException,
      TezException {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_CLASS, "InvalidClassName");
    TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();
  }

  @Test(timeout = 5000)
  public void testInvalidJavaOptsChecker2() throws YarnException, IOException, ServiceException,
      TezException {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_CLIENT_JAVA_OPTS_CHECKER_CLASS, InvalidChecker.class.getName());
    TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();
  }

  @Test(timeout = 5000)
  public void testStopRetriesUntilTerminalState() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_CLIENT_ASYNCHRONOUS_STOP, false);
    conf.setLong(TezConfiguration.TEZ_CLIENT_HARD_KILL_TIMEOUT_MS, HARD_KILL_TIMEOUT);
    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.NEW).thenReturn(YarnApplicationState.KILLED);
    try {
      client.stop();
    } catch (Exception e) {
      Assert.fail("Expected ApplicationNotFoundException");
    }
    verify(client.mockYarnClient, atLeast(2)).getApplicationReport(client.mockAppId);
  }

  @Test(timeout = 20000)
  public void testStopRetriesUntilTimeout() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_CLIENT_ASYNCHRONOUS_STOP, false);
    conf.setLong(TezConfiguration.TEZ_CLIENT_HARD_KILL_TIMEOUT_MS, HARD_KILL_TIMEOUT);
    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.RUNNING);
    long start = System.currentTimeMillis();
    try {
      client.stop();
    } catch (Exception e) {
      Assert.fail("Stop should complete without exception: " + e);
    }
    long end = System.currentTimeMillis();
    verify(client.mockYarnClient, atLeast(2)).getApplicationReport(client.mockAppId);
    Assert.assertTrue("Stop ended before timeout", end - start > HARD_KILL_TIMEOUT);
  }

  @Test(timeout = 5000)
  public void testSubmitHostPopulated() throws YarnException, IOException, ServiceException, TezException {

    TezConfiguration conf = new TezConfiguration();
    configureAndCreateTezClient(conf);
    InetAddress ip = InetAddress.getLocalHost();
    if (ip != null) {
      Assert.assertEquals(ip.getCanonicalHostName(), conf.get(TezConfigurationConstants.TEZ_SUBMIT_HOST));
      Assert.assertEquals(ip.getHostAddress(), conf.get(TezConfigurationConstants.TEZ_SUBMIT_HOST_ADDRESS));
    } else {
      Assert.fail("Failed to retrieve local host information");
    }
  }

  @Test(timeout = 5000)
  public void testClientResubmit() throws Exception {
    TezClientForTest client = configureAndCreateTezClient(null, true, null);
    client.start();
    Map<String, LocalResource> lrDAG = Collections.singletonMap("LR1",
        LocalResource.newInstance(
            URL.newInstance("file", "localhost", 0, "/test1"),
            LocalResourceType.FILE,
            LocalResourceVisibility.PUBLIC, 1, 1));
    Vertex vertex1 = Vertex.create("Vertex1", ProcessorDescriptor.create("P1"), 1,
        Resource.newInstance(1, 1));
    vertex1.setTaskLaunchCmdOpts("-XX:+UseParallelGC -XX:+UseG1GC");
    Vertex vertex2 = Vertex.create("Vertex2", ProcessorDescriptor.create("P2"), 1,
        Resource.newInstance(1, 1));
    vertex2.setTaskLaunchCmdOpts("-XX:+UseParallelGC -XX:+UseG1GC");
    DAG dag = DAG.create("DAG").addVertex(vertex1).addVertex(vertex2).addTaskLocalFiles(lrDAG);
    for (int i = 0; i < 3; ++i) {
      try {
        client.submitDAG(dag);
        Assert.fail("Expected TezUncheckedException here.");
      } catch(TezUncheckedException ex) {
        Assert.assertTrue(ex.getMessage().contains("Invalid/conflicting GC options found"));
      }
    }
    client.stop();
  }

  @Test(timeout = 10000)
  public void testMissingYarnAppStatus() throws Exception {
    // verify an app not found exception is thrown when YARN reports a null app status
    ApplicationId appId1 = ApplicationId.newInstance(0, 1);
    ApplicationReport mockReport = mock(ApplicationReport.class);
    when(mockReport.getApplicationId()).thenReturn(appId1);
    when(mockReport.getYarnApplicationState()).thenReturn(null);
    YarnClient yarnClient = mock(YarnClient.class, RETURNS_DEEP_STUBS);
    when(yarnClient.createApplication().getNewApplicationResponse().getApplicationId()).thenReturn(appId1);
    when(yarnClient.getApplicationReport(appId1)).thenReturn(mockReport);
    TezYarnClient tezClient = new TezYarnClient(yarnClient);
    tezClient.init(new TezConfiguration(false), new YarnConfiguration());
    try {
      tezClient.getApplicationReport(appId1);
      fail("getApplicationReport should have thrown");
    } catch (ApplicationNotFoundException e) {
    }
  }

  @Test(timeout = 30000)
  public void testAMClientHeartbeat() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, 10);
    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();
    long start = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() > (start + 5000)) {
        break;
      }
      Thread.sleep(1000);
    }
    client.stop();
    verify(client.sessionAmProxy, atLeast(3)).getAMStatus(any(RpcController.class),
        any(GetAMStatusRequestProto.class));

    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, -1);
    final TezClientForTest client2 = configureAndCreateTezClient(conf);
    client2.start();
    start = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() > (start + 5000)) {
        break;
      }
      Thread.sleep(1000);
    }
    client2.stop();
    verify(client2.sessionAmProxy, times(0)).getAMStatus(any(RpcController.class),
        any(GetAMStatusRequestProto.class));


  }

  @Test(timeout = 20000)
  public void testAMHeartbeatFailOnGetAMProxy_AppNotStarted() throws Exception {
    int amHeartBeatTimeoutSecs = 3;
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, amHeartBeatTimeoutSecs);

    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.callRealGetSessionAMProxy = true;
    client.start();

    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
        .thenReturn(YarnApplicationState.ACCEPTED);
    Thread.sleep(2 * amHeartBeatTimeoutSecs * 1000);
    assertFalse(client.getAMKeepAliveService().isTerminated());
  }

  @Test(timeout = 20000)
  public void testAMHeartbeatFailOnGetAMProxy_AppFailed() throws Exception {
    int amHeartBeatTimeoutSecs = 3;
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, amHeartBeatTimeoutSecs);

    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.callRealGetSessionAMProxy = true;
    client.start();

    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
      .thenReturn(YarnApplicationState.FAILED);
    Thread.sleep(2 * amHeartBeatTimeoutSecs * 1000);
    assertTrue(client.getAMKeepAliveService().isTerminated());
  }

  @Test(timeout = 20000)
  public void testAMHeartbeatFailOnGetAMStatus() throws Exception {
    int amHeartBeatTimeoutSecs = 3;
    TezConfiguration conf = new TezConfiguration();
    conf.setInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS, amHeartBeatTimeoutSecs);

    final TezClientForTest client = configureAndCreateTezClient(conf);
    client.start();

    when(client.sessionAmProxy.getAMStatus(any(RpcController.class),
      any(GetAMStatusRequestProto.class))).thenThrow(new ServiceException("error"));
    client.callRealGetSessionAMProxy = true;
    when(client.mockYarnClient.getApplicationReport(client.mockAppId).getYarnApplicationState())
      .thenReturn(YarnApplicationState.FAILED);
    Thread.sleep(3 * amHeartBeatTimeoutSecs * 1000);
    assertTrue(client.getAMKeepAliveService().isTerminated());
  }

  //See TEZ-3874
  @Test(timeout = 5000)
  public void testYarnZkDeprecatedConf() {
    Configuration conf = new Configuration(false);
    String val = "hostname:2181";
    conf.set("yarn.resourcemanager.zk-address", val);

    ConfigurationProto confProto = null;
    //Test that Exception is not thrown by createFinalConfProtoForApp
    TezClientUtils.createFinalConfProtoForApp(conf, null);
  }
}

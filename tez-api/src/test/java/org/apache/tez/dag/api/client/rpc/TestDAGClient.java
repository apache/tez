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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.common.CachedEntity;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientImpl;
import org.apache.tez.dag.api.client.DAGClientTimelineImpl;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DAGStatus.State;
import org.apache.tez.dag.api.client.DagStatusSource;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.TimelineReaderFactory.TimelineReaderStrategy;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusStateProto;
import org.apache.tez.dag.api.records.DAGProtos.ProgressProto;
import org.apache.tez.dag.api.records.DAGProtos.StatusGetOptsProto;
import org.apache.tez.dag.api.records.DAGProtos.StringProgressPairProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCounterGroupProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCounterProto;
import org.apache.tez.dag.api.records.DAGProtos.TezCountersProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusStateProto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.util.collections.Sets;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestDAGClient {

  private DAGClient dagClient;
  private ApplicationId mockAppId;
  private ApplicationReport mockAppReport;
  private String dagIdStr;
  private DAGClientAMProtocolBlockingPB mockProxy;
  
  private VertexStatusProto vertexStatusProtoWithoutCounters;
  private VertexStatusProto vertexStatusProtoWithCounters;
  
  private DAGStatusProto dagStatusProtoWithoutCounters;
  private DAGStatusProto dagStatusProtoWithCounters;
  
  
  private void setUpData(){
    // DAG
    ProgressProto dagProgressProto = ProgressProto.newBuilder()
              .setFailedTaskCount(1)
              .setKilledTaskCount(1)
              .setRunningTaskCount(2)
              .setSucceededTaskCount(2)
              .setTotalTaskCount(6)
              .build();
    
    TezCountersProto dagCountersProto=TezCountersProto.newBuilder()
        .addCounterGroups(TezCounterGroupProto.newBuilder()
            .setName("DAGGroup")
            .addCounters(TezCounterProto.newBuilder()
                  .setDisplayName("dag_counter_1")
                  .setValue(99)))
        .build();
    
    dagStatusProtoWithoutCounters = DAGStatusProto.newBuilder()
              .addDiagnostics("Diagnostics_0")
              .setState(DAGStatusStateProto.DAG_RUNNING)
              .setDAGProgress(dagProgressProto)
              .addVertexProgress(
                  StringProgressPairProto.newBuilder().setKey("v1")
                  .setProgress(ProgressProto.newBuilder()
                  .setFailedTaskCount(0)
                  .setSucceededTaskCount(0)
                  .setKilledTaskCount(0))
                  )
              .addVertexProgress(
                  StringProgressPairProto.newBuilder().setKey("v2")
                  .setProgress(ProgressProto.newBuilder()
                  .setFailedTaskCount(1)
                  .setSucceededTaskCount(1)
                  .setKilledTaskCount(1))
                  )
              .build();
    
    dagStatusProtoWithCounters = DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters)
                      .setDagCounters(dagCountersProto)
                      .build();
    
    // Vertex
    ProgressProto vertexProgressProto = ProgressProto.newBuilder()
                    .setFailedTaskCount(1)
                    .setKilledTaskCount(0)
                    .setRunningTaskCount(0)
                    .setSucceededTaskCount(1)
                    .build();
    
    TezCountersProto vertexCountersProto=TezCountersProto.newBuilder()
                    .addCounterGroups(TezCounterGroupProto.newBuilder()
                        .addCounters(TezCounterProto.newBuilder()
                              .setDisplayName("vertex_counter_1")
                              .setValue(99)))
                    .build();
    
    vertexStatusProtoWithoutCounters = VertexStatusProto.newBuilder()
                    .setId("vertex_1")
                    .addDiagnostics("V_Diagnostics_0")
                    .setProgress(vertexProgressProto)
                    .setState(VertexStatusStateProto.VERTEX_SUCCEEDED)  // make sure the waitForCompletion be able to finish
                    .build();
    vertexStatusProtoWithCounters = VertexStatusProto.newBuilder(vertexStatusProtoWithoutCounters)
                    .setVertexCounters(vertexCountersProto)
                    .build();
  }

  private static class DAGCounterRequestMatcher implements ArgumentMatcher<GetDAGStatusRequestProto>{

    @Override
    public boolean matches(GetDAGStatusRequestProto requestProto) {
      return requestProto != null && requestProto.getStatusOptionsCount() != 0
              && requestProto.getStatusOptionsList().get(0) == StatusGetOptsProto.GET_COUNTERS;
    }
  }

  private static class VertexCounterRequestMatcher implements ArgumentMatcher<GetVertexStatusRequestProto>{

    @Override
    public boolean matches(GetVertexStatusRequestProto requestProto) {
      return requestProto != null && requestProto.getStatusOptionsCount() != 0
          && requestProto.getStatusOptionsList().get(0) == StatusGetOptsProto.GET_COUNTERS;
    }
  }
  
  @Before
  public void setUp() throws YarnException, IOException, TezException, ServiceException{

    setUpData();
    
    /////////////// mock //////////////////////
    mockAppId = mock(ApplicationId.class);
    mockAppReport = mock(ApplicationReport.class);
    dagIdStr = "dag_9999_0001_1";
    mockProxy = mock(DAGClientAMProtocolBlockingPB.class);
    // return the response with Counters is the request match the CounterMatcher
    when(mockProxy.getDAGStatus(isNull(), any()))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build());
    when(mockProxy.getDAGStatus(isNull(), argThat(new DAGCounterRequestMatcher())))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithCounters).build());
    
    when(mockProxy.getVertexStatus(isNull(), any()))
      .thenReturn(GetVertexStatusResponseProto.newBuilder().setVertexStatus(vertexStatusProtoWithoutCounters).build());
    when(mockProxy.getVertexStatus(isNull(), argThat(new VertexCounterRequestMatcher())))
      .thenReturn(GetVertexStatusResponseProto.newBuilder().setVertexStatus(vertexStatusProtoWithCounters).build());

    TezConfiguration tezConf = new TezConfiguration();
    dagClient = new DAGClientImpl(mockAppId, dagIdStr, tezConf, null,
        UserGroupInformation.getCurrentUser());
    DAGClientRPCImpl realClient = (DAGClientRPCImpl)((DAGClientImpl)dagClient).getRealClient();
    realClient.appReport = mockAppReport;
    realClient.proxy = mockProxy;
  }
  
  @Test(timeout = 5000)
  public void testApp() throws IOException, TezException, ServiceException{
    assertTrue(dagClient.getExecutionContext().contains(mockAppId.toString()));
    assertEquals(mockAppId.toString(), dagClient.getSessionIdentifierString());
    assertEquals(dagIdStr, dagClient.getDagIdentifierString());
    DAGClientRPCImpl realClient = (DAGClientRPCImpl)((DAGClientImpl)dagClient).getRealClient();
    assertEquals(mockAppReport, realClient.getApplicationReportInternal());
  }
  
  @Test(timeout = 5000)
  public void testDAGStatus() throws Exception{
    DAGStatus resultDagStatus = dagClient.getDAGStatus(null);
    verify(mockProxy, times(1)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setTimeout(0).build());
    assertEquals(new DAGStatus(dagStatusProtoWithoutCounters, DagStatusSource.AM), resultDagStatus);
    System.out.println("DAGStatusWithoutCounter:" + resultDagStatus);
    
    resultDagStatus = dagClient.getDAGStatus(Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy, times(1)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setTimeout(0).addStatusOptions(StatusGetOptsProto.GET_COUNTERS).build());
    assertEquals(new DAGStatus(dagStatusProtoWithCounters, DagStatusSource.AM), resultDagStatus);
    System.out.println("DAGStatusWithCounter:" + resultDagStatus);
  }
  
  @Test(timeout = 5000)
  public void testVertexStatus() throws Exception{
    VertexStatus resultVertexStatus = dagClient.getVertexStatus("v1", null);
    verify(mockProxy).getVertexStatus(null, GetVertexStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setVertexName("v1").build());
    assertEquals(new VertexStatus(vertexStatusProtoWithoutCounters), resultVertexStatus);
    System.out.println("VertexWithoutCounter:" + resultVertexStatus);
    
    resultVertexStatus = dagClient.getVertexStatus("v1", Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy).getVertexStatus(null, GetVertexStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setVertexName("v1").addStatusOptions(StatusGetOptsProto.GET_COUNTERS)
        .build());
    assertEquals(new VertexStatus(vertexStatusProtoWithCounters), resultVertexStatus);
    System.out.println("VertexWithCounter:" + resultVertexStatus);
  }
  
  @Test(timeout = 5000)
  public void testTryKillDAG() throws Exception{
    dagClient.tryKillDAG();
    verify(mockProxy, times(1)).tryKillDAG(null, TryKillDAGRequestProto.newBuilder()
        .setDagId(dagIdStr).build());
  }
  
  @Test(timeout = 5000)
  public void testWaitForCompletion() throws Exception{
    // first time return DAG_RUNNING, second time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(), any()))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters)
          .build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
          (DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters)
              .setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
          .build());

    dagClient.waitForCompletion();
    ArgumentCaptor<RpcController> rpcControllerArgumentCaptor =
        ArgumentCaptor.forClass(RpcController.class);
    ArgumentCaptor<GetDAGStatusRequestProto> argumentCaptor =
        ArgumentCaptor.forClass(GetDAGStatusRequestProto.class);
    verify(mockProxy, times(2))
        .getDAGStatus(rpcControllerArgumentCaptor.capture(), argumentCaptor.capture());
  }

  @Test(timeout = 5000)
  public void testWaitForCompletionWithStatusUpdates() throws Exception{

    // first time and second time return DAG_RUNNING, third time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(), any()))
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
            (DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters)
                .setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
            .build());
    
    //  first time for getVertexSet
    //  second & third time for check completion
    ArgumentCaptor<RpcController> rpcControllerArgumentCaptor =
        ArgumentCaptor.forClass(RpcController.class);
    ArgumentCaptor<GetDAGStatusRequestProto> argumentCaptor =
        ArgumentCaptor.forClass(GetDAGStatusRequestProto.class);
    dagClient.waitForCompletionWithStatusUpdates(null);
    // 2 from initial request - when status isn't cached. 1 for vertex names. 1 for final wait.
    verify(mockProxy, times(4))
        .getDAGStatus(rpcControllerArgumentCaptor.capture(), argumentCaptor.capture());

    when(mockProxy.getDAGStatus(isNull(), any()))
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(
            DAGStatusProto.newBuilder(dagStatusProtoWithCounters)
                .setState(DAGStatusStateProto.DAG_RUNNING).build()).build())
        .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
            (DAGStatusProto.newBuilder(dagStatusProtoWithCounters).setState(
                DAGStatusStateProto.DAG_SUCCEEDED).build())
            .build());

    rpcControllerArgumentCaptor =
        ArgumentCaptor.forClass(RpcController.class);
    argumentCaptor =
        ArgumentCaptor.forClass(GetDAGStatusRequestProto.class);
    dagClient.waitForCompletionWithStatusUpdates(Sets.newSet(StatusGetOpts.GET_COUNTERS));
    // 4 from past invocation in the test, 2 from initial request - when status isn't cached. 1 for vertex names. 1 for final wait.
    verify(mockProxy, times(8))
        .getDAGStatus(rpcControllerArgumentCaptor.capture(), argumentCaptor.capture());
  }

  @Test(timeout = 50000)
  public void testGetDagStatusWithTimeout() throws Exception {
    long startTime;
    long endTime;
    long diff;

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setLong(TezConfiguration.TEZ_DAG_STATUS_POLLINTERVAL_MS, 800l);

    DAGClientImplForTest dagClient = new DAGClientImplForTest(mockAppId, dagIdStr, tezConf, null);
    DAGClientRPCImplForTest dagClientRpc =
        new DAGClientRPCImplForTest(mockAppId, dagIdStr, tezConf, null);
    dagClient.setRealClient(dagClientRpc);

    DAGStatus dagStatus;

    // Fetch from RM. AM not up yet.
    dagClientRpc.setAMProxy(null);
    DAGStatus rmDagStatus =
        new DAGStatus(constructDagStatusProto(DAGStatusStateProto.DAG_SUBMITTED),
            DagStatusSource.RM);
    dagClient.setRmDagStatus(rmDagStatus);

    startTime = System.currentTimeMillis();
    dagStatus = dagClient.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000l);
    endTime = System.currentTimeMillis();
    diff = endTime - startTime;
    assertTrue(diff > 1500l && diff < 2500l);
    // One at start. Second and Third within the sleep. Fourth at final refresh.
    assertEquals(0, dagClientRpc.numGetStatusViaAmInvocations); // No AM available, so no invocations to AM
    assertEquals(4, dagClient.numGetStatusViaRmInvocations);
    assertEquals(DAGStatus.State.SUBMITTED, dagStatus.getState());

    // Fetch from AM. RUNNING
    dagClient.resetCounters();
    dagClientRpc.resetCounters();
    rmDagStatus =
        new DAGStatus(constructDagStatusProto(DAGStatusStateProto.DAG_RUNNING), DagStatusSource.RM);
    dagClient.setRmDagStatus(rmDagStatus);
    dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_RUNNING, -1));

    startTime = System.currentTimeMillis();
    dagStatus = dagClient.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000l);
    endTime = System.currentTimeMillis();
    diff = endTime - startTime;
    assertTrue(diff > 1500l && diff < 2500l);
    // Directly from AM
    assertEquals(0, dagClient.numGetStatusViaRmInvocations);
    // Directly from AM - one refresh. One with timeout.
    assertEquals(2, dagClientRpc.numGetStatusViaAmInvocations);
    assertEquals(DAGStatus.State.RUNNING, dagStatus.getState());


    // Fetch from AM. Success.
    dagClient.resetCounters();
    dagClientRpc.resetCounters();
    rmDagStatus =
        new DAGStatus(constructDagStatusProto(DAGStatusStateProto.DAG_RUNNING), DagStatusSource.RM);
    dagClient.setRmDagStatus(rmDagStatus);
    dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_SUCCEEDED, 1000l));

    startTime = System.currentTimeMillis();
    dagStatus = dagClient.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000l);
    endTime = System.currentTimeMillis();
    diff = endTime - startTime;
    assertTrue(diff > 500l && diff < 1500l);
    // Directly from AM
    assertEquals(0, dagClient.numGetStatusViaRmInvocations);
    // Directly from AM - previous request cached, so single invocation only.
    assertEquals(1, dagClientRpc.numGetStatusViaAmInvocations);
    assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());

  }

  @Test(timeout = 5000)
  public void testDagClientTimelineEnabledCondition() throws IOException {
    String historyLoggingClass = "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService";

    testAtsEnabled(mockAppId, dagIdStr, false, "", true, true);
    testAtsEnabled(mockAppId, dagIdStr, false, historyLoggingClass, false, true);
    testAtsEnabled(mockAppId, dagIdStr, false, historyLoggingClass, true, false);
    testAtsEnabled(mockAppId, dagIdStr, DAGClientTimelineImpl.isSupported(), historyLoggingClass,
        true, true);
  }

  private static void testAtsEnabled(ApplicationId appId, String dagIdStr, boolean expected,
                                     String loggingClass, boolean amHistoryLoggingEnabled,
                                     boolean dagHistoryLoggingEnabled) throws IOException {
    TezConfiguration tezConf = new TezConfiguration();

    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, loggingClass);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED, amHistoryLoggingEnabled);
    tezConf.setBoolean(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED, dagHistoryLoggingEnabled);

    DAGClientImplForTest dagClient = new DAGClientImplForTest(appId, dagIdStr, tezConf, null);
    assertEquals(expected, dagClient.getIsATSEnabled());
  }

  private static class DAGClientRPCImplForTest extends DAGClientRPCImpl {
    private AtomicReference<IOException> faultAMInjectedRef;
    int numGetStatusViaAmInvocations = 0;

    public DAGClientRPCImplForTest(ApplicationId appId, String dagId,
                                   TezConfiguration conf,
                                   @Nullable FrameworkClient frameworkClient) throws IOException {
      super(appId, dagId, conf, frameworkClient, UserGroupInformation.getCurrentUser());
      faultAMInjectedRef = new AtomicReference<>(null);
    }

    void setAMProxy(DAGClientAMProtocolBlockingPB proxy) {
      this.proxy = proxy;
    }

    void resetCounters() {
      numGetStatusViaAmInvocations = 0;
    }

    @Override
    boolean createAMProxyIfNeeded() throws IOException, TezException {
      if (proxy == null) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    DAGStatus getDAGStatusViaAM(Set<StatusGetOpts> statusOptions, long timeout)
        throws IOException, TezException {
      numGetStatusViaAmInvocations++;
      if (faultAMInjectedRef.get() != null) {
        throw faultAMInjectedRef.get();
      }
      return super.getDAGStatusViaAM(statusOptions, timeout);
    }

    void injectAMFault(IOException exception) {
      faultAMInjectedRef.set(exception);
    }
  }

  private static class DAGClientImplForTest extends DAGClientImpl {

    private DAGStatus rmDagStatus;
    int numGetStatusViaRmInvocations = 0;
    private volatile boolean faultInjected;
    public DAGClientImplForTest(ApplicationId appId, String dagId, TezConfiguration conf,
        @Nullable FrameworkClient frameworkClient) throws IOException {
      super(appId, dagId, conf, frameworkClient, UserGroupInformation.getCurrentUser());
    }

    private void setRealClient(DAGClientRPCImplForTest dagClientRpcImplForTest) {
      this.realClient = dagClientRpcImplForTest;
    }

    void setRmDagStatus(DAGStatus rmDagStatus) {
      this.rmDagStatus = rmDagStatus;
    }

    void resetCounters() {
      numGetStatusViaRmInvocations = 0;
    }

    @Override
    protected DAGStatus getDAGStatusViaRM() throws TezException, IOException {
      numGetStatusViaRmInvocations++;
      if (faultInjected) {
        throw new IOException("Fault Injected for RM");
      }
      return rmDagStatus;
    }

    public boolean getIsATSEnabled() {
      return isATSEnabled;
    }

    void injectFault() {
      faultInjected = true;
    }

    DAGStatus getCachedDAGStatus() {
      CachedEntity<DAGStatus> cacheRef = getCachedDAGStatusRef();
      return cacheRef.getValue();
    }

    void enforceExpirationCachedDAGStatus() {
      getCachedDAGStatusRef().enforceExpiration();
    }
  }

  private DAGProtos.DAGStatusProto.Builder constructDagStatusProto(DAGStatusStateProto stateProto) {
    DAGProtos.DAGStatusProto.Builder builder = DAGProtos.DAGStatusProto.newBuilder();
    builder.setState(stateProto);
    return builder;
  }

  private DAGClientAMProtocolBlockingPB createMockProxy(final DAGStatusStateProto stateProto,
                                                        final long timeout) throws
      ServiceException {
    DAGClientAMProtocolBlockingPB mock = mock(DAGClientAMProtocolBlockingPB.class);

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        GetDAGStatusRequestProto request = (GetDAGStatusRequestProto) invocation.getArguments()[1];
        long sleepTime = request.getTimeout();
        if (timeout != -1) {
          sleepTime = timeout;
        }
        Thread.sleep(sleepTime);
        return GetDAGStatusResponseProto.newBuilder().setDagStatus(constructDagStatusProto(
            stateProto)).build();
      }
    }).when(mock).getDAGStatus(isNull(), any());
    return mock;
  }

  @Test
  /* testing idea is borrowed from YARN-5309 */
  public void testTimelineClientCleanup() throws Exception {
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set("yarn.http.policy", "HTTPS_ONLY");

    File testDir = new File(System.getProperty("java.io.tmpdir"));
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestDAGClient.class);
    KeyStoreTestUtil.setupSSLConfig(testDir.getAbsolutePath(), sslConfDir, tezConf, false);

    DAGClientTimelineImpl dagClient =
        new DAGClientTimelineImpl(mockAppId, dagIdStr, tezConf, mock(FrameworkClient.class), 10000);
    Field field = DAGClientTimelineImpl.class.getDeclaredField("timelineReaderStrategy");
    field.setAccessible(true);
    TimelineReaderStrategy strategy = (TimelineReaderStrategy) field.get(dagClient);
    strategy.getHttpClient(); // calls SSLFactory.init

    ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

    while (threadGroup.getParent() != null) {
      threadGroup = threadGroup.getParent();
    }

    Thread[] threads = new Thread[threadGroup.activeCount()];

    threadGroup.enumerate(threads);
    Thread reloaderThread = null;
    for (Thread thread : threads) {
      /* Since HADOOP-16524, the reloader thread's name is changed, let's handle the backward compatibility
       * with a simple OR, as this is just a unit test, it's not worth involving a hadoop version check.
       */
      if ((thread.getName() != null) && (thread.getName().contains("Truststore reloader thread"))
          || (thread.getName().contains("SSL Certificates Store Monitor"))) {
        reloaderThread = thread;
      }
    }
    Assert.assertTrue("Reloader is not alive", reloaderThread.isAlive());

    dagClient.close();
    boolean reloaderStillAlive = true;
    for (int i = 0; i < 10; i++) {
      reloaderStillAlive = reloaderThread.isAlive();
      if (!reloaderStillAlive) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertFalse("Reloader is still alive", reloaderStillAlive);
  }

  @Test(timeout = 50000)
  public void testGetDagStatusWithCachedStatusExpiration() throws Exception {
    long startTime;
    long endTime;
    long diff;

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.setLong(TezConfiguration.TEZ_DAG_STATUS_POLLINTERVAL_MS, 800L);
    tezConf.setLong(TezConfiguration.TEZ_CLIENT_DAG_STATUS_CACHE_TIMEOUT_SECS, 100000L);
    try (DAGClientImplForTest dagClientImpl =
        new DAGClientImplForTest(mockAppId, dagIdStr, tezConf, null)) {
      DAGClientRPCImplForTest dagClientRpc =
          new DAGClientRPCImplForTest(mockAppId, dagIdStr, tezConf, null);
      dagClientImpl.setRealClient(dagClientRpc);

      DAGStatus dagStatus;
      DAGStatus rmDagStatus;

      // Fetch from AM. RUNNING
      rmDagStatus =
          new DAGStatus(constructDagStatusProto(DAGStatusStateProto.DAG_RUNNING),
              DagStatusSource.RM);
      dagClientImpl.setRmDagStatus(rmDagStatus);
      dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_RUNNING, -1));

      startTime = System.currentTimeMillis();
      dagStatus = dagClientImpl.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000L);
      endTime = System.currentTimeMillis();
      diff = endTime - startTime;
      assertTrue(diff > 1500L && diff < 2500L);
      // Directly from AM
      assertEquals(0, dagClientImpl.numGetStatusViaRmInvocations);
      // Directly from AM - one refresh. One with timeout.
      assertEquals(2, dagClientRpc.numGetStatusViaAmInvocations);
      assertEquals(DAGStatus.State.RUNNING, dagStatus.getState());

      // Fetch from AM. Success.
      dagClientImpl.resetCounters();
      dagClientRpc.resetCounters();
      rmDagStatus =
          new DAGStatus(constructDagStatusProto(DAGStatusStateProto.DAG_RUNNING),
              DagStatusSource.RM);
      dagClientImpl.setRmDagStatus(rmDagStatus);
      dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_SUCCEEDED, 1000L));

      startTime = System.currentTimeMillis();
      dagStatus = dagClientImpl.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class), 2000L);
      endTime = System.currentTimeMillis();
      diff = endTime - startTime;
      assertTrue("diff is " + diff, diff > 500L && diff < 1500L);
      // Directly from AM
      assertEquals(0, dagClientImpl.numGetStatusViaRmInvocations);
      // Directly from AM - previous request cached, so single invocation only.
      assertEquals(1, dagClientRpc.numGetStatusViaAmInvocations);
      assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());

      // verify that the cachedDAGStatus is correct
      DAGStatus cachedDagStatus = dagClientImpl.getCachedDAGStatus();
      Assert.assertNotNull(cachedDagStatus);
      Assert.assertSame(dagStatus, cachedDagStatus);

      // When AM proxy throws an exception, the cachedDAGStatus should be returned
      dagClientImpl.resetCounters();
      dagClientRpc.resetCounters();
      dagClientRpc.injectAMFault(new IOException("injected Fault"));
      dagStatus = dagClientImpl.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class));
      // get the Status from the cache
      assertEquals(0, dagClientImpl.numGetStatusViaRmInvocations);
      // Directly from AM - previous request cached, so single invocation only.
      assertEquals(1, dagClientRpc.numGetStatusViaAmInvocations);
      assertEquals(DAGStatus.State.SUCCEEDED, dagStatus.getState());
      Assert.assertSame(dagStatus, cachedDagStatus);

      // test that RM is invoked when the cacheExpires and the AM fails.
      dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_SUCCEEDED, 1000L));
      dagClientRpc.injectAMFault(new IOException("injected AM Fault"));
      dagClientImpl.resetCounters();
      dagClientRpc.resetCounters();
      dagClientImpl.enforceExpirationCachedDAGStatus();
      dagStatus = dagClientImpl.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class));
      // get the Status from the cache
      assertEquals(1, dagClientImpl.numGetStatusViaRmInvocations);
      assertEquals(1, dagClientRpc.numGetStatusViaAmInvocations);
      assertEquals(State.RUNNING, dagStatus.getState());
      Assert.assertNotSame(dagStatus, cachedDagStatus);

      // verify that the cachedDAGStatus is null because AM threw exception before setting the
      // cache.
      cachedDagStatus = dagClientImpl.getCachedDAGStatus();
      Assert.assertNull(cachedDagStatus);
      Assert.assertNotNull(dagStatus);

      // inject fault in RM too. getDAGStatus should return null;
      dagClientImpl.resetCounters();
      dagClientRpc.resetCounters();
      dagClientRpc.setAMProxy(createMockProxy(DAGStatusStateProto.DAG_SUCCEEDED, 1000L));
      dagClientImpl.injectFault();
      try {
        dagClientImpl.getDAGStatus(EnumSet.noneOf(StatusGetOpts.class));
        Assert.fail("The RM should throw IOException");
      } catch (IOException ioException) {
        Assert.assertEquals(ioException.getMessage(), "Fault Injected for RM");
        assertEquals(1, dagClientImpl.numGetStatusViaRmInvocations);
        assertEquals(1, dagClientRpc.numGetStatusViaAmInvocations);
      }
    }
  }
}

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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientImpl;
import org.apache.tez.dag.api.client.DAGClientTimelineImpl;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DagStatusSource;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
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
                    .addDiagnostics("V_Diagnostics_0")
                    .setProgress(vertexProgressProto)
                    .setState(VertexStatusStateProto.VERTEX_SUCCEEDED)  // make sure the waitForCompletion be able to finish
                    .build();
    vertexStatusProtoWithCounters = VertexStatusProto.newBuilder(vertexStatusProtoWithoutCounters)
                    .setVertexCounters(vertexCountersProto)
                    .build();
  }
  
  private static class DAGCounterRequestMatcher extends ArgumentMatcher<GetDAGStatusRequestProto>{

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof GetDAGStatusRequestProto){
        GetDAGStatusRequestProto requestProto = (GetDAGStatusRequestProto)argument;
        return requestProto.getStatusOptionsCount() != 0
            && requestProto.getStatusOptionsList().get(0) == StatusGetOptsProto.GET_COUNTERS;
      }
      return false;
    }
  }
  
  private static class VertexCounterRequestMatcher extends ArgumentMatcher<GetVertexStatusRequestProto>{

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof GetVertexStatusRequestProto){
        GetVertexStatusRequestProto requestProto = (GetVertexStatusRequestProto)argument;
        return requestProto.getStatusOptionsCount() != 0
            && requestProto.getStatusOptionsList().get(0) == StatusGetOptsProto.GET_COUNTERS;
      }
      return false;
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
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build());
    when(mockProxy.getDAGStatus(isNull(RpcController.class), argThat(new DAGCounterRequestMatcher())))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithCounters).build());
    
    when(mockProxy.getVertexStatus(isNull(RpcController.class), any(GetVertexStatusRequestProto.class)))
      .thenReturn(GetVertexStatusResponseProto.newBuilder().setVertexStatus(vertexStatusProtoWithoutCounters).build());
    when(mockProxy.getVertexStatus(isNull(RpcController.class), argThat(new VertexCounterRequestMatcher())))
      .thenReturn(GetVertexStatusResponseProto.newBuilder().setVertexStatus(vertexStatusProtoWithCounters).build());
   
    dagClient = new DAGClientImpl(mockAppId, dagIdStr, new TezConfiguration(), null);
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
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
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
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
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

    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
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
    dagClientRpc.resetCountesr();
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
    dagClientRpc.resetCountesr();
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
  public void testDagClientTimelineEnabledCondition() {
    String historyLoggingClass = "org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService";

    testAtsEnabled(mockAppId, dagIdStr, false, "", true, true);
    testAtsEnabled(mockAppId, dagIdStr, false, historyLoggingClass, false, true);
    testAtsEnabled(mockAppId, dagIdStr, false, historyLoggingClass, true, false);
    testAtsEnabled(mockAppId, dagIdStr, DAGClientTimelineImpl.isSupported(), historyLoggingClass,
        true, true);
  }

  private static void testAtsEnabled(ApplicationId appId, String dagIdStr, boolean expected,
                                     String loggingClass, boolean amHistoryLoggingEnabled,
                                     boolean dagHistoryLoggingEnabled) {
    TezConfiguration tezConf = new TezConfiguration();

    tezConf.set(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, loggingClass);
    tezConf.setBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED, amHistoryLoggingEnabled);
    tezConf.setBoolean(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED, dagHistoryLoggingEnabled);

    DAGClientImplForTest dagClient = new DAGClientImplForTest(appId, dagIdStr, tezConf, null);
    assertEquals(expected, dagClient.getIsATSEnabled());
  }

  private static class DAGClientRPCImplForTest extends DAGClientRPCImpl {

    int numGetStatusViaAmInvocations = 0;

    public DAGClientRPCImplForTest(ApplicationId appId, String dagId,
                                   TezConfiguration conf,
                                   @Nullable FrameworkClient frameworkClient) {
      super(appId, dagId, conf, frameworkClient);
    }

    void setAMProxy(DAGClientAMProtocolBlockingPB proxy) {
      this.proxy = proxy;
    }

    void resetCountesr() {
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
      return super.getDAGStatusViaAM(statusOptions, timeout);
    }
  }

  private static class DAGClientImplForTest extends DAGClientImpl {

    private DAGStatus rmDagStatus;
    int numGetStatusViaRmInvocations = 0;

    public DAGClientImplForTest(ApplicationId appId, String dagId,
                                TezConfiguration conf,
                                @Nullable FrameworkClient frameworkClient) {
      super(appId, dagId, conf, frameworkClient);
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
      return rmDagStatus;
    }

    public boolean getIsATSEnabled() {
      return isATSEnabled;
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

    doAnswer(new Answer() {
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
    }).when(mock).getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class));
    return mock;
  }
}

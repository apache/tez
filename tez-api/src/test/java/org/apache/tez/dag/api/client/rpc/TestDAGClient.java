package org.apache.tez.dag.api.client.rpc;


import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusResponseProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;
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
import org.mockito.ArgumentMatcher;
import org.mockito.internal.util.collections.Sets;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;


public class TestDAGClient {

  private DAGClientRPCImpl dagClient;
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
   
    
    
    dagClient = new DAGClientRPCImpl(mockAppId, dagIdStr, new TezConfiguration());
    dagClient.appReport = mockAppReport;
    ((DAGClientRPCImpl)dagClient).proxy = mockProxy;
  }
  
  @Test
  public void testApp() throws IOException, TezException, ServiceException{
    assertEquals(mockAppId, dagClient.getApplicationId());
    assertEquals(mockAppReport, dagClient.getApplicationReport());
  }
  
  @Test
  public void testDAGStatus() throws Exception{
    DAGStatus resultDagStatus = dagClient.getDAGStatus(null);
    verify(mockProxy, times(1)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).build());
    assertEquals(new DAGStatus(dagStatusProtoWithoutCounters), resultDagStatus);
    System.out.println("DAGStatusWithoutCounter:" + resultDagStatus);
    
    resultDagStatus = dagClient.getDAGStatus(Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy, times(1)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).addStatusOptions(StatusGetOptsProto.GET_COUNTERS).build());
    assertEquals(new DAGStatus(dagStatusProtoWithCounters), resultDagStatus);
    System.out.println("DAGStatusWithCounter:" + resultDagStatus);
  }
  
  @Test
  public void testVertexStatus() throws Exception{
    VertexStatus resultVertexStatus = dagClient.getVertexStatus("v1", null);
    verify(mockProxy).getVertexStatus(null, GetVertexStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setVertexName("v1").build());
    assertEquals(new VertexStatus(vertexStatusProtoWithoutCounters), resultVertexStatus);
    System.out.println("VertexWithoutCounter:" + resultVertexStatus);
    
    resultVertexStatus = dagClient.getVertexStatus("v1", Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy).getVertexStatus(null, GetVertexStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).setVertexName("v1").addStatusOptions(StatusGetOptsProto.GET_COUNTERS).build());
    assertEquals(new VertexStatus(vertexStatusProtoWithCounters), resultVertexStatus);
    System.out.println("VertexWithCounter:" + resultVertexStatus);
  }
  
  @Test
  public void testTryKillDAG() throws Exception{
    dagClient.tryKillDAG();
    verify(mockProxy, times(1)).tryKillDAG(null, TryKillDAGRequestProto.newBuilder()
        .setDagId(dagIdStr).build());
  }
  
  @Test
  public void testWaitForCompletion() throws Exception{
    // first time return DAG_RUNNING, second time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
                    (DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters).setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
                 .build());
      
    dagClient.waitForCompletion();
    verify(mockProxy, times(2)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
      .setDagId(dagIdStr).build());
  }
  
  @Test
  public void testWaitForCompletionWithStatusUpdates() throws Exception{
    // first time return DAG_RUNNING, second time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
                  (DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters).setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
               .build());
    dagClient.waitForCompletionWithStatusUpdates(null, null);
    verify(mockProxy, times(2)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).build());
    
    // first time return DAG_RUNNING, second time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
    .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithCounters).build())
    .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
                (DAGStatusProto.newBuilder(dagStatusProtoWithCounters).setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
             .build());
    dagClient.waitForCompletionWithStatusUpdates(Sets.newSet(new Vertex("v1",null, 1, Resource.newInstance(1024, 1))), 
        Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy, times(2)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).addStatusOptions(StatusGetOptsProto.GET_COUNTERS).build());
  }
  
  @Test
  public void testWaitForCompletionWithAllStatusUpdates() throws Exception{

    // first time and second time return DAG_RUNNING, third time return DAG_SUCCEEDED
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithoutCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
                (DAGStatusProto.newBuilder(dagStatusProtoWithoutCounters).setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
             .build());
    
    //  first time for getVertexSet
    //  second & third time for check completion
    dagClient.waitForCompletionWithAllStatusUpdates(null);
    verify(mockProxy, times(3)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
        .setDagId(dagIdStr).build());

    
    when(mockProxy.getDAGStatus(isNull(RpcController.class), any(GetDAGStatusRequestProto.class)))
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus(dagStatusProtoWithCounters).build())
      .thenReturn(GetDAGStatusResponseProto.newBuilder().setDagStatus
                (DAGStatusProto.newBuilder(dagStatusProtoWithCounters).setState(DAGStatusStateProto.DAG_SUCCEEDED).build())
             .build());
    dagClient.waitForCompletionWithAllStatusUpdates(Sets.newSet(StatusGetOpts.GET_COUNTERS));
    verify(mockProxy, times(3)).getDAGStatus(null, GetDAGStatusRequestProto.newBuilder()
      .setDagId(dagIdStr).addStatusOptions(StatusGetOptsProto.GET_COUNTERS).build());
  }
  
}

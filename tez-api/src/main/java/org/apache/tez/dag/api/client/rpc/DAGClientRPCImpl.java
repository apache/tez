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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusStateProto;

import com.google.protobuf.ServiceException;

public class DAGClientRPCImpl implements DAGClient {
  private static final Log LOG = LogFactory.getLog(DAGClientRPCImpl.class);

  private final ApplicationId appId;
  private final String dagId;
  private final TezConfiguration conf;
  private ApplicationReport appReport;
  private YarnClient yarnClient;
  private DAGClientAMProtocolBlockingPB proxy = null;

  public DAGClientRPCImpl(ApplicationId appId, String dagId,
      TezConfiguration conf) {
    this.appId = appId;
    this.dagId = dagId;
    this.conf = conf;
    yarnClient = new YarnClientImpl();
    yarnClient.init(new YarnConfiguration(conf));
    yarnClient.start();
    appReport = null;
  }

  @Override
  public ApplicationId getApplicationId() {
    return appId;
  }

  @Override
  public DAGStatus getDAGStatus(Set<StatusGetOpts> statusOptions)
      throws IOException, TezException {
    if(createAMProxyIfNeeded()) {
      try {
        return getDAGStatusViaAM(statusOptions);
      } catch (TezException e) {
        resetProxy(e); // create proxy again
      }
    }

    // Later maybe from History
    return getDAGStatusViaRM();
  }

  @Override
  public VertexStatus getVertexStatus(String vertexName,
      Set<StatusGetOpts> statusOptions)
      throws IOException, TezException {
    if(createAMProxyIfNeeded()) {
      try {
        return getVertexStatusViaAM(vertexName, statusOptions);
      } catch (TezException e) {
        resetProxy(e); // create proxy again
      }
    }

    // need AM for this. Later maybe from History
    return null;
  }



  @Override
  public void tryKillDAG() throws TezException, IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("TryKill for app: " + appId + " dag:" + dagId);
    }
    if(createAMProxyIfNeeded()) {
      TryKillDAGRequestProto requestProto =
          TryKillDAGRequestProto.newBuilder().setDagId(dagId).build();
      try {
        proxy.tryKillDAG(null, requestProto);
      } catch (ServiceException e) {
        resetProxy(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
    if(yarnClient != null) {
      yarnClient.stop();
    }
  }

  @Override
  public ApplicationReport getApplicationReport() {
    return appReport;
  }

  void resetProxy(Exception e) {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Resetting AM proxy for app: " + appId + " dag:" + dagId +
          " due to exception :", e);
    }
    proxy = null;
  }

  DAGStatus getDAGStatusViaAM(Set<StatusGetOpts> statusOptions)
      throws IOException, TezException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("GetDAGStatus via AM for app: " + appId + " dag:" + dagId);
    }
    GetDAGStatusRequestProto.Builder requestProtoBuilder =
        GetDAGStatusRequestProto.newBuilder()
          .setDagId(dagId);

    if (statusOptions != null) {
      requestProtoBuilder.addAllStatusOptions(
        DagTypeConverters.convertStatusGetOptsToProto(statusOptions));
    }

    try {
      return new DAGStatus(
        proxy.getDAGStatus(null,
          requestProtoBuilder.build()).getDagStatus());
    } catch (ServiceException e) {
      // TEZ-151 retrieve wrapped TezException
      throw new TezException(e);
    }
  }

  DAGStatus getDAGStatusViaRM() throws TezException, IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("GetDAGStatus via AM for app: " + appId + " dag:" + dagId);
    }
    ApplicationReport appReport;
    try {
      appReport = yarnClient.getApplicationReport(appId);
    } catch (YarnException e) {
      throw new TezException(e);
    }

    if(appReport == null) {
      throw new TezException("Unknown/Invalid appId: " + appId);
    }

    DAGStatusProto.Builder builder = DAGStatusProto.newBuilder();
    DAGStatus dagStatus = new DAGStatus(builder);
    DAGStatusStateProto dagState;
    switch (appReport.getYarnApplicationState()) {
    case NEW:
    case NEW_SAVING:
    case SUBMITTED:
    case ACCEPTED:
      dagState = DAGStatusStateProto.DAG_SUBMITTED;
      break;
    case RUNNING:
      dagState = DAGStatusStateProto.DAG_RUNNING;
      break;
    case FAILED:
      dagState = DAGStatusStateProto.DAG_FAILED;
      break;
    case KILLED:
      dagState = DAGStatusStateProto.DAG_KILLED;
      break;
    case FINISHED:
      switch(appReport.getFinalApplicationStatus()) {
      case UNDEFINED:
      case FAILED:
        dagState = DAGStatusStateProto.DAG_FAILED;
        break;
      case KILLED:
        dagState = DAGStatusStateProto.DAG_KILLED;
        break;
      case SUCCEEDED:
        dagState = DAGStatusStateProto.DAG_SUCCEEDED;
        break;
      default:
        throw new TezUncheckedException("Encountered unknown final application"
          + " status from YARN"
          + ", appState=" + appReport.getYarnApplicationState()
          + ", finalStatus=" + appReport.getFinalApplicationStatus());
      }
      break;
    default:
      throw new TezUncheckedException("Encountered unknown application state"
          + " from YARN, appState=" + appReport.getYarnApplicationState());
    }

    builder.setState(dagState);
    if(appReport.getDiagnostics() != null) {
      builder.addAllDiagnostics(Collections.singleton(appReport.getDiagnostics()));
    }

    return dagStatus;
  }

  VertexStatus getVertexStatusViaAM(String vertexName,
      Set<StatusGetOpts> statusOptions)
      throws TezException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GetVertexStatus via AM for app: " + appId + " dag: " + dagId
          + " vertex: " + vertexName);
    }
    GetVertexStatusRequestProto.Builder requestProtoBuilder =
        GetVertexStatusRequestProto.newBuilder()
          .setDagId(dagId)
          .setVertexName(vertexName);

    if (statusOptions != null) {
      requestProtoBuilder.addAllStatusOptions(
        DagTypeConverters.convertStatusGetOptsToProto(statusOptions));
    }

    try {
      return new VertexStatus(
        proxy.getVertexStatus(null,
          requestProtoBuilder.build()).getVertexStatus());
    } catch (ServiceException e) {
      // TEZ-151 retrieve wrapped TezException
      throw new TezException(e);
    }
  }

  ApplicationReport getAppReport() throws IOException, TezException {
    try {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("App: " + appId + " in state: "
            + appReport.getYarnApplicationState());
      }
      return appReport;
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  boolean createAMProxyIfNeeded() throws IOException, TezException {
    if(proxy != null) {
      // if proxy exist optimistically use it assuming there is no retry
      return true;
    }
    appReport = getAppReport();

    if(appReport == null) {
      return false;
    }
    YarnApplicationState appState = appReport.getYarnApplicationState();
    if(appState != YarnApplicationState.RUNNING) {
      return false;
    }

    // YARN-808. Cannot ascertain if AM is ready until we connect to it.
    // workaround check the default string set by YARN
    if(appReport.getHost() == null || appReport.getHost().equals("N/A") ||
        appReport.getRpcPort() == 0){
      // attempt not running
      return false;
    }

    proxy = TezClientUtils.getAMProxy(conf, appReport.getHost(), appReport.getRpcPort(),
        appReport.getClientToAMToken());
    return true;
  }

}

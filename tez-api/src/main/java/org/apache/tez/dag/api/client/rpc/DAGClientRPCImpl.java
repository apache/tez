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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.tez.common.RPCUtil;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.client.DAGClientInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.DAGNotRunningException;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.DagStatusSource;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;

@Private
public class DAGClientRPCImpl extends DAGClientInternal {

  private static final Logger LOG = LoggerFactory.getLogger(DAGClientRPCImpl.class);

  private static final String DAG_NOT_RUNNING_CLASS_NAME =
      DAGNotRunningException.class.getCanonicalName();
  private final ApplicationId appId;
  private final String dagId;
  private final TezConfiguration conf;
  @VisibleForTesting
  ApplicationReport appReport;
  private final FrameworkClient frameworkClient;
  @VisibleForTesting
  DAGClientAMProtocolBlockingPB proxy = null;

  public DAGClientRPCImpl(ApplicationId appId, String dagId,
      TezConfiguration conf, @Nullable FrameworkClient frameworkClient) {
    this.appId = appId;
    this.dagId = dagId;
    this.conf = conf;
    this.frameworkClient = frameworkClient;
    appReport = null;
  }

  @Override
  public String getExecutionContext() {
    return "Executing on YARN cluster with App id " + appId;
  }

  @Override
  public DAGStatus getDAGStatus(Set<StatusGetOpts> statusOptions)
      throws IOException, TezException, ApplicationNotFoundException {
    return getDAGStatus(statusOptions, 0);
  }


  @Override
  public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions,
      long timeout) throws IOException, TezException, ApplicationNotFoundException {
    if (createAMProxyIfNeeded()) {
      try {
        DAGStatus dagStatus = getDAGStatusViaAM(statusOptions, timeout);
        return dagStatus;
      } catch (TezException e) {
        resetProxy(e); // create proxy again
        throw e;
      } catch (IOException e) {
        resetProxy(e); // create proxy again
        throw e;
      }
    }

    // either the dag is not running or some exception happened
    return null;
  }

  @Override
  public VertexStatus getVertexStatus(String vertexName,
      Set<StatusGetOpts> statusOptions)
      throws IOException, TezException, ApplicationNotFoundException {

    if(createAMProxyIfNeeded()) {
      try {
        return getVertexStatusViaAM(vertexName, statusOptions);
      } catch (TezException e) {
        resetProxy(e); // create proxy again
        throw e;
      } catch (IOException e) {
        resetProxy(e); // create proxy again
        throw e;
      }
    }

    return null;
  }



  @Override
  public void tryKillDAG() throws TezException, IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("TryKill for app: " + appId + " dag:" + dagId);
    }
    try {
      if (createAMProxyIfNeeded()) {
        TryKillDAGRequestProto requestProto =
            TryKillDAGRequestProto.newBuilder().setDagId(dagId).build();
        try {
          proxy.tryKillDAG(null, requestProto);
        } catch (ServiceException e) {
          resetProxy(e);
        }
      }
    } catch (ApplicationNotFoundException e) {
      throw new SessionNotRunning("Application already completed");
    }
  }

  @Override
  public void close() throws IOException {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  protected ApplicationReport getApplicationReportInternal() {
    return appReport;
  }

  void resetProxy(Exception e) {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Resetting AM proxy for app: " + appId + " dag:" + dagId +
          " due to exception :", e);
    }
    proxy = null;
  }

  DAGStatus getDAGStatusViaAM(Set<StatusGetOpts> statusOptions, long timeout)
      throws IOException, TezException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("GetDAGStatus via AM for app: " + appId + " dag:" + dagId);
    }
    GetDAGStatusRequestProto.Builder requestProtoBuilder =
        GetDAGStatusRequestProto.newBuilder()
          .setDagId(dagId).setTimeout(timeout);

    if (statusOptions != null) {
      requestProtoBuilder.addAllStatusOptions(
        DagTypeConverters.convertStatusGetOptsToProto(statusOptions));
    }

    try {
      return new DAGStatus(
        proxy.getDAGStatus(null,
          requestProtoBuilder.build()).getDagStatus(), DagStatusSource.AM);
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      // Should not reach here
      throw new TezException(e);
    }
  }

  VertexStatus getVertexStatusViaAM(String vertexName,
      Set<StatusGetOpts> statusOptions)
      throws TezException, IOException {
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
      RPCUtil.unwrapAndThrowException(e);
      // Should not reach here
      throw new TezException(e);
    }
  }

  ApplicationReport getAppReport() throws IOException, TezException,
      ApplicationNotFoundException {
    try {
      ApplicationReport appReport = frameworkClient.getApplicationReport(appId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("App: " + appId + " in state: "
            + appReport.getYarnApplicationState());
      }
      return appReport;
    } catch (ApplicationNotFoundException e) {
      throw e;
    } catch (YarnException e) {
      throw new TezException(e);
    }
  }

  boolean createAMProxyIfNeeded() throws IOException, TezException,
      ApplicationNotFoundException {
    if(proxy != null) {
      // if proxy exist optimistically use it assuming there is no retry
      return true;
    }
    appReport = null;
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

  @Override
  public DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException {
    // should be used from DAGClientImpl
    throw new TezException("not supported");
  }

  @Override
  public DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<StatusGetOpts> statusGetOpts)
      throws IOException, TezException, InterruptedException {
    // should be used from DAGClientImpl
    throw new TezException("not supported");
  }

}

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
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

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
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetDAGStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.GetVertexStatusRequestProto;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.TryKillDAGRequestProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusStateProto;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;

public class DAGClientRPCImpl implements DAGClient {
  private static final Log LOG = LogFactory.getLog(DAGClientRPCImpl.class);

  private static final long SLEEP_FOR_COMPLETION = 500;
  private final DecimalFormat formatter = new DecimalFormat("###.##%");
  private final ApplicationId appId;
  private final String dagId;
  private final TezConfiguration conf;
  @VisibleForTesting
  ApplicationReport appReport;
  private YarnClient yarnClient;
  @VisibleForTesting
  DAGClientAMProtocolBlockingPB proxy = null;

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

  @Override
  public DAGStatus waitForCompletion() throws IOException, TezException {
    return waitForCompletionWithStatusUpdates(null, EnumSet.noneOf(StatusGetOpts.class));
  }

  @Override
  public DAGStatus waitForCompletionWithAllStatusUpdates(@Nullable Set<StatusGetOpts> statusGetOpts)
      throws IOException, TezException {
    Set<String> vertexSet = getDAGStatus(statusGetOpts).getVertexProgress().keySet();
    return _waitForCompletionWithStatusUpdates(vertexSet, statusGetOpts);
  }

  @Override
  public DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<Vertex> vertices,
      @Nullable Set<StatusGetOpts> statusGetOpts) throws IOException, TezException {
    Set<String> vertexNames = new HashSet<String>();
    if (vertices != null) {
      for (Vertex vertex : vertices) {
        vertexNames.add(vertex.getVertexName());
      }
    }
    return _waitForCompletionWithStatusUpdates(vertexNames, statusGetOpts);
  }

  private DAGStatus _waitForCompletionWithStatusUpdates(@Nullable Set<String> vertexNames,
      @Nullable Set<StatusGetOpts> statusGetOpts) throws IOException, TezException {
    DAGStatus dagStatus;
    boolean initPrinted = false;
    double dagProgress = -1.0; // Print the first one
    // monitoring
    while (true) {
      dagStatus = getDAGStatus(statusGetOpts);
      if (!initPrinted
          && (dagStatus.getState() == DAGStatus.State.INITING || dagStatus.getState() == DAGStatus.State.SUBMITTED)) {
        initPrinted = true; // Print once
        log("Waiting for DAG to start running");
      }
      if (dagStatus.getState() == DAGStatus.State.RUNNING
          || dagStatus.getState() == DAGStatus.State.SUCCEEDED
          || dagStatus.getState() == DAGStatus.State.FAILED
          || dagStatus.getState() == DAGStatus.State.KILLED
          || dagStatus.getState() == DAGStatus.State.ERROR) {
        break;
      }
      try {
        Thread.sleep(SLEEP_FOR_COMPLETION);
      } catch (InterruptedException e) {
        // continue;
      }
    }// End of while(true)
    while (dagStatus.getState() == DAGStatus.State.RUNNING) {
      if (vertexNames != null) {
        dagProgress = monitorProgress(vertexNames, dagProgress, null, dagStatus);
      }
      try {
        Thread.sleep(SLEEP_FOR_COMPLETION);
      } catch (InterruptedException e) {
      }
      dagStatus = getDAGStatus(statusGetOpts);
    }// end of while
    if (vertexNames != null) {
      // Always print the last status irrespective of progress change
      monitorProgress(vertexNames, -1.0, statusGetOpts, dagStatus);
    }
    log("DAG completed. " + "FinalState=" + dagStatus.getState());
    return dagStatus;
  }

  private double monitorProgress(Set<String> vertexNamess, double prevDagProgress,
      Set<StatusGetOpts> opts, DAGStatus dagStatus) throws IOException, TezException {
    Progress progress = dagStatus.getDAGProgress();
    double dagProgress = 0.0;
    if (progress != null && (dagProgress = getProgress(progress)) > prevDagProgress) {
      printDAGStatus(vertexNamess, opts, dagStatus, progress);
    }
    return dagProgress;
  }

  private void printDAGStatus(Set<String> vertexNamess, Set<StatusGetOpts> opts,
      DAGStatus dagStatus, Progress dagProgress) throws IOException, TezException {
    double vProgressFloat = 0.0f;
    log("DAG: State: " + dagStatus.getState() + " Progress: "
        + formatter.format(getProgress(dagProgress)) + " " + dagProgress);
    boolean displayCounter = opts != null ? opts.contains(StatusGetOpts.GET_COUNTERS) : false;
    if (displayCounter) {
      TezCounters counters = dagStatus.getDAGCounters();
      if (counters != null) {
        log("DAG Counters:\n" + counters);
      }
    }
    for (String vertex : vertexNamess) {
      VertexStatus vStatus = getVertexStatus(vertex, opts);
      if (vStatus == null) {
        log("Could not retrieve status for vertex: " + vertex);
        continue;
      }
      Progress vProgress = vStatus.getProgress();
      if (vProgress != null) {
        vProgressFloat = 0.0f;
        if (vProgress.getTotalTaskCount() == 0) {
          vProgressFloat = 1.0f;
        } else if (vProgress.getTotalTaskCount() > 0) {
          vProgressFloat = getProgress(vProgress);
        }
        log("VertexStatus:" + " VertexName: " + vertex + " Progress: "
            + formatter.format(vProgressFloat) + " " + vProgress);
      }
      if (displayCounter) {
        TezCounters counters = vStatus.getVertexCounters();
        if (counters != null) {
          log("Vertex Counters for " + vertex + ":\n" + counters);
        }
      }
    } // end of for loop
  }

  private double getProgress(Progress progress) {
    return (progress.getTotalTaskCount() == 0 ? 0.0 : (double) (progress.getSucceededTaskCount())
        / progress.getTotalTaskCount());
  }

  private void log(String message) {
    LOG.info(message);
  }
}

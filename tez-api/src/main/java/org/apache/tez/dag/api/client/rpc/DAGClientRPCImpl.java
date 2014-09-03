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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
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

@Private
public class DAGClientRPCImpl extends DAGClient {
  private static final Log LOG = LogFactory.getLog(DAGClientRPCImpl.class);

  private static final long SLEEP_FOR_COMPLETION = 500;
  private static final long PRINT_STATUS_INTERVAL_MILLIS = 5000;
  private final DecimalFormat formatter = new DecimalFormat("###.##%");
  private final ApplicationId appId;
  private final String dagId;
  private final TezConfiguration conf;
  private long lastPrintStatusTimeMillis;
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
    if (frameworkClient != null &&
        conf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT)) {
      this.frameworkClient = frameworkClient;
    } else {
      this.frameworkClient = FrameworkClient.createFrameworkClient(conf);
      this.frameworkClient.init(conf, new YarnConfiguration(conf));
      this.frameworkClient.start();
    }
    appReport = null;
  }

  @Override
  public String getExecutionContext() {
    return new String("Executing on YARN cluster with App id " + appId);
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
    if(frameworkClient != null) {
      frameworkClient.stop();
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
      appReport = frameworkClient.getApplicationReport(appId);
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
      ApplicationReport appReport = frameworkClient.getApplicationReport(appId);
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
  public DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException {
    return _waitForCompletionWithStatusUpdates(false, EnumSet.noneOf(StatusGetOpts.class));
  }

  @Override
  public DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<StatusGetOpts> statusGetOpts)
      throws IOException, TezException, InterruptedException {
    return _waitForCompletionWithStatusUpdates(true, statusGetOpts);
  }

  private DAGStatus _waitForCompletionWithStatusUpdates(boolean vertexUpdates,
      @Nullable Set<StatusGetOpts> statusGetOpts) throws IOException, TezException, InterruptedException {
    DAGStatus dagStatus;
    boolean initPrinted = false;
    boolean runningPrinted = false;
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
      Thread.sleep(SLEEP_FOR_COMPLETION);
    }// End of while(true)

    Set<String> vertexNames = Collections.emptySet();
    while (!dagStatus.isCompleted()) {
      if (!runningPrinted) {
        log("DAG initialized: CurrentState=Running");
        runningPrinted = true;
      }
      if (vertexUpdates && vertexNames.isEmpty()) {
        vertexNames = getDAGStatus(statusGetOpts).getVertexProgress().keySet();
      }
      dagProgress = monitorProgress(vertexNames, dagProgress, null, dagStatus);
      Thread.sleep(SLEEP_FOR_COMPLETION);
      dagStatus = getDAGStatus(statusGetOpts);
    }// end of while
    // Always print the last status irrespective of progress change
    monitorProgress(vertexNames, -1.0, statusGetOpts, dagStatus);
    log("DAG completed. " + "FinalState=" + dagStatus.getState());
    return dagStatus;
  }

  private double monitorProgress(Set<String> vertexNames, double prevDagProgress,
      Set<StatusGetOpts> opts, DAGStatus dagStatus) throws IOException, TezException {
    Progress progress = dagStatus.getDAGProgress();
    double dagProgress = prevDagProgress;
    if (progress != null) {
      dagProgress = getProgress(progress);
      boolean progressChanged = dagProgress > prevDagProgress;
      long currentTimeMillis = System.currentTimeMillis();
      long timeSinceLastPrintStatus =  currentTimeMillis - lastPrintStatusTimeMillis;
      boolean printIntervalExpired = timeSinceLastPrintStatus > PRINT_STATUS_INTERVAL_MILLIS;
      if (progressChanged || printIntervalExpired) {
        lastPrintStatusTimeMillis = currentTimeMillis;
        printDAGStatus(vertexNames, opts, dagStatus, progress);
      }
    }

    return dagProgress;
  }

  private void printDAGStatus(Set<String> vertexNames, Set<StatusGetOpts> opts,
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
    for (String vertex : vertexNames) {
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
        log("\tVertexStatus:" + " VertexName: " + vertex + " Progress: "
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

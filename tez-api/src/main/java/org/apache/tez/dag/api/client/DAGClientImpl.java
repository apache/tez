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

package org.apache.tez.dag.api.client;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAGNotRunningException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.rpc.DAGClientRPCImpl;
import org.apache.tez.dag.api.records.DAGProtos;

@Private
public class DAGClientImpl extends DAGClient {
  private static final Logger LOG = LoggerFactory.getLogger(DAGClientImpl.class);

  private final ApplicationId appId;
  private final String dagId;
  private final TezConfiguration conf;
  private final FrameworkClient frameworkClient;

  @VisibleForTesting
  protected DAGClient realClient;
  private boolean dagCompleted = false;
  private boolean isATSEnabled = false;
  private DAGStatus cachedDagStatus = null;
  Map<String, VertexStatus> cachedVertexStatus = new HashMap<String, VertexStatus>();

  private static final long SLEEP_FOR_COMPLETION = 500;
  private static final long PRINT_STATUS_INTERVAL_MILLIS = 5000;
  private final DecimalFormat formatter = new DecimalFormat("###.##%");
  private long lastPrintStatusTimeMillis;
  private EnumSet<VertexStatus.State> vertexCompletionStates = EnumSet.of(
      VertexStatus.State.SUCCEEDED, VertexStatus.State.FAILED, VertexStatus.State.KILLED,
      VertexStatus.State.ERROR);
  private long statusPollInterval;
  private long diagnoticsWaitTimeout;

  public DAGClientImpl(ApplicationId appId, String dagId, TezConfiguration conf,
                       @Nullable FrameworkClient frameworkClient) {
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
    isATSEnabled = conf.get(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "")
            .equals("org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService") &&
            conf.getBoolean(TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED,
                 TezConfiguration.TEZ_DAG_HISTORY_LOGGING_ENABLED_DEFAULT) &&
            conf.getBoolean(TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED,
                 TezConfiguration.TEZ_AM_HISTORY_LOGGING_ENABLED_DEFAULT);

    if (UserGroupInformation.isSecurityEnabled()){
      //TODO: enable ATS integration in kerberos secured cluster - see TEZ-1529
      isATSEnabled = false;
    }

    realClient = new DAGClientRPCImpl(appId, dagId, conf, this.frameworkClient);
    statusPollInterval = conf.getLong(
        TezConfiguration.TEZ_DAG_STATUS_POLLINTERVAL_MS,
        TezConfiguration.TEZ_DAG_STATUS_POLLINTERVAL_MS_DEFAULT);
    if(statusPollInterval < 0) {
      LOG.error("DAG Status poll interval cannot be negative and setting to default value.");
      statusPollInterval = TezConfiguration.TEZ_DAG_STATUS_POLLINTERVAL_MS_DEFAULT;
    }
    this.diagnoticsWaitTimeout = conf.getLong(
        TezConfiguration.TEZ_CLIENT_DIAGNOSTICS_WAIT_TIMEOUT_MS,
        TezConfiguration.TEZ_CLIENT_DIAGNOSTICS_WAIT_TIMEOUT_MS_DEFAULT);
  }

  @Override
  public String getExecutionContext() {
    return realClient.getExecutionContext();
  }

  @Override
  protected ApplicationReport getApplicationReportInternal() {
    return realClient.getApplicationReportInternal();
  }

  @Override
  public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions,
      final long timeout) throws TezException, IOException {

    Preconditions.checkArgument(timeout >= -1, "Timeout must be >= -1");
    // Short circuit a timeout of 0.
    if (timeout == 0) {
      return getDAGStatusInternal(statusOptions, timeout);
    }

    long startTime = System.currentTimeMillis();
    boolean refreshStatus;
    DAGStatus dagStatus;
    if(cachedDagStatus != null) {
      dagStatus = cachedDagStatus;
      refreshStatus = true;
    } else {
      // For the first lookup only. After this cachedDagStatus should be populated.
      dagStatus = getDAGStatus(statusOptions);
      refreshStatus = false;
    }

    // Handling when client dag status init or submitted. This really implies that the RM was
    // contacted to get status. INITING is never used. DAG_INITING implies a DagState of RUNNING.
    if (dagStatus.getState() == DAGStatus.State.INITING
        || dagStatus.getState() == DAGStatus.State.SUBMITTED) {
      long timeoutAbsolute = startTime + timeout;
      while (timeout < 0
          || (timeout > 0 && timeoutAbsolute > System.currentTimeMillis())) {
        if (refreshStatus) {
          // Try fetching the state with a timeout, in case the AM is already up.
          dagStatus = getDAGStatusInternal(statusOptions, timeout);
        }
        refreshStatus = true; // For the next iteration of the loop.

        if (dagStatus.getState() == DAGStatus.State.RUNNING) {
          // Refreshed status indicates that the DAG is running.
          // This status could have come from the AM or the RM - client sleep if RM, otherwise send request to the AM.
          if (dagStatus.getSource() == DagStatusSource.AM) {
            // RUNNING + AM should only happen if timeout is > -1.
            // Otherwise the AM ignored the -1 value, or the AM source in the DAGStatus is invalid.
            Preconditions.checkState(timeout > -1, "Should not reach here with a timeout of -1. File a bug");
            return dagStatus;
          } else {
            // From the RM. Fall through to the Sleep.
          }
        } else if(dagStatus.getState() == DAGStatus.State.SUCCEEDED
            || dagStatus.getState() == DAGStatus.State.FAILED
            || dagStatus.getState() == DAGStatus.State.KILLED
            || dagStatus.getState() == DAGStatus.State.ERROR) {
          // Again, check if this was from the RM. If it was, try getting it from a more informative source.
          if (dagStatus.getSource() == DagStatusSource.RM) {
            return getDAGStatusInternal(statusOptions, 0);
          } else {
            return dagStatus;
          }
        }
        // Sleep before checking again.
        long currentStatusPollInterval;
        if (timeout < 0) {
          currentStatusPollInterval = statusPollInterval;
        } else {
          long remainingTimeout = timeoutAbsolute - System.currentTimeMillis();
          if (remainingTimeout < 0) {
            // Timeout expired. Return the latest known dag status.
            return dagStatus;
          } else {
            currentStatusPollInterval = remainingTimeout < statusPollInterval ? remainingTimeout : statusPollInterval;
          }
        }
        try {
          Thread.sleep(currentStatusPollInterval);
        } catch (InterruptedException e) {
          throw new TezException(e);
        }
      }// End of while
      // Timeout may have expired before a single refresh
      if (refreshStatus) {
        return getDAGStatus(statusOptions);
      } else {
        return dagStatus;
      }
    } else { // Already running, or complete. Fallback to regular dagStatus with a timeout.
      return getDAGStatusInternal(statusOptions, timeout);
    }
  }

  private DAGStatus getDAGStatusInternal(@Nullable Set<StatusGetOpts> statusOptions,
      long timeout) throws TezException, IOException {

    if (!dagCompleted) {
      // fetch from AM. on Error and while DAG is still not completed (could not reach AM, AM got
      // killed). return cached status. This prevents the progress being reset (for ex fetching from
      // RM does not give status).

      // dagCompleted may be reset within getDagStatusViaAM
      final DAGStatus dagStatus = getDAGStatusViaAM(statusOptions, timeout);

      if (!dagCompleted) {
        if (dagStatus != null) {
          cachedDagStatus = dagStatus;
          return dagStatus;
        }
        if (cachedDagStatus != null) {
          // could not get from AM (not reachable/ was killed). return cached status.
          return cachedDagStatus;
        }
      }

      if (isATSEnabled && dagCompleted) {
        switchToTimelineClient();
      }
    }

    if (isATSEnabled && dagCompleted) {
      try {
        // fetch from ATS and return only if status is completed.
        DAGStatus dagStatus = realClient.getDAGStatus(statusOptions);
        if (dagStatus.isCompleted()) {
          return dagStatus;
        }
      } catch (TezException e) {
        if (LOG.isDebugEnabled()) {
          LOG.info("DAGStatus fetch failed." + e.getMessage());
        }
      }
    }

    // dag completed and Timeline service is either not enabled or does not have completion status
    // return cached status if completion info is present.
    if (dagCompleted && cachedDagStatus != null && cachedDagStatus.isCompleted()) {
      return cachedDagStatus;
    }

    // everything else fails rely on RM.
    return getDAGStatusViaRM();
  }

  @Override
  public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions) throws
      TezException, IOException {
    return getDAGStatusInternal(statusOptions, 0);
  }

  @Override
  public VertexStatus getVertexStatus(String vertexName, Set<StatusGetOpts> statusOptions) throws
      IOException, TezException {

    if (!dagCompleted) {
      VertexStatus vertexStatus = getVertexStatusViaAM(vertexName, statusOptions);

      if (!dagCompleted) {
        if (vertexStatus != null) {
          cachedVertexStatus.put(vertexName, vertexStatus);
          return vertexStatus;
        }
        if (cachedVertexStatus.containsKey(vertexName)) {
          return cachedVertexStatus.get(vertexName);
        }
      }

      if (isATSEnabled && dagCompleted) {
        switchToTimelineClient();
      }
    }

    if (isATSEnabled && dagCompleted) {
      try {
        final VertexStatus vertexStatus = realClient.getVertexStatus(vertexName, statusOptions);
        if (vertexCompletionStates.contains(vertexStatus.getState())) {
          return vertexStatus;
        }
      } catch (TezException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("ERROR fetching vertex data from Yarn Timeline. " + e.getMessage());
        }
      }
    }

    if (cachedVertexStatus.containsKey(vertexName)) {
      final VertexStatus vertexStatus = cachedVertexStatus.get(vertexName);
      if (vertexCompletionStates.contains(vertexStatus.getState())) {
        return vertexStatus;
      }
    }

    return null;
  }

  @Override
  public void tryKillDAG() throws IOException, TezException {
    if (!dagCompleted) {
      realClient.tryKillDAG();
    } else {
      LOG.info("TryKill for app: " + appId + " dag:" + dagId + " dag already completed.");
    }
  }

  @Override
  public DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException {
    return _waitForCompletionWithStatusUpdates(false, EnumSet.noneOf(StatusGetOpts.class));
  }

  @Override
  public DAGStatus waitForCompletionWithStatusUpdates(
      @Nullable Set<StatusGetOpts> statusGetOpts) throws IOException, TezException,
      InterruptedException {
    return _waitForCompletionWithStatusUpdates(true, statusGetOpts);
  }

  @Override
  public void close() throws IOException {
    realClient.close();
    if (frameworkClient != null) {
      frameworkClient.stop();
    }
  }

  /**
   * Get the DAG status via the AM
   * @param statusOptions
   * @param timeout
   * @return null if the AM cannot be contacted, otherwise the DAGstatus
   * @throws IOException
   */
  private DAGStatus getDAGStatusViaAM(@Nullable Set<StatusGetOpts> statusOptions,
      long timeout) throws IOException {
    DAGStatus dagStatus = null;
    try {
      dagStatus = realClient.getDAGStatus(statusOptions, timeout);
    } catch (DAGNotRunningException e) {
      dagCompleted = true;
    } catch (TezException e) {
      // can be either due to a n/w issue of due to AM completed.
    }

    if (dagStatus == null && !dagCompleted) {
      checkAndSetDagCompletionStatus();
    }

    return dagStatus;
  }

  private VertexStatus getVertexStatusViaAM(String vertexName, Set<StatusGetOpts> statusOptions) throws
      IOException {
    VertexStatus vertexStatus = null;
    try {
      vertexStatus = realClient.getVertexStatus(vertexName, statusOptions);
    } catch (DAGNotRunningException e) {
      dagCompleted = true;
    } catch (TezException e) {
      // can be either due to a n/w issue of due to AM completed.
    }

    if (vertexStatus == null && !dagCompleted) {
      checkAndSetDagCompletionStatus();
    }

    return vertexStatus;
  }

  /**
   * Get the DAG status via the YARN ResourceManager
   * @return the dag status, inferred from the RM App state. Does not return null.
   * @throws TezException
   * @throws IOException
   */
  @VisibleForTesting
  protected DAGStatus getDAGStatusViaRM() throws TezException, IOException {
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

    DAGProtos.DAGStatusProto.Builder builder = DAGProtos.DAGStatusProto.newBuilder();
    DAGStatus dagStatus = new DAGStatus(builder, DagStatusSource.RM);
    DAGProtos.DAGStatusStateProto dagState;
    switch (appReport.getYarnApplicationState()) {
      case NEW:
      case NEW_SAVING:
      case SUBMITTED:
      case ACCEPTED:
        dagState = DAGProtos.DAGStatusStateProto.DAG_SUBMITTED;
        break;
      case RUNNING:
        dagState = DAGProtos.DAGStatusStateProto.DAG_RUNNING;
        break;
      case FAILED:
        dagState = DAGProtos.DAGStatusStateProto.DAG_FAILED;
        break;
      case KILLED:
        dagState = DAGProtos.DAGStatusStateProto.DAG_KILLED;
        break;
      case FINISHED:
        switch(appReport.getFinalApplicationStatus()) {
          case UNDEFINED:
          case FAILED:
            dagState = DAGProtos.DAGStatusStateProto.DAG_FAILED;
            break;
          case KILLED:
            dagState = DAGProtos.DAGStatusStateProto.DAG_KILLED;
            break;
          case SUCCEEDED:
            dagState = DAGProtos.DAGStatusStateProto.DAG_SUCCEEDED;
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
    // workaround before YARN-2560 is fixed
    if (appReport.getFinalApplicationStatus() == FinalApplicationStatus.FAILED
        || appReport.getFinalApplicationStatus() == FinalApplicationStatus.KILLED) {
      long startTime = System.currentTimeMillis();
      while((appReport.getDiagnostics() == null
          || appReport.getDiagnostics().isEmpty())
          && (System.currentTimeMillis() - startTime) < diagnoticsWaitTimeout) {
        try {
          Thread.sleep(100);
          appReport = frameworkClient.getApplicationReport(appId);
        } catch (YarnException e) {
          throw new TezException(e);
        } catch (InterruptedException e) {
          throw new TezException(e);
        }
      }
    }

    builder.addAllDiagnostics(Collections.singleton(appReport.getDiagnostics()));
    return dagStatus;
  }

  private DAGStatus _waitForCompletionWithStatusUpdates(boolean vertexUpdates,
                                                        @Nullable Set<StatusGetOpts> statusGetOpts) throws IOException, TezException, InterruptedException {
    DAGStatus dagStatus;
    boolean initPrinted = false;
    boolean runningPrinted = false;
    double dagProgress = -1.0; // Print the first one
    // monitoring
    while (true) {
      dagStatus = getDAGStatus(statusGetOpts, SLEEP_FOR_COMPLETION);
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
      dagStatus = getDAGStatus(statusGetOpts, SLEEP_FOR_COMPLETION);
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
    boolean displayCounter = opts != null && opts.contains(StatusGetOpts.GET_COUNTERS);
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

  private void checkAndSetDagCompletionStatus() {
    ApplicationReport appReport = realClient.getApplicationReportInternal();
    if (appReport != null) {
      final YarnApplicationState appState = appReport.getYarnApplicationState();
      if (appState == YarnApplicationState.FINISHED || appState == YarnApplicationState.FAILED ||
          appState == YarnApplicationState.KILLED) {
        dagCompleted = true;
      }
    }
  }

  private void switchToTimelineClient() throws IOException, TezException {
    realClient.close();
    realClient = new DAGClientTimelineImpl(appId, dagId, conf, frameworkClient);
    if (LOG.isDebugEnabled()) {
      LOG.debug("dag completed switching to DAGClientTimelineImpl");
    }
  }

  @VisibleForTesting
  public DAGClient getRealClient() {
    return realClient;
  }

  private double getProgress(Progress progress) {
    return (progress.getTotalTaskCount() == 0 ? 0.0 : (double) (progress.getSucceededTaskCount())
        / progress.getTotalTaskCount());
  }

  private void log(String message) {
    LOG.info(message);
  }
}

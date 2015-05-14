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
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class LocalClient extends FrameworkClient {
  public static final Logger LOG = LoggerFactory.getLogger(LocalClient.class);

  private volatile DAGAppMaster dagAppMaster = null;
  private volatile DAGClientHandler clientHandler = null;
  private Thread dagAmThread;
  private Configuration conf;
  private final long clusterTimeStamp = System.currentTimeMillis();
  private final long TIME_OUT = 60 * 1000;
  private int appIdNumber = 1;
  private boolean isSession;
  private TezApiVersionInfo versionInfo = new TezApiVersionInfo();
  private volatile Throwable amFailException = null;
  private static final String localModeDAGSchedulerClassName =
      "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled";

  public LocalClient() {
  }

  @Override
  public void init(TezConfiguration tezConf, YarnConfiguration yarnConf) {
    this.conf = tezConf;
    tezConf.set("fs.defaultFS", "file:///");
    // Tez libs already in the client's classpath
    this.conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    this.conf.set(TezConfiguration.TEZ_AM_DAG_SCHEDULER_CLASS, localModeDAGSchedulerClassName);
    isSession = tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE,
        TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT);

    // disable web service for local mode.
    this.conf.setBoolean(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, false);
  }


  @Override
  public void start() {
    // LocalClients are shared between TezClient and DAGClients, which can cause stop / start / close
    // to be invoked multiple times. If modifying these methods - this should be factored in.
  }

  @Override
  public void stop() {
    // LocalClients are shared between TezClient and DAGClients, which can cause stop / start / close
    // to be invoked multiple times. If modifying these methods - this should be factored in.
  }

  @Override
  public void close() throws IOException {
    // LocalClients are shared between TezClient and DAGClients, which can cause stop / start / close
    // to be invoked multiple times. If modifying these methods - this should be factored in.

    // Multiple DAGClient's can reuse the LocalClient (for ex session). However there is only a
    // single instance of LocalClient for a TezClient, and dagAppMaster can be cleaned up when
    // the TezClient is stopped, in order not to leak.
    if (dagAppMaster != null) {
      dagAppMaster.stop();
    }
  }

  @Override
  public YarnClientApplication createApplication() throws YarnException, IOException {
    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    ApplicationId appId = ApplicationId.newInstance(clusterTimeStamp, appIdNumber++);
    context.setApplicationId(appId);
    GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
    response.setApplicationId(appId);
    return new YarnClientApplication(response, context);
  }

  @Override
  public ApplicationId submitApplication(ApplicationSubmissionContext appContext) throws IOException, YarnException {
    ApplicationId appId = appContext.getApplicationId();
    startDAGAppMaster(appContext);
    return appId;
  }

  @Override
  public void killApplication(ApplicationId appId) {
    clientHandler.shutdownAM();
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    report.setCurrentApplicationAttemptId(dagAppMaster.getAttemptID());

    AppContext runningAppContext = dagAppMaster.getContext();
    if (runningAppContext != null) {
      DAG dag = runningAppContext.getCurrentDAG();
      if (dag != null) {
        report.setUser(runningAppContext.getUser());
      }
      report.setName(runningAppContext.getApplicationName());
      report.setStartTime(runningAppContext.getStartTime());
    }

    report.setHost(dagAppMaster.getAppNMHost());
    report.setRpcPort(dagAppMaster.getRpcPort());
    report.setClientToAMToken(null);
    report.setYarnApplicationState(convertDAGAppMasterState(dagAppMaster.getState()));
    report.setFinalApplicationStatus(convertDAGAppMasterStateToFinalYARNState(dagAppMaster.getState()));


    List<String> diagnostics = dagAppMaster.getDiagnostics();
    if (diagnostics != null) {
      report.setDiagnostics(diagnostics.toString());
    }
    report.setTrackingUrl("N/A");
    report.setFinishTime(0);
    report.setApplicationResourceUsageReport(null);
    report.setOriginalTrackingUrl("N/A");
    report.setProgress(dagAppMaster.getProgress());
    report.setAMRMToken(null);

    return report;
  }

  protected FinalApplicationStatus convertDAGAppMasterStateToFinalYARNState(
      DAGAppMasterState dagAppMasterState) {
    switch (dagAppMasterState) {
      case NEW:
      case INITED:
      case RECOVERING:
      case IDLE:
      case RUNNING:
        return FinalApplicationStatus.UNDEFINED;
      case SUCCEEDED:
        return FinalApplicationStatus.SUCCEEDED;
      case FAILED:
        return FinalApplicationStatus.FAILED;
      case KILLED:
        return FinalApplicationStatus.KILLED;
      case ERROR:
        return FinalApplicationStatus.FAILED;
      default:
        return FinalApplicationStatus.UNDEFINED;
    }
  }

  protected YarnApplicationState convertDAGAppMasterState(DAGAppMasterState dagAppMasterState) {
    switch (dagAppMasterState) {
    case NEW:
      return YarnApplicationState.NEW;
    case INITED:
    case RECOVERING:
    case IDLE:
    case RUNNING:
      return YarnApplicationState.RUNNING;
    case SUCCEEDED:
      return YarnApplicationState.FINISHED;
    case FAILED:
      return YarnApplicationState.FAILED;
    case KILLED:
      return YarnApplicationState.KILLED;
    case ERROR:
      return YarnApplicationState.FAILED;
    default:
      return YarnApplicationState.SUBMITTED;
    }
  }

  protected void startDAGAppMaster(final ApplicationSubmissionContext appContext) throws IOException {
    if (dagAmThread == null) {
      try {
        dagAmThread = createDAGAppMaster(appContext);
        dagAmThread.start();

        // Wait until DAGAppMaster is started
        long waitingTime = 0;
        while (amFailException == null) {
          if (dagAppMaster != null) {
            DAGAppMasterState dagAMState = dagAppMaster.getState();
            LOG.info("DAGAppMaster state: " + dagAMState);
            if (dagAMState.equals(DAGAppMasterState.NEW)) {
              LOG.info("DAGAppMaster is not started wait for 100ms...");
            } else if (dagAMState.equals(DAGAppMasterState.INITED)) {
              LOG.info("DAGAppMaster is not startetd wait for 100ms...");
            } else if (dagAMState.equals(DAGAppMasterState.ERROR)) {
              throw new TezException("DAGAppMaster got an error during initialization");
            } else if (dagAMState.equals(DAGAppMasterState.KILLED)) {
              throw new TezException("DAGAppMaster is killed");
            } else {
              break;
            }
          }

          if (waitingTime < TIME_OUT) {
            LOG.info("DAGAppMaster is not created wait for 100ms...");
            Thread.sleep(100);
            waitingTime += 100;
          } else {
            throw new TezException("Time out creating DAGAppMaster");
          }
        }
      } catch (Throwable t) {
        LOG.error("Error starting DAGAppMaster", t);
        if (dagAmThread != null) {
          dagAmThread.interrupt();
        }
        throw new IOException(t);
      }
      if (amFailException != null) {
        throw new IOException(amFailException);
      }
    }
  }

  protected Thread createDAGAppMaster(final ApplicationSubmissionContext appContext) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ApplicationId appId = appContext.getApplicationId();

          // Set up working directory for DAGAppMaster
          Path staging = TezCommonUtils.getTezSystemStagingPath(conf, appId.toString());
          Path userDir = TezCommonUtils.getTezSystemStagingPath(conf, appId.toString()+"_wd");
          LOG.info("Using working directory: " + userDir.toUri().getPath());

          FileSystem fs = FileSystem.get(conf);
          // copy data from staging directory to working directory to simulate the resource localizing
          FileUtil.copy(fs, staging, fs, userDir, false, conf);
          // Prepare Environment
          Path logDir = new Path(userDir, "localmode-log-dir");
          Path localDir = new Path(userDir, "localmode-local-dir");
          fs.mkdirs(logDir);
          fs.mkdirs(localDir);

          UserGroupInformation.setConfiguration(conf);
          // Add session specific credentials to the AM credentials.
          ByteBuffer tokens = appContext.getAMContainerSpec().getTokens();

          Credentials amCredentials;
          if (tokens != null) {
            amCredentials = TezCommonUtils.parseCredentialsBytes(tokens.array());
          } else {
            amCredentials = new Credentials();
          }

          // Construct, initialize, and start the DAGAppMaster
          ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(appId, 0);
          ContainerId cId = ContainerId.newInstance(applicationAttemptId, 1);
          String currentHost = InetAddress.getLocalHost().getHostName();
          int nmPort = YarnConfiguration.DEFAULT_NM_PORT;
          int nmHttpPort = YarnConfiguration.DEFAULT_NM_WEBAPP_PORT;
          long appSubmitTime = System.currentTimeMillis();

          dagAppMaster =
              createDAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
                  new SystemClock(), appSubmitTime, isSession, userDir.toUri().getPath(),
                  new String[] {localDir.toUri().getPath()}, new String[] {logDir.toUri().getPath()},
                  amCredentials, UserGroupInformation.getCurrentUser().getShortUserName());
          clientHandler = new DAGClientHandler(dagAppMaster);
          DAGAppMaster.initAndStartAppMaster(dagAppMaster, conf);

        } catch (Throwable t) {
          LOG.error("Error starting DAGAppMaster", t);
          if (dagAppMaster != null) {
            dagAppMaster.stop();
            dagAppMaster = null;
          }
          amFailException = t;
        }
      }
    });

    thread.setName("DAGAppMaster Thread");
    LOG.info("DAGAppMaster thread has been created");

    return thread;
  }
  
  // this can be overridden by test code to create a mock app
  @VisibleForTesting
  protected DAGAppMaster createDAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId cId, String currentHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, boolean isSession, String userDir,
      String[] localDirs, String[] logDirs, Credentials credentials, String jobUserName) {
    return new DAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime, isSession, userDir, localDirs, logDirs,
        versionInfo.getVersion(), 1, credentials, jobUserName);
  }

}

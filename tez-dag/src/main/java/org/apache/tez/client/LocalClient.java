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

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.Logger;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.utils.EnvironmentUpdateUtils;

public class LocalClient extends FrameworkClient {
  public static final Logger LOG = Logger.getLogger(LocalClient.class);

  private volatile DAGAppMaster dagAppMaster = null;
  private volatile DAGClientHandler clientHandler = null;
  private Thread dagAmThread;
  private Configuration conf;
  private final long clusterTimeStamp = System.currentTimeMillis();
  private final long TIME_OUT = 60 * 1000;
  private int appIdNumber = 1;
  private boolean isSession;

  public LocalClient() {
  }

  @Override
  public void init(TezConfiguration tezConf, YarnConfiguration yarnConf) {
    this.conf = yarnConf;
    tezConf.set("fs.defaultFS", "file:///");
    // Tez libs already in the client's classpath
    tezConf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    isSession = tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE,
        TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() throws IOException {
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
  public ApplicationId submitApplication(ApplicationSubmissionContext appContext) {
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

    List<String> diagnostics = dagAppMaster.getDiagnostics();
    if (diagnostics != null) {
      report.setDiagnostics(diagnostics.toString());
    }
    report.setTrackingUrl("N/A");
    report.setFinishTime(0);
    report.setFinalApplicationStatus(null);
    report.setApplicationResourceUsageReport(null);
    report.setOriginalTrackingUrl("N/A");
    report.setProgress(dagAppMaster.getProgress());
    report.setAMRMToken(null);

    return report;
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

  protected void startDAGAppMaster(final ApplicationSubmissionContext appContext) {
    if (dagAmThread == null) {
      try {
        dagAmThread = createDAGAppMaster(appContext);
        dagAmThread.start();

        // Wait until DAGAppMaster is started
        long waitingTime = 0;
        while (true) {
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
        LOG.fatal("Error starting DAGAppMaster", t);
        dagAmThread.interrupt();
        System.exit(0);
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
          Path userDir = TezCommonUtils.getTezSystemStagingPath(conf, appId.toString());
          LOG.info("Using staging directory: " + userDir.toUri().getPath());

          FileSystem fs = FileSystem.get(conf);
          fs.mkdirs(userDir);

          // Prepare Environment
          Path logDir = new Path(userDir, "localmode-log-dir");
          Path localDir = new Path(userDir, "localmode-local-dir");
          fs.mkdirs(logDir);
          fs.mkdirs(localDir);

          EnvironmentUpdateUtils.put(Environment.LOG_DIRS.name(), logDir.toUri().getPath());
          EnvironmentUpdateUtils.put(Environment.LOCAL_DIRS.name(), localDir.toUri().getPath());

          // Add session specific credentials to the AM credentials.
          UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
          ByteBuffer tokens = appContext.getAMContainerSpec().getTokens();

          if (tokens != null) {
            Credentials amCredentials = new Credentials();
            DataInputByteBuffer dibb = new DataInputByteBuffer();
            dibb.reset(tokens);
            amCredentials.readTokenStorageStream(dibb);
            tokens.rewind();
            currentUser.addCredentials(amCredentials);
          }

          // Construct, initialize, and start the DAGAppMaster
          ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(appId, 0);
          ContainerId cId = ContainerId.newInstance(applicationAttemptId, 1);
          String currentHost = InetAddress.getLocalHost().getHostName();
          int nmPort = YarnConfiguration.DEFAULT_NM_PORT;
          int nmHttpPort = YarnConfiguration.DEFAULT_NM_WEBAPP_PORT;
          long appSubmitTime = System.currentTimeMillis();

          dagAppMaster =
              new DAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
                  new SystemClock(),
                  appSubmitTime, isSession, userDir.toUri().getPath());
          clientHandler = new DAGClientHandler(dagAppMaster);
          DAGAppMaster.initAndStartAppMaster(dagAppMaster, currentUser.getShortUserName());

          } catch (Throwable t) {
          LOG.fatal("Error starting DAGAppMaster", t);
          System.exit(1);
        }
      }
    });

    thread.setName("DAGAppMaster Thread");
    LOG.info("DAGAppMaster thread has been created");

    return thread;
  }
}

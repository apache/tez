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
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
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
import org.apache.tez.common.AsyncDispatcher;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.dag.api.DAGSubmissionTimedOut;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientHandler;
import org.apache.tez.dag.api.client.DAGClientImpl;
import org.apache.tez.dag.api.client.DAGClientImplLocal;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolRPC.SubmitDAGRequestProto;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.LocalDAGAppMaster;
import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;

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
  private boolean isLocalWithoutNetwork;

  private static final String localModeDAGSchedulerClassName =
      "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled";

  public LocalClient() {
  }

  @Override
  public void init(TezConfiguration tezConf) {
    this.conf = tezConf;
    // Tez libs already in the client's classpath
    this.conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    this.conf.set(TezConfiguration.TEZ_AM_DAG_SCHEDULER_CLASS, localModeDAGSchedulerClassName);
    isSession = tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE,
        TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT);

    // disable web service for local mode.
    this.conf.setBoolean(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, false);

    this.isLocalWithoutNetwork =
        tezConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE_WITHOUT_NETWORK,
            TezConfiguration.TEZ_LOCAL_MODE_WITHOUT_NETWORK_DEFAULT);
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
    try {
      if (clientHandler != null){
        clientHandler.shutdownAM();
      }
    } catch (TezException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isRunning() {
    return true;
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

  @SuppressWarnings("deprecation")
  protected Thread createDAGAppMaster(final ApplicationSubmissionContext appContext) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ApplicationId appId = appContext.getApplicationId();

          // Set up working directory for DAGAppMaster.
          // The staging directory may be on the default file system, which may or may not
          // be the local FS. For example, when using testing Hive against a pseudo-distributed
          // cluster, it's useful for the default FS to be HDFS. Hive then puts its scratch
          // directories on HDFS, and sets the Tez staging directory to be the session's
          // scratch directory.
          //
          // To handle this case, we need to copy over the staging data back onto the
          // local file system, where the rest of the Tez Child code expects it.
          //
          // NOTE: we base the local working directory path off of the staging path, even
          // though it might be on a different file system. Typically they're both in a
          // path starting with /tmp, but in the future we may want to use a different
          // temp directory locally.
          Path staging = TezCommonUtils.getTezSystemStagingPath(conf, appId.toString());
          FileSystem stagingFs = staging.getFileSystem(conf);

          FileSystem localFs = FileSystem.getLocal(conf);
          Path userDir = localFs.makeQualified(new Path(staging.toUri().getPath() + "_wd"));
          LOG.info("Using working directory: " + userDir.toUri().getPath());

          // copy data from staging directory to working directory to simulate the resource localizing
          FileUtil.copy(stagingFs, staging, localFs, userDir, false, conf);
          // Prepare Environment
          Path logDir = new Path(userDir, "localmode-log-dir");
          Path localDir = new Path(userDir, "localmode-local-dir");
          localFs.mkdirs(logDir);
          localFs.mkdirs(localDir);

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
                  SystemClock.getInstance(), appSubmitTime, isSession, userDir.toUri().getPath(),
                  new String[] {localDir.toUri().getPath()}, new String[] {logDir.toUri().getPath()},
                  amCredentials, UserGroupInformation.getCurrentUser().getShortUserName());
          DAGAppMaster.initAndStartAppMaster(dagAppMaster, conf);
          clientHandler = new DAGClientHandler(dagAppMaster);
          ((AsyncDispatcher)dagAppMaster.getDispatcher()).setDrainEventsOnStop();
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
                                            ContainerId cId, String currentHost, int nmPort,
                                            int nmHttpPort,
                                            Clock clock, long appSubmitTime, boolean isSession,
                                            String userDir,
                                            String[] localDirs, String[] logDirs,
                                            Credentials credentials, String jobUserName) throws
      IOException {

    // Read in additional information about external services
    AMPluginDescriptorProto amPluginDescriptorProto =
        TezUtilsInternal.readUserSpecifiedTezConfiguration(userDir)
            .getAmPluginDescriptor();

    return isLocalWithoutNetwork
      ? new LocalDAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
          SystemClock.getInstance(), appSubmitTime, isSession, userDir, localDirs, logDirs,
          versionInfo.getVersion(), credentials, jobUserName, amPluginDescriptorProto)
      : new DAGAppMaster(applicationAttemptId, cId, currentHost, nmPort, nmHttpPort,
          SystemClock.getInstance(), appSubmitTime, isSession, userDir, localDirs, logDirs,
          versionInfo.getVersion(), credentials, jobUserName, amPluginDescriptorProto);
  }

  @Override
  public TezAppMasterStatus getAMStatus(Configuration configuration, ApplicationId appId,
      UserGroupInformation ugi) throws TezException, ServiceException, IOException {
    if (isLocalWithoutNetwork) {
      if (clientHandler == null) {
        return TezAppMasterStatus.INITIALIZING;
      }
      return clientHandler.getTezAppMasterStatus();
    }
    return super.getAMStatus(configuration, appId, ugi);
  }

  @Override
  public DAGClient submitDag(org.apache.tez.dag.api.DAG dag, SubmitDAGRequestProto request,
      String clientName, ApplicationId sessionAppId, long clientTimeout, UserGroupInformation ugi,
      TezConfiguration tezConf) throws IOException, TezException, DAGSubmissionTimedOut {

    Map<String, LocalResource> additionalResources = null;
    if (request.hasAdditionalAmResources()) {
      additionalResources =
          DagTypeConverters.convertFromPlanLocalResources(request.getAdditionalAmResources());
    }

    String dagId = dagAppMaster.submitDAGToAppMaster(request.getDAGPlan(), additionalResources);
    return getDAGClient(sessionAppId, dagId, tezConf, ugi);
  }

  @Override
  public DAGClient getDAGClient(ApplicationId appId, String dagId, TezConfiguration tezConf,
      UserGroupInformation ugi) {
    return isLocalWithoutNetwork
      ? new DAGClientImplLocal(appId, dagId, tezConf, this, ugi, new BiFunction<Set<StatusGetOpts>, Long, DAGStatus>() {
        @Override
        public DAGStatus apply(Set<StatusGetOpts> statusOpts, Long timeout) {
          try {
            return clientHandler.getDAGStatus(dagId, statusOpts, timeout);
          } catch (TezException e) {
            throw new RuntimeException(e);
          }
        }
      }, new BiFunction<Set<StatusGetOpts>, String, VertexStatus>() {
        @Override
        public VertexStatus apply(Set<StatusGetOpts> statusOpts, String vertexName) {
          try {
            return clientHandler.getVertexStatus(dagId, vertexName, statusOpts);
          } catch (TezException e) {
            throw new RuntimeException(e);
          }
        }
      }) : new DAGClientImpl(appId, dagId, tezConf, this, ugi);
  }

  @Override
  public boolean shutdownSession(Configuration configuration, ApplicationId sessionAppId,
      UserGroupInformation ugi) throws TezException, IOException, ServiceException {
    if (isLocalWithoutNetwork) {
      if (clientHandler != null){
        clientHandler.shutdownAM();
      }
      return true;
    }
    return super.shutdownSession(configuration, sessionAppId, ugi);
  }
}

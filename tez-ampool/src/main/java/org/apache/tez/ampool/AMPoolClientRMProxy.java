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

package org.apache.tez.ampool;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.v2.app2.lazy.LazyAMConfig;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.Records;

public class AMPoolClientRMProxy extends AbstractService
    implements ClientRMProtocol {

  private static final Log LOG = LogFactory.getLog(
      AMPoolClientRMProxy.class);

  private final AMPoolContext context;
  private Server server;
  InetSocketAddress clientBindAddress;

  public AMPoolClientRMProxy(AMPoolContext context) {
    super(AMPoolClientRMProxy.class.getName());
    this.context = context;
  }

  @Override
  public void init(Configuration conf) {
    clientBindAddress = getBindAddress(conf);

    InetSocketAddress rmAddress =
        conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);

    if (clientBindAddress.equals(rmAddress)) {
      throw new RuntimeException(
          "AMPoolService's RM proxy is running on the same address"
          + " as the main RM"
          + ", AMPoolServiceBindAddress=" + clientBindAddress.toString()
          + ", RMBindAddress=" + rmAddress.toString());
    }

    super.init(conf);
  }

  @Override
  public void start() {
    LOG.info("Starting AMPoolClientRMProxy service");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    LOG.info("Starting ClientRMProtocol listener at"
        + clientBindAddress.toString());
    this.server =
      rpc.getServer(ClientRMProtocol.class, this,
            clientBindAddress,
            conf, null,
            conf.getInt(AMPoolConfiguration.RM_PROXY_CLIENT_THREAD_COUNT,
                AMPoolConfiguration.DEFAULT_RM_PROXY_CLIENT_THREAD_COUNT));

    this.server.start();
    clientBindAddress = conf.updateConnectAddr(
        AMPoolConfiguration.RM_PROXY_CLIENT_ADDRESS,
        server.getListenerAddress());
    LOG.info("Started ClientRMProtocol listener at"
        + clientBindAddress.toString());
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping AMPoolClientRMProxy service");
    if (this.server != null) {
        this.server.stop();
    }
    super.stop();
  }

  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(AMPoolConfiguration.RM_PROXY_CLIENT_ADDRESS,
            AMPoolConfiguration.DEFAULT_RM_PROXY_CLIENT_ADDRESS,
            AMPoolConfiguration.DEFAULT_RM_PROXY_CLIENT_PORT);
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnRemoteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnRemoteException {
    ApplicationId appId = context.getNewApplicationId();
    if (appId != null) {
      LOG.info("Received a getNewApplication request"
          + ", assignedAppId=" + appId.toString());
      GetNewApplicationResponse response = Records.newRecord(
          GetNewApplicationResponse.class);
      response.setApplicationId(appId);
      response.setMaximumResourceCapability(context.getMaxResourceCapability());
      response.setMinimumResourceCapability(context.getMinResourceCapability());
      return response;
    }
    LOG.info("Received a getNewApplication request, proxying to real RM");
    return context.getRMYarnClient().getNewApplication();
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnRemoteException {
    // TODO handle kills for managed AMs
    context.getRMYarnClient().killApplication(
        request.getApplicationId());
    return Records.newRecord(KillApplicationResponse.class);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnRemoteException {
    GetApplicationReportResponse response = Records.newRecord(
        GetApplicationReportResponse.class);
    ApplicationId applicationId = request.getApplicationId();
    ApplicationReport applicationReport =
        context.getRMYarnClient().getApplicationReport(applicationId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received an application report request"
          + ", applicationId=" + applicationId);
    }

    // Handle submitted jobs that have not been picked up by AM
    long jobPickUpTime = -1;
    if ((applicationReport != null
        && applicationReport.getYarnApplicationState() != null
        && applicationReport.getYarnApplicationState()
            == YarnApplicationState.RUNNING)
        && context.isManagedApp(applicationId)) {
      AMContext amContext = context.getAMContext(applicationId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AMContext is null? = " + (amContext == null));
        if (amContext != null) {
          LOG.debug("AMContext dump: " + amContext.toString());
        }
      }
      if (amContext == null || amContext.getSubmissionContext() == null) {
        applicationReport.setYarnApplicationState(
            YarnApplicationState.NEW);
      } else {
        // TODO fix
        // Needs a pingback from the AM to really fix this correctly
        // 5 second delay is not necessarily a guarantee
        int pollWaitInterval = getConfig().getInt(
            LazyAMConfig.POLLING_INTERVAL_SECONDS,
            LazyAMConfig.DEFAULT_POLLING_INTERVAL_SECONDS) * 5000;
        if (amContext.getCurrentApplicationAttemptId() == null
            || amContext.getJobPickUpTime() <= 0
            || (System.currentTimeMillis() <
              (amContext.getJobPickUpTime() + pollWaitInterval))) {
          jobPickUpTime = amContext.getJobPickUpTime();
          applicationReport.setYarnApplicationState(
              YarnApplicationState.SUBMITTED);
        }
      }
      LOG.info("Received an application report request"
          + ", applicationId=" + applicationId
          + ", applicationState=" + applicationReport.getYarnApplicationState()
          + ", jobPickupTime=" + jobPickUpTime);
    }
    response.setApplicationReport(applicationReport);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    GetClusterMetricsResponse response = Records.newRecord(
        GetClusterMetricsResponse.class);
    response.setClusterMetrics(
        context.getRMYarnClient().getYarnClusterMetrics());
    return response;
  }

  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {
    GetAllApplicationsResponse response = Records.newRecord(
        GetAllApplicationsResponse.class);
    response.setApplicationList(
        context.getRMYarnClient().getApplicationList());
    return response;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    GetClusterNodesResponse response = Records.newRecord(
        GetClusterNodesResponse.class);
    response.setNodeReports(context.getRMYarnClient().getNodeReports());
    return response;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    GetQueueInfoResponse response = Records.newRecord(
        GetQueueInfoResponse.class);
    response.setQueueInfo(context.getRMYarnClient().getQueueInfo(
        request.getQueueName()));
    return response;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    GetQueueUserAclsInfoResponse response =
        Records.newRecord(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(
        context.getRMYarnClient().getQueueAclsInfo());
    return response;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnRemoteException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    if (!context.isManagedApp(
        request.getApplicationSubmissionContext().getApplicationId())) {
      LOG.info("Received a submitApplication request for non-managed app"
          + ", proxying to real RM"
          + ", appId="
          + request.getApplicationSubmissionContext().getApplicationId());
      context.getRMYarnClient().submitApplication(
          request.getApplicationSubmissionContext());
      return Records.newRecord(SubmitApplicationResponse.class);
    }
    LOG.info("Received a submitApplication request for managed app"
        + ", appId="
        + request.getApplicationSubmissionContext().getApplicationId());
    return context.submitApplication(request);
  }

}

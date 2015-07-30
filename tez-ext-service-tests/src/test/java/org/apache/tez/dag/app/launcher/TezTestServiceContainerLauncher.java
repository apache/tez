/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.launcher;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.TezTestServiceCommunicator;
import org.apache.tez.service.TezTestServiceConfConstants;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTestServiceContainerLauncher extends ContainerLauncher {

  // TODO Support interruptability of tasks which haven't yet been launched.

  // TODO May need multiple connections per target machine, depending upon how synchronization is handled in the RPC layer

  static final Logger LOG = LoggerFactory.getLogger(TezTestServiceContainerLauncher.class);

  private final String tokenIdentifier;
  private final int servicePort;
  private final TezTestServiceCommunicator communicator;
  private final ApplicationAttemptId appAttemptId;
  private final Configuration conf;


  // Configuration passed in here to set up final parameters
  public TezTestServiceContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    super(containerLauncherContext);
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getInitialUserPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    int numThreads = conf.getInt(
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS_DEFAULT);

    this.servicePort = conf.getInt(
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT, -1);
    Preconditions.checkArgument(servicePort > 0,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT + " must be set");
    this.communicator = new TezTestServiceCommunicator(numThreads);
    this.tokenIdentifier = getContext().getApplicationAttemptId().getApplicationId().toString();
    this.appAttemptId = getContext().getApplicationAttemptId();
  }

  @Override
  public void start() {
    communicator.init(conf);
    communicator.start();
  }

  @Override
  public void shutdown() {
    communicator.stop();
  }

  @Override
  public void launchContainer(final ContainerLaunchRequest launchRequest) {
    RunContainerRequestProto runRequest = null;
    try {
      runRequest = constructRunContainerRequest(launchRequest);
    } catch (IOException e) {
      getContext().containerLaunchFailed(launchRequest.getContainerId(),
          "Failed to construct launch request, " + StringUtils.stringifyException(e));
      return;
    }
    communicator.runContainer(runRequest, launchRequest.getNodeId().getHost(),
        launchRequest.getNodeId().getPort(),
        new TezTestServiceCommunicator.ExecuteRequestCallback<TezTestServiceProtocolProtos.RunContainerResponseProto>() {
          @Override
          public void setResponse(TezTestServiceProtocolProtos.RunContainerResponseProto response) {
            LOG.info(
                "Container: " + launchRequest.getContainerId() + " launch succeeded on host: " +
                    launchRequest.getNodeId());
            getContext().containerLaunched(launchRequest.getContainerId());
          }

          @Override
          public void indicateError(Throwable t) {
            LOG.error(
                "Failed to launch container: " + launchRequest.getContainerId() + " on host: " +
                    launchRequest.getNodeId(), t);
            sendContainerLaunchFailedMsg(launchRequest.getContainerId(), t);
          }
        });
  }

  @Override
  public void stopContainer(ContainerStopRequest stopRequest) {
    LOG.info("Ignoring stopContainer for event: " + stopRequest);
    // that the container is actually done (normally received from RM)
    // TODO Sending this out for an un-launched container is invalid
    getContext().containerStopRequested(stopRequest.getContainerId());
  }

  private RunContainerRequestProto constructRunContainerRequest(ContainerLaunchRequest launchRequest) throws
      IOException {
    RunContainerRequestProto.Builder builder = RunContainerRequestProto.newBuilder();
    Preconditions.checkArgument(launchRequest.getTaskCommunicatorName().equals(
        TezConstants.getTezYarnServicePluginName()));
    InetSocketAddress address = (InetSocketAddress) getContext().getTaskCommunicatorMetaInfo(launchRequest.getTaskCommunicatorName());
    builder.setAmHost(address.getHostName()).setAmPort(address.getPort());
    builder.setAppAttemptNumber(appAttemptId.getAttemptId());
    builder.setApplicationIdString(appAttemptId.getApplicationId().toString());
    builder.setTokenIdentifier(tokenIdentifier);
    builder.setContainerIdString(launchRequest.getContainerId().toString());
    builder.setCredentialsBinary(
        ByteString.copyFrom(launchRequest.getContainerLaunchContext().getTokens()));
    // TODO Avoid reading this from the environment
    builder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId, Throwable t) {
    getContext().containerLaunchFailed(containerId, t == null ? "" : t.getMessage());
  }


}

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

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TezTestServiceCommunicator;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.service.TezTestServiceConfConstants;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos;
import org.apache.tez.test.service.rpc.TezTestServiceProtocolProtos.RunContainerRequestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezTestServiceContainerLauncher extends AbstractService implements ContainerLauncher {

  // TODO Support interruptability of tasks which haven't yet been launched.

  // TODO May need multiple connections per target machine, depending upon how synchronization is handled in the RPC layer

  static final Logger LOG = LoggerFactory.getLogger(TezTestServiceContainerLauncher.class);

  private final AppContext context;
  private final String tokenIdentifier;
  private final TaskAttemptListener tal;
  private final int servicePort;
  private final TezTestServiceCommunicator communicator;
  private final Clock clock;
  private final ApplicationAttemptId appAttemptId;


  // Configuration passed in here to set up final parameters
  public TezTestServiceContainerLauncher(AppContext appContext, Configuration conf,
                                         TaskAttemptListener tal) {
    super(TezTestServiceContainerLauncher.class.getName());
    this.clock = appContext.getClock();
    int numThreads = conf.getInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_AM_COMMUNICATOR_NUM_THREADS_DEFAULT);

    this.servicePort = conf.getInt(TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT, -1);
    Preconditions.checkArgument(servicePort > 0,
        TezTestServiceConfConstants.TEZ_TEST_SERVICE_RPC_PORT + " must be set");
    this.communicator = new TezTestServiceCommunicator(numThreads);
    this.context = appContext;
    this.tokenIdentifier = context.getApplicationID().toString();
    this.appAttemptId = appContext.getApplicationAttemptId();
    this.tal = tal;
  }

  @Override
  public void serviceInit(Configuration conf) {
    communicator.init(conf);
  }

  @Override
  public void serviceStart() {
    communicator.start();
  }

  @Override
  public void serviceStop() {
    communicator.stop();
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    switch (event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        final NMCommunicatorLaunchRequestEvent launchEvent = (NMCommunicatorLaunchRequestEvent) event;
        RunContainerRequestProto runRequest = constructRunContainerRequest(launchEvent);
        communicator.runContainer(runRequest, launchEvent.getNodeId().getHost(),
            launchEvent.getNodeId().getPort(),
            new TezTestServiceCommunicator.ExecuteRequestCallback<TezTestServiceProtocolProtos.RunContainerResponseProto>() {
              @Override
              public void setResponse(TezTestServiceProtocolProtos.RunContainerResponseProto response) {
                LOG.info("Container: " + launchEvent.getContainerId() + " launch succeeded on host: " + launchEvent.getNodeId());
                context.getEventHandler().handle(new AMContainerEventLaunched(launchEvent.getContainerId()));
                ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
                    launchEvent.getContainerId(), clock.getTime(), context.getApplicationAttemptId());
                context.getHistoryHandler().handle(new DAGHistoryEvent(
                    null, lEvt));
              }

              @Override
              public void indicateError(Throwable t) {
                LOG.error("Failed to launch container: " + launchEvent.getContainer() + " on host: " + launchEvent.getNodeId(), t);
                sendContainerLaunchFailedMsg(launchEvent.getContainerId(), t);
              }
            });
        break;
      case CONTAINER_STOP_REQUEST:
        LOG.info("DEBUG: Ignoring STOP_REQUEST for event: " + event);
        // that the container is actually done (normally received from RM)
        // TODO Sending this out for an un-launched container is invalid
        context.getEventHandler().handle(new AMContainerEvent(event.getContainerId(),
            AMContainerEventType.C_NM_STOP_SENT));
        break;
    }
  }

  private RunContainerRequestProto constructRunContainerRequest(NMCommunicatorLaunchRequestEvent event) {
    RunContainerRequestProto.Builder builder = RunContainerRequestProto.newBuilder();
    InetSocketAddress address = tal.getTaskCommunicator(event.getTaskCommId()).getAddress();
    builder.setAmHost(address.getHostName()).setAmPort(address.getPort());
    builder.setAppAttemptNumber(appAttemptId.getAttemptId());
    builder.setApplicationIdString(appAttemptId.getApplicationId().toString());
    builder.setTokenIdentifier(tokenIdentifier);
    builder.setContainerIdString(event.getContainer().getId().toString());
    builder.setCredentialsBinary(
        ByteString.copyFrom(event.getContainerLaunchContext().getTokens()));
    // TODO Avoid reading this from the environment
    builder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  void sendContainerLaunchFailedMsg(ContainerId containerId, Throwable t) {
    context.getEventHandler().handle(new AMContainerEventLaunchFailed(containerId, t == null ? "" : t.getMessage()));
  }
}

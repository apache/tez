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

package org.apache.tez.dag.app;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezConverterUtils;
import org.apache.tez.common.TezLocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TaskCommunicator;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TaskHeartbeatRequest;
import org.apache.tez.dag.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.security.authorize.TezAMPolicyProvider;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class TezTaskCommunicatorImpl extends TaskCommunicator {

  private static final Logger LOG = LoggerFactory.getLogger(TezTaskCommunicatorImpl.class);

  private static final ContainerTask TASK_FOR_INVALID_JVM = new ContainerTask(
      null, true, null, null, false);

  private final TezTaskUmbilicalProtocol taskUmbilical;

  protected final ConcurrentMap<ContainerId, ContainerInfo> registeredContainers =
      new ConcurrentHashMap<>();
  protected final ConcurrentMap<TezTaskAttemptID, ContainerId> attemptToContainerMap =
      new ConcurrentHashMap<>();


  protected final String tokenIdentifier;
  protected final Token<JobTokenIdentifier> sessionToken;
  protected final Configuration conf;
  protected InetSocketAddress address;

  protected volatile Server server;

  public static final class ContainerInfo {

    ContainerInfo(ContainerId containerId, String host, int port) {
      this.containerId = containerId;
      this.host = host;
      this.port = port;
    }

    final ContainerId containerId;
    public final String host;
    public final int port;
    TezHeartbeatResponse lastResponse = null;
    TaskSpec taskSpec = null;
    long lastRequestId = 0;
    Map<String, LocalResource> additionalLRs = null;
    Credentials credentials = null;
    boolean credentialsChanged = false;
    boolean taskPulled = false;

    void reset() {
      taskSpec = null;
      additionalLRs = null;
      credentials = null;
      credentialsChanged = false;
      taskPulled = false;
    }
  }



  /**
   * Construct the service.
   */
  public TezTaskCommunicatorImpl(TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);
    this.taskUmbilical = new TezTaskUmbilicalProtocolImpl();
    this.tokenIdentifier = taskCommunicatorContext.getApplicationAttemptId().getApplicationId().toString();
    this.sessionToken = TokenCache.getSessionToken(taskCommunicatorContext.getCredentials());
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(
          "Unable to parse user payload for " + TezTaskCommunicatorImpl.class.getSimpleName(), e);
    }
  }

  @Override
  public void start() {
    startRpcServer();
  }

  @Override
  public void shutdown() {
    stopRpcServer();
  }

  protected void startRpcServer() {
    try {
      JobTokenSecretManager jobTokenSecretManager =
          new JobTokenSecretManager();
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, sessionToken);

      server = new RPC.Builder(conf)
          .setProtocol(TezTaskUmbilicalProtocol.class)
          .setBindAddress("0.0.0.0")
          .setPort(0)
          .setInstance(taskUmbilical)
          .setNumHandlers(
              conf.getInt(TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                  TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
          .setPortRangeConfig(TezConfiguration.TEZ_AM_TASK_AM_PORT_RANGE)
          .setSecretManager(jobTokenSecretManager).build();

      // Enable service authorization?
      if (conf.getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        refreshServiceAcls(conf, new TezAMPolicyProvider());
      }

      server.start();
      InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
      this.address = NetUtils.createSocketAddrForHost(
          serverBindAddress.getAddress().getCanonicalHostName(),
          serverBindAddress.getPort());
      LOG.info("Instantiated TezTaskCommunicator RPC at " + this.address);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  protected void stopRpcServer() {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  protected Configuration getConf() {
    return this.conf;
  }

  private void refreshServiceAcls(Configuration configuration,
                                  PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, String host, int port) {
    ContainerInfo oldInfo = registeredContainers.putIfAbsent(containerId,
        new ContainerInfo(containerId, host, port));
    if (oldInfo != null) {
      throw new TezUncheckedException("Multiple registrations for containerId: " + containerId);
    }
  }

  @Override
  public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason, String diagnostics) {
    ContainerInfo containerInfo = registeredContainers.remove(containerId);
    if (containerInfo != null) {
      synchronized(containerInfo) {
        if (containerInfo.taskSpec != null && containerInfo.taskSpec.getTaskAttemptID() != null) {
          attemptToContainerMap.remove(containerInfo.taskSpec.getTaskAttemptID());
        }
      }
    }
  }

  @Override
  public void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                         Map<String, LocalResource> additionalResources,
                                         Credentials credentials, boolean credentialsChanged,
                                         int priority) {

    ContainerInfo containerInfo = registeredContainers.get(containerId);
    Preconditions.checkNotNull(containerInfo,
        "Cannot register task attempt: " + taskSpec.getTaskAttemptID() + " to unknown container: " +
            containerId);
    synchronized (containerInfo) {
      if (containerInfo.taskSpec != null) {
        throw new TezUncheckedException(
            "Cannot register task: " + taskSpec.getTaskAttemptID() + " to container: " +
                containerId + " , with pre-existing assignment: " +
                containerInfo.taskSpec.getTaskAttemptID());
      }
      containerInfo.taskSpec = taskSpec;
      containerInfo.additionalLRs = additionalResources;
      containerInfo.credentials = credentials;
      containerInfo.credentialsChanged = credentialsChanged;
      containerInfo.taskPulled = false;

      ContainerId oldId = attemptToContainerMap.putIfAbsent(taskSpec.getTaskAttemptID(), containerId);
      if (oldId != null) {
        throw new TezUncheckedException(
            "Attempting to register an already registered taskAttempt with id: " +
                taskSpec.getTaskAttemptID() + " to containerId: " + containerId +
                ". Already registered to containerId: " + oldId);
      }
    }
  }


  @Override
  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID, TaskAttemptEndReason endReason, String diagnostics) {
    ContainerId containerId = attemptToContainerMap.remove(taskAttemptID);
    if(containerId == null) {
      LOG.warn("Unregister task attempt: " + taskAttemptID + " from unknown container");
      return;
    }
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      LOG.warn("Unregister task attempt: " + taskAttemptID +
          " from non-registered container: " + containerId);
      return;
    }
    synchronized (containerInfo) {
      containerInfo.reset();
      attemptToContainerMap.remove(taskAttemptID);
    }
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception {
    // Empty. Not registering, or expecting any updates.
  }

  @Override
  public void dagComplete(String dagName) {
    // Nothing to do at the moment. Some of the TODOs from TaskAttemptListener apply here.
  }

  @Override
  public Object getMetaInfo() {
    return address;
  }

  protected String getTokenIdentifier() {
    return tokenIdentifier;
  }

  protected Token<JobTokenIdentifier> getSessionToken() {
    return sessionToken;
  }

  public TezTaskUmbilicalProtocol getUmbilical() {
    return this.taskUmbilical;
  }

  private class TezTaskUmbilicalProtocolImpl implements TezTaskUmbilicalProtocol {

    @Override
    public ContainerTask getTask(ContainerContext containerContext) throws IOException {
      ContainerTask task = null;
      if (containerContext == null || containerContext.getContainerIdentifier() == null) {
        LOG.info("Invalid task request with an empty containerContext or containerId");
        task = TASK_FOR_INVALID_JVM;
      } else {
        ContainerId containerId = ConverterUtils.toContainerId(containerContext
            .getContainerIdentifier());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Container with id: " + containerId + " asked for a task");
        }
        task = getContainerTask(containerId);
        if (task != null && !task.shouldDie()) {
          getContext()
              .taskStartedRemotely(task.getTaskSpec().getTaskAttemptID(), containerId);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("getTask returning task: " + task);
      }
      return task;
    }

    @Override
    public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
      return getContext().canCommit(taskAttemptId);
    }

    @Override
    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) throws IOException,
        TezException {
      ContainerId containerId = ConverterUtils.toContainerId(request.getContainerIdentifier());
      long requestId = request.getRequestId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received heartbeat from container"
            + ", request=" + request);
      }

      ContainerInfo containerInfo = registeredContainers.get(containerId);
      if (containerInfo == null) {
        LOG.warn("Received task heartbeat from unknown container with id: " + containerId +
            ", asking it to die");
        TezHeartbeatResponse response = new TezHeartbeatResponse();
        response.setLastRequestId(requestId);
        response.setShouldDie();
        return response;
      }

      synchronized (containerInfo) {
        if (containerInfo.lastRequestId == requestId) {
          LOG.warn("Old sequenceId received: " + requestId
              + ", Re-sending last response to client");
          return containerInfo.lastResponse;
        }
      }



      TezHeartbeatResponse response = new TezHeartbeatResponse();
      TezTaskAttemptID taskAttemptID = request.getCurrentTaskAttemptID();
      if (taskAttemptID != null) {
        TaskHeartbeatResponse tResponse;
        synchronized (containerInfo) {
          ContainerId containerIdFromMap = attemptToContainerMap.get(taskAttemptID);
          if (containerIdFromMap == null || !containerIdFromMap.equals(containerId)) {
            throw new TezException("Attempt " + taskAttemptID
                + " is not recognized for heartbeat");
          }

          if (containerInfo.lastRequestId + 1 != requestId) {
            throw new TezException("Container " + containerId
                + " has invalid request id. Expected: "
                + containerInfo.lastRequestId + 1
                + " and actual: " + requestId);
          }
        }
        TaskHeartbeatRequest tRequest = new TaskHeartbeatRequest(request.getContainerIdentifier(),
            request.getCurrentTaskAttemptID(), request.getEvents(), request.getStartIndex(),
            request.getPreRoutedStartIndex(), request.getMaxEvents());
        tResponse = getContext().heartbeat(tRequest);
        response.setEvents(tResponse.getEvents());
        response.setNextFromEventId(tResponse.getNextFromEventId());
        response.setNextPreRoutedEventId(tResponse.getNextPreRoutedEventId());
      }
      response.setLastRequestId(requestId);
      containerInfo.lastRequestId = requestId;
      containerInfo.lastResponse = response;
      return response;
    }


    // TODO Remove this method once we move to the Protobuf RPC engine
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return versionID;
    }

    // TODO Remove this method once we move to the Protobuf RPC engine
    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, protocol,
          clientVersion, clientMethodsHash);
    }
  }

  private ContainerTask getContainerTask(ContainerId containerId) throws IOException {
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    ContainerTask task = null;
    if (containerInfo == null) {
      if (getContext().isKnownContainer(containerId)) {
        LOG.info("Container with id: " + containerId
            + " is valid, but no longer registered, and will be killed");
      } else {
        LOG.info("Container with id: " + containerId
            + " is invalid and will be killed");
      }
      task = TASK_FOR_INVALID_JVM;
    } else {
      synchronized (containerInfo) {
        if (containerInfo.taskSpec != null) {
          if (!containerInfo.taskPulled) {
            containerInfo.taskPulled = true;
            task = constructContainerTask(containerInfo);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Task " + containerInfo.taskSpec.getTaskAttemptID() +
                  " already sent to container: " + containerId);
            }
            task = null;
          }
        } else {
          task = null;
          if (LOG.isDebugEnabled()) {
            LOG.debug("No task assigned yet for running container: " + containerId);
          }
        }
      }
    }
    return task;
  }

  private ContainerTask constructContainerTask(ContainerInfo containerInfo) throws IOException {
    return new ContainerTask(containerInfo.taskSpec, false,
        convertLocalResourceMap(containerInfo.additionalLRs), containerInfo.credentials,
        containerInfo.credentialsChanged);
  }

  private Map<String, TezLocalResource> convertLocalResourceMap(Map<String, LocalResource> ylrs)
      throws IOException {
    Map<String, TezLocalResource> tlrs = Maps.newHashMap();
    if (ylrs != null) {
      for (Map.Entry<String, LocalResource> ylrEntry : ylrs.entrySet()) {
        TezLocalResource tlr;
        try {
          tlr = TezConverterUtils.convertYarnLocalResourceToTez(ylrEntry.getValue());
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
        tlrs.put(ylrEntry.getKey(), tlr);
      }
    }
    return tlrs;
  }

  protected ContainerInfo getContainerInfo(ContainerId containerId) {
    return registeredContainers.get(containerId);
  }

  protected ContainerId getContainerForAttempt(TezTaskAttemptID taskAttemptId) {
    return attemptToContainerMap.get(taskAttemptId);
  }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections4.ListUtils;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.ContainerContext;
import org.apache.tez.common.ContainerTask;
import org.apache.tez.common.TezConverterUtils;
import org.apache.tez.common.TezLocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.app.security.authorize.TezAMPolicyProvider;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.common.security.JobTokenSecretManager;

import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class TaskAttemptListenerImpTezDag extends AbstractService implements
    TezTaskUmbilicalProtocol, TaskAttemptListener {

  private static final ContainerTask TASK_FOR_INVALID_JVM = new ContainerTask(
      null, true, null, null, false);

  private static final Logger LOG = LoggerFactory
      .getLogger(TaskAttemptListenerImpTezDag.class);

  private final AppContext context;

  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final ContainerHeartbeatHandler containerHeartbeatHandler;
  private final JobTokenSecretManager jobTokenSecretManager;
  private InetSocketAddress address;
  private Server server;

  static class ContainerInfo {
    ContainerInfo() {
      this.lastReponse = null;
      this.lastRequestId = 0;
      this.amContainerTask = null;
      this.taskPulled = false;
    }
    long lastRequestId;
    TezHeartbeatResponse lastReponse;
    AMContainerTask amContainerTask;
    boolean taskPulled;
  }

  private ConcurrentMap<TezTaskAttemptID, ContainerId> attemptToInfoMap =
      new ConcurrentHashMap<TezTaskAttemptID, ContainerId>();

  private ConcurrentHashMap<ContainerId, ContainerInfo> registeredContainers =
      new ConcurrentHashMap<ContainerId, ContainerInfo>();

  public TaskAttemptListenerImpTezDag(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh,
      JobTokenSecretManager jobTokenSecretManager) {
    super(TaskAttemptListenerImpTezDag.class.getName());
    this.context = context;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
  }

  @Override
  public void serviceStart() {
    startRpcServer();
  }

  protected void startRpcServer() {
    Configuration conf = getConfig();
    if (!conf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT)) {
      try {
        server = new RPC.Builder(conf)
            .setProtocol(TezTaskUmbilicalProtocol.class)
            .setBindAddress("0.0.0.0")
            .setPort(0)
            .setInstance(this)
            .setNumHandlers(
                conf.getInt(TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                    TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
            .setSecretManager(jobTokenSecretManager).build();

        // Enable service authorization?
        if (conf.getBoolean(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
            false)) {
          refreshServiceAcls(conf, new TezAMPolicyProvider());
        }

        server.start();
        this.address = NetUtils.getConnectAddress(server);
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    } else {
      try {
        this.address = new InetSocketAddress(InetAddress.getLocalHost(), 0);
      } catch (UnknownHostException e) {
        throw new TezUncheckedException(e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not starting TaskAttemptListener RPC in LocalMode");
      }
    }
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() {
    stopRpcServer();
  }

  protected void stopRpcServer() {
    if (server != null) {
      server.stop();
    }
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol,
        clientVersion, clientMethodsHash);
  }

  @Override
  public ContainerTask getTask(ContainerContext containerContext)
      throws IOException {

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
      if (!registeredContainers.containsKey(containerId)) {
        if(context.getAllContainers().get(containerId) == null) {
          LOG.info("Container with id: " + containerId
              + " is invalid and will be killed");
        } else {
          LOG.info("Container with id: " + containerId
              + " is valid, but no longer registered, and will be killed");
        }
        task = TASK_FOR_INVALID_JVM;
      } else {
        pingContainerHeartbeatHandler(containerId);
        task = getContainerTask(containerId);
        if (task == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No task current assigned to Container with id: " + containerId);
          }
        } else if (task == TASK_FOR_INVALID_JVM) { 
          LOG.info("Container with id: " + containerId
              + " is valid, but no longer registered, and will be killed. Race condition.");          
        } else {
          context.getEventHandler().handle(
              new TaskAttemptEventStartedRemotely(task.getTaskSpec()
                  .getTaskAttemptID(), containerId, context
                  .getApplicationACLs()));
          LOG.info("Container with id: " + containerId + " given task: "
              + task.getTaskSpec().getTaskAttemptID());
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("getTask returning task: " + task);
    }
    return task;
  }

  /**
   * Child checking whether it can commit.
   *
   * <br/>
   * Repeatedly polls the ApplicationMaster whether it
   * {@link Task#canCommit(TezTaskAttemptID)} This is * a legacy from the
   * centralized commit protocol handling by the JobTracker.
   */
  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
    LOG.info("Commit go/no-go request from " + taskAttemptId.toString());
    // An attempt is asking if it can commit its output. This can be decided
    // only by the task which is managing the multiple attempts. So redirect the
    // request there.
    taskHeartbeatHandler.progressing(taskAttemptId);
    pingContainerHeartbeatHandler(taskAttemptId);

    DAG job = context.getCurrentDAG();
    Task task =
        job.getVertex(taskAttemptId.getTaskID().getVertexID()).
            getTask(taskAttemptId.getTaskID());
    return task.canCommit(taskAttemptId);
  }

  @Override
  public void unregisterTaskAttempt(TezTaskAttemptID attemptId) {
    ContainerId containerId = attemptToInfoMap.get(attemptId);
    if(containerId == null) {
      LOG.warn("Unregister task attempt: " + attemptId + " from unknown container");
      return;
    }
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if(containerInfo == null) {
      LOG.warn("Unregister task attempt: " + attemptId +
          " from non-registered container: " + containerId);
      return;
    }
    synchronized (containerInfo) {
      containerInfo.amContainerTask = null;
      attemptToInfoMap.remove(attemptId);
    }

  }

  @Override
  public void dagComplete(DAG dag) {
    // TODO TEZ-2335. Cleanup TaskHeartbeat handler structures.
    // TODO TEZ-2345. Also cleanup attemptInfo map, so that any tasks which heartbeat are told to die.
    // Container structures remain unchanged - since they could be re-used across restarts.
    // This becomes more relevant when task kills without container kills are allowed.

    // TODO TEZ-2336. Send a signal to containers indicating DAG completion.
  }

  @Override
  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }

  @Override
  public void registerRunningContainer(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerId: " + containerId
          + " registered with TaskAttemptListener");
    }
    ContainerInfo oldInfo = registeredContainers.put(containerId, new ContainerInfo());
    if(oldInfo != null) {
      throw new TezUncheckedException(
          "Multiple registrations for containerId: " + containerId);
    }
  }

  @Override
  public void registerTaskAttempt(AMContainerTask amContainerTask,
      ContainerId containerId) {
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if(containerInfo == null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to unknown container: " + containerId);
    }
    synchronized (containerInfo) {
      if(containerInfo.amContainerTask != null) {
        throw new TezUncheckedException("Registering task attempt: "
            + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
            + " with existing assignment to: " + containerInfo.amContainerTask.getTask().getTaskAttemptID());
      }
      containerInfo.amContainerTask = amContainerTask;
      containerInfo.taskPulled = false;

      ContainerId containerIdFromMap =
          attemptToInfoMap.put(amContainerTask.getTask().getTaskAttemptID(), containerId);
      if(containerIdFromMap != null) {
        throw new TezUncheckedException("Registering task attempt: "
            + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
            + " when already assigned to: " + containerIdFromMap);
      }
    }
  }

  @Override
  public void unregisterRunningContainer(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Container from TaskAttemptListener: "
          + containerId);
    }
    registeredContainers.remove(containerId);
  }

  private void pingContainerHeartbeatHandler(ContainerId containerId) {
    containerHeartbeatHandler.pinged(containerId);
  }

  private void pingContainerHeartbeatHandler(TezTaskAttemptID taskAttemptId) {
    ContainerId containerId = attemptToInfoMap.get(taskAttemptId);
    if (containerId != null) {
      containerHeartbeatHandler.pinged(containerId);
    } else {
      LOG.warn("Handling communication from attempt: " + taskAttemptId
          + ", ContainerId not known for this attempt");
    }
  }

  @Override
  public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request)
      throws IOException, TezException {
    ContainerId containerId = ConverterUtils.toContainerId(request
        .getContainerIdentifier());
    long requestId = request.getRequestId();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received heartbeat from container"
          + ", request=" + request);
    }

    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if(containerInfo == null) {
      LOG.warn("Received task heartbeat from unknown container with id: " + containerId +
          ", asking it to die");
      TezHeartbeatResponse response = new TezHeartbeatResponse();
      response.setLastRequestId(requestId);
      response.setShouldDie();
      return response;
    }

    synchronized (containerInfo) {
      pingContainerHeartbeatHandler(containerId);

      if(containerInfo.lastRequestId == requestId) {
        LOG.warn("Old sequenceId received: " + requestId
            + ", Re-sending last response to client");
        return containerInfo.lastReponse;
      }

      TezHeartbeatResponse response = new TezHeartbeatResponse();
      response.setLastRequestId(requestId);

      TezTaskAttemptID taskAttemptID = request.getCurrentTaskAttemptID();
      if (taskAttemptID != null) {
        ContainerId containerIdFromMap = attemptToInfoMap.get(taskAttemptID);
        if(containerIdFromMap == null || !containerIdFromMap.equals(containerId)) {
          throw new TezException("Attempt " + taskAttemptID
            + " is not recognized for heartbeat");
        }

        if(containerInfo.lastRequestId+1 != requestId) {
          throw new TezException("Container " + containerId
              + " has invalid request id. Expected: "
              + containerInfo.lastRequestId+1
              + " and actual: " + requestId);
        }

        List<TezEvent> inEvents = request.getEvents();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ping from " + taskAttemptID.toString() +
              " events: " + (inEvents != null? inEvents.size() : -1));
        }

        List<TezEvent> otherEvents = new ArrayList<TezEvent>();
        for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
          final EventType eventType = tezEvent.getEventType();
          if (eventType == EventType.TASK_STATUS_UPDATE_EVENT ||
              eventType == EventType.TASK_ATTEMPT_COMPLETED_EVENT) {
            context.getEventHandler()
                .handle(getTaskAttemptEventFromTezEvent(taskAttemptID, tezEvent));
          } else {
            otherEvents.add(tezEvent);
          }
        }
        if(!otherEvents.isEmpty()) {
          TezVertexID vertexId = taskAttemptID.getTaskID().getVertexID();
          context.getEventHandler().handle(
              new VertexEventRouteEvent(vertexId, Collections.unmodifiableList(otherEvents)));
        }
        taskHeartbeatHandler.pinged(taskAttemptID);
        List<TezEvent> outEvents = context
            .getCurrentDAG()
            .getVertex(taskAttemptID.getTaskID().getVertexID())
            .getTask(taskAttemptID.getTaskID())
            .getTaskAttemptTezEvents(taskAttemptID, request.getStartIndex(),
                request.getMaxEvents());
        response.setEvents(outEvents);
      }
      containerInfo.lastRequestId = requestId;
      containerInfo.lastReponse = response;
      return response;
    }
  }

  private TaskAttemptEvent getTaskAttemptEventFromTezEvent(TezTaskAttemptID taskAttemptID,
                                                           TezEvent tezEvent) {
    final EventType eventType = tezEvent.getEventType();
    TaskAttemptEvent taskAttemptEvent;
    switch (eventType) {
      case TASK_STATUS_UPDATE_EVENT:
        {
          taskAttemptEvent = new TaskAttemptEventStatusUpdate(taskAttemptID,
              (TaskStatusUpdateEvent) tezEvent.getEvent());
        }
        break;
      case TASK_ATTEMPT_COMPLETED_EVENT:
        {
          taskAttemptEvent = new TaskAttemptEvent(taskAttemptID, TaskAttemptEventType.TA_DONE);
        }
        break;
      default:
        throw new TezUncheckedException("unknown event type " + eventType);
    }
    return taskAttemptEvent;
  }

  private Map<String, TezLocalResource> convertLocalResourceMap(Map<String, LocalResource> ylrs)
      throws IOException {
    Map<String, TezLocalResource> tlrs = Maps.newHashMap();
    if (ylrs != null) {
      for (Entry<String, LocalResource> ylrEntry : ylrs.entrySet()) {
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

  private ContainerTask getContainerTask(ContainerId containerId) throws IOException {
    ContainerTask containerTask = null;
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      // This can happen if an unregisterTask comes in after we've done the initial checks for
      // registered containers. (Race between getTask from the container, and a potential STOP_CONTAINER
      // from somewhere within the AM)
      // Implies that an un-registration has taken place and the container needs to be asked to die.
      LOG.info("Container with id: " + containerId
          + " is valid, but no longer registered, and will be killed");
      containerTask = TASK_FOR_INVALID_JVM;
    } else {
      synchronized (containerInfo) {
        if (containerInfo.amContainerTask != null) {
          if (!containerInfo.taskPulled) {
            containerInfo.taskPulled = true;
            AMContainerTask amContainerTask = containerInfo.amContainerTask;
            containerTask = new ContainerTask(amContainerTask.getTask(), false,
                convertLocalResourceMap(amContainerTask.getAdditionalResources()),
                amContainerTask.getCredentials(), amContainerTask.haveCredentialsChanged());
          } else {
            containerTask = null;
          }
        } else {
          containerTask = null;
        }
      }
    }
    return containerTask;
  }
}

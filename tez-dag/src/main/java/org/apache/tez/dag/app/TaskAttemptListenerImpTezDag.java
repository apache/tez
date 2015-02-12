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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections4.ListUtils;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TaskCommunicator;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.dag.api.TaskHeartbeatRequest;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.rm.TaskSchedulerService;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.common.security.JobTokenSecretManager;


@SuppressWarnings("unchecked")
@InterfaceAudience.Private
public class TaskAttemptListenerImpTezDag extends AbstractService implements
    TaskAttemptListener, TaskCommunicatorContext {

  private static final Logger LOG = LoggerFactory
      .getLogger(TaskAttemptListenerImpTezDag.class);

  private final AppContext context;
  private TaskCommunicator taskCommunicator;

  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final ContainerHeartbeatHandler containerHeartbeatHandler;

  private final TaskHeartbeatResponse RESPONSE_SHOULD_DIE = new TaskHeartbeatResponse(true, null);

  private final ConcurrentMap<TezTaskAttemptID, ContainerId> registeredAttempts =
      new ConcurrentHashMap<TezTaskAttemptID, ContainerId>();
  private final ConcurrentMap<ContainerId, ContainerInfo> registeredContainers =
      new ConcurrentHashMap<ContainerId, ContainerInfo>();

  // Defined primarily to work around ConcurrentMaps not accepting null values
  private static final class ContainerInfo {
    TezTaskAttemptID taskAttemptId;
    ContainerInfo(TezTaskAttemptID taskAttemptId) {
      this.taskAttemptId = taskAttemptId;
    }
  }

  private static final ContainerInfo NULL_CONTAINER_INFO = new ContainerInfo(null);


  public TaskAttemptListenerImpTezDag(AppContext context,
                                      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh,
                                      // TODO TEZ-2003 pre-merge. Remove reference to JobTokenSecretManager.
                                      JobTokenSecretManager jobTokenSecretManager) {
    super(TaskAttemptListenerImpTezDag.class.getName());
    this.context = context;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
    this.taskCommunicator = new TezTaskCommunicatorImpl(this);
  }

  @Override
  public void serviceInit(Configuration conf) {
    String taskCommClassName = conf.get(TezConfiguration.TEZ_AM_TASK_COMMUNICATOR_CLASS);
    if (taskCommClassName == null) {
      LOG.info("Using Default Task Communicator");
      this.taskCommunicator = new TezTaskCommunicatorImpl(this);
    } else {
      LOG.info("Using TaskCommunicator: " + taskCommClassName);
      Class<? extends TaskCommunicator> taskCommClazz = (Class<? extends TaskCommunicator>) ReflectionUtils
          .getClazz(taskCommClassName);
      try {
        Constructor<? extends TaskCommunicator> ctor = taskCommClazz.getConstructor(TaskCommunicatorContext.class);
        ctor.setAccessible(true);
        this.taskCommunicator = ctor.newInstance(this);
      } catch (NoSuchMethodException e) {
        throw new TezUncheckedException(e);
      } catch (InvocationTargetException e) {
        throw new TezUncheckedException(e);
      } catch (InstantiationException e) {
        throw new TezUncheckedException(e);
      } catch (IllegalAccessException e) {
        throw new TezUncheckedException(e);
      }
    }
  }

  @Override
  public void serviceStart() {
    taskCommunicator.init(getConfig());
    taskCommunicator.start();
  }

  @Override
  public void serviceStop() {
    if (taskCommunicator != null) {
      taskCommunicator.stop();
      taskCommunicator = null;
    }
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return context.getApplicationAttemptId();
  }

  @Override
  public Credentials getCredentials() {
    return context.getAppCredentials();
  }

  @Override
  public TaskHeartbeatResponse heartbeat(TaskHeartbeatRequest request)
      throws IOException, TezException {
    ContainerId containerId = ConverterUtils.toContainerId(request
        .getContainerIdentifier());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received heartbeat from container"
          + ", request=" + request);
    }

    if (!registeredContainers.containsKey(containerId)) {
      LOG.warn("Received task heartbeat from unknown container with id: " + containerId +
          ", asking it to die");
      return RESPONSE_SHOULD_DIE;
    }

    // A heartbeat can come in anytime. The AM may have made a decision to kill a running task/container
    // meanwhile. If the decision is processed through the pipeline before the heartbeat is processed,
    // the heartbeat will be dropped. Otherwise the heartbeat will be processed - and the system
    // know how to handle this - via FailedInputEvents for example (relevant only if the heartbeat has events).
    // So - avoiding synchronization.

    pingContainerHeartbeatHandler(containerId);
    List<TezEvent> outEvents = null;
    TezTaskAttemptID taskAttemptID = request.getTaskAttemptId();
    if (taskAttemptID != null) {
      ContainerId containerIdFromMap = registeredAttempts.get(taskAttemptID);
      if (containerIdFromMap == null || !containerIdFromMap.equals(containerId)) {
        // This can happen when a task heartbeats. Meanwhile the container is unregistered.
        // The information will eventually make it through to the plugin via a corresponding unregister.
        // There's a race in that case between the unregister making it through, and this method returning.
        // TODO TEZ-2003. An exception back is likely a better approach than sending a shouldDie = true,
        // so that the plugin can handle the scenario. Alternately augment the response with error codes.
        // Error codes would be better than exceptions.
        LOG.info("Attempt: " + taskAttemptID + " is not recognized for heartbeats");
        return RESPONSE_SHOULD_DIE;
      }

      List<TezEvent> inEvents = request.getEvents();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ping from " + taskAttemptID.toString() +
            " events: " + (inEvents != null ? inEvents.size() : -1));
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
      outEvents = context
          .getCurrentDAG()
          .getVertex(taskAttemptID.getTaskID().getVertexID())
          .getTask(taskAttemptID.getTaskID())
          .getTaskAttemptTezEvents(taskAttemptID, request.getStartIndex(),
              request.getMaxEvents());
    }
    return new TaskHeartbeatResponse(false, outEvents);
  }

  @Override
  public boolean isKnownContainer(ContainerId containerId) {
    return context.getAllContainers().get(containerId) != null;
  }

  @Override
  public void taskStartedRemotely(TezTaskAttemptID taskAttemptID, ContainerId containerId) {
    context.getEventHandler().handle(new TaskAttemptEventStartedRemotely(taskAttemptID, containerId, null));
    pingContainerHeartbeatHandler(containerId);
  }

  /**
   * Child checking whether it can commit.
   * <p/>
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
  public InetSocketAddress getAddress() {
    return taskCommunicator.getAddress();
  }

  // The TaskAttemptListener register / unregister methods in this class are not thread safe.
  // The Tez framework should not invoke these methods from multiple threads.
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
      LOG.debug("ContainerId: " + containerId + " registered with TaskAttemptListener");
    }
    ContainerInfo oldInfo = registeredContainers.put(containerId, NULL_CONTAINER_INFO);
    if (oldInfo != null) {
      throw new TezUncheckedException(
          "Multiple registrations for containerId: " + containerId);
    }
    NodeId nodeId = context.getAllContainers().get(containerId).getContainer().getNodeId();
    taskCommunicator.registerRunningContainer(containerId, nodeId.getHost(), nodeId.getPort());
  }

  @Override
  public void unregisterRunningContainer(ContainerId containerId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Container from TaskAttemptListener: " + containerId);
    }
    ContainerInfo containerInfo = registeredContainers.remove(containerId);
    if (containerInfo.taskAttemptId != null) {
      registeredAttempts.remove(containerInfo.taskAttemptId);
    }
    taskCommunicator.registerContainerEnd(containerId);
  }

  @Override
  public void registerTaskAttempt(AMContainerTask amContainerTask,
                                  ContainerId containerId) {
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to unknown container: " + containerId);
    }
    if (containerInfo.taskAttemptId != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " with existing assignment to: " +
          containerInfo.taskAttemptId);
    }

    if (containerInfo.taskAttemptId != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " with existing assignment to: " +
          containerInfo.taskAttemptId);
    }

    // Explicitly putting in a new entry so that synchronization is not required on the existing element in the map.
    registeredContainers.put(containerId, new ContainerInfo(amContainerTask.getTask().getTaskAttemptID()));

    ContainerId containerIdFromMap = registeredAttempts.put(
        amContainerTask.getTask().getTaskAttemptID(), containerId);
    if (containerIdFromMap != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " when already assigned to: " + containerIdFromMap);
    }
    taskCommunicator.registerRunningTaskAttempt(containerId, amContainerTask.getTask(),
        amContainerTask.getAdditionalResources(), amContainerTask.getCredentials(),
        amContainerTask.haveCredentialsChanged());
  }

  @Override
  public void unregisterTaskAttempt(TezTaskAttemptID attemptId) {
    ContainerId containerId = registeredAttempts.remove(attemptId);
    if (containerId == null) {
      LOG.warn("Unregister task attempt: " + attemptId + " from unknown container");
      return;
    }
    ContainerInfo containerInfo = registeredContainers.get(containerId);
    if (containerInfo == null) {
      LOG.warn("Unregister task attempt: " + attemptId +
          " from non-registered container: " + containerId);
      return;
    }
    // Explicitly putting in a new entry so that synchronization is not required on the existing element in the map.
    registeredContainers.put(containerId, NULL_CONTAINER_INFO);
    taskCommunicator.unregisterRunningTaskAttempt(attemptId);
  }

  private void pingContainerHeartbeatHandler(ContainerId containerId) {
    containerHeartbeatHandler.pinged(containerId);
  }

  private void pingContainerHeartbeatHandler(TezTaskAttemptID taskAttemptId) {
    ContainerId containerId = registeredAttempts.get(taskAttemptId);
    if (containerId != null) {
      containerHeartbeatHandler.pinged(containerId);
    } else {
      LOG.warn("Handling communication from attempt: " + taskAttemptId
          + ", ContainerId not known for this attempt");
    }
  }


  public TaskCommunicator getTaskCommunicator() {
    return taskCommunicator;
  }
}

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.yarn.event.Event;
import org.apache.tez.Utils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicator;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskHeartbeatResponse;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.serviceplugins.api.TaskHeartbeatRequest;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptKilled;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTezEventUpdate;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.rm.container.AMContainerTask;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TezEvent;


@SuppressWarnings("unchecked")
@InterfaceAudience.Private
public class TaskCommunicatorManager extends AbstractService implements
    TaskCommunicatorManagerInterface {

  private static final Logger LOG = LoggerFactory
      .getLogger(TaskCommunicatorManager.class);

  private final AppContext context;
  private final TaskCommunicatorWrapper[] taskCommunicators;
  private final TaskCommunicatorContext[] taskCommunicatorContexts;
  protected final ServicePluginLifecycleAbstractService []taskCommunicatorServiceWrappers;

  protected final TaskHeartbeatHandler taskHeartbeatHandler;
  protected final ContainerHeartbeatHandler containerHeartbeatHandler;

  private final TaskHeartbeatResponse RESPONSE_SHOULD_DIE = new TaskHeartbeatResponse(true, null, 0, 0);

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


  @VisibleForTesting
  @InterfaceAudience.Private
  /**
   * Only for testing.
   */
  public TaskCommunicatorManager(TaskCommunicator taskCommunicator, AppContext appContext,
                                 TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh) {
    super(TaskCommunicatorManager.class.getName());
    this.context = appContext;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
    taskCommunicators =
        new TaskCommunicatorWrapper[]{new TaskCommunicatorWrapper(taskCommunicator)};
    taskCommunicatorContexts = new TaskCommunicatorContext[]{taskCommunicator.getContext()};
    taskCommunicatorServiceWrappers = new ServicePluginLifecycleAbstractService[]{
        new ServicePluginLifecycleAbstractService(taskCommunicator)};
  }

  public TaskCommunicatorManager(AppContext context,
                                 TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh,
                                 List<NamedEntityDescriptor> taskCommunicatorDescriptors) throws TezException {
    super(TaskCommunicatorManager.class.getName());
    this.context = context;
    this.taskHeartbeatHandler = thh;
    this.containerHeartbeatHandler = chh;
    Preconditions.checkArgument(
        taskCommunicatorDescriptors != null && !taskCommunicatorDescriptors.isEmpty(),
        "TaskCommunicators must be specified");
    this.taskCommunicators = new TaskCommunicatorWrapper[taskCommunicatorDescriptors.size()];
    this.taskCommunicatorContexts = new TaskCommunicatorContext[taskCommunicatorDescriptors.size()];
    this.taskCommunicatorServiceWrappers = new ServicePluginLifecycleAbstractService[taskCommunicatorDescriptors.size()];
    for (int i = 0 ; i < taskCommunicatorDescriptors.size() ; i++) {
      UserPayload userPayload = taskCommunicatorDescriptors.get(i).getUserPayload();
      taskCommunicatorContexts[i] = new TaskCommunicatorContextImpl(context, this, userPayload, i);
      taskCommunicators[i] = new TaskCommunicatorWrapper(createTaskCommunicator(taskCommunicatorDescriptors.get(i), i));
      taskCommunicatorServiceWrappers[i] =
          new ServicePluginLifecycleAbstractService(taskCommunicators[i].getTaskCommunicator());
    }
    // TODO TEZ-2118 Start using taskCommunicator indices properly
  }

  @Override
  public void serviceStart() {
    // TODO Why is init tied to serviceStart
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      taskCommunicatorServiceWrappers[i].init(getConfig());
      taskCommunicatorServiceWrappers[i].start();
    }
  }

  @Override
  public void serviceStop() {
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      taskCommunicatorServiceWrappers[i].stop();
    }
  }

  @VisibleForTesting
  TaskCommunicator createTaskCommunicator(NamedEntityDescriptor taskCommDescriptor,
                                          int taskCommIndex) throws TezException {
    if (taskCommDescriptor.getEntityName().equals(TezConstants.getTezYarnServicePluginName())) {
      return createDefaultTaskCommunicator(taskCommunicatorContexts[taskCommIndex]);
    } else if (taskCommDescriptor.getEntityName()
        .equals(TezConstants.getTezUberServicePluginName())) {
      return createUberTaskCommunicator(taskCommunicatorContexts[taskCommIndex]);
    } else {
      return createCustomTaskCommunicator(taskCommunicatorContexts[taskCommIndex],
          taskCommDescriptor);
    }
  }

  @VisibleForTesting
  TaskCommunicator createDefaultTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) {
    LOG.info("Creating Default Task Communicator");
    return new TezTaskCommunicatorImpl(taskCommunicatorContext);
  }

  @VisibleForTesting
  TaskCommunicator createUberTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) {
    LOG.info("Creating Default Local Task Communicator");
    return new TezLocalTaskCommunicatorImpl(taskCommunicatorContext);
  }

  @VisibleForTesting
  TaskCommunicator createCustomTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext,
                                                NamedEntityDescriptor taskCommDescriptor)
                                                    throws TezException {
    LOG.info("Creating TaskCommunicator {}:{} " + taskCommDescriptor.getEntityName(),
        taskCommDescriptor.getClassName());
    Class<? extends TaskCommunicator> taskCommClazz =
        (Class<? extends TaskCommunicator>) ReflectionUtils
            .getClazz(taskCommDescriptor.getClassName());
    try {
      Constructor<? extends TaskCommunicator> ctor =
          taskCommClazz.getConstructor(TaskCommunicatorContext.class);
      return ctor.newInstance(taskCommunicatorContext);
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new TezUncheckedException(e);
    }
  }

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
    TaskAttemptEventInfo eventInfo = new TaskAttemptEventInfo(0, null, 0);
    TezTaskAttemptID taskAttemptID = request.getTaskAttemptId();
    if (taskAttemptID != null) {
      ContainerId containerIdFromMap = registeredAttempts.get(taskAttemptID);
      if (containerIdFromMap == null || !containerIdFromMap.equals(containerId)) {
        // This can happen when a task heartbeats. Meanwhile the container is unregistered.
        // The information will eventually make it through to the plugin via a corresponding unregister.
        // There's a race in that case between the unregister making it through, and this method returning.
        // TODO TEZ-2003 (post) TEZ-2666. An exception back is likely a better approach than sending a shouldDie = true,
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

      long currTime = context.getClock().getTime();
      // taFinishedEvents - means the TaskAttemptFinishedEvent
      // taGeneratedEvents - for recovery, means the events generated by this task attempt and is needed by its downstream vertices
      // eventsForVertex - including all the taGeneratedEvents and other events such as INPUT_READ_ERROR_EVENT/INPUT_FAILED_EVENT
      // taGeneratedEvents is routed both to TaskAttempt & Vertex. Route to Vertex is for performance consideration
      // taFinishedEvents must be routed before taGeneratedEvents
      List<TezEvent> taFinishedEvents = new ArrayList<TezEvent>();
      List<TezEvent> taGeneratedEvents = new ArrayList<TezEvent>();
      List<TezEvent> eventsForVertex = new ArrayList<TezEvent>();
      TaskAttemptEventStatusUpdate taskAttemptEvent = null;
      boolean readErrorReported = false;
      for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
        // for now, set the event time on the AM when it is received.
        // this avoids any time disparity between machines.
        tezEvent.setEventReceivedTime(currTime);
        final EventType eventType = tezEvent.getEventType();
        if (eventType == EventType.TASK_STATUS_UPDATE_EVENT) {
          // send TA_STATUS_UPDATE before TA_DONE/TA_FAILED/TA_KILLED otherwise Status may be missed
          taskAttemptEvent = new TaskAttemptEventStatusUpdate(taskAttemptID,
              (TaskStatusUpdateEvent) tezEvent.getEvent());
        } else if (eventType == EventType.TASK_ATTEMPT_COMPLETED_EVENT
           || eventType == EventType.TASK_ATTEMPT_FAILED_EVENT) {
          taFinishedEvents.add(tezEvent);
        } else {
          if (eventType == EventType.INPUT_READ_ERROR_EVENT) {
            readErrorReported = true;
          }
          if (eventType == EventType.DATA_MOVEMENT_EVENT
            || eventType == EventType.COMPOSITE_DATA_MOVEMENT_EVENT
            || eventType == EventType.ROOT_INPUT_INITIALIZER_EVENT
            || eventType == EventType.VERTEX_MANAGER_EVENT) {
            taGeneratedEvents.add(tezEvent);
          }
          eventsForVertex.add(tezEvent);
        }
      }
      if (taskAttemptEvent != null) {
        taskAttemptEvent.setReadErrorReported(readErrorReported);
        sendEvent(taskAttemptEvent);
      }
      // route taGeneratedEvents to TaskAttempt
      if (!taGeneratedEvents.isEmpty()) {
        sendEvent(new TaskAttemptEventTezEventUpdate(taskAttemptID, taGeneratedEvents));
      }
      // route events to TaskAttempt
      Preconditions.checkArgument(taFinishedEvents.size() <= 1, "Multiple TaskAttemptFinishedEvent");
      for (TezEvent e : taFinishedEvents) {
        EventMetaData sourceMeta = e.getSourceInfo();
        switch (e.getEventType()) {
        case TASK_ATTEMPT_FAILED_EVENT:
          TaskAttemptTerminationCause errCause = null;
          switch (sourceMeta.getEventGenerator()) {
          case INPUT:
            errCause = TaskAttemptTerminationCause.INPUT_READ_ERROR;
            break;
          case PROCESSOR:
            errCause = TaskAttemptTerminationCause.APPLICATION_ERROR;
            break;
          case OUTPUT:
            errCause = TaskAttemptTerminationCause.OUTPUT_WRITE_ERROR;
            break;
          case SYSTEM:
            errCause = TaskAttemptTerminationCause.FRAMEWORK_ERROR;
            break;
          default:
            throw new TezUncheckedException("Unknown EventProducerConsumerType: " +
                sourceMeta.getEventGenerator());
          }
          TaskAttemptFailedEvent taskFailedEvent =(TaskAttemptFailedEvent) e.getEvent();
          sendEvent(
               new TaskAttemptEventAttemptFailed(sourceMeta.getTaskAttemptID(),
                   TaskAttemptEventType.TA_FAILED,
                  "Error: " + taskFailedEvent.getDiagnostics(),
                    errCause));
          break;
        case TASK_ATTEMPT_COMPLETED_EVENT:
          sendEvent(
              new TaskAttemptEvent(sourceMeta.getTaskAttemptID(), TaskAttemptEventType.TA_DONE));
          break;
        default:
          throw new TezUncheckedException("Unhandled tez event type: "
             + e.getEventType());
        }
      }
      if (!eventsForVertex.isEmpty()) {
        TezVertexID vertexId = taskAttemptID.getTaskID().getVertexID();
        sendEvent(
            new VertexEventRouteEvent(vertexId, Collections.unmodifiableList(eventsForVertex)));
      }
      taskHeartbeatHandler.pinged(taskAttemptID);
      eventInfo = context
          .getCurrentDAG()
          .getVertex(taskAttemptID.getTaskID().getVertexID())
          .getTaskAttemptTezEvents(taskAttemptID, request.getStartIndex(), request.getPreRoutedStartIndex(),
              request.getMaxEvents());
    }
    return new TaskHeartbeatResponse(false, eventInfo.getEvents(), eventInfo.getNextFromEventId(), eventInfo.getNextPreRoutedFromEventId());
  }

  public void taskAlive(TezTaskAttemptID taskAttemptId) {
    taskHeartbeatHandler.pinged(taskAttemptId);
  }

  public void containerAlive(ContainerId containerId) {
    pingContainerHeartbeatHandler(containerId);
  }

  public void taskStartedRemotely(TezTaskAttemptID taskAttemptID, ContainerId containerId) {
    sendEvent(new TaskAttemptEventStartedRemotely(taskAttemptID, containerId, null));
    pingContainerHeartbeatHandler(containerId);
  }

  public void taskKilled(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         String diagnostics) {
    // Regular flow via TaskAttempt will take care of un-registering from the heartbeat handler,
    // and messages from the scheduler will release the container.
    // TODO TEZ-2003 (post) TEZ-2671 Maybe consider un-registering here itself, since the task is not active anymore,
    // instead of waiting for the unregister to flow through the Container.
    // Fix along the same lines as TEZ-2124 by introducing an explict context.
    sendEvent(new TaskAttemptEventAttemptKilled(taskAttemptId,
        diagnostics, TezUtilsInternal.fromTaskAttemptEndReason(
        taskAttemptEndReason)));
  }

  public void taskFailed(TezTaskAttemptID taskAttemptId, TaskAttemptEndReason taskAttemptEndReason,
                         String diagnostics) {
    // Regular flow via TaskAttempt will take care of un-registering from the heartbeat handler,
    // and messages from the scheduler will release the container.
    // TODO TEZ-2003 (post) TEZ-2671 Maybe consider un-registering here itself, since the task is not active anymore,
    // instead of waiting for the unregister to flow through the Container.
    // Fix along the same lines as TEZ-2124 by introducing an explict context.
    sendEvent(new TaskAttemptEventAttemptFailed(taskAttemptId,
        TaskAttemptEventType.TA_FAILED, diagnostics, TezUtilsInternal.fromTaskAttemptEndReason(
        taskAttemptEndReason)));
  }

  public void vertexStateUpdateNotificationReceived(VertexStateUpdate event, int taskCommIndex) {
    try {
      taskCommunicators[taskCommIndex].onVertexStateUpdated(event);
    } catch (Exception e) {
      String msg = "Error in TaskCommunicator when handling vertex state update notification"
          + ", communicator=" + Utils.getTaskCommIdentifierString(taskCommIndex, context)
          + ", vertexName=" + event.getVertexName()
          + ", vertexState=" + event.getVertexState();
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }


  /**
   * Child checking whether it can commit.
   * <p/>
   * <br/>
   * Repeatedly polls the ApplicationMaster whether it
   * {@link Task#canCommit(TezTaskAttemptID)} This is * a legacy from the
   * centralized commit protocol handling by the JobTracker.
   */
//  @Override
  public boolean canCommit(TezTaskAttemptID taskAttemptId) throws IOException {
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

  // The TaskAttemptListener register / unregister methods in this class are not thread safe.
  // The Tez framework should not invoke these methods from multiple threads.
  @Override
  public void dagComplete(DAG dag) {
    // TODO TEZ-2335. Cleanup TaskHeartbeat handler structures.
    // TODO TEZ-2345. Also cleanup attemptInfo map, so that any tasks which heartbeat are told to die.
    // Container structures remain unchanged - since they could be re-used across restarts.
    // This becomes more relevant when task kills without container kills are allowed.

    // TODO TEZ-2336. Send a signal to containers indicating DAG completion.

    // Inform all communicators of the dagCompletion.
    for (int i = 0 ; i < taskCommunicators.length ; i++) {
      try {
        ((TaskCommunicatorContextImpl) taskCommunicatorContexts[i]).dagCompleteStart(dag);
        taskCommunicators[i].dagComplete(dag.getID().getId());
        ((TaskCommunicatorContextImpl) taskCommunicatorContexts[i]).dagCompleteEnd();
      } catch (Exception e) {
        String msg = "Error in TaskCommunicator when notifying for DAG completion"
            + ", communicator=" + Utils.getTaskCommIdentifierString(i, context);
        LOG.error(msg, e);
        sendEvent(
            new DAGAppMasterEventUserServiceFatalError(
                DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
                msg, e));
      }
    }

  }

  @Override
  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, int taskCommId) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ContainerId: " + containerId + " registered with TaskAttemptListener");
    }
    ContainerInfo oldInfo = registeredContainers.put(containerId, NULL_CONTAINER_INFO);
    if (oldInfo != null) {
      throw new TezUncheckedException(
          "Multiple registrations for containerId: " + containerId);
    }
    NodeId nodeId = context.getAllContainers().get(containerId).getContainer().getNodeId();
    try {
      taskCommunicators[taskCommId].registerRunningContainer(containerId, nodeId.getHost(),
          nodeId.getPort());
    } catch (Exception e) {
      String msg = "Error in TaskCommunicator when registering running Container"
          + ", communicator=" + Utils.getTaskCommIdentifierString(taskCommId, context)
          + ", containerId=" + containerId
          + ", nodeId=" + nodeId;
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }

  @Override
  public void unregisterRunningContainer(ContainerId containerId, int taskCommId, ContainerEndReason endReason, String diagnostics) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unregistering Container from TaskAttemptListener: " + containerId);
    }
    ContainerInfo containerInfo = registeredContainers.remove(containerId);
    if (containerInfo.taskAttemptId != null) {
      registeredAttempts.remove(containerInfo.taskAttemptId);
    }
    try {
      taskCommunicators[taskCommId].registerContainerEnd(containerId, endReason, diagnostics);
    } catch (Exception e) {
      String msg = "Error in TaskCommunicator when unregistering Container"
          + ", communicator=" + Utils.getTaskCommIdentifierString(taskCommId, context)
          + ", containerId=" + containerId;
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }

  @Override
  public void registerTaskAttempt(AMContainerTask amContainerTask,
                                  ContainerId containerId, int taskCommId) {
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

    // Explicitly putting in a new entry so that synchronization is not required on the existing element in the map.
    registeredContainers.put(containerId, new ContainerInfo(amContainerTask.getTask().getTaskAttemptID()));

    ContainerId containerIdFromMap = registeredAttempts.put(
        amContainerTask.getTask().getTaskAttemptID(), containerId);
    if (containerIdFromMap != null) {
      throw new TezUncheckedException("Registering task attempt: "
          + amContainerTask.getTask().getTaskAttemptID() + " to container: " + containerId
          + " when already assigned to: " + containerIdFromMap);
    }
    try {
      taskCommunicators[taskCommId].registerRunningTaskAttempt(containerId, amContainerTask.getTask(),
          amContainerTask.getAdditionalResources(), amContainerTask.getCredentials(),
          amContainerTask.haveCredentialsChanged(), amContainerTask.getPriority());
    } catch (Exception e) {
      String msg = "Error in TaskCommunicator when registering Task Attempt"
          + ", communicator=" + Utils.getTaskCommIdentifierString(taskCommId, context)
          + ", containerId=" + containerId
          + ", taskId=" + amContainerTask.getTask().getTaskAttemptID();
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }

  @Override
  public void unregisterTaskAttempt(TezTaskAttemptID attemptId, int taskCommId, TaskAttemptEndReason endReason, String diagnostics) {
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
    try {
      taskCommunicators[taskCommId].unregisterRunningTaskAttempt(attemptId, endReason, diagnostics);
    } catch (Exception e) {
      String msg = "Error in TaskCommunicator when unregistering Task Attempt"
          + ", communicator=" + Utils.getTaskCommIdentifierString(taskCommId, context)
          + ", containerId=" + containerId
          + ", taskId=" + attemptId;
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_COMMUNICATOR_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }

  @Override
  public TaskCommunicatorWrapper getTaskCommunicator(int taskCommIndex) {
    return taskCommunicators[taskCommIndex];
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

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    context.getEventHandler().handle(event);
  }
}

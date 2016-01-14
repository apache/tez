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

package org.apache.tez.dag.app.rm;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.Utils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext.AppFinalStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TaskLocationHint.TaskBasedLocationAffinity;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventSchedulingServiceError;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerEventTASucceeded;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.app.rm.node.AMNodeEventContainerAllocated;
import org.apache.tez.dag.app.rm.node.AMNodeEventNodeCountUpdated;
import org.apache.tez.dag.app.rm.node.AMNodeEventStateChanged;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptEnded;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptSucceeded;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;

import com.google.common.base.Preconditions;


public class TaskSchedulerManager extends AbstractService implements
                                               EventHandler<AMSchedulerEvent> {
  static final Logger LOG = LoggerFactory.getLogger(TaskSchedulerManager.class);

  static final String APPLICATION_ID_PLACEHOLDER = "__APPLICATION_ID__";
  static final String HISTORY_URL_BASE = "__HISTORY_URL_BASE__";

  protected final AppContext appContext;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final String historyUrl;
  private DAGAppMaster dagAppMaster;
  private Map<ApplicationAccessType, String> appAcls = null;
  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;
  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;
  final DAGClientServer clientService;
  private final ContainerSignatureMatcher containerSignatureMatcher;
  private int cachedNodeCount = -1;
  private AtomicBoolean shouldUnregisterFlag =
      new AtomicBoolean(false);
  private final WebUIService webUI;
  private final NamedEntityDescriptor[] taskSchedulerDescriptors;
  protected final TaskSchedulerWrapper[] taskSchedulers;
  protected final ServicePluginLifecycleAbstractService []taskSchedulerServiceWrappers;

  // Single executor service shared by all Schedulers for context callbacks
  @VisibleForTesting
  final ExecutorService appCallbackExecutor;

  private final boolean isPureLocalMode;
  // If running in non local-only mode, the YARN task scheduler will always run to take care of
  // registration with YARN and heartbeats to YARN.
  // Splitting registration and heartbeats is not straight-forward due to the taskScheduler being
  // tied to a ContainerRequestType.
  // Custom AppIds to avoid container conflicts if there's multiple sources
  private final long SCHEDULER_APP_ID_BASE = 111101111;
  private final long SCHEDULER_APP_ID_INCREMENT = 111111111;

  BlockingQueue<AMSchedulerEvent> eventQueue
                              = new LinkedBlockingQueue<AMSchedulerEvent>();

  // Not tracking container / task to schedulerId. Instead relying on everything flowing through
  // the system and being propagated back via events.

  @VisibleForTesting
  @InterfaceAudience.Private
  /**
   * For Testing only
   */
  public TaskSchedulerManager(TaskScheduler taskScheduler, AppContext appContext,
                              ContainerSignatureMatcher containerSignatureMatcher,
                              DAGClientServer clientService, ExecutorService appCallbackExecutor) {
    super(TaskSchedulerManager.class.getName());
    this.appContext = appContext;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.clientService = clientService;
    this.eventHandler = appContext.getEventHandler();
    this.appCallbackExecutor = appCallbackExecutor;
    this.taskSchedulers = new TaskSchedulerWrapper[]{new TaskSchedulerWrapper(taskScheduler)};
    this.taskSchedulerServiceWrappers = new ServicePluginLifecycleAbstractService[]{
        new ServicePluginLifecycleAbstractService<>(taskScheduler)};
    this.taskSchedulerDescriptors = null;
    this.webUI = null;
    this.historyUrl = null;
    this.isPureLocalMode = false;
  }

  /**
   *
   * @param appContext
   * @param clientService
   * @param eventHandler
   * @param containerSignatureMatcher
   * @param webUI
   * @param schedulerDescriptors the list of scheduler descriptors. Tez internal classes will not have the class names populated.
   *                         An empty list defaults to using the YarnTaskScheduler as the only source.
   * @param isPureLocalMode whether the AM is running in local mode
   */
  @SuppressWarnings("rawtypes")
  public TaskSchedulerManager(AppContext appContext,
                              DAGClientServer clientService, EventHandler eventHandler,
                              ContainerSignatureMatcher containerSignatureMatcher,
                              WebUIService webUI,
                              List<NamedEntityDescriptor> schedulerDescriptors,
                              boolean isPureLocalMode) {
    super(TaskSchedulerManager.class.getName());
    Preconditions.checkArgument(schedulerDescriptors != null && !schedulerDescriptors.isEmpty(),
        "TaskSchedulerDescriptors must be specified");
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.clientService = clientService;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.webUI = webUI;
    this.historyUrl = getHistoryUrl();
    this.isPureLocalMode = isPureLocalMode;
    this.appCallbackExecutor = createAppCallbackExecutorService();
    if (this.webUI != null) {
      this.webUI.setHistoryUrl(this.historyUrl);
    }

    this.taskSchedulerDescriptors = schedulerDescriptors.toArray(new NamedEntityDescriptor[schedulerDescriptors.size()]);

    taskSchedulers = new TaskSchedulerWrapper[this.taskSchedulerDescriptors.length];
    taskSchedulerServiceWrappers = new ServicePluginLifecycleAbstractService[this.taskSchedulerDescriptors.length];
  }

  public Map<ApplicationAccessType, String> getApplicationAcls() {
    return appAcls;
  }

  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("TaskScheduler notified that iSignalled was : " + isSignalled);
  }

  public int getNumClusterNodes() {
    return cachedNodeCount;
  }
  
  public Resource getAvailableResources(int schedulerId) {
    try {
      return taskSchedulers[schedulerId].getAvailableResources();
    } catch (Exception e) {
      String msg = "Error in TaskScheduler while getting available resources"
          + ", schedule=" + Utils.getTaskSchedulerIdentifierString(schedulerId, appContext);
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      throw new RuntimeException(e);
    }
  }

  public Resource getTotalResources(int schedulerId) {
    try {
      return taskSchedulers[schedulerId].getTotalResources();
    } catch (Exception e) {
      String msg = "Error in TaskScheduler while getting total resources"
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(schedulerId, appContext);
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      throw new RuntimeException(e);
    }
  }

  private ExecutorService createAppCallbackExecutorService() {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("TaskSchedulerAppCallbackExecutor #%d")
            .setDaemon(true)
            .build());
  }

  public synchronized void handleEvent(AMSchedulerEvent sEvent) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing the event " + sEvent.toString());
    }
    switch (sEvent.getType()) {
    case S_TA_LAUNCH_REQUEST:
      handleTaLaunchRequest((AMSchedulerEventTALaunchRequest) sEvent);
      break;
    case S_TA_ENDED: // TaskAttempt considered complete.
      AMSchedulerEventTAEnded event = (AMSchedulerEventTAEnded)sEvent;
      switch(event.getState()) {
      case FAILED:
      case KILLED:
        handleTAUnsuccessfulEnd(event);
        break;
      case SUCCEEDED:
        handleTASucceeded(event);
        break;
      default:
        throw new TezUncheckedException("Unexpected TA_ENDED state: " + event.getState());
      }
      break;
    case S_CONTAINER_DEALLOCATE:
      handleContainerDeallocate((AMSchedulerEventDeallocateContainer)sEvent);
      break;
    case S_NODE_UNBLACKLISTED:
      // fall through
    case S_NODE_BLACKLISTED:
      handleNodeBlacklistUpdate((AMSchedulerEventNodeBlacklistUpdate)sEvent);
      break;
    case S_NODE_UNHEALTHY:
      break;
    case S_NODE_HEALTHY:
      // Consider changing this to work like BLACKLISTING.
      break;
    default:
      break;
    }
  }

  @Override
  public void handle(AMSchedulerEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new TezUncheckedException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    eventHandler.handle(event);
  }

  private void handleNodeBlacklistUpdate(AMSchedulerEventNodeBlacklistUpdate event) {
    boolean invalidEventType = false;
    try {
      if (event.getType() == AMSchedulerEventType.S_NODE_BLACKLISTED) {
        taskSchedulers[event.getSchedulerId()].blacklistNode(event.getNodeId());
      } else if (event.getType() == AMSchedulerEventType.S_NODE_UNBLACKLISTED) {
        taskSchedulers[event.getSchedulerId()].unblacklistNode(event.getNodeId());
      } else {
        invalidEventType = true;
      }
    } catch (Exception e) {
      String msg = "Error in TaskScheduler for handling node blacklisting"
          + ", eventType=" + event.getType()
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext);
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      return;
    }
    if (invalidEventType) {
      throw new TezUncheckedException("Invalid event type: " + event.getType());
    }
  }

  private void handleContainerDeallocate(
                                  AMSchedulerEventDeallocateContainer event) {
    ContainerId containerId = event.getContainerId();
    // TODO what happens to the task that was connected to this container?
    // current assumption is that it will eventually call handleTaStopRequest
    //TaskAttempt taskAttempt = (TaskAttempt)
    try {
      taskSchedulers[event.getSchedulerId()].deallocateContainer(containerId);
    } catch (Exception e) {
      String msg = "Error in TaskScheduler for handling Container De-allocation"
          + ", eventType=" + event.getType()
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
          + ", containerId=" + containerId;
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      return;
    }
    // TODO does this container need to be stopped via C_STOP_REQUEST
    sendEvent(new AMContainerEventStopRequest(containerId));
  }

  private void handleTAUnsuccessfulEnd(AMSchedulerEventTAEnded event) {
    TaskAttempt attempt = event.getAttempt();
    // Propagate state and failure cause (if any) when informing the scheduler about the de-allocation.
    boolean wasContainerAllocated = false;
    try {
      wasContainerAllocated = taskSchedulers[event.getSchedulerId()]
          .deallocateTask(attempt, false, event.getTaskAttemptEndReason(), event.getDiagnostics());
    } catch (Exception e) {
      String msg = "Error in TaskScheduler for handling Task De-allocation"
          + ", eventType=" + event.getType()
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
          + ", taskAttemptId=" + attempt.getID();
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      return;
    }
    // use stored value of container id in case the scheduler has removed this
    // assignment because the task has been deallocated earlier.
    // retroactive case
    ContainerId attemptContainerId = attempt.getAssignedContainerID();

    if(!wasContainerAllocated) {
      LOG.info("Task: " + attempt.getID() +
          " has no container assignment in the scheduler");
      if (attemptContainerId != null) {
        LOG.error("No container allocated to task: " + attempt.getID()
            + " according to scheduler. Task reported container id: "
            + attemptContainerId);
      }
    }

    if (attemptContainerId != null) {
      // TODO either ways send the necessary events
      // Ask the container to stop.
      sendEvent(new AMContainerEventStopRequest(attemptContainerId));
      // Inform the Node - the task has asked to be STOPPED / has already
      // stopped.
      // AMNodeImpl blacklisting logic does not account for KILLED attempts.
      sendEvent(new AMNodeEventTaskAttemptEnded(appContext.getAllContainers().
          get(attemptContainerId).getContainer().getNodeId(), event.getSchedulerId(),
          attemptContainerId,
          attempt.getID(), event.getState() == TaskAttemptState.FAILED));
    }
  }

  private void handleTASucceeded(AMSchedulerEventTAEnded event) {
    TaskAttempt attempt = event.getAttempt();
    ContainerId usedContainerId = event.getUsedContainerId();

    // This could be null if a task fails / is killed before a container is
    // assigned to it.
    if (event.getUsedContainerId() != null) {
      sendEvent(new AMContainerEventTASucceeded(usedContainerId,
          event.getAttemptID()));
      sendEvent(new AMNodeEventTaskAttemptSucceeded(appContext.getAllContainers().
          get(usedContainerId).getContainer().getNodeId(), event.getSchedulerId(), usedContainerId,
          event.getAttemptID()));
    }

    boolean wasContainerAllocated = false;

    try {
      wasContainerAllocated = taskSchedulers[event.getSchedulerId()].deallocateTask(attempt,
        true, null, event.getDiagnostics());
    } catch (Exception e) {
      String msg = "Error in TaskScheduler for handling Task De-allocation"
          + ", eventType=" + event.getType()
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
          + ", taskAttemptId=" + attempt.getID();
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      return;
    }

    if (!wasContainerAllocated) {
      LOG.error("De-allocated successful task: " + attempt.getID()
          + ", but TaskScheduler reported no container assigned to task");
    }
  }

  private void handleTaLaunchRequest(AMSchedulerEventTALaunchRequest event) {
    TaskAttempt taskAttempt = event.getTaskAttempt();
    TaskLocationHint locationHint = event.getLocationHint();
    String hosts[] = null;
    String racks[] = null;
    if (locationHint != null) {
      TaskBasedLocationAffinity taskAffinity = locationHint.getAffinitizedTask();
      if (taskAffinity != null) {
        Vertex vertex = appContext.getCurrentDAG().getVertex(taskAffinity.getVertexName());
        Preconditions.checkNotNull(vertex, "Invalid vertex in task based affinity " + taskAffinity 
            + " for attempt: " + taskAttempt.getID());
        int taskIndex = taskAffinity.getTaskIndex(); 
        Preconditions.checkState(taskIndex >=0 && taskIndex < vertex.getTotalTasks(), 
            "Invalid taskIndex in task based affinity " + taskAffinity 
            + " for attempt: " + taskAttempt.getID());
        TaskAttempt affinityAttempt = vertex.getTask(taskIndex).getSuccessfulAttempt();
        if (affinityAttempt != null) {
          Preconditions.checkNotNull(affinityAttempt.getAssignedContainerID(), affinityAttempt.getID());
          try {
            taskSchedulers[event.getSchedulerId()].allocateTask(taskAttempt,
                event.getCapability(),
                affinityAttempt.getAssignedContainerID(),
                Priority.newInstance(event.getPriority()),
                event.getContainerContext(),
                event);
          } catch (Exception e) {
            String msg = "Error in TaskScheduler for handling Task Allocation"
                + ", eventType=" + event.getType()
                + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
                + ", taskAttemptId=" + taskAttempt.getID();
            LOG.error(msg, e);
            sendEvent(
                new DAGAppMasterEventUserServiceFatalError(
                    DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
                    msg, e));
          }
          return;
        }
        LOG.info("No attempt for task affinity to " + taskAffinity + " for attempt "
            + taskAttempt.getID() + " Ignoring.");
        // fall through with null hosts/racks
      } else {
        hosts = (locationHint.getHosts() != null) ? locationHint
            .getHosts().toArray(
                new String[locationHint.getHosts().size()]) : null;
        racks = (locationHint.getRacks() != null) ? locationHint.getRacks()
            .toArray(new String[locationHint.getRacks().size()]) : null;
      }
    }

    try {
      taskSchedulers[event.getSchedulerId()].allocateTask(taskAttempt,
          event.getCapability(),
          hosts,
          racks,
          Priority.newInstance(event.getPriority()),
          event.getContainerContext(),
          event);
    } catch (Exception e) {
      String msg = "Error in TaskScheduler for handling Task Allocation"
          + ", eventType=" + event.getType()
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(event.getSchedulerId(), appContext)
          + ", taskAttemptId=" + taskAttempt.getID();
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
    }
  }

  @VisibleForTesting
  TaskScheduler createTaskScheduler(String host, int port, String trackingUrl,
                                    AppContext appContext,
                                    NamedEntityDescriptor taskSchedulerDescriptor,
                                    long customAppIdIdentifier,
                                    int schedulerId) throws TezException {
    TaskSchedulerContext rawContext =
        new TaskSchedulerContextImpl(this, appContext, schedulerId, trackingUrl,
            customAppIdIdentifier, host, port, taskSchedulerDescriptor.getUserPayload());
    TaskSchedulerContext wrappedContext = wrapTaskSchedulerContext(rawContext);
    String schedulerName = taskSchedulerDescriptor.getEntityName();
    if (schedulerName.equals(TezConstants.getTezYarnServicePluginName())) {
      return createYarnTaskScheduler(wrappedContext, schedulerId);
    } else if (schedulerName.equals(TezConstants.getTezUberServicePluginName())) {
      return createUberTaskScheduler(wrappedContext, schedulerId);
    } else {
      return createCustomTaskScheduler(wrappedContext, taskSchedulerDescriptor, schedulerId);
    }
  }

  @VisibleForTesting
  TaskSchedulerContext wrapTaskSchedulerContext(TaskSchedulerContext rawContext) {
    return new TaskSchedulerContextImplWrapper(rawContext, appCallbackExecutor);
  }

  @VisibleForTesting
  TaskScheduler createYarnTaskScheduler(TaskSchedulerContext taskSchedulerContext,
                                        int schedulerId) {
    LOG.info("Creating TaskScheduler: YarnTaskSchedulerService");
    return new YarnTaskSchedulerService(taskSchedulerContext);
  }

  @VisibleForTesting
  TaskScheduler createUberTaskScheduler(TaskSchedulerContext taskSchedulerContext,
                                        int schedulerId) {
    LOG.info("Creating TaskScheduler: Local TaskScheduler with clusterIdentifier={}",
        taskSchedulerContext.getCustomClusterIdentifier());
    return new LocalTaskSchedulerService(taskSchedulerContext);
  }

  @SuppressWarnings("unchecked")
  TaskScheduler createCustomTaskScheduler(TaskSchedulerContext taskSchedulerContext,
                                          NamedEntityDescriptor taskSchedulerDescriptor,
                                          int schedulerId) throws TezException {
    LOG.info("Creating custom TaskScheduler {}:{} with clusterIdentifier={}", taskSchedulerDescriptor.getEntityName(),
        taskSchedulerDescriptor.getClassName(), taskSchedulerContext.getCustomClusterIdentifier());
    return ReflectionUtils.createClazzInstance(taskSchedulerDescriptor.getClassName(),
        new Class[]{TaskSchedulerContext.class},
        new Object[]{taskSchedulerContext});
  }

  @VisibleForTesting
  protected void instantiateSchedulers(String host, int port, String trackingUrl,
                                       AppContext appContext) throws TezException {
    // Iterate over the list and create all the taskSchedulers
    int j = 0;
    for (int i = 0; i < taskSchedulerDescriptors.length; i++) {
      long customAppIdIdentifier;
      if (isPureLocalMode || taskSchedulerDescriptors[i].getEntityName().equals(
          TezConstants.getTezYarnServicePluginName())) { // Use the app identifier from the appId.
        customAppIdIdentifier = appContext.getApplicationID().getClusterTimestamp();
      } else {
        customAppIdIdentifier = SCHEDULER_APP_ID_BASE + (j++ * SCHEDULER_APP_ID_INCREMENT);
      }
      taskSchedulers[i] = new TaskSchedulerWrapper(createTaskScheduler(host, port,
          trackingUrl, appContext, taskSchedulerDescriptors[i], customAppIdIdentifier, i));
      taskSchedulerServiceWrappers[i] =
          new ServicePluginLifecycleAbstractService<>(taskSchedulers[i].getTaskScheduler());
    }
  }

  
  @Override
  public synchronized void serviceStart() throws Exception {
    InetSocketAddress serviceAddr = clientService.getBindAddress();
    dagAppMaster = appContext.getAppMaster();
    // if web service is enabled then set tracking url. else disable it (value = "").
    // the actual url set on the rm web ui will be the proxy url set by WebAppProxyServlet, which
    // always try to connect to AM and proxy the response. hence it wont work if the webUIService
    // is not enabled.
    String trackingUrl = (webUI != null) ? webUI.getTrackingURL() : "";
    instantiateSchedulers(serviceAddr.getHostName(), serviceAddr.getPort(), trackingUrl, appContext);

    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      taskSchedulerServiceWrappers[i].init(getConfig());
      taskSchedulerServiceWrappers[i].start();
      if (shouldUnregisterFlag.get()) {
        // Flag may have been set earlier when task scheduler was not initialized
        // External services could need to talk to some other entity.
        taskSchedulers[i].setShouldUnregister();
      }
    }

    this.eventHandlingThread = new Thread("TaskSchedulerEventHandlerThread") {
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            if (TaskSchedulerManager.this.eventQueue.peek() == null) {
              notifyForTest();
            }
            event = TaskSchedulerManager.this.eventQueue.take();
          } catch (InterruptedException e) {
            if(!stopEventHandling) {
              LOG.warn("Continuing after interrupt : ", e);
            }
            continue;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the TaskScheduler", t);
            // Kill the AM.
            sendEvent(new DAGAppMasterEvent(DAGAppMasterEventType.INTERNAL_ERROR));
            return;
          } finally {
            notifyForTest();
          }
        }
      }
    };
    this.eventHandlingThread.start();
  }
  
  protected void notifyForTest() {
  }

  public void initiateStop() {
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      try {
        taskSchedulers[i].getTaskScheduler().initiateStop();
      } catch (Exception e) {
        // Ignore for now as scheduler stop invoked on shutdown
        LOG.error("Failed to do a clean initiateStop for Scheduler: "
            + Utils.getTaskSchedulerIdentifierString(i, appContext), e);
      }
    }
  }

  @Override
  public void serviceStop() throws InterruptedException {
    synchronized(this) {
      this.stopEventHandling = true;
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      if (taskSchedulers[i] != null) {
        taskSchedulerServiceWrappers[i].stop();
      }
    }
    LOG.info("Shutting down AppCallbackExecutor");
    appCallbackExecutor.shutdownNow();
    appCallbackExecutor.awaitTermination(1000l, TimeUnit.MILLISECONDS);
  }

  // TaskSchedulerAppCallback methods with schedulerId, where relevant
  public synchronized void taskAllocated(int schedulerId, Object task,
                                           Object appCookie,
                                           Container container) {
    AMSchedulerEventTALaunchRequest event =
        (AMSchedulerEventTALaunchRequest) appCookie;
    ContainerId containerId = container.getId();
    if (appContext.getAllContainers()
        .addContainerIfNew(container, schedulerId, event.getLauncherId(),
            event.getTaskCommId())) {
      appContext.getNodeTracker().nodeSeen(container.getNodeId(), schedulerId);
      sendEvent(new AMNodeEventContainerAllocated(container
          .getNodeId(), schedulerId, container.getId()));
    }


    TaskAttempt taskAttempt = event.getTaskAttempt();
    // TODO - perhaps check if the task still needs this container
    // because the deallocateTask downcall may have raced with the
    // taskAllocated() upcall
    assert task.equals(taskAttempt);
 
    if (appContext.getAllContainers().get(containerId).getState() == AMContainerState.ALLOCATED) {
      sendEvent(new AMContainerEventLaunchRequest(containerId, taskAttempt.getVertexID(),
          event.getContainerContext(), event.getLauncherId(), event.getTaskCommId()));
    }
    sendEvent(new AMContainerEventAssignTA(containerId, taskAttempt.getID(),
        event.getRemoteTaskSpec(), event.getContainerContext().getLocalResources(), event
            .getContainerContext().getCredentials(), event.getPriority()));
  }

  public synchronized void containerCompleted(int schedulerId, Object task, ContainerStatus containerStatus) {
    // SchedulerId isn't used here since no node updates are sent out
    // Inform the Containers about completion.
    AMContainer amContainer = appContext.getAllContainers().get(containerStatus.getContainerId());
    if (amContainer != null) {
      String message = "Container completed. ";
      TaskAttemptTerminationCause errCause = TaskAttemptTerminationCause.CONTAINER_EXITED;
      int exitStatus = containerStatus.getExitStatus();
      if (exitStatus == ContainerExitStatus.PREEMPTED) {
        message = "Container preempted externally. ";
        errCause = TaskAttemptTerminationCause.EXTERNAL_PREEMPTION;
      } else if (exitStatus == ContainerExitStatus.DISKS_FAILED) {
        message = "Container disk failed. ";
        errCause = TaskAttemptTerminationCause.NODE_DISK_ERROR;
      } else if (exitStatus != ContainerExitStatus.SUCCESS){
        message = "Container failed, exitCode=" + exitStatus + ". ";
      }
      if (containerStatus.getDiagnostics() != null) {
        message += containerStatus.getDiagnostics();
      }
      sendEvent(new AMContainerEventCompleted(amContainer.getContainerId(), exitStatus, message, errCause));
    }
  }

  public synchronized void containerBeingReleased(int schedulerId, ContainerId containerId) {
    // SchedulerId isn't used here since no node updates are sent out
    AMContainer amContainer = appContext.getAllContainers().get(containerId);
    if (amContainer != null) {
      sendEvent(new AMContainerEventStopRequest(containerId));
    }
  }

  @SuppressWarnings("unchecked")
  public synchronized void nodesUpdated(int schedulerId, List<NodeReport> updatedNodes) {
    for (NodeReport nr : updatedNodes) {
      // Scheduler will find out from the node, if at all.
      // Relying on the RM to not allocate containers on an unhealthy node.
      eventHandler.handle(new AMNodeEventStateChanged(nr, schedulerId));
    }
  }

  public synchronized void appShutdownRequested(int schedulerId) {
    // This can happen if the RM has been restarted. If it is in that state,
    // this application must clean itself up.
    LOG.info("App shutdown requested by scheduler {}", schedulerId);
    sendEvent(new DAGAppMasterEvent(DAGAppMasterEventType.AM_REBOOT));
  }

  public synchronized void setApplicationRegistrationData(
      int schedulerId,
      Resource maxContainerCapability,
      Map<ApplicationAccessType, String> appAcls, 
      ByteBuffer clientAMSecretKey) {
    this.appContext.getClusterInfo().setMaxContainerCapability(
        maxContainerCapability);
    this.appAcls = appAcls;
    this.clientService.setClientAMSecretKey(clientAMSecretKey);
  }

  // Not synchronized to avoid deadlocks from TaskScheduler callbacks.
  // TaskScheduler uses a separate thread for it's callbacks. Since this method
  // returns a value which is required, the TaskScheduler wait for the call to
  // complete and can hence lead to a deadlock if called from within a TSEH lock.
  public AppFinalStatus getFinalAppStatus() {
    FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
    StringBuffer sb = new StringBuffer();
    if (dagAppMaster == null) {
      finishState = FinalApplicationStatus.UNDEFINED;
      sb.append("App not yet initialized");
    } else {
      DAGAppMasterState appMasterState = dagAppMaster.getState();
      if (appMasterState == DAGAppMasterState.SUCCEEDED) {
        finishState = FinalApplicationStatus.SUCCEEDED;
      } else if (appMasterState == DAGAppMasterState.KILLED
          || (appMasterState == DAGAppMasterState.RUNNING && isSignalled)) {
        finishState = FinalApplicationStatus.KILLED;
      } else if (appMasterState == DAGAppMasterState.FAILED
          || appMasterState == DAGAppMasterState.ERROR) {
        finishState = FinalApplicationStatus.FAILED;
      } else {
        finishState = FinalApplicationStatus.UNDEFINED;
      }
      List<String> diagnostics = dagAppMaster.getDiagnostics();
      if(diagnostics != null) {
        for (String s : diagnostics) {
          sb.append(s).append("\n");
        }
      }
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Setting job diagnostics to " + sb.toString());
    }

    // if history url is set use the same, if historyUrl is set to "" then rm ui disables the
    // history url
    return new AppFinalStatus(finishState, sb.toString(), historyUrl);
  }



  // Not synchronized to avoid deadlocks from TaskScheduler callbacks.
  // TaskScheduler uses a separate thread for it's callbacks. Since this method
  // returns a value which is required, the TaskScheduler wait for the call to
  // complete and can hence lead to a deadlock if called from within a TSEH lock.
  public float getProgress(int schedulerId) {
    // at this point allocate has been called and so node count must be available
    // may change after YARN-1722
    // This is a heartbeat in from the scheduler into the APP, and is being used to piggy-back and
    // node updates from the cluster.

    // Doubles as a mechanism to update node counts periodically. Hence schedulerId required.

    // TODO Handle this in TEZ-2124. Need a way to know which scheduler is calling in.
    int nodeCount = 0;
    try {
      nodeCount = taskSchedulers[0].getClusterNodeCount();
    } catch (Exception e) {
      String msg = "Error in TaskScheduler while getting node count"
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(schedulerId, appContext);
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
      throw new RuntimeException(e);
    }
    if (nodeCount != cachedNodeCount) {
      cachedNodeCount = nodeCount;
      sendEvent(new AMNodeEventNodeCountUpdated(cachedNodeCount, schedulerId));
    }
    return dagAppMaster.getProgress();
  }

  public void onError(int schedulerId, Throwable t) {
    LOG.info("Error reported by scheduler {} - {}", schedulerId, t);
    sendEvent(new DAGAppMasterEventSchedulingServiceError(t));
  }

  public void dagCompleted() {
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      try {
        taskSchedulers[i].dagComplete();
      } catch (Exception e) {
        String msg = "Error in TaskScheduler when notified for Dag Completion"
            + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(i, appContext);
        LOG.error(msg, e);
        sendEvent(
            new DAGAppMasterEventUserServiceFatalError(
                DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
                msg, e));
      }
    }
  }

  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }

  public void preemptContainer(int schedulerId, ContainerId containerId) {
    // TODO Why is this making a call back into the scheduler, when the call is originating from there.
    // An AMContainer instance should already exist if an attempt is being made to preempt it
    AMContainer amContainer = appContext.getAllContainers().get(containerId);
    try {
      taskSchedulers[amContainer.getTaskSchedulerIdentifier()].deallocateContainer(containerId);
    } catch (Exception e) {
      String msg = "Error in TaskScheduler when preempting container"
          + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(amContainer.getTaskSchedulerIdentifier(), appContext)
          + ", containerId=" + containerId;
      LOG.error(msg, e);
      sendEvent(
          new DAGAppMasterEventUserServiceFatalError(
              DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
              msg, e));
    }
    // Inform the Containers about completion.
    sendEvent(new AMContainerEventCompleted(containerId, ContainerExitStatus.INVALID,
        "Container preempted internally", TaskAttemptTerminationCause.INTERNAL_PREEMPTION));
  }

  public void setShouldUnregisterFlag() {
    LOG.info("TaskScheduler notified that it should unregister from RM");
    this.shouldUnregisterFlag.set(true);
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      if (this.taskSchedulers[i] != null) {
        try {
          this.taskSchedulers[i].setShouldUnregister();
        } catch (Exception e) {
          String msg = "Error in TaskScheduler when setting Unregister Flag"
              + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(i, appContext);
          LOG.error(msg, e);
          sendEvent(
              new DAGAppMasterEventUserServiceFatalError(
                  DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
                  msg, e));
        }
      }
    }
  }

  public ContainerSignatureMatcher getContainerSignatureMatcher() {
    return containerSignatureMatcher;
  }

  public boolean hasUnregistered() {
    boolean result = true;
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      // Explicitly not catching any exceptions around this API
      // No clear route to recover. Better to crash.
      try {
        result = result & this.taskSchedulers[i].hasUnregistered();
      } catch (Exception e) {
        String msg = "Error in TaskScheduler when checking if a scheduler has unregistered"
            + ", scheduler=" + Utils.getTaskSchedulerIdentifierString(i, appContext);
        LOG.error(msg, e);
        sendEvent(
            new DAGAppMasterEventUserServiceFatalError(
                DAGAppMasterEventType.TASK_SCHEDULER_SERVICE_FATAL_ERROR,
                msg, e));
      }
      if (result == false) {
        return result;
      }
    }
    return result;
  }

  @VisibleForTesting
  public String getHistoryUrl() {
    Configuration config = this.appContext.getAMConf();
    String historyUrl = "";

    String historyUrlTemplate = config.get(TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE,
            TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE_DEFAULT);
    String historyUrlBase = config.get(TezConfiguration.TEZ_HISTORY_URL_BASE, "");

    if (!historyUrlTemplate.isEmpty() &&
        !historyUrlBase.isEmpty()) {
      // replace the placeholders, while tolerating extra or missing "/" in input. replace all
      // instances of consecutive "/" with single (except for the http(s):// case
      historyUrl = historyUrlTemplate
          .replaceAll(APPLICATION_ID_PLACEHOLDER, appContext.getApplicationID().toString())
          .replaceAll(HISTORY_URL_BASE, historyUrlBase)
          .replaceAll("([^:])/{2,}", "$1/");

      // make sure we have a valid scheme
      if (!historyUrl.startsWith("http")) {
        historyUrl = "http://" + historyUrl;
      }
    }

    return historyUrl;
  }

}

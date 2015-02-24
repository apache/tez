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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.tez.dag.api.TezConstants;
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
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerEventStopRequest;
import org.apache.tez.dag.app.rm.container.AMContainerEventTASucceeded;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.rm.container.ContainerSignatureMatcher;
import org.apache.tez.dag.app.rm.node.AMNodeEventContainerAllocated;
import org.apache.tez.dag.app.rm.node.AMNodeEventNodeCountUpdated;
import org.apache.tez.dag.app.rm.node.AMNodeEventStateChanged;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptEnded;
import org.apache.tez.dag.app.rm.node.AMNodeEventTaskAttemptSucceeded;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;

import com.google.common.base.Preconditions;


public class TaskSchedulerEventHandler extends AbstractService
                                         implements TaskSchedulerAppCallback,
                                               EventHandler<AMSchedulerEvent> {
  static final Logger LOG = LoggerFactory.getLogger(TaskSchedulerEventHandler.class);

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
  private final String[] taskSchedulerClasses;
  protected final TaskSchedulerService []taskSchedulers;

  private final boolean isPureLocalMode;
  // If running in non local-only mode, the YARN task scheduler will always run to take care of
  // registration with YARN and heartbeats to YARN.
  // Splitting registration and heartbeats is not straigh-forward due to the taskScheduler being
  // tied to a ContainerRequestType.
  private final int yarnTaskSchedulerIndex;
  // Custom AppIds to avoid container conflicts if there's multiple sources
  private final long SCHEDULER_APP_ID_BASE = 111101111;
  private final long SCHEDULER_APP_ID_INCREMENT = 111111111;

  BlockingQueue<AMSchedulerEvent> eventQueue
                              = new LinkedBlockingQueue<AMSchedulerEvent>();

  // Not tracking container / task to schedulerId. Instead relying on everything flowing through
  // the system and being propagated back via events.

  /**
   *
   * @param appContext
   * @param clientService
   * @param eventHandler
   * @param containerSignatureMatcher
   * @param webUI
   * @param schedulerClasses the list of scheduler classes / codes. Tez internal classes are represented as codes.
   *                         An empty list defaults to using the YarnTaskScheduler as the only source.
   */
  @SuppressWarnings("rawtypes")
  public TaskSchedulerEventHandler(AppContext appContext,
      DAGClientServer clientService, EventHandler eventHandler, 
      ContainerSignatureMatcher containerSignatureMatcher, WebUIService webUI,
      String [] schedulerClasses, boolean isPureLocalMode) {
    super(TaskSchedulerEventHandler.class.getName());
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.clientService = clientService;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.webUI = webUI;
    this.historyUrl = getHistoryUrl();
    this.isPureLocalMode = isPureLocalMode;
    if (this.webUI != null) {
      this.webUI.setHistoryUrl(this.historyUrl);
    }

    // Override everything for pure local mode
    if (isPureLocalMode) {
      this.taskSchedulerClasses = new String[] {TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT};
      this.yarnTaskSchedulerIndex = -1;
    } else {
      if (schedulerClasses == null || schedulerClasses.length ==0) {
        this.taskSchedulerClasses = new String[] {TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT};
        this.yarnTaskSchedulerIndex = 0;
      } else {
        // Ensure the YarnScheduler will be setup and note it's index. This will be responsible for heartbeats and YARN registration.
        int foundYarnTaskSchedulerIndex = -1;
        for (int i = 0 ; i < schedulerClasses.length ; i++) {
          if (schedulerClasses[i].equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT)) {
            foundYarnTaskSchedulerIndex = i;
            break;
          }
        }
        if (foundYarnTaskSchedulerIndex == -1) { // Not found. Add at the end.
          this.taskSchedulerClasses = new String[schedulerClasses.length+1];
          foundYarnTaskSchedulerIndex = this.taskSchedulerClasses.length -1;
          for (int i = 0 ; i < schedulerClasses.length ; i++) { // Copy over the rest.
            this.taskSchedulerClasses[i] = schedulerClasses[i];
          }
          this.taskSchedulerClasses[foundYarnTaskSchedulerIndex] = TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT;
        } else {
          this.taskSchedulerClasses = schedulerClasses;
        }
        this.yarnTaskSchedulerIndex = foundYarnTaskSchedulerIndex;
      }
    }
    taskSchedulers = new TaskSchedulerService[this.taskSchedulerClasses.length];
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
    return taskSchedulers[schedulerId].getAvailableResources();
  }

  public Resource getTotalResources(int schedulerId) {
    return taskSchedulers[schedulerId].getTotalResources();
  }

  public synchronized void handleEvent(AMSchedulerEvent sEvent) {
    LOG.info("Processing the event " + sEvent.toString());
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
        throw new TezUncheckedException("Unexecpted TA_ENDED state: " + event.getState());
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
    if (event.getType() == AMSchedulerEventType.S_NODE_BLACKLISTED) {
      taskSchedulers[event.getSchedulerId()].blacklistNode(event.getNodeId());
    } else if (event.getType() == AMSchedulerEventType.S_NODE_UNBLACKLISTED) {
      taskSchedulers[event.getSchedulerId()].unblacklistNode(event.getNodeId());
    } else {
      throw new TezUncheckedException("Invalid event type: " + event.getType());
    }
  }

  private void handleContainerDeallocate(
                                  AMSchedulerEventDeallocateContainer event) {
    ContainerId containerId = event.getContainerId();
    // TODO what happens to the task that was connected to this container?
    // current assumption is that it will eventually call handleTaStopRequest
    //TaskAttempt taskAttempt = (TaskAttempt)
    taskSchedulers[event.getSchedulerId()].deallocateContainer(containerId);
    // TODO does this container need to be stopped via C_STOP_REQUEST
    sendEvent(new AMContainerEventStopRequest(containerId));
  }

  private void handleTAUnsuccessfulEnd(AMSchedulerEventTAEnded event) {
    TaskAttempt attempt = event.getAttempt();
    boolean wasContainerAllocated = taskSchedulers[event.getSchedulerId()].deallocateTask(attempt, false);
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
      sendEvent(new AMNodeEventTaskAttemptEnded(appContext.getAllContainers().
          get(attemptContainerId).getContainer().getNodeId(), attemptContainerId,
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
          get(usedContainerId).getContainer().getNodeId(), usedContainerId,
          event.getAttemptID()));
    }

    boolean wasContainerAllocated = taskSchedulers[event.getSchedulerId()].deallocateTask(attempt,
        true);
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
          taskSchedulers[event.getSchedulerId()].allocateTask(taskAttempt,
              event.getCapability(),
              affinityAttempt.getAssignedContainerID(),
              Priority.newInstance(event.getPriority()),
              event.getContainerContext(),
              event);
          return;
        }
        LOG.info("Attempt: " + taskAttempt.getID() + " has task based affinity to " + taskAffinity 
            + " but no locality information exists for it. Ignoring hint.");
        // fall through with null hosts/racks
      } else {
        hosts = (locationHint.getHosts() != null) ? locationHint
            .getHosts().toArray(
                new String[locationHint.getHosts().size()]) : null;
        racks = (locationHint.getRacks() != null) ? locationHint.getRacks()
            .toArray(new String[locationHint.getRacks().size()]) : null;
      }
    }

    taskSchedulers[event.getSchedulerId()].allocateTask(taskAttempt,
        event.getCapability(),
        hosts,
        racks,
        Priority.newInstance(event.getPriority()),
        event.getContainerContext(),
        event);
  }

  private TaskSchedulerService createTaskScheduler(String host, int port, String trackingUrl,
                                                   AppContext appContext,
                                                   String schedulerClassName,
                                                   long customAppIdIdentifier) {
    if (schedulerClassName.equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT)) {
      LOG.info("Creating TaskScheduler: YarnTaskSchedulerService");
      return new YarnTaskSchedulerService(this, this.containerSignatureMatcher,
          host, port, trackingUrl, appContext);
    } else if (schedulerClassName.equals(TezConstants.TEZ_AM_SERVICE_PLUGINS_LOCAL_MODE_NAME_DEFAULT)) {
      LOG.info("Creating TaskScheduler: Local TaskScheduler");
      return new LocalTaskSchedulerService(this, this.containerSignatureMatcher,
          host, port, trackingUrl, customAppIdIdentifier, appContext);
    } else {
      LOG.info("Creating custom TaskScheduler: " + schedulerClassName);
      // TODO TEZ-2003 Temporary reflection with specific parameters. Remove once there is a clean interface.
      Class<? extends TaskSchedulerService> taskSchedulerClazz =
          (Class<? extends TaskSchedulerService>) ReflectionUtils.getClazz(schedulerClassName);
      try {
        Constructor<? extends TaskSchedulerService> ctor = taskSchedulerClazz
            .getConstructor(TaskSchedulerAppCallback.class, AppContext.class, String.class,
                int.class, String.class, long.class, Configuration.class);
        ctor.setAccessible(true);
        return ctor.newInstance(this, appContext, host, port, trackingUrl, customAppIdIdentifier,
            getConfig());
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

  @VisibleForTesting
  protected void instantiateScheduelrs(String host, int port, String trackingUrl, AppContext appContext) {
    // TODO Add error checking for components being used in the Vertex when running in pure local mode.
    // Iterate over the list and create all the taskSchedulers
    int j = 0;
    for (int i = 0; i < taskSchedulerClasses.length; i++) {
      long customAppIdIdentifier;
      if (isPureLocalMode || taskSchedulerClasses[i].equals(
          TezConstants.TEZ_AM_SERVICE_PLUGINS_NAME_DEFAULT)) { // Use the app identifier from the appId.
        customAppIdIdentifier = appContext.getApplicationID().getClusterTimestamp();
      } else {
        customAppIdIdentifier = SCHEDULER_APP_ID_BASE + (j++ * SCHEDULER_APP_ID_INCREMENT);
      }
      LOG.info("ClusterIdentifier for TaskScheduler [" + i + ":" + taskSchedulerClasses[i] + "]=" +
          customAppIdIdentifier);
      taskSchedulers[i] = createTaskScheduler(host, port,
          trackingUrl, appContext, taskSchedulerClasses[i], customAppIdIdentifier);
    }
  }

  
  @Override
  public synchronized void serviceStart() {
    InetSocketAddress serviceAddr = clientService.getBindAddress();
    dagAppMaster = appContext.getAppMaster();
    // if web service is enabled then set tracking url. else disable it (value = "").
    // the actual url set on the rm web ui will be the proxy url set by WebAppProxyServlet, which
    // always try to connect to AM and proxy the response. hence it wont work if the webUIService
    // is not enabled.
    String trackingUrl = (webUI != null) ? webUI.getTrackingURL() : "";
    instantiateScheduelrs(serviceAddr.getHostName(), serviceAddr.getPort(), trackingUrl, appContext);

    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      taskSchedulers[i].init(getConfig());
      taskSchedulers[i].start();
      if (shouldUnregisterFlag.get()) {
        // Flag may have been set earlier when task scheduler was not initialized
        // TODO TEZ-2003 Should setRegister / unregister be part of APIs when not YARN specific ?
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
            if (TaskSchedulerEventHandler.this.eventQueue.peek() == null) {
              notifyForTest();
            }
            event = TaskSchedulerEventHandler.this.eventQueue.take();
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
    taskScheduler.initiateStop();
  }

  @Override
  public void serviceStop() {
    synchronized(this) {
      this.stopEventHandling = true;
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      if (taskSchedulers[i] != null) {
        taskSchedulers[i].stop();
      }
    }
  }

  // TaskSchedulerAppCallback methods
  @Override
  public synchronized void taskAllocated(Object task,
                                           Object appCookie,
                                           Container container) {
    AMSchedulerEventTALaunchRequest event =
        (AMSchedulerEventTALaunchRequest) appCookie;
    ContainerId containerId = container.getId();
    if (appContext.getAllContainers()
        .addContainerIfNew(container, event.getSchedulerId(), event.getLauncherId(),
            event.getTaskCommId())) {
      appContext.getNodeTracker().nodeSeen(container.getNodeId());
      sendEvent(new AMNodeEventContainerAllocated(container
          .getNodeId(), container.getId()));
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
    sendEvent(new DAGEventSchedulerUpdateTAAssigned(taskAttempt, container));
    sendEvent(new AMContainerEventAssignTA(containerId, taskAttempt.getID(),
        event.getRemoteTaskSpec(), event.getContainerContext().getLocalResources(), event
            .getContainerContext().getCredentials(), event.getPriority()));
  }

  @Override
  public synchronized void containerCompleted(Object task, ContainerStatus containerStatus) {
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

  @Override
  public synchronized void containerBeingReleased(ContainerId containerId) {
    AMContainer amContainer = appContext.getAllContainers().get(containerId);
    if (amContainer != null) {
      sendEvent(new AMContainerEventStopRequest(containerId));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void nodesUpdated(List<NodeReport> updatedNodes) {
    for (NodeReport nr : updatedNodes) {
      // Scheduler will find out from the node, if at all.
      // Relying on the RM to not allocate containers on an unhealthy node.
      eventHandler.handle(new AMNodeEventStateChanged(nr));
    }
  }

  @Override
  public synchronized void appShutdownRequested() {
    // This can happen if the RM has been restarted. If it is in that state,
    // this application must clean itself up.
    LOG.info("App shutdown requested by scheduler");
    sendEvent(new DAGAppMasterEvent(DAGAppMasterEventType.AM_REBOOT));
  }

  @Override
  public synchronized void setApplicationRegistrationData(
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
  @Override
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
  @Override
  public float getProgress() {
    // at this point allocate has been called and so node count must be available
    // may change after YARN-1722
    // This is a heartbeat in from the scheduler into the APP, and is being used to piggy-back and
    // node updates from the cluster.
    // TODO Handle this in TEZ-2124. Need a way to know which scheduler is calling in.
    int nodeCount = taskSchedulers[0].getClusterNodeCount();
    if (nodeCount != cachedNodeCount) {
      cachedNodeCount = nodeCount;
      sendEvent(new AMNodeEventNodeCountUpdated(cachedNodeCount));
    }
    return dagAppMaster.getProgress();
  }

  @Override
  public void onError(Throwable t) {
    LOG.info("Error reported by scheduler", t);
    sendEvent(new DAGAppMasterEventSchedulingServiceError(t));
  }

  public void dagCompleted() {
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      taskSchedulers[i].dagComplete();
    }
  }

  public void dagSubmitted() {
    // Nothing to do right now. Indicates that a new DAG has been submitted and
    // the context has updated information.
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    // TODO Why is this making a call back into the scheduler, when the call is originating from there.
    // An AMContainer instance should already exist if an attempt is being made to preempt it
    AMContainer amContainer = appContext.getAllContainers().get(containerId);
    taskSchedulers[amContainer.getTaskSchedulerIdentifier()].deallocateContainer(containerId);
    // Inform the Containers about completion.
    sendEvent(new AMContainerEventCompleted(containerId, ContainerExitStatus.INVALID,
        "Container preempted internally", TaskAttemptTerminationCause.INTERNAL_PREEMPTION));
  }

  public void setShouldUnregisterFlag() {
    LOG.info("TaskScheduler notified that it should unregister from RM");
    this.shouldUnregisterFlag.set(true);
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      if (this.taskSchedulers[i] != null) {
        // TODO TEZ-2003 registration required for all schedulers ?
        this.taskSchedulers[i].setShouldUnregister();
      }
    }
  }

  public boolean hasUnregistered() {
    boolean result = true;
    for (int i = 0 ; i < taskSchedulers.length ; i++) {
      // TODO TEZ-2003 registration required for all schedulers ?
      result |= this.taskSchedulers[i].hasUnregistered();
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

    String loggingClass =  config.get(TezConfiguration.TEZ_HISTORY_LOGGING_SERVICE_CLASS, "");
    String historyUrlTemplate = config.get(TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE,
            TezConfiguration.TEZ_AM_TEZ_UI_HISTORY_URL_TEMPLATE_DEFAULT);
    String historyUrlBase = config.get(TezConfiguration.TEZ_HISTORY_URL_BASE, "");


    if (loggingClass.equals("org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService") &&
        !historyUrlTemplate.isEmpty() &&
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

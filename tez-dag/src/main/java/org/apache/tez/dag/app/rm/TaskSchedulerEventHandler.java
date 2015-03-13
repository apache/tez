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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tez.dag.api.TezConfiguration;
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
  static final Log LOG = LogFactory.getLog(TaskSchedulerEventHandler.class);

  static final String APPLICATION_ID_PLACEHOLDER = "__APPLICATION_ID__";
  static final String HISTORY_URL_BASE = "__HISTORY_URL_BASE__";

  protected final AppContext appContext;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final String historyUrl;
  protected TaskSchedulerService taskScheduler;
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

  BlockingQueue<AMSchedulerEvent> eventQueue
                              = new LinkedBlockingQueue<AMSchedulerEvent>();

  @SuppressWarnings("rawtypes")
  public TaskSchedulerEventHandler(AppContext appContext,
      DAGClientServer clientService, EventHandler eventHandler, 
      ContainerSignatureMatcher containerSignatureMatcher, WebUIService webUI) {
    super(TaskSchedulerEventHandler.class.getName());
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.clientService = clientService;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.webUI = webUI;
    this.historyUrl = getHistoryUrl();
    if (this.webUI != null) {
      this.webUI.setHistoryUrl(this.historyUrl);
    }
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
  
  public Resource getAvailableResources() {
    return taskScheduler.getAvailableResources();
  }

  public Resource getTotalResources() {
    return taskScheduler.getTotalResources();
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
        handleTAUnsuccessfulEnd((AMSchedulerEventTAEnded) sEvent);
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
      taskScheduler.blacklistNode(event.getNodeId());
    } else if (event.getType() == AMSchedulerEventType.S_NODE_UNBLACKLISTED) {
      taskScheduler.unblacklistNode(event.getNodeId());
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
    taskScheduler.deallocateContainer(containerId);
    // TODO does this container need to be stopped via C_STOP_REQUEST
    sendEvent(new AMContainerEventStopRequest(containerId));
  }

  private void handleTAUnsuccessfulEnd(AMSchedulerEventTAEnded event) {
    TaskAttempt attempt = event.getAttempt();
    boolean wasContainerAllocated = taskScheduler.deallocateTask(attempt, false);
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

    boolean wasContainerAllocated = taskScheduler.deallocateTask(attempt, true);
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
          taskScheduler.allocateTask(taskAttempt,
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
    
    taskScheduler.allocateTask(taskAttempt,
                               event.getCapability(),
                               hosts,
                               racks,
                               Priority.newInstance(event.getPriority()),
                               event.getContainerContext(),
                               event);
  }


  protected TaskSchedulerService createTaskScheduler(String host, int port,
      String trackingUrl, AppContext appContext) {
    boolean isLocal = getConfig().getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
        TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);
    if (isLocal) {
      return new LocalTaskSchedulerService(this, this.containerSignatureMatcher,
          host, port, trackingUrl, appContext);
    }
    else {
      return new YarnTaskSchedulerService(this, this.containerSignatureMatcher,
          host, port, trackingUrl, appContext);
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
    taskScheduler = createTaskScheduler(serviceAddr.getHostName(),
        serviceAddr.getPort(), trackingUrl, appContext);
    taskScheduler.init(getConfig());
    taskScheduler.start();
    if (shouldUnregisterFlag.get()) {
      // Flag may have been set earlier when task scheduler was not initialized
      taskScheduler.setShouldUnregister();
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
  
  @Override
  public void serviceStop() {
    synchronized(this) {
      this.stopEventHandling = true;
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }
    if (taskScheduler != null) {
      ((AbstractService)taskScheduler).stop();
    }
  }

  // TaskSchedulerAppCallback methods
  @Override
  public synchronized void taskAllocated(Object task,
                                           Object appCookie,
                                           Container container) {
    ContainerId containerId = container.getId();
    if (appContext.getAllContainers().addContainerIfNew(container)) {
      appContext.getNodeTracker().nodeSeen(container.getNodeId());
      sendEvent(new AMNodeEventContainerAllocated(container
          .getNodeId(), container.getId()));
    }

    AMSchedulerEventTALaunchRequest event =
                         (AMSchedulerEventTALaunchRequest) appCookie;
    TaskAttempt taskAttempt = event.getTaskAttempt();
    // TODO - perhaps check if the task still needs this container
    // because the deallocateTask downcall may have raced with the
    // taskAllocated() upcall
    assert task.equals(taskAttempt);
 
    if (appContext.getAllContainers().get(containerId).getState() == AMContainerState.ALLOCATED) {
      sendEvent(new AMContainerEventLaunchRequest(containerId, taskAttempt.getVertexID(),
          event.getContainerContext()));
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
        message = "Container failed. ";
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
    int nodeCount = taskScheduler.getClusterNodeCount();
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
    taskScheduler.resetMatchLocalityForAllHeldContainers();
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    taskScheduler.deallocateContainer(containerId);
    // Inform the Containers about completion.
    sendEvent(new AMContainerEventCompleted(containerId, ContainerExitStatus.INVALID,
        "Container preempted internally", TaskAttemptTerminationCause.INTERNAL_PREEMPTION));
  }

  public void setShouldUnregisterFlag() {
    LOG.info("TaskScheduler notified that it should unregister from RM");
    this.shouldUnregisterFlag.set(true);
    if (this.taskScheduler != null) {
      this.taskScheduler.setShouldUnregister();
    }
  }

  public boolean hasUnregistered() {
    return this.taskScheduler.hasUnregistered();
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

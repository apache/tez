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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEventSchedulerUpdateTAAssigned;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
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

public class TaskSchedulerEventHandler extends AbstractService
                                         implements TaskSchedulerAppCallback,
                                               EventHandler<AMSchedulerEvent> {
  static final Log LOG = LogFactory.getLog(TaskSchedulerEventHandler.class);

  protected final AppContext appContext;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  protected TaskScheduler taskScheduler;
  private DAGAppMaster dagAppMaster;
  private Map<ApplicationAccessType, String> appAcls = null;
  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;
  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;
  final DAGClientServer clientService;
  private final ContainerSignatureMatcher containerSignatureMatcher;
  private int cachedNodeCount = -1;

  BlockingQueue<AMSchedulerEvent> eventQueue
                              = new LinkedBlockingQueue<AMSchedulerEvent>();

  @SuppressWarnings("rawtypes")
  public TaskSchedulerEventHandler(AppContext appContext,
      DAGClientServer clientService, EventHandler eventHandler, ContainerSignatureMatcher containerSignatureMatcher) {
    super(TaskSchedulerEventHandler.class.getName());
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.clientService = clientService;
    this.containerSignatureMatcher = containerSignatureMatcher;
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
    case S_CONTAINERS_ALLOCATED:
      break;
    case S_CONTAINER_COMPLETED:
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
    /*MRxTaskAttemptID aId = event.getAttemptID();
    attemptToLaunchRequestMap.remove(aId);
    // TODO XXX: This remove may need to be deferred. Possible for a SUCCESSFUL taskAttempt to fail,
    // which means the scheduler needs to remember taskAttempt to container assignments for a longer time.
    boolean removed = pendingReduces.remove(aId);
    if (!removed) {
      removed = scheduledRequests.remove(aId);
      if (!removed) {
        // Maybe assigned.
        ContainerId containerId = assignedRequests.remove(aId);
        if (containerId != null) {
          // Ask the container to stop.
          sendEvent(new AMContainerEvent(containerId,
              AMContainerEventType.C_STOP_REQUEST));
          // Inform the Node - the task has asked to be STOPPED / has already
          // stopped.
          sendEvent(new AMNodeEventTaskAttemptEnded(containerMap
              .get(containerId).getContainer().getNodeId(), containerId,
              event.getAttemptID(), event.getState() == TaskAttemptState.FAILED));
        } else {
          LOG.warn("Received a STOP request for absent taskAttempt: "
              + event.getAttemptID());
          // This could be generated in case of recovery, with unhealthy nodes/
          // fetch failures. Can be ignored, since Recovered containers don't
          // need to be stopped.
        }
      }
    }*/

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
    /*
    // TODO XXX Remember the assigned containerId even after task success.
    // Required for TOO_MANY_FETCH_FAILURES
    attemptToLaunchRequestMap.remove(event.getAttemptID());
    ContainerId containerId = assignedRequests.remove(event.getAttemptID());
    if (containerId != null) { // TODO Should not be null. Confirm.
      sendEvent(new AMContainerTASucceededEvent(containerId,
          event.getAttemptID()));
      sendEvent(new AMNodeEventTaskAttemptSucceeded(containerMap
          .get(containerId).getContainer().getNodeId(), containerId,
          event.getAttemptID()));
      containerAvailable(containerId);
    } else {
      LOG.warn("Received TaskAttemptSucceededEvent for unmapped TaskAttempt: "
          + event.getAttemptID() + ". Full event: " + event);
    }*/

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
    /**
         // Add to queue of pending tasks.
    recalculateReduceSchedule = true;
    attemptToLaunchRequestMap.put(event.getAttemptID(), event);
    if (event.getAttemptID().getTaskID().getTaskType() == TaskType.MAP) {
      mapResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.MAP, mapResourceReqt);
      event.getCapability().setMemory(mapResourceReqt);
      scheduledRequests.addMap(event);
    } else { // Reduce
      reduceResourceReqt = maybeComputeNormalizedRequestForType(event,
          TaskType.REDUCE, reduceResourceReqt);
      event.getCapability().setMemory(reduceResourceReqt);
      if (event.isRescheduled()) {
        pendingReduces.addFirst(new ContainerRequestInfo(new ContainerRequest(
            event.getCapability(), event.getHosts(), event.getRacks(),
            PRIORITY_REDUCE), event));
      } else {
        pendingReduces.addLast(new ContainerRequestInfo(new ContainerRequest(
            event.getCapability(), event.getHosts(), event.getRacks(),
            PRIORITY_REDUCE), event));
      }
    }
     */
    // TODO resource adjustment needs to move into dag
    /*Resource mapResourceReqt = maybeComputeNormalizedRequestForType(event,
        TaskType.MAP, mapResourceReqt);
    event.getCapability().setMemory(mapResourceReqt);*/
    TaskAttempt taskAttempt = event.getTaskAttempt();
    taskScheduler.allocateTask(taskAttempt,
                               event.getCapability(),
                               event.getHosts(),
                               event.getRacks(),
                               event.getPriority(),
                               event.getContainerContext(),
                               event);
  }


  protected TaskScheduler createTaskScheduler(String host, int port,
      String trackingUrl, AppContext appContext) {
    return new TaskScheduler(this, this.containerSignatureMatcher,
      host, port, trackingUrl, appContext);
  }
  
  @Override
  public synchronized void serviceStart() {
    InetSocketAddress serviceAddr = clientService.getBindAddress();
    dagAppMaster = appContext.getAppMaster();
    taskScheduler = createTaskScheduler(serviceAddr.getHostName(),
        serviceAddr.getPort(), "", appContext);
    taskScheduler.init(getConfig());
    taskScheduler.start();
    this.eventHandlingThread = new Thread("TaskSchedulerEventHandlerThread") {
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
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
          }
        }
      }
    };
    this.eventHandlingThread.start();
  }

  @Override
  public void serviceStop() {
    synchronized(this) {
      this.stopEventHandling = true;
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }
    if (taskScheduler != null) {
      taskScheduler.stop();
    }
  }

  // TaskSchedulerAppCallback methods
  @Override
  public synchronized void taskAllocated(Object task,
                                           Object appCookie,
                                           Container container) {
    ContainerId containerId = container.getId();
    if (appContext.getAllContainers().addContainerIfNew(container)) {
      appContext.getAllNodes().nodeSeen(container.getNodeId());
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
            .getContainerContext().getCredentials()));
  }

  @Override
  public synchronized void containerCompleted(Object task, ContainerStatus containerStatus) {
    // Inform the Containers about completion.
    AMContainer amContainer = appContext.getAllContainers().get(containerStatus.getContainerId());
    if (amContainer != null) {
      sendEvent(new AMContainerEventCompleted(containerStatus));
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

    String historyUrl = "";
    /*String historyUrl = JobHistoryUtils.getHistoryUrl(getConfig(),
        appContext.getApplicationID());
    LOG.info("History url is " + historyUrl);*/

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
    LOG.info("Error reported by scheduler");
    sendEvent(new DAGAppMasterEvent(DAGAppMasterEventType.INTERNAL_ERROR));
  }

  public void dagCompleted() {
    taskScheduler.resetMatchLocalityForAllHeldContainers();
  }

  @Override
  public void preemptContainer(ContainerId containerId) {
    taskScheduler.deallocateContainer(containerId);
    // Inform the Containers about completion.
    sendEvent(new AMContainerEventCompleted(ContainerStatus.newInstance(
        containerId, ContainerState.COMPLETE, "Container Preempted Internally", -1), true));
  }

  public void setShouldUnregisterFlag() {
    this.taskScheduler.setShouldUnregister();
    LOG.info("TaskScheduler notified that it should unregister from RM");
  }

}

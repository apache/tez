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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.DAGAppMasterState;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.rm.TaskScheduler.TaskSchedulerAppCallback;
import org.apache.tez.dag.app.rm.container.AMContainerEventAssignTA;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventCompleted;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainerState;
import org.apache.tez.dag.app.rm.container.AMContainerEventTASucceeded;
import org.apache.tez.dag.app.rm.node.AMNodeEventContainerAllocated;
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
  private TaskScheduler taskScheduler;
  private DAGAppMaster dagAppMaster;
  private Map<ApplicationAccessType, String> appAcls = null;
  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;
  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;
  final DAGClientServer clientService;

  BlockingQueue<AMSchedulerEvent> eventQueue
                              = new LinkedBlockingQueue<AMSchedulerEvent>();

  public TaskSchedulerEventHandler(AppContext appContext,
      DAGClientServer clientService) {
    super(TaskSchedulerEventHandler.class.getName());
    this.appContext = appContext;
    this.eventHandler = appContext.getEventHandler();
    this.clientService = clientService;
  }
  
  public Map<ApplicationAccessType, String> getApplicationAcls() {
    return appAcls;
  }
  
  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("RMCommunicator notified that iSignalled was : " + isSignalled);
  }
  
  public Resource getAvailableResources() {
    return taskScheduler.getClusterAvailableResources();
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
    case S_NODE_BLACKLISTED:
      break;
    case S_NODE_UNHEALTHY:
      break;
    case S_NODE_HEALTHY:
      // Consider changing this to work like BLACKLISTING.
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
  
  
  private void handleContainerDeallocate(
                                  AMSchedulerEventDeallocateContainer event) {
    ContainerId containerId = event.getContainerId();
    // TODO what happens to the task that was connected to this container?
    // current assumption is that it will eventually call handleTaStopRequest
    //TaskAttempt taskAttempt = (TaskAttempt) 
    taskScheduler.deallocateContainer(containerId);
    // TODO does this container need to be stopped via C_STOP_REQUEST
    sendEvent(new AMContainerEvent(containerId,
                                   AMContainerEventType.C_STOP_REQUEST));
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
    Container container = taskScheduler.deallocateTask(attempt);
    // use stored value of container id in case the scheduler has removed this
    // assignment because the task has been deallocated earlier. 
    // retroactive case
    ContainerId attemptContainerId = attempt.getAssignedContainerID();
    
    if(container != null) {
      // use scheduler container since it exists
      ContainerId containerId = container.getId();
      assert attemptContainerId==null || attemptContainerId.equals(containerId);
      attemptContainerId = containerId;
    } else {
      LOG.info("Task: " + attempt.getID() +
               " has no container assignment in the scheduler");
    }

    if (attemptContainerId != null) {
      // TODO either ways send the necessary events 
      // Ask the container to stop.
      sendEvent(new AMContainerEvent(attemptContainerId,
                                     AMContainerEventType.C_STOP_REQUEST));
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
    Container container = taskScheduler.deallocateTask(attempt);
    if(container != null) {
      ContainerId containerId = container.getId();
      assert containerId.equals(event.getUsedContainerId());
      sendEvent(new AMContainerEventTASucceeded(containerId,
                    event.getAttemptID()));
      // Inform the Node - the task has asked to be STOPPED / has already
      // stopped.
      sendEvent(new AMNodeEventTaskAttemptSucceeded(appContext.getAllContainers().
          get(containerId).getContainer().getNodeId(), containerId,
          event.getAttemptID()));
      // TODO this is where reuse will happen
      sendEvent(new AMContainerEvent(containerId,
          AMContainerEventType.C_STOP_REQUEST));
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
                               event);
  }
  
  @Override
  public synchronized void serviceStart() {
    // FIXME hack alert how is this supposed to support multiple DAGs?
    // Answer: this is shared across dags. need job==app-dag-master
    // TODO set heartbeat value from conf here
    InetSocketAddress serviceAddr = clientService.getBindAddress();
    taskScheduler = 
        new TaskScheduler(appContext.getApplicationAttemptId(),
                          this,
                          serviceAddr.getHostName(),
                          serviceAddr.getPort(),
                          "");
    taskScheduler.init(getConfig());

    dagAppMaster = appContext.getAppMaster();
    taskScheduler.start();
    this.eventHandlingThread = new Thread() {
      @Override
      public void run() {

        AMSchedulerEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = TaskSchedulerEventHandler.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
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
  public synchronized void serviceStop() {
    this.stopEventHandling = true;
    if (eventHandlingThread != null)
      eventHandlingThread.interrupt();
    taskScheduler.stop();
  }
  
  // TaskSchedulerAppCallback methods
  @Override
  public synchronized void taskAllocated(Object task, 
                                           Object appCookie, 
                                           Container container) {
    /*
    availableContainerIds.addAll(event.getContainerIds());

    completedMaps = getJob().getCompletedMaps();
    completedReduces = getJob().getCompletedReduces();
    int completedTasks = completedMaps + completedReduces;

    if (lastCompletedTasks != completedTasks) {
      recalculateReduceSchedule = true;
      lastCompletedTasks = completedTasks;
    }

    if (event.didHeadroomChange() || event.getContainerIds().size() > 0) {
      recalculateReduceSchedule = true;
    }
    schedule();
    .....
          // Update resource requests
      requestor.decContainerReq(assigned.getContainerRequest());
  
      // TODO Maybe: ApplicationACLs should be populated into the appContext from the RMCommunicator.
      ContainerId containerId = allocated.getId();
      if (appContext.getAllContainers().get(containerId).getState() == AMContainerState.ALLOCATED) {
        AMSchedulerTALaunchRequestEvent tlrEvent = attemptToLaunchRequestMap
            .get(assigned.getAttemptId());
        JobConf jobConf = new JobConf(getJob().getConf());
  
        AMContainerEventLaunchRequest launchRequest = new AMContainerEventLaunchRequest(
            containerId, jobId, assigned.getAttemptId().getTaskId()
                .getTaskType(), tlrEvent.getJobToken(),
            tlrEvent.getCredentials(), shouldProfileTaskAttempt(
                jobConf, tlrEvent.getRemoteTaskContext()), jobConf);
  
        eventHandler.handle(launchRequest);
      }
      eventHandler.handle(new AMContainerEventAssignTA(containerId,
          assigned.getAttemptId(), attemptToLaunchRequestMap.get(
              assigned.getAttemptId()).getRemoteTaskContext()));
  
      assignedRequests.add(allocated, assigned.getAttemptId());
    */
    
    ContainerId containerId = container.getId();
    appContext.getAllContainers().addContainerIfNew(container);
    appContext.getAllNodes().nodeSeen(container.getNodeId());   
    sendEvent(new AMNodeEventContainerAllocated(container
        .getNodeId(), container.getId()));
    
    AMSchedulerEventTALaunchRequest event = 
                         (AMSchedulerEventTALaunchRequest) appCookie;
    TaskAttempt taskAttempt = event.getTaskAttempt();
    // TODO - perhaps check if the task still needs this container
    // because the deallocateTask downcall may have raced with the 
    // taskAllocated() upcall
    assert task.equals(taskAttempt);
    if (appContext.getAllContainers().get(containerId).getState() 
        == AMContainerState.ALLOCATED) {

      sendEvent(new AMContainerEventLaunchRequest(
          containerId,
          taskAttempt.getVertexID(),
          event.getJobToken(),
          // TODO getConf from AMSchedulerEventTALaunchRequest
          event.getCredentials(), false, event.getConf(),
          taskAttempt.getLocalResources(),
          taskAttempt.getEnvironment(),
          taskAttempt.getJavaOpts()));
    }
    sendEvent(new AMContainerEventAssignTA(containerId,
        taskAttempt.getID(), event.getRemoteTaskContext()));
  }

  @Override
  public synchronized void containerCompleted(Object task, ContainerStatus containerStatus) {
    // Inform the Containers about completion.
    sendEvent(new AMContainerEventCompleted(containerStatus));
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
    // TODO TEZ-34 change event to reboot and send to app master
    sendEvent(new DAGEvent(appContext.getDAGID(),
                           DAGEventType.INTERNAL_ERROR));
  }

  @Override
  public synchronized void setApplicationRegistrationData(
      Resource maxContainerCapability,
      Map<ApplicationAccessType, String> appAcls) {
    this.appContext.getClusterInfo().setMaxContainerCapability(
        maxContainerCapability);
    this.appAcls = appAcls;
  }

  @Override
  public synchronized AppFinalStatus getFinalAppStatus() {
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
      for (String s : dagAppMaster.getDiagnostics()) {
        sb.append(s).append("\n");
      }
    }
    LOG.info("Setting job diagnostics to " + sb.toString());

    String historyUrl = "";
    /*String historyUrl = JobHistoryUtils.getHistoryUrl(getConfig(),
        appContext.getApplicationID());
    LOG.info("History url is " + historyUrl);*/
    
    return new AppFinalStatus(finishState, sb.toString(), historyUrl);
  }

  @Override
  public synchronized float getProgress() {
    return dagAppMaster.getProgress();
  }

  @Override
  public void onError(Exception e) {
    sendEvent(new DAGEvent(appContext.getDAGID(),
        DAGEventType.INTERNAL_ERROR));
  }

}

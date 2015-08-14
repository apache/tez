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

package org.apache.tez.dag.app.rm.container;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.event.DiagnosableEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminatedBySystem;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminating;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventNodeFailed;
import org.apache.tez.dag.app.rm.AMSchedulerEventDeallocateContainer;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorStopRequestEvent;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class AMContainerImpl implements AMContainer {

  private static final Logger LOG = LoggerFactory.getLogger(AMContainerImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final ContainerId containerId;
  // Container to be used for getters on capability, locality etc.
  private final Container container;
  private final AppContext appContext;
  private final ContainerHeartbeatHandler containerHeartbeatHandler;
  private final TaskAttemptListener taskAttemptListener;
  protected final EventHandler eventHandler;
  private final ContainerSignatureMatcher signatureMatcher;

  private final List<TezTaskAttemptID> completedAttempts =
      new LinkedList<TezTaskAttemptID>();

  private long idleTimeBetweenTasks = 0;
  private long lastTaskFinishTime;

  private TezDAGID lastTaskDAGID;
  
  // An assign can happen even during wind down. e.g. NodeFailure caused the
  // wind down, and an allocation was pending in the AMScheduler. This could
  // be modelled as a separate state.
  private boolean nodeFailed = false;

  private TezTaskAttemptID currentAttempt;
  private long currentAttemptAllocationTime;
  private List<TezTaskAttemptID> failedAssignments;

  private boolean inError = false;

  @VisibleForTesting
  Map<String, LocalResource> containerLocalResources;
  @VisibleForTesting
  Map<String, LocalResource> additionalLocalResources;

  private Credentials credentials;
  private boolean credentialsChanged = false;
  
  // TODO Consider registering with the TAL, instead of the TAL pulling.
  // Possibly after splitting TAL and ContainerListener.

  // TODO What should be done with pendingAttempts. Nullify when handled ?
  // Add them to failed ta list ? Some historic information should be maintained.

  // TODO Create a generic ERROR state. Container tries informing relevant components in this case.


  private final StateMachine<AMContainerState, AMContainerEventType, AMContainerEvent> stateMachine;
  private static final StateMachineFactory
      <AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>
      stateMachineFactory =
      new StateMachineFactory<AMContainerImpl, AMContainerState, AMContainerEventType, AMContainerEvent>(
      AMContainerState.ALLOCATED)

      .addTransition(AMContainerState.ALLOCATED, AMContainerState.LAUNCHING,
          AMContainerEventType.C_LAUNCH_REQUEST, new LaunchRequestTransition())
      .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED,
          AMContainerEventType.C_ASSIGN_TA,
          new AssignTaskAttemptAtAllocatedTransition())
      .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED,
          AMContainerEventType.C_COMPLETED,
          new CompletedAtAllocatedTransition())
      .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED,
          AMContainerEventType.C_STOP_REQUEST,
          new StopRequestAtAllocatedTransition())
      .addTransition(AMContainerState.ALLOCATED, AMContainerState.COMPLETED,
          AMContainerEventType.C_NODE_FAILED,
          new NodeFailedAtAllocatedTransition())
      .addTransition(
          AMContainerState.ALLOCATED,
          AMContainerState.COMPLETED,
          EnumSet.of(AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED,
              AMContainerEventType.C_TIMED_OUT), new ErrorTransition())
      .addTransition(
          AMContainerState.LAUNCHING,
          EnumSet.of(AMContainerState.LAUNCHING, AMContainerState.STOP_REQUESTED),
          AMContainerEventType.C_ASSIGN_TA, new AssignTaskAttemptTransition())
      .addTransition(AMContainerState.LAUNCHING,
          EnumSet.of(AMContainerState.IDLE, AMContainerState.RUNNING),
          AMContainerEventType.C_LAUNCHED, new LaunchedTransition())
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING,
          AMContainerEventType.C_LAUNCH_FAILED, new LaunchFailedTransition())
      // Is assuming the pullAttempt will be null.
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.COMPLETED,
          AMContainerEventType.C_COMPLETED,
          new CompletedAtLaunchingTransition())
      .addTransition(AMContainerState.LAUNCHING,
          AMContainerState.STOP_REQUESTED, AMContainerEventType.C_STOP_REQUEST,
          new StopRequestAtLaunchingTransition())
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING,
          AMContainerEventType.C_NODE_FAILED,
          new NodeFailedAtLaunchingTransition())
      .addTransition(
          AMContainerState.LAUNCHING,
          AMContainerState.STOP_REQUESTED,
          EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED,
              AMContainerEventType.C_TIMED_OUT),
          new ErrorAtLaunchingTransition())

      .addTransition(AMContainerState.IDLE,
          EnumSet.of(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED),
          AMContainerEventType.C_ASSIGN_TA,
          new AssignTaskAttemptTransition())
      .addTransition(AMContainerState.IDLE, AMContainerState.COMPLETED,
          AMContainerEventType.C_COMPLETED, new CompletedAtIdleTransition())
      .addTransition(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_STOP_REQUEST,
          new StopRequestAtIdleTransition())
      .addTransition(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_TIMED_OUT, new TimedOutAtIdleTransition())
      .addTransition(AMContainerState.IDLE, AMContainerState.STOPPING,
          AMContainerEventType.C_NODE_FAILED, new NodeFailedAtIdleTransition())
      .addTransition(
          AMContainerState.IDLE,
          AMContainerState.STOP_REQUESTED,
          EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST,
              AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED),
          new ErrorAtIdleTransition())

      .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_ASSIGN_TA,
          new AssignTaskAttemptAtRunningTransition())
      .addTransition(AMContainerState.RUNNING, AMContainerState.IDLE,
          AMContainerEventType.C_TA_SUCCEEDED,
          new TASucceededAtRunningTransition())
      .addTransition(AMContainerState.RUNNING, AMContainerState.COMPLETED,
          AMContainerEventType.C_COMPLETED, new CompletedAtRunningTransition())
      .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_STOP_REQUEST,
          new StopRequestAtRunningTransition())
      .addTransition(AMContainerState.RUNNING, AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_TIMED_OUT, new TimedOutAtRunningTransition())
      .addTransition(AMContainerState.RUNNING, AMContainerState.STOPPING,
          AMContainerEventType.C_NODE_FAILED,
          new NodeFailedAtRunningTransition())
      .addTransition(
          AMContainerState.RUNNING,
          AMContainerState.STOP_REQUESTED,
          EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST,
              AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED),
          new ErrorAtRunningTransition())

      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.STOP_REQUESTED, AMContainerEventType.C_ASSIGN_TA,
          new AssignTAAtWindDownTransition())
      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.COMPLETED, AMContainerEventType.C_COMPLETED,
          new CompletedAtWindDownTransition())
      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.STOPPING, AMContainerEventType.C_NM_STOP_SENT)
      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.STOPPING, AMContainerEventType.C_NM_STOP_FAILED,
          new NMStopRequestFailedTransition())
      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.STOPPING, AMContainerEventType.C_NODE_FAILED,
          new NodeFailedAtNMStopRequestedTransition())
      .addTransition(
          AMContainerState.STOP_REQUESTED,
          AMContainerState.STOP_REQUESTED,
          EnumSet.of(AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_STOP_REQUEST,
              AMContainerEventType.C_TIMED_OUT))
      .addTransition(AMContainerState.STOP_REQUESTED,
          AMContainerState.STOP_REQUESTED,
          AMContainerEventType.C_LAUNCH_REQUEST,
          new ErrorAtNMStopRequestedTransition())



      .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING,
          AMContainerEventType.C_ASSIGN_TA, new AssignTAAtWindDownTransition())
      // TODO This transition is wrong. Should be a noop / error.
      .addTransition(AMContainerState.STOPPING, AMContainerState.COMPLETED,
          AMContainerEventType.C_COMPLETED, new CompletedAtWindDownTransition())
      .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING,
          AMContainerEventType.C_NODE_FAILED, new NodeFailedBaseTransition())
      .addTransition(
          AMContainerState.STOPPING,
          AMContainerState.STOPPING,
          EnumSet.of(AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_STOP_REQUEST,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED,
              AMContainerEventType.C_TIMED_OUT))
      .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING,
          AMContainerEventType.C_LAUNCH_REQUEST,
          new ErrorAtStoppingTransition())

      .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED,
          AMContainerEventType.C_ASSIGN_TA, new AssignTAAtCompletedTransition())
      .addTransition(AMContainerState.COMPLETED, AMContainerState.COMPLETED,
          AMContainerEventType.C_NODE_FAILED, new NodeFailedBaseTransition())
      .addTransition(
          AMContainerState.COMPLETED,
          AMContainerState.COMPLETED,
          EnumSet.of(AMContainerEventType.C_LAUNCH_REQUEST,
              AMContainerEventType.C_LAUNCHED,
              AMContainerEventType.C_LAUNCH_FAILED,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_COMPLETED,
              AMContainerEventType.C_STOP_REQUEST,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED,
              AMContainerEventType.C_TIMED_OUT))

        .installTopology();

  // Note: Containers will not reach their final state if the RM link is broken,
  // AM shutdown should not wait for this.

  // Attempting to use a container based purely on reosurces required, etc needs
  // additional change - JvmID, YarnChild, etc depend on TaskType.
  public AMContainerImpl(Container container, ContainerHeartbeatHandler chh,
      TaskAttemptListener tal, ContainerSignatureMatcher signatureMatcher,
      AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.container = container;
    this.containerId = container.getId();
    this.eventHandler = appContext.getEventHandler();
    this.signatureMatcher = signatureMatcher;
    this.appContext = appContext;
    this.containerHeartbeatHandler = chh;
    this.taskAttemptListener = tal;
    this.failedAssignments = new LinkedList<TezTaskAttemptID>();
    this.stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public AMContainerState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public Container getContainer() {
    return this.container;
  }

  @Override
  public List<TezTaskAttemptID> getAllTaskAttempts() {
    readLock.lock();
    try {
      List<TezTaskAttemptID> allAttempts = new LinkedList<TezTaskAttemptID>();
      allAttempts.addAll(this.completedAttempts);
      allAttempts.addAll(this.failedAssignments);
      if (this.currentAttempt != null) {
        allAttempts.add(this.currentAttempt);
      }
      return allAttempts;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezTaskAttemptID getCurrentTaskAttempt() {
    readLock.lock();
    try {
      return this.currentAttempt;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getCurrentTaskAttemptAllocationTime() {
    readLock.lock();
    try {
      return this.currentAttemptAllocationTime;
    } finally {
      readLock.unlock();
    }
  }

  public boolean isInErrorState() {
    return inError;
  }

  @Override
  public void handle(AMContainerEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing AMContainerEvent " + event.getContainerId()
          + " of type " + event.getType() + " while in state: " + getState()
          + ". Event: " + event);
    }
    this.writeLock.lock();
    try {
      final AMContainerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for ContainerId "
            + this.containerId, e);
        inError = true;
        // TODO Can't set state to COMPLETED. Add a default error state.
      }
      if (oldState != getState()) {
        LOG.info("AMContainer " + this.containerId + " transitioned from "
            + oldState + " to " + getState()
            + " via event " + event.getType());
      }
    } finally {
      writeLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                   Start of Transition Classes                            //
  //////////////////////////////////////////////////////////////////////////////

  protected static class LaunchRequestTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventLaunchRequest event = (AMContainerEventLaunchRequest) cEvent;
      
      ContainerContext containerContext = event.getContainerContext();
      // Clone - don't use the object that is passed in, since this is likely to
      // be modified here.
      container.containerLocalResources = new HashMap<String, LocalResource>(
          containerContext.getLocalResources());
      container.credentials = containerContext.getCredentials();
      container.credentialsChanged = true;

      TezDAGID dagId = null;
      Map<String, LocalResource> dagLocalResources = null;
      if (container.appContext.getCurrentDAG() != null) {
        dagId = container.appContext.getCurrentDAG().getID();
        dagLocalResources = container.appContext.getCurrentDAG().getLocalResources();
      }
      ContainerLaunchContext clc = AMContainerHelpers.createContainerLaunchContext(
          dagId, dagLocalResources,
          container.appContext.getApplicationACLs(),
          container.getContainerId(),
          containerContext.getLocalResources(),
          containerContext.getEnvironment(),
          containerContext.getJavaOpts(),
          container.taskAttemptListener.getAddress(), containerContext.getCredentials(),
          container.appContext, container.container.getResource(),
          container.appContext.getAMConf());

      // Registering now, so that in case of delayed NM response, the child
      // task is not told to die since the TAL does not know about the container.
      container.registerWithTAListener();
      container.sendStartRequestToNM(clc);
      LOG.info("Sending Launch Request for Container with id: " +
          container.container.getId());
    }
  }

  protected static class AssignTaskAttemptAtAllocatedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventAssignTA event = (AMContainerEventAssignTA) cEvent;
      container.inError = true;
      container.registerFailedAttempt(event.getTaskAttemptId());
      container.maybeSendNodeFailureForFailedAssignment(event
          .getTaskAttemptId());
      container.sendTerminatedToTaskAttempt(event.getTaskAttemptId(),
          "AMScheduler Error: TaskAttempt allocated to unlaunched container: " +
              container.getContainerId(), TaskAttemptTerminationCause.FRAMEWORK_ERROR);
      container.deAllocate();
      LOG.warn("Unexpected TA Assignment: TAId: " + event.getTaskAttemptId() +
          "  for ContainerId: " + container.getContainerId() +
          " while in state: " + container.getState());
    }
  }

  protected static class CompletedAtAllocatedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted)cEvent;
      String diag = event.getDiagnostics();
      if (!(diag == null || diag.equals(""))) {
        LOG.info("Container " + container.getContainerId()
            + " exited with diagnostics set to " + diag);
      }
    }
  }

  protected static class StopRequestAtAllocatedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.deAllocate();
    }
  }

  protected static class NodeFailedAtAllocatedTransition extends
      NodeFailedBaseTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.deAllocate();
    }
  }

  protected static class ErrorTransition extends ErrorBaseTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.deAllocate();
      LOG.info(
          "Unexpected event type: " + cEvent.getType() + " while in state: " +
              container.getState() + ". Event: " + cEvent);

    }
  }

  protected static class AssignTaskAttemptTransition implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    public AMContainerState transition(
        AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventAssignTA event = (AMContainerEventAssignTA) cEvent;
      if (LOG.isDebugEnabled()) {
        LOG.debug("AssignTaskAttempt at state " + container.getState() + ", attempt: " +
            ((AMContainerEventAssignTA) cEvent).getRemoteTaskSpec());
      }
      if (container.currentAttempt != null) {
        // This may include a couple of additional (harmless) unregister calls
        // to the taskAttemptListener and containerHeartbeatHandler - in case
        // of assign at any state prior to IDLE.
        container.handleExtraTAAssign(event, container.currentAttempt);
        return AMContainerState.STOP_REQUESTED;
      }
      
      Map<String, LocalResource> taskLocalResources = event.getRemoteTaskLocalResources();
      Preconditions.checkState(container.additionalLocalResources == null,
          "No additional resources should be pending when assigning a new task");
      container.additionalLocalResources = container.signatureMatcher.getAdditionalResources(
          container.containerLocalResources, taskLocalResources);
      // Register the additional resources back for this container.
      container.containerLocalResources.putAll(container.additionalLocalResources);
      container.currentAttempt = event.getTaskAttemptId();
      container.currentAttemptAllocationTime = container.appContext.getClock().getTime();
      if (LOG.isDebugEnabled()) {
        LOG.debug("AssignTA: attempt: " + event.getRemoteTaskSpec());
        LOG.debug("AdditionalLocalResources: " + container.additionalLocalResources);
      }

      TezDAGID currentDAGID = container.appContext.getCurrentDAGID();
      if (!currentDAGID.equals(container.lastTaskDAGID)) {
        // Will be null for the first task.
        container.credentialsChanged = true;
        container.credentials = event.getCredentials();
        container.lastTaskDAGID = currentDAGID;
      } else {
        container.credentialsChanged = false;
      }

      if (container.lastTaskFinishTime != 0) {
        // This effectively measures the time during which nothing was scheduler to execute on a container.
        // The time from this point to the task actually being available to containers needs to be computed elsewhere.
        long idleTimeDiff =
            System.currentTimeMillis() - container.lastTaskFinishTime;
        container.idleTimeBetweenTasks += idleTimeDiff;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Computing idle (scheduling) time for container: " +
              container.getContainerId() + ", lastFinishTime: " +
              container.lastTaskFinishTime + ", Incremented by: " +
              idleTimeDiff);
        }
      }

      LOG.info("Assigned taskAttempt + [" + container.currentAttempt +
          "] to container: [" + container.getContainerId() + "]");
      AMContainerTask amContainerTask = new AMContainerTask(
          event.getRemoteTaskSpec(), container.additionalLocalResources,
          container.credentialsChanged ? container.credentials : null, container.credentialsChanged,
          event.getPriority());
      container.registerAttemptWithListener(amContainerTask);
      container.additionalLocalResources = null;
      container.credentialsChanged = false;
      if (container.getState() == AMContainerState.IDLE) {
        return AMContainerState.RUNNING;
      } else {
        return container.getState();
      }
    }
  }

  protected static class LaunchedTransition
      implements MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {
    @Override
    public AMContainerState transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.registerWithContainerListener();
      if (container.currentAttempt != null) {
        return AMContainerState.RUNNING;
      } else {
        return AMContainerState.IDLE;
      }
    }
  }

  protected static class LaunchFailedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.currentAttempt != null) {
        AMContainerEventLaunchFailed event = (AMContainerEventLaunchFailed) cEvent;
        // for a properly setup cluster this should almost always be an app error
        // need to differentiate between launch failed due to framework/cluster or app
        container.sendTerminatingToTaskAttempt(container.currentAttempt,
            event.getMessage(), TaskAttemptTerminationCause.CONTAINER_LAUNCH_FAILED);
      }
      container.unregisterFromTAListener();
      container.deAllocate();
    }
  }

  protected static class CompletedAtLaunchingTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted) cEvent;
      if (container.currentAttempt!= null) {
        String errorMessage = getMessage(container, event);
        if (event.isSystemAction()) {
          container.sendContainerTerminatedBySystemToTaskAttempt(container.currentAttempt,
              errorMessage, event.getTerminationCause());
        } else {
          container
              .sendTerminatedToTaskAttempt(
                  container.currentAttempt,
                  errorMessage,
                  // if termination cause is generic exited then replace with specific
                  (event.getTerminationCause() == TaskAttemptTerminationCause.CONTAINER_EXITED ? 
                      TaskAttemptTerminationCause.CONTAINER_LAUNCH_FAILED : event.getTerminationCause()));
        }
        container.registerFailedAttempt(container.currentAttempt);
        container.currentAttempt = null;
        LOG.warn(errorMessage);
      }
      container.containerLocalResources = null;
      container.additionalLocalResources = null;
      container.unregisterFromTAListener();
      String diag = event.getDiagnostics();
      if (!(diag == null || diag.equals(""))) {
        LOG.info("Container " + container.getContainerId()
            + " exited with diagnostics set to " + diag);
      }
      container.logStopped(event.getContainerExitStatus());
    }

    public String getMessage(AMContainerImpl container,
        AMContainerEventCompleted event) {
      return "Container" + container.getContainerId()
          + " finished while trying to launch. Diagnostics: ["
          + event.getDiagnostics() +"]";
    }
  }

  protected static class StopRequestAtLaunchingTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.currentAttempt != null) {
        container.sendTerminatingToTaskAttempt(container.currentAttempt,
            getMessage(container, cEvent), TaskAttemptTerminationCause.CONTAINER_STOPPED);
      }
      container.unregisterFromTAListener();
      container.logStopped(container.currentAttempt == null ?
          ContainerExitStatus.SUCCESS 
          : ContainerExitStatus.INVALID);
      container.sendStopRequestToNM();
    }

    public String getMessage(
        AMContainerImpl container, AMContainerEvent event) {
      return "Container " + container.getContainerId() +
          " received a STOP_REQUEST";
    }
  }

  protected static class NodeFailedBaseTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {

      if (container.nodeFailed) {
        // ignore duplicates
        return;
      }
      container.nodeFailed = true;
      String errorMessage = "Node " + container.getContainer().getNodeId() + " failed. ";
      if (cEvent instanceof DiagnosableEvent) {
        errorMessage += ((DiagnosableEvent) cEvent).getDiagnosticInfo();
      }

      for (TezTaskAttemptID taId : container.failedAssignments) {
        container.sendNodeFailureToTA(taId, errorMessage, TaskAttemptTerminationCause.NODE_FAILED);
      }
      for (TezTaskAttemptID taId : container.completedAttempts) {
        container.sendNodeFailureToTA(taId, errorMessage, TaskAttemptTerminationCause.NODE_FAILED);
      }

      if (container.currentAttempt != null) {
        // Will be null in COMPLETED state.
        container.sendNodeFailureToTA(container.currentAttempt, errorMessage,
            TaskAttemptTerminationCause.NODE_FAILED);
        container.sendTerminatingToTaskAttempt(container.currentAttempt, errorMessage,
            TaskAttemptTerminationCause.NODE_FAILED);
      }
      container.logStopped(ContainerExitStatus.ABORTED);
    }
  }

  protected static class NodeFailedAtLaunchingTransition
      extends NodeFailedBaseTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterFromTAListener();
      container.deAllocate();
    }
  }

  protected static class ErrorAtLaunchingTransition
      extends ErrorBaseTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      if (container.currentAttempt != null) {
        container.sendTerminatingToTaskAttempt(container.currentAttempt,
            "Container " + container.getContainerId() +
                " hit an invalid transition - " + cEvent.getType() + " at " +
                container.getState(), TaskAttemptTerminationCause.FRAMEWORK_ERROR);
      }
      container.logStopped(ContainerExitStatus.ABORTED);
      container.unregisterFromTAListener();
      container.sendStopRequestToNM();
    }
  }

  protected static class CompletedAtIdleTransition
      extends CompletedAtLaunchingTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterFromContainerListener();
      if (LOG.isDebugEnabled()) {
        LOG.debug("TotalIdleTimeBetweenTasks for container: "
            + container.getContainerId() + " = "
            + container.idleTimeBetweenTasks);
      }
    }

    @Override
    public String getMessage(
        AMContainerImpl container, AMContainerEventCompleted event) {
      return "Container " + container.getContainerId()
          + " finished with diagnostics set to ["
          + event.getDiagnostics() + "]";
    }
  }

  protected static class StopRequestAtIdleTransition
      extends StopRequestAtLaunchingTransition {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterFromContainerListener();
    }
  }

  protected static class TimedOutAtIdleTransition
      extends StopRequestAtIdleTransition {

    public String getMessage(
        AMContainerImpl container, AMContainerEvent event) {
      return "Container " + container.getContainerId() +
          " timed out";
    }
  }

  protected static class NodeFailedAtIdleTransition
      extends NodeFailedAtLaunchingTransition {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterFromContainerListener();
    }
  }

  protected static class ErrorAtIdleTransition
      extends ErrorAtLaunchingTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterFromContainerListener();
    }
  }

  protected static class AssignTaskAttemptAtRunningTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {

      AMContainerEventAssignTA event = (AMContainerEventAssignTA) cEvent;
      container.unregisterAttemptFromListener(container.currentAttempt);
      container.handleExtraTAAssign(event, container.currentAttempt);
    }
  }

  protected static class TASucceededAtRunningTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.lastTaskFinishTime = System.currentTimeMillis();
      container.completedAttempts.add(container.currentAttempt);
      container.unregisterAttemptFromListener(container.currentAttempt);
      container.currentAttempt = null;
    }
  }

  protected static class CompletedAtRunningTransition
      extends CompletedAtIdleTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted) cEvent;
      if (event.isSystemAction()) {
        container.sendContainerTerminatedBySystemToTaskAttempt(container.currentAttempt,
            getMessage(container, event), event.getTerminationCause());
      } else {
        container.sendTerminatedToTaskAttempt(container.currentAttempt,
            getMessage(container, event), event.getTerminationCause());
      }
      container.unregisterAttemptFromListener(container.currentAttempt);
      container.registerFailedAttempt(container.currentAttempt);
      container.currentAttempt= null;
      super.transition(container, cEvent);
    }
  }

  protected static class StopRequestAtRunningTransition
      extends StopRequestAtIdleTransition {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.unregisterAttemptFromListener(container.currentAttempt);
      super.transition(container, cEvent);
    }
  }

  protected static class TimedOutAtRunningTransition
      extends StopRequestAtRunningTransition {
    @Override
    public String getMessage(
        AMContainerImpl container, AMContainerEvent event) {
      return "Container " + container.getContainerId() +
          " timed out";
    }
  }

  protected static class NodeFailedAtRunningTransition
      extends NodeFailedAtIdleTransition {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.currentAttempt);
    }
  }

  protected static class ErrorAtRunningTransition
      extends ErrorAtIdleTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.currentAttempt);
      container.sendTerminatingToTaskAttempt(container.currentAttempt,
          "Container " + container.getContainerId() +
              " hit an invalid transition - " + cEvent.getType() + " at " +
              container.getState(), TaskAttemptTerminationCause.FRAMEWORK_ERROR);
    }
  }

  protected static class AssignTAAtWindDownTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventAssignTA event = (AMContainerEventAssignTA) cEvent;
      container.inError = true;
      String errorMessage = "AttemptId: " + event.getTaskAttemptId() +
          " cannot be allocated to container: " + container.getContainerId() +
          " in " + container.getState() + " state";
      container.maybeSendNodeFailureForFailedAssignment(event.getTaskAttemptId());
      container.sendTerminatingToTaskAttempt(event.getTaskAttemptId(), errorMessage,
          TaskAttemptTerminationCause.CONTAINER_EXITED);
      container.registerFailedAttempt(event.getTaskAttemptId());
    }
  }

  protected static class CompletedAtWindDownTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted) cEvent;
      String diag = event.getDiagnostics();
      for (TezTaskAttemptID taId : container.failedAssignments) {
        container.sendTerminatedToTaskAttempt(taId, diag, 
            TaskAttemptTerminationCause.CONTAINER_EXITED);
      }
      if (container.currentAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.currentAttempt, diag,
            TaskAttemptTerminationCause.CONTAINER_EXITED);
        container.registerFailedAttempt(container.currentAttempt);
        container.currentAttempt = null;
      }
      if (!(diag == null || diag.equals(""))) {
        LOG.info("Container " + container.getContainerId()
            + " exited with diagnostics set to " + diag);
      }
      container.containerLocalResources = null;
      container.additionalLocalResources = null;
    }
  }

  protected static class NMStopRequestFailedTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.deAllocate();
    }
  }

  protected static class NodeFailedAtNMStopRequestedTransition
      extends NodeFailedBaseTransition {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.deAllocate();
    }
  }

  protected static class ErrorAtNMStopRequestedTransition
      extends ErrorBaseTransition {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
    }
  }

  protected static class ErrorAtStoppingTransition
      extends ErrorBaseTransition {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
    }
  }

  protected static class ErrorBaseTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.inError = true;
    }
  }

  protected static class AssignTAAtCompletedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      // TODO CREUSE CRITICAL: This is completely incorrect. COMPLETED comes
      // from RMComm directly to the container. Meanwhile, the scheduler may
      // think the container is still around and assign a task to it. The task
      // ends up getting a CONTAINER_KILLED message. Task could handle this by
      // asking for a reschedule in this case. Will end up FAILING the task instead of KILLING it.
      container.inError = true;
      AMContainerEventAssignTA event = (AMContainerEventAssignTA) cEvent;
      String errorMessage = "AttemptId: " + event.getTaskAttemptId()
          + " cannot be allocated to container: " + container.getContainerId()
          + " in COMPLETED state";
      container.maybeSendNodeFailureForFailedAssignment(event.getTaskAttemptId());
      container.sendTerminatedToTaskAttempt(event.getTaskAttemptId(),
          errorMessage, TaskAttemptTerminationCause.FRAMEWORK_ERROR);
      container.registerFailedAttempt(event.getTaskAttemptId());
    }
  }

  private void handleExtraTAAssign(
      AMContainerEventAssignTA event, TezTaskAttemptID currentTaId) {
    this.inError = true;
    String errorMessage = "AMScheduler Error: Multiple simultaneous " +
        "taskAttempt allocations to: " + this.getContainerId() +
        ". Attempts: " + currentTaId + ", " + event.getTaskAttemptId() +
        ". Current state: " + this.getState();
    this.maybeSendNodeFailureForFailedAssignment(event.getTaskAttemptId());
    this.sendTerminatingToTaskAttempt(event.getTaskAttemptId(), errorMessage,
        TaskAttemptTerminationCause.FRAMEWORK_ERROR);
    this.sendTerminatingToTaskAttempt(currentTaId, errorMessage,
        TaskAttemptTerminationCause.FRAMEWORK_ERROR);
    this.registerFailedAttempt(event.getTaskAttemptId());
    LOG.warn(errorMessage);
    this.logStopped(ContainerExitStatus.INVALID);
    this.sendStopRequestToNM();
    this.unregisterFromTAListener();
    this.unregisterFromContainerListener();
  }

  protected void registerFailedAttempt(TezTaskAttemptID taId) {
    failedAssignments.add(taId);
  }

  private void logStopped(int exitStatus) {
    final Clock clock = appContext.getClock();
    final HistoryEventHandler historyHandler = appContext.getHistoryHandler();
    ContainerStoppedEvent lEvt = new ContainerStoppedEvent(containerId,
        clock.getTime(), 
        exitStatus, 
        appContext.getApplicationAttemptId());
    historyHandler.handle(
        new DAGHistoryEvent(appContext.getCurrentDAGID(),lEvt));
  }
  
  protected void deAllocate() {
    sendEvent(new AMSchedulerEventDeallocateContainer(containerId));
  }

  protected void sendTerminatedToTaskAttempt(
      TezTaskAttemptID taId, String message, TaskAttemptTerminationCause errCause) {
    sendEvent(new TaskAttemptEventContainerTerminated(taId, message, errCause));
  }
  
  protected void sendContainerTerminatedBySystemToTaskAttempt(
    TezTaskAttemptID taId, String message, TaskAttemptTerminationCause errorCause) {
      sendEvent(new TaskAttemptEventContainerTerminatedBySystem(taId, message, errorCause));
  }

  protected void sendTerminatingToTaskAttempt(TezTaskAttemptID taId,
      String message, TaskAttemptTerminationCause errorCause) {
    sendEvent(new TaskAttemptEventContainerTerminating(taId, message, errorCause));
  }

  protected void maybeSendNodeFailureForFailedAssignment(TezTaskAttemptID taId) {
    if (this.nodeFailed) {
      this.sendNodeFailureToTA(taId, "Node Failed", TaskAttemptTerminationCause.NODE_FAILED);
    }
  }

  protected void sendNodeFailureToTA(TezTaskAttemptID taId, String message, 
      TaskAttemptTerminationCause errorCause) {
    sendEvent(new TaskAttemptEventNodeFailed(taId, message, errorCause));
  }

  protected void sendStartRequestToNM(ContainerLaunchContext clc) {
    sendEvent(new NMCommunicatorLaunchRequestEvent(clc, container));
  }

  protected void sendStopRequestToNM() {
    sendEvent(new NMCommunicatorStopRequestEvent(containerId,
        container.getNodeId(), container.getContainerToken()));
  }

  protected void unregisterAttemptFromListener(TezTaskAttemptID attemptId) {
    taskAttemptListener.unregisterTaskAttempt(attemptId);
  }

  protected void registerAttemptWithListener(AMContainerTask amContainerTask) {
    taskAttemptListener.registerTaskAttempt(amContainerTask, this.containerId);
  }

  protected void registerWithTAListener() {
    taskAttemptListener.registerRunningContainer(containerId);
  }

  protected void unregisterFromTAListener() {
    this.taskAttemptListener.unregisterRunningContainer(containerId);
  }

  protected void registerWithContainerListener() {
    this.containerHeartbeatHandler.register(this.containerId);
  }

  protected void unregisterFromContainerListener() {
    this.containerHeartbeatHandler.unregister(this.containerId);
  }



}

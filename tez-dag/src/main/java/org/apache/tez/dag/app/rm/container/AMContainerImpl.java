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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerPreempted;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminating;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventNodeFailed;
import org.apache.tez.dag.app.rm.AMSchedulerEventDeallocateContainer;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorStopRequestEvent;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
//import org.apache.tez.dag.app.dag.event.TaskAttemptEventDiagnosticsUpdate;
import org.apache.tez.runtime.api.impl.TaskSpec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class AMContainerImpl implements AMContainer {

  private static final Log LOG = LogFactory.getLog(AMContainerImpl.class);

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

  // TODO Maybe this should be pulled from the TaskAttempt.s
  private final Map<TezTaskAttemptID, TaskSpec> remoteTaskMap =
      new HashMap<TezTaskAttemptID, TaskSpec>();

  // TODO ?? Convert to list and hash.

  private long idleTimeBetweenTasks = 0;
  private long lastTaskFinishTime;

  private TezDAGID lastTaskDAGID;
  
  // An assign can happen even during wind down. e.g. NodeFailure caused the
  // wind down, and an allocation was pending in the AMScheduler. This could
  // be modelled as a separate state.
  private boolean nodeFailed = false;

  private TezTaskAttemptID pendingAttempt;
  private TezTaskAttemptID runningAttempt;
  private List<TezTaskAttemptID> failedAssignments;
  private TezTaskAttemptID pullAttempt;

  private AMContainerTask noAllocationContainerTask;

  private static final AMContainerTask NO_MORE_TASKS = new AMContainerTask(
      true, null, null, null, false);
  private static final AMContainerTask WAIT_TASK = new AMContainerTask(false,
      null, null, null, false);

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
              AMContainerEventType.C_PULL_TA,
              AMContainerEventType.C_TA_SUCCEEDED,
              AMContainerEventType.C_NM_STOP_SENT,
              AMContainerEventType.C_NM_STOP_FAILED,
              AMContainerEventType.C_TIMED_OUT), new ErrorTransition())

      .addTransition(
          AMContainerState.LAUNCHING,
          EnumSet.of(AMContainerState.LAUNCHING,
              AMContainerState.STOP_REQUESTED),
          AMContainerEventType.C_ASSIGN_TA, new AssignTaskAttemptTransition())
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.IDLE,
          AMContainerEventType.C_LAUNCHED, new LaunchedTransition())
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.STOPPING,
          AMContainerEventType.C_LAUNCH_FAILED, new LaunchFailedTransition())
      // TODO CREUSE : Maybe, consider sending back an attempt if the container
      // asks for one in this state. Waiting for a LAUNCHED event from the
      // NMComm may delay the task allocation.
      .addTransition(AMContainerState.LAUNCHING, AMContainerState.LAUNCHING,
          AMContainerEventType.C_PULL_TA)
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
          EnumSet.of(AMContainerState.IDLE, AMContainerState.STOP_REQUESTED),
          AMContainerEventType.C_ASSIGN_TA,
          new AssignTaskAttemptAtIdleTransition())
      .addTransition(AMContainerState.IDLE,
          EnumSet.of(AMContainerState.RUNNING, AMContainerState.IDLE),
          AMContainerEventType.C_PULL_TA, new PullTAAtIdleTransition())
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
      .addTransition(AMContainerState.RUNNING, AMContainerState.RUNNING,
          AMContainerEventType.C_PULL_TA)
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
          AMContainerState.STOP_REQUESTED, AMContainerEventType.C_PULL_TA,
          new PullTAAfterStopTransition())
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
      .addTransition(AMContainerState.STOPPING, AMContainerState.STOPPING,
          AMContainerEventType.C_PULL_TA, new PullTAAfterStopTransition())
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
          AMContainerEventType.C_PULL_TA, new PullTAAfterStopTransition())
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
    this.noAllocationContainerTask = WAIT_TASK;
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
      if (this.pendingAttempt != null) {
        allAttempts.add(this.pendingAttempt);
      }
      if (this.runningAttempt != null) {
        allAttempts.add(this.runningAttempt);
      }
      return allAttempts;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<TezTaskAttemptID> getQueuedTaskAttempts() {
    readLock.lock();
    try {
      if (pendingAttempt != null) {
        return Collections.singletonList(this.pendingAttempt);
      } else {
        return Collections.emptyList();
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TezTaskAttemptID getRunningTaskAttempt() {
    readLock.lock();
    try {
      return this.runningAttempt;
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

  // Push the TaskAttempt to the TAL, instead of the TAL pulling when a JVM asks
  // for a TaskAttempt.
  public AMContainerTask pullTaskContext() {
    this.writeLock.lock();
    try {
      this.handle(
          new AMContainerEvent(containerId, AMContainerEventType.C_PULL_TA));
      if (pullAttempt == null) {
        // As a later optimization, it should be possible for a running container to localize
        // additional resources before a task is assigned to the container.
        return noAllocationContainerTask;
      } else {
        // Avoid sending credentials if credentials have not changed.
        AMContainerTask amContainerTask = new AMContainerTask(false,
            remoteTaskMap.remove(pullAttempt), this.additionalLocalResources,
            this.credentialsChanged ? this.credentials : null, this.credentialsChanged);
        this.additionalLocalResources = null;
        this.credentialsChanged = false;
        return amContainerTask;
      }
    } finally {
      this.pullAttempt = null;
      this.writeLock.unlock();
    }
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

      ContainerLaunchContext clc = AMContainerHelpers.createContainerLaunchContext(
          container.appContext.getCurrentDAGID(),
          container.appContext.getApplicationACLs(),
          container.getContainerId(),
          containerContext.getLocalResources(),
          containerContext.getEnvironment(),
          containerContext.getJavaOpts(),
          container.taskAttemptListener.getAddress(), containerContext.getCredentials(),
          container.appContext);

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
              container.getContainerId());
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
      String diag = event.getContainerStatus().getDiagnostics();
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
      if (container.pendingAttempt != null) {
        // This may include a couple of additional (harmless) unregister calls
        // to the taskAttemptListener and containerHeartbeatHandler - in case
        // of assign at any state prior to IDLE.
        container.handleExtraTAAssign(event, container.pendingAttempt);
        // TODO XXX: Verify that it's ok to send in a NM_STOP_REQUEST. The
        // NMCommunicator should be able to handle this. The STOP_REQUEST would
        // only go out after the START_REQUEST.
        return AMContainerState.STOP_REQUESTED;
      }
      
      Map<String, LocalResource> taskLocalResources = event.getRemoteTaskLocalResources();
      Preconditions.checkState(container.additionalLocalResources == null,
          "No additional resources should be pending when assigning a new task");
      container.additionalLocalResources = container.signatureMatcher.getAdditionalResources(
          container.containerLocalResources, taskLocalResources);
      // Register the additional resources back for this container.
      container.containerLocalResources.putAll(container.additionalLocalResources);
      container.pendingAttempt = event.getTaskAttemptId();
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

      container.remoteTaskMap
          .put(event.getTaskAttemptId(), event.getRemoteTaskSpec());
      return container.getState();
    }
  }

  protected static class LaunchedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.registerWithContainerListener();
    }
  }

  protected static class LaunchFailedTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        AMContainerEventLaunchFailed event = (AMContainerEventLaunchFailed) cEvent;
        container.sendTerminatingToTaskAttempt(container.pendingAttempt,
            event.getMessage());
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
      if (container.pendingAttempt != null) {
        String errorMessage = getMessage(container, event);
        if (event.isPreempted()) {
          container.sendPreemptedToTaskAttempt(container.pendingAttempt,
              errorMessage);
        } else {
          container.sendTerminatedToTaskAttempt(container.pendingAttempt,
              errorMessage);
        }
        container.registerFailedAttempt(container.pendingAttempt);
        container.pendingAttempt = null;
        LOG.warn(errorMessage);
      }
      container.containerLocalResources = null;
      container.additionalLocalResources = null;
      container.unregisterFromTAListener();
      String diag = event.getContainerStatus().getDiagnostics();
      if (!(diag == null || diag.equals(""))) {
        LOG.info("Container " + container.getContainerId()
            + " exited with diagnostics set to " + diag);
      }
      container.logStopped(event.isPreempted() ?
            ContainerExitStatus.PREEMPTED
          : ContainerExitStatus.SUCCESS);
    }

    public String getMessage(AMContainerImpl container,
        AMContainerEventCompleted event) {
      return "Container" + container.getContainerId()
          + (event.isPreempted() ? " PREEMPTED" : " COMPLETED")
          + " while trying to launch. Diagnostics: ["
          + event.getContainerStatus().getDiagnostics() +"]";
    }
  }

  protected static class StopRequestAtLaunchingTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {

    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        container.sendTerminatingToTaskAttempt(container.pendingAttempt,
            getMessage(container, cEvent));
      }
      container.unregisterFromTAListener();
      container.logStopped(container.pendingAttempt == null ? 
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
      String errorMessage = null;
      if (cEvent instanceof DiagnosableEvent) {
        errorMessage = ((DiagnosableEvent) cEvent).getDiagnosticInfo();
      }

      for (TezTaskAttemptID taId : container.failedAssignments) {
        container.sendNodeFailureToTA(taId, errorMessage);
      }
      for (TezTaskAttemptID taId : container.completedAttempts) {
        container.sendNodeFailureToTA(taId, errorMessage);
      }

      if (container.pendingAttempt != null) {
        // Will be null in COMPLETED state.
        container.sendNodeFailureToTA(container.pendingAttempt, errorMessage);
        container.sendTerminatingToTaskAttempt(container.pendingAttempt, "Node failure");
      }
      if (container.runningAttempt != null) {
        // Will be null in COMPLETED state.
        container.sendNodeFailureToTA(container.runningAttempt, errorMessage);
        container.sendTerminatingToTaskAttempt(container.runningAttempt, "Node failure");
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
      if (container.pendingAttempt != null) {
        container.sendTerminatingToTaskAttempt(container.pendingAttempt,
            "Container " + container.getContainerId() +
                " hit an invalid transition - " + cEvent.getType() + " at " +
                container.getState());
      }
      container.logStopped(ContainerExitStatus.ABORTED);
      container.sendStopRequestToNM();
      container.unregisterFromTAListener();
    }
  }

  protected static class AssignTaskAttemptAtIdleTransition
      extends AssignTaskAttemptTransition {
    @Override
    public AMContainerState transition(
        AMContainerImpl container, AMContainerEvent cEvent) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AssignTAAtIdle: attempt: " +
            ((AMContainerEventAssignTA) cEvent).getRemoteTaskSpec());
      }
      return super.transition(container, cEvent);
    }
  }

  protected static class PullTAAtIdleTransition implements
      MultipleArcTransition<AMContainerImpl, AMContainerEvent, AMContainerState> {

    @Override
    public AMContainerState transition(
        AMContainerImpl container, AMContainerEvent cEvent) {
      if (container.pendingAttempt != null) {
        // This will be invoked as part of the PULL_REQUEST - so pullAttempt pullAttempt
        // should ideally only end up being populated during the duration of this call,
        // which is in a write lock. pullRequest() should move this to the running state.
        container.pullAttempt = container.pendingAttempt;
        container.runningAttempt = container.pendingAttempt;
        container.pendingAttempt = null;
        if (container.lastTaskFinishTime != 0) {
          long idleTimeDiff =
              System.currentTimeMillis() - container.lastTaskFinishTime;
          container.idleTimeBetweenTasks += idleTimeDiff;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Computing idle time for container: " +
                container.getContainerId() + ", lastFinishTime: " +
                container.lastTaskFinishTime + ", Incremented by: " +
                idleTimeDiff);
          }
        }
        LOG.info("Assigned taskAttempt + [" + container.runningAttempt +
            "] to container: [" + container.getContainerId() + "]");
        return AMContainerState.RUNNING;
      } else {
        return AMContainerState.IDLE;
      }
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
          + (event.isPreempted() ? " PREEMPTED" : " COMPLETED")
          + " with diagnostics set to ["
          + event.getContainerStatus().getDiagnostics() + "]";
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
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.handleExtraTAAssign(event, container.runningAttempt);
    }
  }

  protected static class TASucceededAtRunningTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.lastTaskFinishTime = System.currentTimeMillis();
      container.completedAttempts.add(container.runningAttempt);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.runningAttempt = null;
    }
  }

  protected static class CompletedAtRunningTransition
      extends CompletedAtIdleTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted) cEvent;
      if (event.isPreempted()) {
        container.sendPreemptedToTaskAttempt(container.runningAttempt,
            getMessage(container, event));
      } else {
        container.sendTerminatedToTaskAttempt(container.runningAttempt,
            getMessage(container, event));
      }
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.registerFailedAttempt(container.runningAttempt);
      container.runningAttempt = null;
      super.transition(container, cEvent);
    }
  }

  protected static class StopRequestAtRunningTransition
      extends StopRequestAtIdleTransition {
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {

      container.unregisterAttemptFromListener(container.runningAttempt);
      container.sendTerminatingToTaskAttempt(container.runningAttempt,
          " Container" + container.getContainerId() +
              " received a STOP_REQUEST");
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
      container.unregisterAttemptFromListener(container.runningAttempt);
    }
  }

  protected static class ErrorAtRunningTransition
      extends ErrorAtIdleTransition {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      super.transition(container, cEvent);
      container.unregisterAttemptFromListener(container.runningAttempt);
      container.sendTerminatingToTaskAttempt(container.runningAttempt,
          "Container " + container.getContainerId() +
              " hit an invalid transition - " + cEvent.getType() + " at " +
              container.getState());
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
      container.sendTerminatingToTaskAttempt(event.getTaskAttemptId(), errorMessage);
      container.registerFailedAttempt(event.getTaskAttemptId());
    }
  }

  // Hack to some extent. This allocation should be done while entering one of
  // the post-running states, insetad of being a transition on the post stop
  // states.
  protected static class PullTAAfterStopTransition
      implements SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      container.noAllocationContainerTask = NO_MORE_TASKS;
    }
  }

  protected static class CompletedAtWindDownTransition implements
      SingleArcTransition<AMContainerImpl, AMContainerEvent> {
    @Override
    public void transition(AMContainerImpl container, AMContainerEvent cEvent) {
      AMContainerEventCompleted event = (AMContainerEventCompleted) cEvent;
      String diag = event.getContainerStatus().getDiagnostics();
      for (TezTaskAttemptID taId : container.failedAssignments) {
        container.sendTerminatedToTaskAttempt(taId, diag);
      }
      if (container.pendingAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.pendingAttempt, diag);
        container.registerFailedAttempt(container.pendingAttempt);
        container.pendingAttempt = null;
      }
      if (container.runningAttempt != null) {
        container.sendTerminatedToTaskAttempt(container.runningAttempt, diag);
        container.registerFailedAttempt(container.runningAttempt);
        container.runningAttempt = null;
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
          errorMessage);
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
    this.sendTerminatingToTaskAttempt(event.getTaskAttemptId(), errorMessage);
    this.sendTerminatingToTaskAttempt(currentTaId, errorMessage);
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
      TezTaskAttemptID taId, String message) {
    sendEvent(new TaskAttemptEventContainerTerminated(taId, message));
  }
  
  protected void sendPreemptedToTaskAttempt(
    TezTaskAttemptID taId, String message) {
      sendEvent(new TaskAttemptEventContainerPreempted(taId, message));
  }

  protected void sendTerminatingToTaskAttempt(TezTaskAttemptID taId,
      String message) {
    sendEvent(new TaskAttemptEventContainerTerminating(taId, message));
  }

  protected void maybeSendNodeFailureForFailedAssignment(TezTaskAttemptID taId) {
    if (this.nodeFailed) {
      this.sendNodeFailureToTA(taId, "Node Failed");
    }
  }

  protected void sendNodeFailureToTA(TezTaskAttemptID taId, String message) {
    sendEvent(new TaskAttemptEventNodeFailed(taId, message));
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

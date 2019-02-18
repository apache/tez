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

package org.apache.tez.dag.app.rm.node;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.AMSchedulerEventNodeBlacklistUpdate;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventNodeFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

public class AMNodeImpl implements AMNode {

  private static final Logger LOG = LoggerFactory.getLogger(AMNodeImpl.class);

  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final NodeId nodeId;
  private final int schedulerId;
  private final AppContext appContext;
  private final int maxTaskFailuresPerNode;
  private boolean blacklistingEnabled;
  private boolean ignoreBlacklisting = false;
  private boolean nodeUpdatesRescheduleEnabled;
  private final Set<TezTaskAttemptID> failedAttemptIds = Sets.newHashSet();

  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;

  @VisibleForTesting
  final Set<ContainerId> containers = new LinkedHashSet<>();
  final Set<ContainerId> completedContainers = new HashSet<>();
  int numFailedTAs = 0;
  int numSuccessfulTAs = 0;

  private static final ContainerCompletedTransition CONTAINER_COMPLETED_TRANSITION =
      new ContainerCompletedTransition();

  private final StateMachine<AMNodeState, AMNodeEventType, AMNodeEvent> stateMachine;

  private static StateMachineFactory
  <AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent>
  stateMachineFactory =
  new StateMachineFactory<AMNodeImpl, AMNodeState, AMNodeEventType, AMNodeEvent>(
  AMNodeState.ACTIVE)
        // Transitions from ACTIVE state.
      .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
          AMNodeEventType.N_CONTAINER_ALLOCATED,
          new ContainerAllocatedTransition())
      .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
          AMNodeEventType.N_TA_SUCCEEDED, new TaskAttemptSucceededTransition())
      .addTransition(AMNodeState.ACTIVE,
          EnumSet.of(AMNodeState.ACTIVE, AMNodeState.BLACKLISTED),
          AMNodeEventType.N_TA_ENDED, new TaskAttemptFailedTransition())
      .addTransition(AMNodeState.ACTIVE, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_TURNED_UNHEALTHY,
          new NodeTurnedUnhealthyTransition())
      .addTransition(AMNodeState.ACTIVE,
          EnumSet.of(AMNodeState.ACTIVE, AMNodeState.BLACKLISTED),
          AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED,
          new IgnoreBlacklistingDisabledTransition())
      .addTransition(AMNodeState.ACTIVE, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED,
          new IgnoreBlacklistingStateChangeTransition(true))
      .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
          AMNodeEventType.N_TURNED_HEALTHY)
      .addTransition(AMNodeState.ACTIVE, AMNodeState.ACTIVE,
          AMNodeEventType.N_CONTAINER_COMPLETED, CONTAINER_COMPLETED_TRANSITION)

      // Transitions from BLACKLISTED state.
      .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
          AMNodeEventType.N_CONTAINER_ALLOCATED,
          new ContainerAllocatedWhileBlacklistedTransition())
      .addTransition(AMNodeState.BLACKLISTED,
          EnumSet.of(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE),
          AMNodeEventType.N_TA_SUCCEEDED,
          new TaskAttemptSucceededWhileBlacklistedTransition())
      .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
          AMNodeEventType.N_TA_ENDED, new CountFailedTaskAttemptTransition())
      .addTransition(AMNodeState.BLACKLISTED, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_TURNED_UNHEALTHY,
          new NodeTurnedUnhealthyTransition())
      .addTransition(AMNodeState.BLACKLISTED, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED,
          new IgnoreBlacklistingStateChangeTransition(true))
      .addTransition(AMNodeState.BLACKLISTED, AMNodeState.BLACKLISTED,
          AMNodeEventType.N_CONTAINER_COMPLETED, CONTAINER_COMPLETED_TRANSITION)
      .addTransition(
          AMNodeState.BLACKLISTED,
          AMNodeState.BLACKLISTED,
          EnumSet.of(AMNodeEventType.N_TURNED_HEALTHY,
              AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED),
          new GenericErrorTransition())

      // Transitions from FORCED_ACTIVE state.
      .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_CONTAINER_ALLOCATED,
          new ContainerAllocatedTransition())
      .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_TA_SUCCEEDED, new TaskAttemptSucceededTransition())
      .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_TA_ENDED, new CountFailedTaskAttemptTransition())
      .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_TURNED_UNHEALTHY,
          new NodeTurnedUnhealthyTransition())
      .addTransition(AMNodeState.FORCED_ACTIVE,
          EnumSet.of(AMNodeState.BLACKLISTED, AMNodeState.ACTIVE),
          AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED,
          new IgnoreBlacklistingDisabledTransition())
      .addTransition(AMNodeState.FORCED_ACTIVE, AMNodeState.FORCED_ACTIVE,
          AMNodeEventType.N_CONTAINER_COMPLETED, CONTAINER_COMPLETED_TRANSITION)
      .addTransition(
          AMNodeState.FORCED_ACTIVE,
          AMNodeState.FORCED_ACTIVE,
          EnumSet.of(AMNodeEventType.N_TURNED_HEALTHY,
              AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED),
          new GenericErrorTransition())

      // Transitions from UNHEALTHY state.
      .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_CONTAINER_ALLOCATED,
          new ContainerAllocatedWhileUnhealthyTransition())
      .addTransition(
          AMNodeState.UNHEALTHY,
          AMNodeState.UNHEALTHY,
          EnumSet
              .of(AMNodeEventType.N_TA_SUCCEEDED, AMNodeEventType.N_TA_ENDED))
      .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED,
          new IgnoreBlacklistingStateChangeTransition(false))
      .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED,
          new IgnoreBlacklistingStateChangeTransition(true))
      .addTransition(AMNodeState.UNHEALTHY,
          EnumSet.of(AMNodeState.ACTIVE, AMNodeState.FORCED_ACTIVE),
          AMNodeEventType.N_TURNED_HEALTHY, new NodeTurnedHealthyTransition())
      .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_CONTAINER_COMPLETED, CONTAINER_COMPLETED_TRANSITION)
      .addTransition(AMNodeState.UNHEALTHY, AMNodeState.UNHEALTHY,
          AMNodeEventType.N_TURNED_UNHEALTHY, new GenericErrorTransition())

        .installTopology();


  @SuppressWarnings("rawtypes")
  public AMNodeImpl(NodeId nodeId, int schedulerId, int maxTaskFailuresPerNode,
      EventHandler eventHandler, boolean blacklistingEnabled,
      boolean rescheduleOnUnhealthyNode, AppContext appContext) {
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.nodeId = nodeId;
    this.schedulerId = schedulerId;
    this.appContext = appContext;
    this.eventHandler = eventHandler;
    this.blacklistingEnabled = blacklistingEnabled;
    this.nodeUpdatesRescheduleEnabled = rescheduleOnUnhealthyNode;
    this.maxTaskFailuresPerNode = maxTaskFailuresPerNode;
    this.stateMachine = stateMachineFactory.make(this);
    // TODO Handle the case where a node is created due to the RM reporting it's
    // state as UNHEALTHY
  }

  @Override
  public NodeId getNodeId() {
    return this.nodeId;
  }

  @Override
  public AMNodeState getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public List<ContainerId> getContainers() {
    this.readLock.lock();
    try {
      List<ContainerId> cIds = new LinkedList<ContainerId>(this.containers);
      return cIds;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void handle(AMNodeEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing AMNodeEvent " + event.getNodeId()
          + " of type " + event.getType() + " while in state: " + getState()
          + ". Event: " + event);
    }
    this.writeLock.lock();
    try {
      final AMNodeState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle event " + event.getType()
            + " at current state " + oldState + " for NodeId " + this.nodeId, e);
        // TODO Should this fail the job ?
      }
      if (oldState != getState()) {
        LOG.info("AMNode " + this.nodeId + " transitioned from " + oldState
            + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /* Check whether this node needs to be blacklisted based on node specific information */
  protected boolean qualifiesForBlacklisting() {
    return blacklistingEnabled && (numFailedTAs >= maxTaskFailuresPerNode);
  }

  /* Blacklist the node with the AMNodeTracker and check if the node should be blacklisted */
  protected boolean registerBadNodeAndShouldBlacklist() {
    return appContext.getNodeTracker().registerBadNodeAndShouldBlacklist(this, schedulerId);
  }

  protected void blacklistSelf() {
    for (ContainerId c : containers) {
      sendEvent(new AMContainerEventNodeFailed(c, "Node blacklisted"));
    }
    // these containers are not useful anymore
    containers.clear();
    sendEvent(new AMSchedulerEventNodeBlacklistUpdate(getNodeId(), true, schedulerId));
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                   Start of Transition Classes                            //
  //////////////////////////////////////////////////////////////////////////////

  protected static class ContainerAllocatedTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      node.containers.add(event.getContainerId());
    }
  }

  protected static class TaskAttemptSucceededTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numSuccessfulTAs++;
    }
  }

  protected static class TaskAttemptFailedTransition implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventTaskAttemptEnded event = (AMNodeEventTaskAttemptEnded) nEvent;
      LOG.info("Attempt " + (event.failed() ? "failed" : "killed") + "on node: " + node.getNodeId()
          + " TA: " + event.getTaskAttemptId()
          + ", container: " + event.getContainerId() + ", numFailedTAs: "
          + node.numFailedTAs);
      if (event.failed()) {
        // ignore duplicate attempt ids
        if (node.failedAttemptIds.add(event.getTaskAttemptId())) {
          // new failed container on node
          node.numFailedTAs++;
          if (node.qualifiesForBlacklisting()) {
            if (node.registerBadNodeAndShouldBlacklist()) {
              LOG.info("Too many task attempt failures. " +
                  "Blacklisting node: " + node.getNodeId());
              node.blacklistSelf();
              return AMNodeState.BLACKLISTED;
            } else {
              // Stay in ACTIVE state. Move to FORCED_ACTIVE only when an explicit message is received.
            }
          }
        }
      }
      return AMNodeState.ACTIVE;
    }
  }

  // Forgetting about past errors. Will go back to ACTIVE, not FORCED_ACTIVE
  protected static class NodeTurnedUnhealthyTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      if (node.nodeUpdatesRescheduleEnabled) {
        for (ContainerId c : node.containers) {
          node.sendEvent(new AMContainerEventNodeFailed(c, "Node failed"));
        }
        // Resetting counters.
        node.numFailedTAs = 0;
        node.numSuccessfulTAs = 0;
      }
    }
  }

  protected static class IgnoreBlacklistingDisabledTransition implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {

    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.ignoreBlacklisting = false;
      if (node.qualifiesForBlacklisting()) {
        if (node.registerBadNodeAndShouldBlacklist()) {
          LOG.info("Too many previous task failures after blacklisting re-enabled. " +
              "Blacklisting node: " + node.getNodeId());
          node.blacklistSelf();
          return AMNodeState.BLACKLISTED;
        } else {
          // Stay in ACTIVE state. Move to FORCED_ACTIVE only when an explicit message is received.
        }
      }
      return AMNodeState.ACTIVE;
    }
  }

  protected static class IgnoreBlacklistingStateChangeTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {

    private boolean ignore;

    public IgnoreBlacklistingStateChangeTransition(boolean ignore) {
      this.ignore = ignore;
    }

    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.ignoreBlacklisting = ignore;
      if (node.getState() == AMNodeState.BLACKLISTED) {
        node.sendEvent(new AMSchedulerEventNodeBlacklistUpdate(node.getNodeId(), false, node.schedulerId));
      }
    }
  }

  protected static class ContainerAllocatedWhileBlacklistedTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      node.sendEvent(new AMContainerEvent(event.getContainerId(),
          AMContainerEventType.C_STOP_REQUEST));
    }
  }

  protected static class TaskAttemptSucceededWhileBlacklistedTransition
      implements MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.numSuccessfulTAs++;
      return AMNodeState.BLACKLISTED;
      // For now, always blacklisted. May change at a later point to re-enable
      // the node.
    }
  }

  protected static class CountFailedTaskAttemptTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventTaskAttemptEnded event = (AMNodeEventTaskAttemptEnded) nEvent;
      if (event.failed())
        node.numFailedTAs++;
    }
  }

  protected static class GenericErrorTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {

    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      LOG.warn("Invalid event: " + nEvent.getType() + " while in state: "
          + node.getState() + ". Ignoring." + " Event: " + nEvent);
    }
  }

  protected static class ContainerAllocatedWhileUnhealthyTransition implements
      SingleArcTransition<AMNodeImpl, AMNodeEvent> {
    @Override
    public void transition(AMNodeImpl node, AMNodeEvent nEvent) {
      AMNodeEventContainerAllocated event = (AMNodeEventContainerAllocated) nEvent;
      LOG.info("Node: " + node.getNodeId()
          + " got allocated a contaienr with id: " + event.getContainerId()
          + " while in UNHEALTHY state. Releasing it.");
      node.sendEvent(new AMContainerEventNodeFailed(event.getContainerId(),
          "new container assigned on failed node " + node.getNodeId()));
    }
  }

  protected static class NodeTurnedHealthyTransition implements
      MultipleArcTransition<AMNodeImpl, AMNodeEvent, AMNodeState> {
    @Override
    public AMNodeState transition(AMNodeImpl node, AMNodeEvent nEvent) {
      node.containers.clear();
      if (node.ignoreBlacklisting) {
        return AMNodeState.FORCED_ACTIVE;
      } else {
        return AMNodeState.ACTIVE;
      }
    }
  }

  protected static class ContainerCompletedTransition
      implements SingleArcTransition<AMNodeImpl, AMNodeEvent> {

    @Override
    public void transition(AMNodeImpl amNode, AMNodeEvent amNodeEvent) {
      AMNodeEventContainerCompleted cc =
          (AMNodeEventContainerCompleted) amNodeEvent;
      amNode.completedContainers.add(cc.getContainerId());
    }
  }

  @Override
  public boolean isUnhealthy() {
    this.readLock.lock();
    try {
      return getState() == AMNodeState.UNHEALTHY;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public boolean isBlacklisted() {
    this.readLock.lock();
    try {
      return getState() == AMNodeState.BLACKLISTED;
    } finally {
      this.readLock.unlock();
    }
  }
  
  @Override
  public boolean isUsable() {
    return !(isUnhealthy() || isBlacklisted());
  }

  @Override
  public void dagComplete(DAG dag) {
    this.writeLock.lock();
    try {
      int countBefore = containers.size();
      int countCompleted = completedContainers.size();


      // Actual functionality.
      containers.removeAll(completedContainers);
      completedContainers.clear();

      int countAfter = containers.size();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Node {}, cleaning up knownContainers. current={}, completed={}, postCleanup={}",
            getNodeId(), countBefore, countCompleted, countAfter);
      }

    } finally {
      this.writeLock.unlock();
    }
  }
}

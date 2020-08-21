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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;

import com.google.common.annotations.VisibleForTesting;

public class AMNodeTracker extends AbstractService implements
    EventHandler<AMNodeEvent> {
  
  static final Logger LOG = LoggerFactory.getLogger(AMNodeTracker.class);
  
  private final ConcurrentMap<Integer, PerSourceNodeTracker> perSourceNodeTrackers;

  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext appContext;

  // Not final since it's setup in serviceInit
  private int maxTaskFailuresPerNode;
  private boolean nodeBlocklistingEnabled;
  private int blocklistDisablePercent;
  private boolean nodeUpdatesRescheduleEnabled;

  @SuppressWarnings("rawtypes")
  public AMNodeTracker(EventHandler eventHandler, AppContext appContext) {
    super("AMNodeTracker");
    this.perSourceNodeTrackers = new ConcurrentHashMap<>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;
  }
  
  @Override
  public synchronized void serviceInit(Configuration conf) {
    this.maxTaskFailuresPerNode = conf.getInt(
        TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 
        TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE_DEFAULT);
    this.nodeBlocklistingEnabled = conf.getBoolean(
        TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_ENABLED,
        TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_ENABLED_DEFAULT);
    this.blocklistDisablePercent = conf.getInt(
          TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_IGNORE_THRESHOLD,
          TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_IGNORE_THRESHOLD_DEFAULT);
    this.nodeUpdatesRescheduleEnabled = conf.getBoolean(
          TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS,
          TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS_DEFAULT);

    LOG.info("blocklistDisablePercent is " + blocklistDisablePercent +
        ", blocklistingEnabled: " + nodeBlocklistingEnabled +
        ", maxTaskFailuresPerNode: " + maxTaskFailuresPerNode +
        ", nodeUpdatesRescheduleEnabled: " + nodeUpdatesRescheduleEnabled);

    if (blocklistDisablePercent < -1 || blocklistDisablePercent > 100) {
      throw new TezUncheckedException("Invalid blocklistDisablePercent: "
          + blocklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }
  }

  public void nodeSeen(NodeId nodeId, int schedulerId) {
    PerSourceNodeTracker nodeTracker = getAndCreateIfNeededPerSourceTracker(schedulerId);
    nodeTracker.nodeSeen(nodeId);
  }


  boolean registerBadNodeAndShouldBlocklist(AMNode amNode, int schedulerId) {
    return perSourceNodeTrackers.get(schedulerId).registerBadNodeAndShouldBlocklist(amNode);
  }

  public void handle(AMNodeEvent rEvent) {
    // No synchronization required until there's multiple dispatchers.
    switch (rEvent.getType()) {
      case N_CONTAINER_ALLOCATED:
      case N_CONTAINER_COMPLETED:
      case N_TA_SUCCEEDED:
      case N_TA_ENDED:
      case N_IGNORE_BLOCKLISTING_ENABLED:
      case N_IGNORE_BLOCKLISTING_DISABLED:
        // All of these will only be seen after a node has been registered.
        perSourceNodeTrackers.get(rEvent.getSchedulerId()).handle(rEvent);
        break;
      case N_TURNED_UNHEALTHY:
      case N_TURNED_HEALTHY:
      case N_NODE_COUNT_UPDATED:
        // These events can be seen without a node having been marked as 'seen' before
        getAndCreateIfNeededPerSourceTracker(rEvent.getSchedulerId()).handle(rEvent);
        break;
    }
  }

  public AMNode get(NodeId nodeId, int schedulerId) {
    return perSourceNodeTrackers.get(schedulerId).get(nodeId);
  }

  /**
   * Retrieve the number of nodes from this source on which containers may be running
   *
   * This number may differ from the total number of nodes available from the source
   *
   * @param schedulerId the schedulerId for which the node count is required
   * @return the number of nodes from the scheduler on which containers have been allocated
   */
  public int getNumNodes(int schedulerId) {
    return perSourceNodeTrackers.get(schedulerId).getNumNodes();
  }

  @Private
  @VisibleForTesting
  public boolean isBlocklistingIgnored(int schedulerId) {
    return perSourceNodeTrackers.get(schedulerId).isBlocklistingIgnored();
  }

  public void dagComplete(DAG dag) {
    for (PerSourceNodeTracker perSourceNodeTracker : perSourceNodeTrackers.values()) {
      perSourceNodeTracker.dagComplete(dag);
    }
  }

  private PerSourceNodeTracker getAndCreateIfNeededPerSourceTracker(int schedulerId) {
    PerSourceNodeTracker nodeTracker = perSourceNodeTrackers.get(schedulerId);
    if (nodeTracker == null) {
      nodeTracker =
          new PerSourceNodeTracker(schedulerId, eventHandler, appContext, maxTaskFailuresPerNode,
                  nodeBlocklistingEnabled, blocklistDisablePercent,
              nodeUpdatesRescheduleEnabled);
      PerSourceNodeTracker old = perSourceNodeTrackers.putIfAbsent(schedulerId, nodeTracker);
      nodeTracker = old != null ? old : nodeTracker;
    }
    return nodeTracker;
  }


}

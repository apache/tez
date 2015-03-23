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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.AppContext;

import com.google.common.annotations.VisibleForTesting;

public class AMNodeTracker extends AbstractService implements
    EventHandler<AMNodeEvent> {
  
  static final Logger LOG = LoggerFactory.getLogger(AMNodeTracker.class);
  
  private final ConcurrentHashMap<NodeId, AMNode> nodeMap;
  private final ConcurrentHashMap<String, Set<NodeId>> blacklistMap;
  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext appContext;
  private int numClusterNodes;
  private boolean ignoreBlacklisting = false;
  private int maxTaskFailuresPerNode;
  private boolean nodeBlacklistingEnabled;
  private int blacklistDisablePercent;
  float currentIgnoreBlacklistingCountThreshold = 0;
  
  @SuppressWarnings("rawtypes")
  public AMNodeTracker(EventHandler eventHandler, AppContext appContext) {
    super("AMNodeMap");
    this.nodeMap = new ConcurrentHashMap<NodeId, AMNode>();
    this.blacklistMap = new ConcurrentHashMap<String, Set<NodeId>>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;
  }
  
  @Override
  public synchronized void serviceInit(Configuration conf) {
    this.maxTaskFailuresPerNode = conf.getInt(
        TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 
        TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE_DEFAULT);
    this.nodeBlacklistingEnabled = conf.getBoolean(
        TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED,
        TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED_DEFAULT);
    this.blacklistDisablePercent = conf.getInt(
          TezConfiguration.TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD,
          TezConfiguration.TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD_DEFAULT);

    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent +
        ", blacklistingEnabled: " + nodeBlacklistingEnabled + 
        ", maxTaskFailuresPerNode: " + maxTaskFailuresPerNode);

    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
      throw new TezUncheckedException("Invalid blacklistDisablePercent: "
          + blacklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }
  }
  
  public void nodeSeen(NodeId nodeId) {
    if (nodeMap.putIfAbsent(nodeId, new AMNodeImpl(nodeId, maxTaskFailuresPerNode,
        eventHandler, nodeBlacklistingEnabled, appContext)) == null) {
      LOG.info("Adding new node: " + nodeId);
    }
  }

  private void addToBlackList(NodeId nodeId) {
    String host = nodeId.getHost();

    if (!blacklistMap.containsKey(host)) {
      blacklistMap.putIfAbsent(host, new HashSet<NodeId>());
    }
    Set<NodeId> nodes = blacklistMap.get(host);

    if (!nodes.contains(nodeId)) {
      nodes.add(nodeId);
    }
  }

  boolean registerBadNodeAndShouldBlacklist(AMNode amNode) {
    if (nodeBlacklistingEnabled) {
      addToBlackList(amNode.getNodeId());
      computeIgnoreBlacklisting();
      return !ignoreBlacklisting;
    } else {
      return false;
    }
  }

  public void handle(AMNodeEvent rEvent) {
    // No synchronization required until there's multiple dispatchers.
    NodeId nodeId = rEvent.getNodeId();
    switch (rEvent.getType()) {
    case N_NODE_COUNT_UPDATED:
      AMNodeEventNodeCountUpdated event = (AMNodeEventNodeCountUpdated) rEvent;
      numClusterNodes = event.getNodeCount();
      LOG.info("Num cluster nodes = " + numClusterNodes);
      recomputeCurrentIgnoreBlacklistingThreshold();
      computeIgnoreBlacklisting();
      break;
    case N_TURNED_UNHEALTHY:
    case N_TURNED_HEALTHY:
      AMNode amNode = nodeMap.get(nodeId);
      if (amNode == null) {
        LOG.info("Ignoring RM Health Update for unknown node: " + nodeId);
      } else {
        amNode.handle(rEvent);
      }
      break;
    default:
      nodeMap.get(nodeId).handle(rEvent);
    }
  }

  private void recomputeCurrentIgnoreBlacklistingThreshold() {
    if (nodeBlacklistingEnabled && blacklistDisablePercent != -1) {
      currentIgnoreBlacklistingCountThreshold =
          (float) numClusterNodes * blacklistDisablePercent / 100;
    }
  }

  // May be incorrect if there's multiple NodeManagers running on a single host.
  // knownNodeCount is based on node managers, not hosts. blacklisting is
  // currently based on hosts.
  protected void computeIgnoreBlacklisting() {

    boolean stateChanged = false;

    if (!nodeBlacklistingEnabled || blacklistDisablePercent == -1 || blacklistMap.size() == 0) {
      return;
    }
    if (blacklistMap.size() >= currentIgnoreBlacklistingCountThreshold) {
      if (ignoreBlacklisting == false) {
        ignoreBlacklisting = true;
        LOG.info("Ignore Blacklisting set to true. Known: " + numClusterNodes
            + ", Blacklisted: " + blacklistMap.size());
        stateChanged = true;
      }
    } else {
      if (ignoreBlacklisting == true) {
        ignoreBlacklisting = false;
        LOG.info("Ignore blacklisting set to false. Known: "
            + numClusterNodes + ", Blacklisted: " + blacklistMap.size());
        stateChanged = true;
      }
    }

    if (stateChanged) {
      sendIngoreBlacklistingStateToNodes();
    }
  }

  private void sendIngoreBlacklistingStateToNodes() {
    AMNodeEventType eventType =
        ignoreBlacklisting ? AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED
        : AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED;
    for (NodeId nodeId : nodeMap.keySet()) {
      sendEvent(new AMNodeEvent(nodeId, eventType));
    }
  }

  public AMNode get(NodeId nodeId) {
    return nodeMap.get(nodeId);
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  public int getNumNodes() {
    return nodeMap.size();
  }

  @Private
  @VisibleForTesting
  public boolean isBlacklistingIgnored() {
    return this.ignoreBlacklisting;
  }
}

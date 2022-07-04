/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerSourceNodeTracker {

  static final Logger LOG = LoggerFactory.getLogger(PerSourceNodeTracker.class);

  private final int sourceId;
  private final ConcurrentHashMap<NodeId, AMNode> nodeMap;
  private final ConcurrentHashMap<String, Set<NodeId>> blacklistMap;

  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext appContext;

  private final int maxTaskFailuresPerNode;
  private final boolean nodeBlacklistingEnabled;
  private final int blacklistDisablePercent;
  private final boolean nodeUpdatesRescheduleEnabled;

  private int numClusterNodes;
  float currentIgnoreBlacklistingCountThreshold = 0;
  private boolean ignoreBlacklisting = false;

  @SuppressWarnings("rawtypes")
  public PerSourceNodeTracker(int sourceId, EventHandler eventHandler, AppContext appContext,
                              int maxTaskFailuresPerNode, boolean nodeBlacklistingEnabled,
                              int blacklistDisablePercent,
                              boolean nodeUpdatesRescheduleEnabled) {
    this.sourceId = sourceId;
    this.nodeMap = new ConcurrentHashMap<>();
    this.blacklistMap = new ConcurrentHashMap<>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;

    this.maxTaskFailuresPerNode = maxTaskFailuresPerNode;
    this.nodeBlacklistingEnabled = nodeBlacklistingEnabled;
    this.blacklistDisablePercent = blacklistDisablePercent;
    this.nodeUpdatesRescheduleEnabled = nodeUpdatesRescheduleEnabled;
  }

  public void nodeSeen(NodeId nodeId) {
    if (nodeMap.putIfAbsent(nodeId, new AMNodeImpl(nodeId, sourceId, maxTaskFailuresPerNode,
        eventHandler, nodeBlacklistingEnabled, nodeUpdatesRescheduleEnabled,
        appContext)) == null) {
      LOG.info("Adding new node {} to nodeTracker {}", nodeId, sourceId);
    }
  }

  public AMNode get(NodeId nodeId) {
    return nodeMap.get(nodeId);
  }

  public int getNumNodes() {
    return nodeMap.size();
  }

  public int getNumActiveNodes() {
    return (int) nodeMap.values().stream().filter(node -> node.getState() == AMNodeState.ACTIVE).count();
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
          // This implies that the node exists on the cluster, but is not running a container for
          // this application.
        } else {
          amNode.handle(rEvent);
        }
        break;
      default:
        amNode = nodeMap.get(nodeId);
        amNode.handle(rEvent);
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

  @InterfaceAudience.Private
  @VisibleForTesting
  public boolean isBlacklistingIgnored() {
    return this.ignoreBlacklisting;
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

  private void sendIngoreBlacklistingStateToNodes() {
    AMNodeEventType eventType =
        ignoreBlacklisting ? AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED
            : AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED;
    for (NodeId nodeId : nodeMap.keySet()) {
      sendEvent(new AMNodeEvent(nodeId, sourceId, eventType));
    }
  }

  public void dagComplete(DAG dag) {
    for (AMNode amNode : nodeMap.values()) {
      amNode.dagComplete(dag);
    }
    // TODO TEZ-2337 Maybe reset failures from previous DAGs
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }
}

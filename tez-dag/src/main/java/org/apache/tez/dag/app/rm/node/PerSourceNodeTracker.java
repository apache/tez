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
  private final ConcurrentHashMap<String, Set<NodeId>> blocklistMap;

  @SuppressWarnings("rawtypes")
  private final EventHandler eventHandler;
  private final AppContext appContext;

  private final int maxTaskFailuresPerNode;
  private final boolean nodeBlocklistingEnabled;
  private final int blocklistDisablePercent;
  private final boolean nodeUpdatesRescheduleEnabled;

  private int numClusterNodes;
  float currentIgnoreBlocklistingCountThreshold = 0;
  private boolean ignoreBlocklisting = false;

  @SuppressWarnings("rawtypes")
  public PerSourceNodeTracker(int sourceId, EventHandler eventHandler, AppContext appContext,
                              int maxTaskFailuresPerNode, boolean nodeBlocklistingEnabled,
                              int blocklistDisablePercent,
                              boolean nodeUpdatesRescheduleEnabled) {
    this.sourceId = sourceId;
    this.nodeMap = new ConcurrentHashMap<>();
    this.blocklistMap = new ConcurrentHashMap<>();
    this.eventHandler = eventHandler;
    this.appContext = appContext;

    this.maxTaskFailuresPerNode = maxTaskFailuresPerNode;
    this.nodeBlocklistingEnabled = nodeBlocklistingEnabled;
    this.blocklistDisablePercent = blocklistDisablePercent;
    this.nodeUpdatesRescheduleEnabled = nodeUpdatesRescheduleEnabled;
  }



  public void nodeSeen(NodeId nodeId) {
    if (nodeMap.putIfAbsent(nodeId, new AMNodeImpl(nodeId, sourceId, maxTaskFailuresPerNode,
        eventHandler, nodeBlocklistingEnabled, nodeUpdatesRescheduleEnabled,
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

  public void handle(AMNodeEvent rEvent) {
    // No synchronization required until there's multiple dispatchers.
    NodeId nodeId = rEvent.getNodeId();
    switch (rEvent.getType()) {
      case N_NODE_COUNT_UPDATED:
        AMNodeEventNodeCountUpdated event = (AMNodeEventNodeCountUpdated) rEvent;
        numClusterNodes = event.getNodeCount();
        LOG.info("Num cluster nodes = " + numClusterNodes);
        recomputeCurrentIgnoreBlocklistingThreshold();
        computeIgnoreBlocklisting();
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

  boolean registerBadNodeAndShouldBlocklist(AMNode amNode) {
    if (nodeBlocklistingEnabled) {
      addToBlockList(amNode.getNodeId());
      computeIgnoreBlocklisting();
      return !ignoreBlocklisting;
    } else {
      return false;
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public boolean isBlocklistingIgnored() {
    return this.ignoreBlocklisting;
  }

  private void recomputeCurrentIgnoreBlocklistingThreshold() {
    if (nodeBlocklistingEnabled && blocklistDisablePercent != -1) {
      currentIgnoreBlocklistingCountThreshold =
          (float) numClusterNodes * blocklistDisablePercent / 100;
    }
  }

  // May be incorrect if there's multiple NodeManagers running on a single host.
  // knownNodeCount is based on node managers, not hosts. blocklisting is
  // currently based on hosts.
  protected void computeIgnoreBlocklisting() {

    boolean stateChanged = false;

    if (!nodeBlocklistingEnabled || blocklistDisablePercent == -1 || blocklistMap.size() == 0) {
      return;
    }
    if (blocklistMap.size() >= currentIgnoreBlocklistingCountThreshold) {
      if (ignoreBlocklisting == false) {
        ignoreBlocklisting = true;
        LOG.info("Ignore Blocklisting set to true. Known: " + numClusterNodes
            + ", Blocklisted: " + blocklistMap.size());
        stateChanged = true;
      }
    } else {
      if (ignoreBlocklisting == true) {
        ignoreBlocklisting = false;
        LOG.info("Ignore blocklisting set to false. Known: "
            + numClusterNodes + ", Blocklisted: " + blocklistMap.size());
        stateChanged = true;
      }
    }

    if (stateChanged) {
      sendIngoreBlocklistingStateToNodes();
    }
  }

  private void addToBlockList(NodeId nodeId) {
    String host = nodeId.getHost();

    if (!blocklistMap.containsKey(host)) {
      blocklistMap.putIfAbsent(host, new HashSet<NodeId>());
    }
    Set<NodeId> nodes = blocklistMap.get(host);

    if (!nodes.contains(nodeId)) {
      nodes.add(nodeId);
    }
  }

  private void sendIngoreBlocklistingStateToNodes() {
    AMNodeEventType eventType =
        ignoreBlocklisting ? AMNodeEventType.N_IGNORE_BLOCKLISTING_ENABLED
            : AMNodeEventType.N_IGNORE_BLOCKLISTING_DISABLED;
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

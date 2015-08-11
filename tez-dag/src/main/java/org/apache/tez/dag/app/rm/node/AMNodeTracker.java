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
  private boolean nodeBlacklistingEnabled;
  private int blacklistDisablePercent;

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

  public void nodeSeen(NodeId nodeId, int sourceId) {
    PerSourceNodeTracker nodeTracker = getAndCreateIfNeededPerSourceTracker(sourceId);
    nodeTracker.nodeSeen(nodeId);
  }


  boolean registerBadNodeAndShouldBlacklist(AMNode amNode, int sourceId) {
    return perSourceNodeTrackers.get(sourceId).registerBadNodeAndShouldBlacklist(amNode);
  }

  public void handle(AMNodeEvent rEvent) {
    // No synchronization required until there's multiple dispatchers.
    switch (rEvent.getType()) {
      case N_CONTAINER_ALLOCATED:
      case N_TA_SUCCEEDED:
      case N_TA_ENDED:
      case N_IGNORE_BLACKLISTING_ENABLED:
      case N_IGNORE_BLACKLISTING_DISABLED:
        // All of these will only be seen after a node has been registered.
        perSourceNodeTrackers.get(rEvent.getSourceId()).handle(rEvent);
        break;
      case N_TURNED_UNHEALTHY:
      case N_TURNED_HEALTHY:
      case N_NODE_COUNT_UPDATED:
        // These events can be seen without a node having been marked as 'seen' before
        getAndCreateIfNeededPerSourceTracker(rEvent.getSourceId()).handle(rEvent);
        break;
    }
  }

  public AMNode get(NodeId nodeId, int sourceId) {
    return perSourceNodeTrackers.get(sourceId).get(nodeId);
  }

  public int getNumNodes(int sourceId) {
    return perSourceNodeTrackers.get(sourceId).getNumNodes();
  }

  @Private
  @VisibleForTesting
  public boolean isBlacklistingIgnored(int sourceId) {
    return perSourceNodeTrackers.get(sourceId).isBlacklistingIgnored();
  }

  public void dagComplete(DAG dag) {
    // TODO TEZ-2337 Maybe reset failures from previous DAGs
  }

  private PerSourceNodeTracker getAndCreateIfNeededPerSourceTracker(int sourceId) {
    PerSourceNodeTracker nodeTracker = perSourceNodeTrackers.get(sourceId);
    if (nodeTracker == null) {
      nodeTracker =
          new PerSourceNodeTracker(sourceId, eventHandler, appContext, maxTaskFailuresPerNode,
              nodeBlacklistingEnabled, blacklistDisablePercent);
      PerSourceNodeTracker old = perSourceNodeTrackers.putIfAbsent(sourceId, nodeTracker);
      nodeTracker = old != null ? old : nodeTracker;
    }
    return nodeTracker;
  }


}

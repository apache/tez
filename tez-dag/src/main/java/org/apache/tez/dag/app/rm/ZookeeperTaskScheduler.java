/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tez.dag.app.rm;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TaskScheduler implementation that discovers externally managed workers via Zookeeper. This
 * scheduler does not communicate with YARN RM/NM and manages containers discovered via ZK.
 */
public class ZookeeperTaskScheduler extends TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTaskScheduler.class);

  private record TaskRequest(Object task, Object clientCookie) {}

  private CuratorFramework zkClient;
  private CuratorCache workerCache;

  private final Queue<Container> availableContainers = new ConcurrentLinkedQueue<>();
  private final Queue<TaskRequest> pendingTasks = new ConcurrentLinkedQueue<>();

  public ZookeeperTaskScheduler(TaskSchedulerContext taskSchedulerContext) {
    super(taskSchedulerContext);
  }

  @Override
  public void initialize() throws Exception {
    Configuration conf;
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getInitialUserPayload());
    } catch (IOException e) {
      LOG.warn("Failed to derive configuration from UserPayload, using default Configuration");
      conf = new Configuration(false);
    }

    String zkQuorum = conf.get(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM);
    String appId = getContext().getApplicationAttemptId().getApplicationId().toString();

    ZkConfig zkconfig = new ZkConfig(conf);
    zkClient = CuratorFrameworkFactory.newClient(zkQuorum, zkconfig.getRetryPolicy());
    zkClient.start();


    String workerPath = zkconfig.getZkTaskNameSpace() + "/" + appId;

    if (zkClient.checkExists().forPath(workerPath) == null) {
      zkClient.create().creatingParentsIfNeeded().forPath(workerPath);
    }

    workerCache = CuratorCache.build(zkClient, workerPath);

    CuratorCacheListener listener =
        CuratorCacheListener.builder()
            .forCreates(
                node -> {
                  String path = node.getPath();
                  if (path.equals(workerPath)) {
                    return;
                  }
                  String containerIdStr = path.substring(path.lastIndexOf('/') + 1);
                  String host = new String(node.getData());

                  LOG.info("Discovered new TezChild via ZK: {} on host {}", containerIdStr, host);

                  try {
                    ContainerId cId = ContainerId.fromString(containerIdStr);
                    NodeId mockNodeId = NodeId.newInstance(host, 0);

                    // Using hardcoded resource value for now
                    Container mockContainer =
                        Container.newInstance(
                            cId,
                            mockNodeId,
                            "dummy:0",
                            Resource.newInstance(1024, 1),
                            Priority.newInstance(1),
                            null);

                    availableContainers.offer(mockContainer);

                    // Inform the AM IMMEDIATELY so it adds the cotainer to the Idle Pool
                    getContext().containerAllocated(mockContainer);
                    tryAllocate();

                  } catch (Exception e) {
                    LOG.error(
                        "Failed to parse and allocate discovered container: {}", containerIdStr, e);
                  }
                })
            .forDeletes(
                node -> {
                  LOG.warn("TezChild removed from ZK: {}", node.getPath());
                })
            .build();

    workerCache.listenable().addListener(listener);
  }

  @Override
  public void start() throws Exception {
    workerCache.start();
  }

  @Override
  public void shutdown() throws Exception {
    if (workerCache != null) {
      workerCache.close();
    }
    if (zkClient != null) {
      zkClient.close();
    }
  }

  @Override
  public Resource getAvailableResources() {
    // Return hardcoded resource based on available containers
    return Resource.newInstance(1024 * availableContainers.size(), availableContainers.size());
  }

  @Override
  public Resource getTotalResources() {
    // Hardcoded total resources
    return Resource.newInstance(1024 * 100, 100);
  }

  @Override
  public int getClusterNodeCount() {
    return availableContainers.size();
  }

  @Override
  public void blacklistNode(NodeId nodeId) {
    // No-op for standalone mode
  }

  @Override
  public void unblacklistNode(NodeId nodeId) {
    // No-op for standalone mode
  }

  @Override
  public void allocateTask(
      Object task,
      Resource capability,
      String[] hosts,
      String[] racks,
      Priority priority,
      Object containerSignature,
      Object clientCookie) {
    pendingTasks.offer(new TaskRequest(task, clientCookie));
    tryAllocate();
  }

  @Override
  public void allocateTask(
      Object task,
      Resource capability,
      ContainerId containerId,
      Priority priority,
      Object containerSignature,
      Object clientCookie) {
    // Affinity requested, but fallback to general allocation in standalone mode
    allocateTask(task, capability, null, null, priority, containerSignature, clientCookie);
  }

  private synchronized void tryAllocate() {
    while (!pendingTasks.isEmpty() && !availableContainers.isEmpty()) {
      TaskRequest request = pendingTasks.poll();
      Container container = availableContainers.poll();

      if (request != null && container != null) {
        LOG.info("Assigning Task to container: {}", container.getId().toString());
        getContext().taskAllocated(request.task(), request.clientCookie(), container);
      }
    }
  }

  @Override
  public boolean deallocateTask(
      Object task,
      boolean taskComplete,
      TaskAttemptEndReason endReason,
      @Nullable String diagnostics) {
    // This scheduler doesn't manage container reuse explicitly, so just return true
    return true;
  }

  @Override
  public Object deallocateContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public void setShouldUnregister() {
    // No-op
  }

  @Override
  public boolean hasUnregistered() {
    return false;
  }

  @Override
  public void dagComplete() {
    // No-op
  }
}

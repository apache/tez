/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api.client.registry.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.client.registry.AMRegistryUtils;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator/Zookeeper impl of AMRegistry (for internal use only)
 * Clients should use org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistryClient instead.
 */
@InterfaceAudience.Private
public class ZkAMRegistry extends AMRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistry.class);

  private final List<AMRecord> amRecords = Collections.synchronizedList(new ArrayList<>());
  private final String externalId;

  private CuratorFramework client = null;
  private String namespace = null;
  private ZkConfig zkConfig = null;

  public ZkAMRegistry(String externalId) {
    super("ZkAMRegistry");
    this.externalId = externalId;
  }

  @Override
  public void serviceInit(Configuration conf) {
    zkConfig = new ZkConfig(conf);
    this.client = zkConfig.createCuratorFramework();
    this.namespace = zkConfig.getZkNamespace();
    LOG.info("ZkAMRegistry initialized");
  }

  @Override
  public void serviceStart() throws Exception {
    client.start();
    LOG.info("ZkAMRegistry started");
  }

  /**
   * Shuts down the service by removing all {@link AMRecord} entries from ZooKeeper
   * that were created by this instance.
   *
   * <p>After all removal attempts, the ZooKeeper client is closed and the shutdown
   * is logged.</p>
   *
   * @throws Exception if a failure occurs while closing the ZooKeeper client
   */
  @Override
  public void serviceStop() throws Exception {
    for (AMRecord amRecord : new ArrayList<>(amRecords)) {
      try {
        remove(amRecord);
      } catch (Exception e) {
        LOG.warn("Exception while trying to remove AMRecord: {}", amRecord, e);
      }
    }
    client.close();
    LOG.info("ZkAMRegistry shutdown");
  }

  //Serialize AMRecord to ServiceRecord and deliver the JSON bytes to
  //zkNode at the path:  <TEZ_AM_REGISTRY_NAMESPACE>/<appId>
  @Override
  public void add(AMRecord server) throws Exception {
    String json = AMRegistryUtils.recordToJsonString(server);
    try {
      final String path = pathFor(server);
      client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
      LOG.info("Added AMRecord to zkpath {}", path);
    } catch (KeeperException.NoNodeException nne) {
      client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL)
          .forPath(pathFor(server), json.getBytes(StandardCharsets.UTF_8));
    }
    amRecords.add(server);
  }

  @Override
  public void remove(AMRecord server) throws Exception {
    amRecords.remove(server);
    final String path = pathFor(server);
    client.delete().forPath(path);
    LOG.info("Deleted AMRecord from zkpath {}", path);
  }

  @Override
  public ApplicationId generateNewId() throws Exception {
    createNamespaceIfNotExists();
    long namespaceCreationTime = getNamespaceCreationTime();

    boolean success = false;
    long startTime = System.currentTimeMillis();
    RetryPolicy retryPolicy = zkConfig.getRetryPolicy();
    int tryId = 0;
    for (int i = 0; (i < zkConfig.getCuratorMaxRetries()) && !success; i++) {
      List<String> children = client.getChildren().forPath(namespace);
      if (children != null && !children.isEmpty()) {
        children.sort(Collections.reverseOrder());
        String last = children.getFirst();
        ApplicationId lastAppId = ApplicationId.fromString(last);
        tryId = lastAppId.getId() + 1;
      }
      ApplicationId tryAppId = ApplicationId.newInstance(namespaceCreationTime, tryId);
      try {
        client
            .create()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(namespace + "/" + tryAppId.toString(), new byte[0]);
        LOG.debug("Successfully created application id {} for namespace {}", tryAppId, namespace);
        success = true;
      } catch (KeeperException.NodeExistsException nodeExists) {
        LOG.info("Node already exists in ZK for application id {}", tryId);
        long elapsedTime = System.currentTimeMillis() - startTime;
        retryPolicy.allowRetry(i + 1, elapsedTime, RetryLoop.getDefaultRetrySleeper());
        tryId++;
      }
    }
    if (success) {
      return ApplicationId.newInstance(namespaceCreationTime, tryId);
    } else {
      throw new RuntimeException("Could not obtain unique ApplicationId after " +
          zkConfig.getCuratorMaxRetries() + " tries");
    }
  }

  @Override
  public AMRecord createAmRecord(ApplicationId appId,  String hostName, String hostIp, int port, String computeName) {
    return new AMRecord(appId, hostName, hostIp, port, externalId, computeName);
  }

  private long getNamespaceCreationTime() throws Exception {
    Stat stat = client.checkExists().forPath(namespace);
    return stat.getCtime();
  }

  private void createNamespaceIfNotExists() throws Exception {
    try {
      client.create().creatingParentContainersIfNeeded().forPath(namespace);
    } catch (KeeperException.NodeExistsException nodeExists) {
      LOG.info("Namespace already exists, will use existing: {}", namespace);
    }
  }

  private String pathFor(AMRecord record) {
    return namespace + "/" + record.getApplicationId().toString();
  }
}

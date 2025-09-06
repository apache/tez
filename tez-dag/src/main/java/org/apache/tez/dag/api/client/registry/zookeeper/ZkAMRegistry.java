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

package org.apache.tez.dag.api.client.registry.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator/Zookeeper impl of AMRegistry (for internal use only)
 * Clients should use org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistryClient instead
 */
@InterfaceAudience.Private
public class ZkAMRegistry extends AMRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistry.class);

  private CuratorFramework client = null;
  private String namespace = null;
  private List<AMRecord> amRecords = new ArrayList<>();
  private ZkConfig zkConfig = null;
  private boolean started = false;
  private String externalId;

  public ZkAMRegistry(String externalId) {
    super("ZkAMRegistry");
    this.externalId = externalId;
  }

  @Override
  public void serviceInit(Configuration conf) {
    if(zkConfig == null) {
      zkConfig = new ZkConfig(conf);
      this.client = zkConfig.createCuratorFramework();
      this.namespace = zkConfig.getZkNamespace();
      LOG.info("AMRegistryZkImpl initialized");
    }
  }

  @Override public void serviceStart() throws Exception {
    if(!started) {
      client.start();
      started = true;
      LOG.info("AMRegistryZkImpl started");
    }
  }

  //Deletes from Zookeeper AMRecords that were added by this instance
  @Override public void serviceStop() throws Exception {
    List<AMRecord> records = new ArrayList<>(amRecords);
    for(AMRecord amRecord : records) {
      remove(amRecord);
    }
    client.close();
    LOG.info("AMRegistryZkImpl shutdown");
  }

  //Serialize AMRecord to ServiceRecord and deliver the JSON bytes to
  //zkNode at the path:  <TEZ_AM_REGISTRY_NAMESPACE>/<appId>
  @Override public void add(AMRecord server) throws Exception {
    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
    String json = marshal.toJson(server.toServiceRecord());
    try {
      final String path = namespace + "/" + server.getApplicationId().toString();
      client.setData().forPath(path, json.getBytes());
      LOG.info("Added AMRecord to zkpath {}", path);
    } catch(KeeperException.NoNodeException nne) {
      client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(namespace + "/" + server.getApplicationId().toString(), json.getBytes());
    }
    amRecords.add(server);
  }

  @Override public void remove(AMRecord server) throws Exception {
    amRecords.remove(server);
    final String path = namespace + "/" + server.getApplicationId().toString();
    client.delete().forPath(path);
    LOG.info("Deleted AMRecord from zkpath {}", path);
  }

  @Override
  public Optional<ApplicationId> generateNewId() throws Exception {
    createNamespaceIfNotExists();
    long namespaceCreationTime = getNamespaceCreationTime();

    boolean success = false;
    long startTime = System.currentTimeMillis();
    RetryPolicy retryPolicy = zkConfig.getRetryPolicy();
    int tryId = 0;
    for(int i = 0; (i < zkConfig.getCuratorMaxRetries()) && !success; i++) {
      List<String> children = client.getChildren().forPath(namespace);
      if((children != null) && (children.size() != 0)) {
        Collections.sort(children, Collections.reverseOrder());
        String last = children.get(0);
        ApplicationId lastAppId = ApplicationId.fromString(last);
        tryId = lastAppId.getId() + 1;
      }
      ApplicationId tryAppId = ApplicationId.newInstance(namespaceCreationTime, tryId);
      try {
        client
            .create()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(namespace + "/" + tryAppId.toString(), new byte[0]);
        success = true;
      } catch(KeeperException.NodeExistsException nodeExists) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        retryPolicy.allowRetry(i + 1, elapsedTime, RetryLoop.getDefaultRetrySleeper());
        tryId++;
      }
    }
    if(success) {
      return Optional.of(ApplicationId.newInstance(namespaceCreationTime, tryId));
    } else {
      throw new RuntimeException("Could not obtain unique ApplicationId after " +
          zkConfig.getCuratorMaxRetries() + " tries");
    }
  }

  @Override public AMRecord createAmRecord(ApplicationId appId, String hostName, int port) {
    return new AMRecord(appId, hostName, port, externalId);
  }

  private long getNamespaceCreationTime() throws Exception {
    Stat stat = client.checkExists().forPath(namespace);
    return stat.getCtime();
  }

  private void createNamespaceIfNotExists() throws Exception {
    try {
      client.create().creatingParentContainersIfNeeded().forPath(namespace);
    } catch(KeeperException.NodeExistsException nodeExists) {
      LOG.info("Namespace already exists, will use existing: {}", namespace);
    }
  }
}

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

package org.apache.tez.client.registry.zookeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.shaded.com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClient;
import org.apache.tez.client.registry.AMRegistryClientListener;
import org.apache.tez.dag.api.TezConfiguration;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator/Zookeeper implementation of {@link AMRegistryClient}.
 */
@InterfaceAudience.Public
public final class ZkAMRegistryClient extends AMRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZkAMRegistryClient.class);
  private static final Map<String, ZkAMRegistryClient> INSTANCES = new HashMap<>();

  private final Configuration conf;
  //Cache of known AMs
  private final ConcurrentHashMap<String, AMRecord> amRecordCache = new ConcurrentHashMap<>();
  private CuratorFramework client;

  private ZkAMRegistryClient(final Configuration conf) {
    this.conf = conf;
  }

  public static synchronized ZkAMRegistryClient getClient(final Configuration conf) {
    String namespace = conf.get(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE);
    ZkAMRegistryClient registry = INSTANCES.get(namespace);
    if (registry == null) {
      registry = new ZkAMRegistryClient(conf);
      INSTANCES.put(namespace, registry);
    }
    LOG.info("Returning tez AM registry ({}) for namespace '{}'", System.identityHashCode(registry), namespace);
    return registry;
  }

  /**
   * Deserializes a {@link ServiceRecord} from ZooKeeper data and converts it into an {@link AMRecord}
   * for caching.
   *
   * @param childData the ZooKeeper node data containing a serialized {@link ServiceRecord}
   * @return an {@link AMRecord} constructed from the deserialized {@link ServiceRecord}, or {@code null}
   * if no data is present
   * @throws IOException if the data cannot be deserialized into a {@link ServiceRecord}
   */
  public static AMRecord getAMRecord(final ChildData childData) throws IOException {
    // not a leaf path. Only leaf path contains AMRecord
    if (!childData.getPath().contains(ApplicationId.appIdStrPrefix)) {
      return null;
    }
    byte[] data = childData.getData();
    // only the path appeared, there is no data yet
    if (data.length == 0) {
      return null;
    }
    String value = new String(data, Charsets.UTF_8);
    try {
      RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
      ServiceRecord serviceRecord = marshal.fromJson(value);
      return new AMRecord(serviceRecord);
    } catch (JsonParseException e) {
      //Not a json AMRecord (SRV), could be some other data.
      LOG.warn("Non-json data received while de-serializing AMRecord: {}. Ignoring...", value);
      return null;
    }
  }

  public void start() throws Exception {
    ZkConfig zkConf = new ZkConfig(this.conf);
    client = zkConf.createCuratorFramework();
    final TreeCache cache = new TreeCache(client, zkConf.getZkNamespace());
    client.start();
    cache.start();
    cache.getListenable().addListener(new ZkRegistryListener());
  }

  @Override
  public AMRecord getRecord(String appId) {
    if (amRecordCache.get(appId) == null) {
      return null;
    }
    //Return a copy
    return new AMRecord(amRecordCache.get(appId));
  }

  @Override
  public List<AMRecord> getAllRecords() {
    return amRecordCache.values().stream().map(AMRecord::new).collect(Collectors.toList());
  }

  @Override
  public synchronized void addListener(AMRegistryClientListener listener) {
    getListeners().add(listener);
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Callback listener for ZooKeeper events that updates the local cache
   * when child nodes under the monitored path change.
   */
  private class ZkRegistryListener implements TreeCacheListener {

    @Override
    public void childEvent(final CuratorFramework clientParam, final TreeCacheEvent event) throws Exception {
      Preconditions.checkArgument(clientParam != null && clientParam.getState() == CuratorFrameworkState.STARTED,
          "Curator client is not started");

      ChildData childData = event.getData();
      switch (event.getType()) {
        case NODE_ADDED:
          if (isEmpty(childData)) {
            LOG.info("AppId allocated: {}", childData.getPath());
          } else {
            AMRecord amRecord = getAMRecord(childData);
            if (amRecord != null) {
              LOG.info("AM registered with data: {}. Notifying {} listeners.", amRecord, getListeners().size());
              amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
              notifyOnAdded(amRecord);
            }
          }
          break;
        case NODE_UPDATED:
          if (isEmpty(childData)) {
            throw new RuntimeException("AM updated with empty data");
          } else {
            AMRecord amRecord = getAMRecord(childData);
            if (amRecord != null) {
              LOG.info("AM updated data: {}. Notifying {} listeners.", amRecord, getListeners().size());
              amRecordCache.put(amRecord.getApplicationId().toString(), amRecord);
              notifyOnAdded(amRecord);
            }
          }
          break;
        case NODE_REMOVED:
          if (isEmpty(childData)) {
            LOG.info("Unused AppId unregistered: {}", childData.getPath());
          } else {
            AMRecord amRecord = getAMRecord(childData);
            if (amRecord != null) {
              LOG.info("AM removed: {}. Notifying {} listeners.", amRecord, getListeners().size());
              amRecordCache.remove(amRecord.getApplicationId().toString(), amRecord);
              notifyOnRemoved(amRecord);
            }
          }
          break;
        default:
          if (childData == null) {
            LOG.info("Ignored event {}", event.getType());
          } else {
            LOG.info("Ignored event {} for {}", event.getType(), childData.getPath());
          }
      }
    }

    private boolean isEmpty(ChildData childData) {
      return childData == null || childData.getData() == null || childData.getData().length == 0;
    }
  }
}

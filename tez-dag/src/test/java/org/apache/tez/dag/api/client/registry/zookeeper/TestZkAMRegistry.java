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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryUtils;
import org.apache.tez.client.registry.zookeeper.ZkConfig;
import org.apache.tez.dag.api.TezConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link ZkAMRegistry}.
 *
 * <p>This test class focuses on the low-level AM registry implementation that runs
 * inside the AM process. It validates that:</p>
 * <ul>
 *   <li>Unique {@link ApplicationId}s are generated and persisted in ZooKeeper.</li>
 *   <li>{@link AMRecord}s are written to and removed from ZooKeeper at the expected paths.</li>
 * </ul>
 */
public class TestZkAMRegistry {

  private TestingServer zkServer;

  @Before
  public void setup() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();
  }

  @After
  public void teardown() throws Exception {
    if (zkServer != null) {
      zkServer.close();
    }
  }

  @Test
  public void testGenerateNewIdProducesUniqueIds() throws Exception {
    TezConfiguration conf = createTezConf();
    try (ZkAMRegistry registry = new ZkAMRegistry("external-id")) {
      registry.init(conf);
      registry.start();

      ApplicationId first = registry.generateNewId();
      ApplicationId second = registry.generateNewId();

      assertNotNull(first);
      assertNotNull(second);
      assertEquals("Cluster timestamps should match", first.getClusterTimestamp(), second.getClusterTimestamp());
      assertEquals("Second id should be first id + 1", first.getId() + 1, second.getId());
    }
  }

  @Test(timeout = 120000)
  public void testGenerateNewIdFromParallelThreads() throws Exception {
    final int threadCount = 50;

    TezConfiguration conf = createTezConf();
    // this is the maxRetries for ExponentialBackoffRetry, let's use it to be able to test high concurrency
    conf.setInt(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES, 29);

    try (ZkAMRegistry registry = new ZkAMRegistry("external-id")) {
      registry.init(conf);
      registry.start();

      ExecutorService executor = Executors.newFixedThreadPool(threadCount);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threadCount);

      Set<ApplicationId> ids = Collections.synchronizedSet(new HashSet<>());

      List<Future<?>> asyncTasks = new ArrayList<>();

      for (int i = 0; i < threadCount; i++) {
        asyncTasks.add(CompletableFuture.runAsync(() -> {
          try {
            // Ensure all threads start generateNewId as simultaneously as possible
            startLatch.await();
            ApplicationId id = registry.generateNewId();
            assertNotNull(id);
            ids.add(id);
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            doneLatch.countDown();
          }
        }, executor));
      }

      // release all threads
      startLatch.countDown();

      // run the tasks
      try {
        CompletableFuture.allOf(asyncTasks.toArray(new CompletableFuture[0])).get();
      } catch (ExecutionException e) { // ExecutionException wraps the original exception
        throw new RuntimeException(e.getCause());
      } finally {
        executor.shutdown();
      }
      assertEquals(String.format("All generated ids should be unique, ids found: %s", ids), threadCount, ids.size());

      // additionally ensure cluster timestamp is the same for all IDs
      long clusterTs = ids.iterator().next().getClusterTimestamp();
      for (ApplicationId id : ids) {
        assertEquals("Cluster timestamps should match for all generated ids", clusterTs, id.getClusterTimestamp());
      }
    }
  }

  @Test
  public void testAddAndRemoveAmRecordUpdatesZooKeeper() throws Exception {
    TezConfiguration conf = createTezConf();

    // Use a separate ZkConfig/Curator to inspect ZooKeeper state
    ZkConfig zkConfig = new ZkConfig(conf);

    try (ZkAMRegistry registry = new ZkAMRegistry("external-id");
         CuratorFramework checkClient = zkConfig.createCuratorFramework()) {
      registry.init(conf);
      registry.start();

      checkClient.start();

      ApplicationId appId = registry.generateNewId();
      AMRecord record = registry.createAmRecord(
          appId, "localhost", "127.0.0.1", 10000, "default-compute");

      // Add record and verify node contents
      registry.add(record);

      String path = zkConfig.getZkNamespace() + "/" + appId.toString();
      byte[] data = checkClient.getData().forPath(path);

      assertNotNull("Data should be written to ZooKeeper for AMRecord", data);
      String json = new String(data, StandardCharsets.UTF_8);
      String expectedJson = AMRegistryUtils.recordToJsonString(record);
      assertEquals("Stored AMRecord JSON should match expected", expectedJson, json);

      // Remove record and ensure node is deleted
      registry.remove(record);
      assertNull("Node should be removed from ZooKeeper after remove()", checkClient.checkExists().forPath(path));
    }
  }

  private TezConfiguration createTezConf() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + zkServer.getPort());
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-namespace");
    return conf;
  }
}

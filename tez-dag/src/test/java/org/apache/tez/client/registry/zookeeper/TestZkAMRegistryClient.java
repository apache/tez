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
package org.apache.tez.client.registry.zookeeper;

import static org.apache.tez.frameworkplugins.FrameworkMode.STANDALONE_ZOOKEEPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClientListener;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.LocalNodeContext;
import org.apache.tez.dag.app.MockDAGAppMaster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link ZkAMRegistryClient}.
 * <p>
 * This test class validates the ZooKeeper-based AM (Application Master) registry and discovery
 * mechanism. It tests that when a DAGAppMaster is started with STANDALONE_ZOOKEEPER framework mode,
 * it properly registers itself to ZooKeeper and can be discovered by a {@link ZkAMRegistryClient}.
 * </p>
 * <p>
 * The tests use an embedded ZooKeeper {@link TestingServer} to avoid external dependencies
 * and ensure test isolation.
 * </p>
 */
public class TestZkAMRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkAMRegistryClient.class);
  private static final File TEST_DIR = new File(System.getProperty("test.build.data", "target"),
      TestZkAMRegistryClient.class.getName()).getAbsoluteFile();

  /**
   * Embedded ZooKeeper server for testing. Uses Apache Curator's {@link TestingServer}
   * to provide an in-memory ZooKeeper instance.
   */
  private static TestingServer zkServer;

  /**
   * ZooKeeper-based AM registry client used to discover and retrieve AM records.
   */
  private ZkAMRegistryClient registryClient;

  /**
   * Mock DAGAppMaster instance that registers itself to the ZooKeeper registry.
   */
  private DAGAppMaster dagAppMaster;

  @BeforeAll
  public static void setup() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();
    LOG.info("Started ZooKeeper test server on port: {}", zkServer.getPort());
  }

  @AfterEach
  public void teardownEach() throws Exception {
    if (dagAppMaster != null) {
      dagAppMaster.stop();
    }
    IOUtils.closeQuietly(registryClient);
  }

  @AfterAll
  public static void teardown() throws Exception {
    IOUtils.closeQuietly(zkServer);
  }

  /**
   * Tests the complete ZooKeeper-based AM registry and discovery flow.
   * <p>
   * This test validates the following workflow:
   * </p>
   * <ol>
   *   <li>Configure Tez with STANDALONE_ZOOKEEPER framework mode</li>
   *   <li>Create and start a {@link ZkAMRegistryClient} with an event listener</li>
   *   <li>Start a {@link MockDAGAppMaster} which registers itself to ZooKeeper</li>
   *   <li>Verify that the registry client's listener is notified of the AM registration</li>
   *   <li>Verify the AM record can be retrieved via {@link ZkAMRegistryClient#getRecord(ApplicationId)}</li>
   *   <li>Verify the AM appears in the list from {@link ZkAMRegistryClient#getAllRecords()}</li>
   *   <li>Validate all expected fields (host, port, applicationId) are correctly set</li>
   * </ol>
   * <p>
   * The test uses a {@link CountDownLatch} to synchronize between the AM registration
   * event and the test assertions, ensuring the AM has fully registered before validation.
   * </p>
   *
   * @throws Exception if any part of the test fails
   */
  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testZkAmRegistryDiscovery() throws Exception {
    TezConfiguration tezConf = getTezConfForZkDiscovery();

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);

    CountDownLatch amRegisteredLatch = new CountDownLatch(1);
    AtomicBoolean amDiscovered = new AtomicBoolean(false);

    // Create and start the ZkAMRegistryClient
    registryClient = ZkAMRegistryClient.getClient(tezConf);
    registryClient.addListener(
        new AMRegistryClientListener() {
          @Override
          public void onAdd(AMRecord amRecord) {
            LOG.info("AM added to registry: {}", amRecord);
            if (amRecord.getApplicationId().equals(appId)) {
              amDiscovered.set(true);
              amRegisteredLatch.countDown();
            }
          }

          @Override
          public void onRemove(AMRecord amRecord) {
            LOG.info("AM removed from registry: {}", amRecord);
          }
        });
    registryClient.start();

    String workingDir = TEST_DIR.toString();
    String[] localDirs = new String[] {TEST_DIR.toString()};
    String[] logDirs = new String[] {TEST_DIR + "/logs"};
    String jobUserName = UserGroupInformation.getCurrentUser().getShortUserName();

    dagAppMaster =
        new MockDAGAppMaster(
            attemptId,
            containerId,
            SystemClock.getInstance(),
            System.currentTimeMillis(),
            true,
            workingDir,
            localDirs,
            logDirs,
            new AtomicBoolean(true),
            false,
            false,
            new Credentials(),
            jobUserName,
            1,
            1,
            new LocalNodeContext("localhost", 0, 0));

    dagAppMaster.init(tezConf);
    dagAppMaster.start();

    // Wait for AM to be registered in ZooKeeper
    boolean registered = amRegisteredLatch.await(30, TimeUnit.SECONDS);
    assertTrue(registered, "AM was not registered in ZooKeeper within timeout");
    assertTrue(amDiscovered.get(), "AM was not discovered by registry client");

    // Verify the AM record is available through the registry client
    AMRecord amRecord = registryClient.getRecord(appId);
    assertNotNull(amRecord, "AM record should be retrievable from registry");
    assertEquals(appId, amRecord.getApplicationId(), "Application ID should match");
    assertNotNull(amRecord.getHostName(), "Host should be set");
    assertTrue(amRecord.getPort() > 0, "Port should be positive");

    // Verify getAllRecords also returns the AM
    List<AMRecord> allRecords = registryClient.getAllRecords();
    assertNotNull(allRecords, "getAllRecords should not return null");
    assertFalse(allRecords.isEmpty(), "getAllRecords should contain at least one record");

    boolean found = false;
    for (AMRecord record : allRecords) {
      if (record.getApplicationId().equals(appId)) {
        found = true;
        break;
      }
    }
    assertTrue(found, "AM record should be in getAllRecords");
  }

  private TezConfiguration getTezConfForZkDiscovery() {
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, STANDALONE_ZOOKEEPER.name());
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + zkServer.getPort());
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, TEST_DIR.toString());
    return tezConf;
  }
}

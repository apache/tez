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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.zookeeper.CreateMode;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link ZkFrameworkClient}.
 * <p>
 * This test class validates the ZooKeeper-based framework client that discovers
 * and communicates with Application Masters through ZooKeeper registry.
 * </p>
 */
public class TestZkFrameworkClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkFrameworkClient.class);
  private static final File TEST_DIR = new File(System.getProperty("test.build.data", "target"),
      TestZkFrameworkClient.class.getName()).getAbsoluteFile();

  private static TestingServer zkServer;
  private ZkFrameworkClient zkFrameworkClient;
  private CuratorFramework curatorClient;

  @BeforeAll
  public static void setup() throws Exception {
    zkServer = new TestingServer(true);
    LOG.info("Started ZooKeeper test server on port: {}", zkServer.getPort());
  }

  @AfterEach
  public void teardownEach() throws Exception {
    if (zkFrameworkClient != null) {
      zkFrameworkClient.close();
    }
    IOUtils.closeQuietly(curatorClient);
  }

  @AfterAll
  public static void teardown() throws Exception {
    IOUtils.closeQuietly(zkServer);
  }

  /**
   * Tests initialization and lifecycle methods of ZkFrameworkClient.
   */
  @Test
  public void testInitAndLifecycle() throws Exception {
    TezConfiguration tezConf = createTezConf();

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);

    assertFalse(zkFrameworkClient.isRunning(), "Client should not be running after init");

    zkFrameworkClient.start();
    assertTrue(zkFrameworkClient.isRunning(), "Client should be running after start");

    zkFrameworkClient.stop();
    assertFalse(zkFrameworkClient.isRunning(), "Client should not be running after stop");
  }

  /**
   * Tests retrieving application report when AM is registered in ZooKeeper.
   */
  @Test
  public void testGetApplicationReportWithRegisteredAM() throws Exception {
    TezConfiguration tezConf = createTezConf();

    // Register a mock AM in ZooKeeper
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    String testHostName = "test-host";
    int testPort = 12345;
    registerMockAM(tezConf, appId, testHostName, testPort);

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    LambdaTestUtils.await(1000, 100, () -> zkFrameworkClient.isZkInitialized());

    ApplicationReport report = zkFrameworkClient.getApplicationReport(appId);

    assertNotNull(report, "Application report should not be null");
    assertEquals(appId, report.getApplicationId(), "Application ID should match");
    assertEquals(testHostName, report.getHost(), "Host should match");
    assertEquals(testPort, report.getRpcPort(), "Port should match");
    assertEquals(testHostName, zkFrameworkClient.getAmHost(), "AM host should be cached");
    assertEquals(testPort, zkFrameworkClient.getAmPort(), "AM port should be cached");
  }

  /**
   * Tests retrieving application report when AM is not found in ZooKeeper.
   */
  @Test
  public void testGetApplicationReportWithMissingAM() throws Exception {
    TezConfiguration tezConf = createTezConf();

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);

    LambdaTestUtils.await(1000, 100, () -> zkFrameworkClient.isZkInitialized());

    ApplicationReport report = zkFrameworkClient.getApplicationReport(appId);

    assertNotNull(report, "Application report should not be null");
    assertEquals(appId, report.getApplicationId(), "Application ID should match");
    assertEquals(FinalApplicationStatus.FAILED, report.getFinalApplicationStatus(), "Final status should be FAILED");
    assertTrue(report.getDiagnostics().contains("AM record not found"), "Diagnostics should mention missing AM");
  }

  /**
   * Tests creating application from AM record.
   */
  @Test
  public void testCreateApplication() throws Exception {
    TezConfiguration tezConf = createTezConf();

    // Register a mock AM in ZooKeeper
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    registerMockAM(tezConf, appId, "test-host", 12345);

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    LambdaTestUtils.await(1000, 100, () -> zkFrameworkClient.isZkInitialized());

    // Need to call getApplicationReport first to populate amRecord
    zkFrameworkClient.getApplicationReport(appId);

    YarnClientApplication clientApp = zkFrameworkClient.createApplication();

    assertNotNull(clientApp, "YarnClientApplication should not be null");
    assertNotNull(clientApp.getApplicationSubmissionContext(), "ApplicationSubmissionContext should not be null");
    assertEquals(appId, clientApp.getApplicationSubmissionContext().getApplicationId(), "Application ID should match");
    assertNotNull(clientApp.getNewApplicationResponse(), "GetNewApplicationResponse should not be null");
    assertEquals(appId, clientApp.getNewApplicationResponse().getApplicationId(),
        "Response application ID should match");
  }

  /**
   * Tests kill application method.
   */
  @Test
  public void testKillApplication() throws Exception {
    TezConfiguration tezConf = createTezConf();

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    // Give time for ZK registry to initialize
    Thread.sleep(500);

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);

    // Should not throw exception
    zkFrameworkClient.killApplication(appId);
  }

  private TezConfiguration createTezConf() {
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, "STANDALONE_ZOOKEEPER");
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + zkServer.getPort());
    tezConf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/tez-test-" + System.currentTimeMillis());
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, TEST_DIR.toString());
    return tezConf;
  }

  private void registerMockAM(TezConfiguration tezConf, ApplicationId appId, String hostName, int port)
      throws Exception {
    // Create AM record and publish it directly to ZooKeeper
    AMRecord amRecord = new AMRecord(appId, hostName, "127.0.0.1", port, "test-external-id", "test-compute");
    ServiceRecord serviceRecord = amRecord.toServiceRecord();

    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal();
    String json = marshal.toJson(serviceRecord);

    // Use Curator to write directly to ZooKeeper
    ZkConfig zkConfig = new ZkConfig(tezConf);
    curatorClient = zkConfig.createCuratorFramework();
    curatorClient.start();

    // Wait for connection
    curatorClient.blockUntilConnected();

    String namespace = zkConfig.getZkNamespace();
    String path = namespace + "/" + appId.toString();

    // Create parent directories if needed
    curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
        .forPath(path, json.getBytes(StandardCharsets.UTF_8));

    LOG.info("Registered mock AM to ZK path: {}", path);
  }
}

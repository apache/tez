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

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.zookeeper.CreateMode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

  private TestingServer zkServer;
  private ZkFrameworkClient zkFrameworkClient;
  private CuratorFramework curatorClient;

  @Before
  public void setup() throws Exception {
    zkServer = new TestingServer(true);
    LOG.info("Started ZooKeeper test server on port: {}", zkServer.getPort());
  }

  @After
  public void teardown() throws Exception {
    if (zkFrameworkClient != null) {
      zkFrameworkClient.close();
    }
    IOUtils.closeQuietly(curatorClient);
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

    assertTrue("Client should be running after init", zkFrameworkClient.isRunning());

    zkFrameworkClient.start();
    assertTrue("Client should be running after start", zkFrameworkClient.isRunning());

    zkFrameworkClient.stop();
    assertFalse("Client should not be running after stop", zkFrameworkClient.isRunning());
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
    String testHostIp = "127.0.0.1";
    int testPort = 12345;
    registerMockAM(tezConf, appId, testHostName, testHostIp, testPort);

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    // Give time for ZK registry to initialize
    Thread.sleep(500);

    ApplicationReport report = zkFrameworkClient.getApplicationReport(appId);

    assertNotNull("Application report should not be null", report);
    assertEquals("Application ID should match", appId, report.getApplicationId());
    assertEquals("Host should match", testHostName, report.getHost());
    assertEquals("Port should match", testPort, report.getRpcPort());
    assertEquals("Application state should be RUNNING", YarnApplicationState.RUNNING,
        report.getYarnApplicationState());
    assertEquals("AM host should be cached", testHostName, zkFrameworkClient.getAmHost());
    assertEquals("AM port should be cached", testPort, zkFrameworkClient.getAmPort());
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

    // Give time for ZK registry to initialize
    Thread.sleep(500);

    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationReport report = zkFrameworkClient.getApplicationReport(appId);

    assertNotNull("Application report should not be null", report);
    assertEquals("Application ID should match", appId, report.getApplicationId());
    assertEquals("Application state should be FINISHED", YarnApplicationState.FINISHED,
        report.getYarnApplicationState());
    assertEquals("Final status should be FAILED", FinalApplicationStatus.FAILED,
        report.getFinalApplicationStatus());
    assertTrue("Diagnostics should mention missing AM",
        report.getDiagnostics().contains("AM record not found"));
  }

  /**
   * Tests creating application from AM record.
   */
  @Test
  public void testCreateApplication() throws Exception {
    TezConfiguration tezConf = createTezConf();

    // Register a mock AM in ZooKeeper
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    registerMockAM(tezConf, appId, "test-host", "127.0.0.1", 12345);

    zkFrameworkClient = new ZkFrameworkClient();
    zkFrameworkClient.init(tezConf);
    zkFrameworkClient.start();

    // Give time for ZK registry to initialize
    Thread.sleep(500);

    // Need to call getApplicationReport first to populate amRecord
    zkFrameworkClient.getApplicationReport(appId);

    YarnClientApplication clientApp = zkFrameworkClient.createApplication();

    assertNotNull("YarnClientApplication should not be null", clientApp);
    assertNotNull("ApplicationSubmissionContext should not be null", clientApp.getApplicationSubmissionContext());
    assertEquals("Application ID should match", appId, clientApp.getApplicationSubmissionContext().getApplicationId());
    assertNotNull("GetNewApplicationResponse should not be null", clientApp.getNewApplicationResponse());
    assertEquals("Response application ID should match",
        appId, clientApp.getNewApplicationResponse().getApplicationId());
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

  private void registerMockAM(TezConfiguration tezConf, ApplicationId appId, String hostName, String hostIp, int port) throws Exception {
    // Create AM record and publish it directly to ZooKeeper
    AMRecord amRecord = new AMRecord(appId, hostName, hostIp, port, "test-external-id", "test-compute");
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

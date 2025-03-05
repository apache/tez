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
package org.apache.tez.test;

import static org.apache.tez.test.TestSecureShuffle.generateCertificate;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistry;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestZkAMRegistryClient {

  private static final String KEYSTORE_PASSWORD = "secret";
  private static final String TRUSTSTORE_PASSWORD = "changeit";
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
          + TestZkAMRegistryClient.class.getName() + "-tmpDir";
  private static File keysStoresDir = new File(TEST_ROOT_DIR, "keystores");
  private static String serverKS;
  private static String trustKS;

  private static TestingServer zkServer;
  private static Integer clientPort;
  private static Integer secureClientPort;

  @BeforeAll
  public static void setupZookeeperTestServer() throws Exception {
    clientPort = InstanceSpec.getRandomPort();
    secureClientPort = InstanceSpec.getRandomPort();

    setupKeyStores();

    Map<String, Object> customProperties = ImmutableMap.of(
      // NettyServerCnxnFactory required for SSL/TLS support
      "serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory",
      // secureClientPort opens a new port for secure connections
      "secureClientPort", Integer.toString(secureClientPort),
      "ssl.clientAuth", "none",
      "ssl.keyStore.location", serverKS,
      "ssl.keyStore.password", KEYSTORE_PASSWORD,
      "ssl.trustStore.location", trustKS,
      "ssl.trustStore.password", TRUSTSTORE_PASSWORD,
      "ssl.keyStore.type", "JKS",
      "ssl.trustStore.type", "JKS"
    );

    // the clientPort parameter causes an insecure port to be opened
    InstanceSpec spec = new InstanceSpec(null, clientPort, -1, -1, true, 1, -1, -1, customProperties);
    zkServer = new TestingServer(spec, true);
  }

  @AfterAll
  public static void shutdownZookeeperTestServer() throws IOException {
    zkServer.stop();
  }

  public void enableZookeeperSecureClientWithJVMProperties() {
    System.setProperty("zookeeper.client.secure", "true");
    System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
  }

  @AfterEach
  public void clearZookeeperSecureClientJVMProperties() {
    System.clearProperty("zookeeper.client.secure");
    System.clearProperty("zookeeper.clientCnxnSocket");
  }

  @Test
  @Timeout(30)
  public void testZkAMRegistryClient() throws Exception {
    // configure zookeeper connection to use the insecure client port
    Configuration conf = new Configuration();
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-am-registry");
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + clientPort);

    runAmRecordTestWithConfiguration(conf);
  }

  @Test
  @Timeout(30)
  public void testZkAMRegistryClientWithSecureClientJVMProperties() throws Exception {
    // this affects all zookeeper clients in JVM
    enableZookeeperSecureClientWithJVMProperties();

    // configure zookeeper connection to use the secure client port
    Configuration conf = new Configuration();
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-am-registry-with-secure-client-jvm-properties");
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + secureClientPort);

    runAmRecordTestWithConfiguration(conf);
  }

  @Test
  @Timeout(30)
  public void testZkAMRegistryClientWithSecureZookeeperPort() throws Exception {
    // configure zookeeper connection to use the secure client port without JVM properties
    Configuration conf = new Configuration();
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-am-registry-with-secure-connection");
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + secureClientPort);
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_SSL_ENABLE, "true");
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION, trustKS);
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD);

    runAmRecordTestWithConfiguration(conf);
  }

  @Test
  @Timeout(30)
  public void testZkAMRegistryClientWithInsecureZookeeperPort() throws Exception {
    // this affects all zookeeper clients in JVM
    enableZookeeperSecureClientWithJVMProperties();

    // override the JVM properties above and configure zookeeper connection
    // to use the insecure client port
    Configuration conf = new Configuration();
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-am-registry-with-insecure-connection");
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:" + clientPort);
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_SSL_ENABLE, "false");

    runAmRecordTestWithConfiguration(conf);
  }

  private void runAmRecordTestWithConfiguration(Configuration conf) throws Exception {
    String zkAMRegistryId = "testRegistry" + System.currentTimeMillis();
    try (ZkAMRegistry registry = new ZkAMRegistry(zkAMRegistryId)) {
      registry.init(conf);
      registry.start();

      ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
      AMRecord amRecordRegistered = new AMRecord(appId, "hostName", "testHostIp", 1234, "testExternalId", "testComputeName");
      registry.add(amRecordRegistered);

      ZkAMRegistryClient registryClient = ZkAMRegistryClient.getClient(conf);
      registryClient.start();

      // information registered in registry eventually reaches the registry client
      AMRecord amRecordFetched = registryClient.getRecord(appId);
      while (amRecordFetched == null) {
        Thread.sleep(1000);
        amRecordFetched = registryClient.getRecord(appId);
      }
      assertEquals(amRecordFetched, amRecordRegistered);

      registryClient.close();
    }
  }

  /**
   * Create keystore and truststore for the tests
   *
   * @throws Exception
   */
  private static void setupKeyStores() throws Exception {
    keysStoresDir.mkdirs();
    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();

    String localhostName = InetAddress.getLocalHost().getHostName();
    KeyPair sKP = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate sCert =
            generateCertificate("CN="+localhostName+", O=server", sKP, 30, "SHA256WITHRSA");
    serverKS = keysStoresDir.getAbsolutePath() + "/serverKS.jks";
    KeyStoreTestUtil.createKeyStore(serverKS, KEYSTORE_PASSWORD, "server", sKP.getPrivate(), sCert);
    certs.put("server", sCert);
    trustKS = keysStoresDir.getAbsolutePath() + "/trustKS.jks";
    KeyStoreTestUtil.createTrustStore(trustKS, TRUSTSTORE_PASSWORD, certs);
  }

}

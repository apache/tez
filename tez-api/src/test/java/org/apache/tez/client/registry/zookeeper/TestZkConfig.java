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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import org.junit.Test;

public class TestZkConfig {

  @Test
  public void testBasicConfiguration() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-namespace");

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals("localhost:2181", zkConfig.getZkQuorum());
    assertEquals("/tez-external-sessions/test-namespace", zkConfig.getZkNamespace());
  }

  @Test
  public void testDefaultValues() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");

    ZkConfig zkConfig = new ZkConfig(conf);

    Configuration defaultConf = new Configuration();
    long expectedBackoffSleep = defaultConf.getTimeDuration(
        TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP,
        TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP_DEFAULT, TimeUnit.MILLISECONDS);
    long expectedSessionTimeout = defaultConf.getTimeDuration(
        TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    long expectedConnectionTimeout = defaultConf.getTimeDuration(
        TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);

    assertEquals(expectedBackoffSleep, zkConfig.getCuratorBackoffSleepMs());
    assertEquals(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES_DEFAULT, zkConfig.getCuratorMaxRetries());
    assertEquals(expectedSessionTimeout, zkConfig.getSessionTimeoutMs());
    assertEquals(expectedConnectionTimeout, zkConfig.getConnectionTimeoutMs());
  }

  @Test
  public void testCustomConfigurationValues() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "zk1:2181,zk2:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/custom-namespace");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP, "2000ms");
    conf.setInt(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES, 5);
    conf.set(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT, "200000ms");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT, "20000ms");

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals("zk1:2181,zk2:2181", zkConfig.getZkQuorum());
    assertEquals("/tez-external-sessions/custom-namespace", zkConfig.getZkNamespace());
    assertEquals(2000, zkConfig.getCuratorBackoffSleepMs());
    assertEquals(5, zkConfig.getCuratorMaxRetries());
    assertEquals(200000, zkConfig.getSessionTimeoutMs());
    assertEquals(20000, zkConfig.getConnectionTimeoutMs());
  }

  @Test
  public void testNamespaceWithLeadingSlash() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/namespace-with-slash");

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals("/tez-external-sessions/namespace-with-slash", zkConfig.getZkNamespace());
  }

  @Test
  public void testNamespaceWithoutLeadingSlash() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "namespace-without-slash");

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals("/tez-external-sessions/namespace-without-slash", zkConfig.getZkNamespace());
  }

  @Test
  public void testComputeGroupsDisabled() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-namespace");
    conf.setBoolean(TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS, false);

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals("/tez-external-sessions/test-namespace", zkConfig.getZkNamespace());
  }

  @Test
  public void testComputeGroupsEnabledWithoutEnvVar() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "/test-namespace");
    conf.setBoolean(TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS, true);

    // When compute groups are enabled but env var is not set, namespace should not include sub-namespace
    ZkConfig zkConfig = new ZkConfig(conf);

    // Namespace should start with base namespace (env var not set, so no sub-namespace added)
    assertEquals("/tez-external-sessions/test-namespace", zkConfig.getZkNamespace());
  }

  @Test
  public void testGetRetryPolicy() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP, "1500ms");
    conf.setInt(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES, 4);

    ZkConfig zkConfig = new ZkConfig(conf);
    RetryPolicy retryPolicy = zkConfig.getRetryPolicy();

    assertNotNull(retryPolicy);
    // Verify it's an ExponentialBackoffRetry instance
    assertEquals("org.apache.curator.retry.ExponentialBackoffRetry", retryPolicy.getClass().getName());
  }

  @Test
  public void testTimeUnitSupport() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    // Test different time units
    conf.set(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP, "2s");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT, "3m");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT, "5s");

    ZkConfig zkConfig = new ZkConfig(conf);

    assertEquals(2000, zkConfig.getCuratorBackoffSleepMs());
    assertEquals(180000, zkConfig.getSessionTimeoutMs());
    assertEquals(5000, zkConfig.getConnectionTimeoutMs());

    // Unit-less values should default to milliseconds
    conf.set(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP, "2000");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT, "300000");
    conf.set(TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT, "15000");

    ZkConfig unitlessConfig = new ZkConfig(conf);
    assertEquals(2000, unitlessConfig.getCuratorBackoffSleepMs());
    assertEquals(300000, unitlessConfig.getSessionTimeoutMs());
    assertEquals(15000, unitlessConfig.getConnectionTimeoutMs());
  }

  @Test
  public void testCreateCuratorFramework() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");

    ZkConfig zkConfig = new ZkConfig(conf);
    CuratorFramework curator = zkConfig.createCuratorFramework();

    assertNotNull(curator);
    assertEquals(zkConfig.getZkQuorum(), curator.getZookeeperClient().getCurrentConnectionString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullZkQuorum() {
    TezConfiguration conf = new TezConfiguration();
    // Don't set zkQuorum
    new ZkConfig(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyZkQuorum() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "");
    new ZkConfig(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullNamespace() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, null);
    new ZkConfig(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyNamespace() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    conf.set(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE, "");
    new ZkConfig(conf);
  }

  @Test
  public void testDefaultNamespace() {
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    // Don't set namespace, should use default
    ZkConfig zkConfig = new ZkConfig(conf);
    assertEquals("/tez-external-sessions" + TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE_DEFAULT,
        zkConfig.getZkNamespace());
  }
}

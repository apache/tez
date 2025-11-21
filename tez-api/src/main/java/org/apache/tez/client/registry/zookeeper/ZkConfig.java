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

package org.apache.tez.client.registry.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkConfig {
  private static final Logger LOG = LoggerFactory.getLogger(ZkConfig.class);

  // if namespace defined in config is 'foo' and COMPUTE_GROUP_NAME env is 'bar' then the zkpaths will be of format
  // /tez-external-sessions/foo/bar
  private final static String ZK_NAMESPACE_PREFIX = "/tez-external-sessions";
  public final static String COMPUTE_GROUP_NAME_ENV = "COMPUTE_GROUP_NAME";
  public final static String DEFAULT_COMPUTE_GROUP_NAME = "default-compute";

  private final String zkQuorum;
  private final String zkNamespace;
  private final int curatorBackoffSleepMs;
  private final int curatorMaxRetries;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;

  public ZkConfig(Configuration conf) {
    zkQuorum = conf.get(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(zkQuorum), "zkQuorum cannot be null or empty");

    String fullZkNamespace = ZK_NAMESPACE_PREFIX;

    String namespace = conf.get(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE,
        TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE_DEFAULT);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "namespace cannot be null or empty");

    fullZkNamespace = appendNamespace(fullZkNamespace, namespace);

    boolean enableComputeGroups = conf.getBoolean(TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS,
        TezConfiguration.TEZ_AM_REGISTRY_ENABLE_COMPUTE_GROUPS_DEFAULT);
    if (enableComputeGroups) {
      final String subNamespace = System.getenv(COMPUTE_GROUP_NAME_ENV);
      if (subNamespace != null && !subNamespace.isEmpty()) {
        fullZkNamespace = appendNamespace(fullZkNamespace, subNamespace);
        LOG.info("Compute groups enabled: subNamespace: {} fullZkNamespace: {}", subNamespace, fullZkNamespace);
      }
    } else {
      LOG.info("Compute groups disabled: fullZkNamespace: {}", fullZkNamespace);
    }
    zkNamespace = fullZkNamespace;
    LOG.info("Using ZK namespace: {}", fullZkNamespace);

    curatorBackoffSleepMs = Math.toIntExact(conf.getTimeDuration(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP,
        TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP_DEFAULT, TimeUnit.MILLISECONDS));
    curatorMaxRetries = conf.getInt(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES,
        TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES_DEFAULT);
    sessionTimeoutMs = Math.toIntExact(conf.getTimeDuration(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS));
    connectionTimeoutMs = Math.toIntExact(conf.getTimeDuration(TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS));
  }

  public String getZkQuorum() {
    return zkQuorum;
  }

  public String getZkNamespace() {
    return zkNamespace;
  }

  public int getCuratorBackoffSleepMs() {
    return curatorBackoffSleepMs;
  }

  public int getCuratorMaxRetries() {
    return curatorMaxRetries;
  }

  public int getSessionTimeoutMs() {
    return sessionTimeoutMs;
  }

  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public RetryPolicy getRetryPolicy() {
    return new ExponentialBackoffRetry(getCuratorBackoffSleepMs(), getCuratorMaxRetries());
  }

  public CuratorFramework createCuratorFramework() {
    return CuratorFrameworkFactory.newClient(
        getZkQuorum(),
        getSessionTimeoutMs(),
        getConnectionTimeoutMs(),
        getRetryPolicy()
    );
  }

  /**
   * Appends a namespace to the given prefix, inserting a path separator between
   * them if necessary.
   *
   * @param prefix    the initial path prefix to which the namespace is appended; must not be null
   * @param namespace the namespace segment to append; must not be null
   * @return the concatenation of {@code prefix} and {@code namespace} with a separator inserted if needed
   */
  private String appendNamespace(String prefix, String namespace) {
    boolean hasSlash = namespace.startsWith(Path.SEPARATOR);
    return prefix + (hasSlash ? namespace : Path.SEPARATOR + namespace);
  }
}

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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Preconditions;

public class ZkConfig {

  private String zkQuorum;
  private String zkNamespace;
  private int curatorBackoffSleep;
  private int curatorMaxRetries;
  private int sessionTimeoutMs;
  private int connectionTimeoutMs;

  public ZkConfig(Configuration conf) {
    zkQuorum = conf.get(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM);
    Preconditions.checkNotNull(zkQuorum);
    zkNamespace = conf.get(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE,
        TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE_DEFAULT);
    Preconditions.checkNotNull(zkNamespace);
    curatorBackoffSleep = conf.getInt(TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP,
        TezConfiguration.TEZ_AM_CURATOR_BACKOFF_SLEEP_DEFAULT);
    curatorMaxRetries = conf.getInt(TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES,
        TezConfiguration.TEZ_AM_CURATOR_MAX_RETRIES_DEFAULT);
    sessionTimeoutMs = conf.getInt(TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_SESSION_TIMEOUT_DEFAULT);
    connectionTimeoutMs = conf.getInt(TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT,
        TezConfiguration.TEZ_AM_CURATOR_CONNECTION_TIMEOUT_DEFAULT);
  }

  public String getZkQuorum() {
    return zkQuorum;
  }

  public String getZkNamespace() {
    return zkNamespace;
  }

  public int getCuratorBackoffSleep() {
    return curatorBackoffSleep;
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
    return new ExponentialBackoffRetry(getCuratorBackoffSleep(), getCuratorMaxRetries());
  }

  public CuratorFramework createCuratorFramework() {
    return CuratorFrameworkFactory.newClient(
        getZkQuorum(),
        getSessionTimeoutMs(),
        getConnectionTimeoutMs(),
        getRetryPolicy()
    );
  }
}

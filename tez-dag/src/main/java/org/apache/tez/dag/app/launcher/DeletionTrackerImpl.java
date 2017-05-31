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

package org.apache.tez.dag.app.launcher;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeletionTrackerImpl extends DeletionTracker {
  private static final Logger LOG = LoggerFactory.getLogger(DeletionTrackerImpl.class);
  private Map<NodeId, Integer> nodeIdShufflePortMap = new HashMap<NodeId, Integer>();
  private ExecutorService dagCleanupService;

  public DeletionTrackerImpl(Configuration conf) {
    super(conf);
    this.dagCleanupService = new ThreadPoolExecutor(0, conf.getInt(TezConfiguration.TEZ_AM_DAG_CLEANUP_THREAD_COUNT_LIMIT,
        TezConfiguration.TEZ_AM_DAG_CLEANUP_THREAD_COUNT_LIMIT_DEFAULT), 10,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ShuffleDeleteTracker #%d").build());
  }

  @Override
  public void dagComplete(TezDAGID dag, JobTokenSecretManager jobTokenSecretManager) {
    super.dagComplete(dag, jobTokenSecretManager);
    for (Map.Entry<NodeId, Integer> entry : nodeIdShufflePortMap.entrySet()) {
      NodeId nodeId = entry.getKey();
      int shufflePort = entry.getValue();
      //TODO: add check for healthy node
      if (shufflePort != TezRuntimeUtils.INVALID_PORT) {
        DagDeleteRunnable dagDeleteRunnable = new DagDeleteRunnable(nodeId, shufflePort, dag,
            TezRuntimeUtils.getHttpConnectionParams(conf), jobTokenSecretManager);
        try {
          dagCleanupService.submit(dagDeleteRunnable);
        } catch (RejectedExecutionException rejectedException) {
          LOG.info("Ignoring deletion request for " + dagDeleteRunnable);
        }
      }
    }
  }

  @Override
  public void addNodeShufflePort(NodeId nodeId, int port) {
    if (port != TezRuntimeUtils.INVALID_PORT) {
      if(nodeIdShufflePortMap.get(nodeId) == null) {
        nodeIdShufflePortMap.put(nodeId, port);
      }
    }
  }

  @VisibleForTesting
  Map<NodeId, Integer> getNodeIdShufflePortMap() {
    return nodeIdShufflePortMap;
  }

  @Override
  public void shutdown() {
    if (dagCleanupService != null) {
      dagCleanupService.shutdownNow();
      dagCleanupService = null;
    }
    nodeIdShufflePortMap = null;
  }
}

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
package org.apache.tez.dag.app.launcher;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.DagContainerLauncher;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A No-Op Container Launcher where TezChild processes are started externally (e.g., via Docker
 * Compose or Kubernetes).
 */
public class NoOpContainerLauncher extends DagContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpContainerLauncher.class);
  private DeletionTracker deletionTracker = null;
  private Configuration conf;

  public NoOpContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    super(containerLauncherContext);
    try {
      this.conf = TezUtils.createConfFromUserPayload(containerLauncherContext.getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException("Failed to parse user payload", e);
    }
  }

  @Override
  public void start() {
    boolean isTezShuffle = org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.isTezShuffleHandler(conf);
    boolean dagDelete = isTezShuffle && conf.getBoolean(TezConfiguration.TEZ_AM_DAG_CLEANUP_ON_COMPLETION,
        TezConfiguration.TEZ_AM_DAG_CLEANUP_ON_COMPLETION_DEFAULT);
    boolean vertexDelete = isTezShuffle && conf.getInt(TezConfiguration.TEZ_AM_VERTEX_CLEANUP_HEIGHT,
        TezConfiguration.TEZ_AM_VERTEX_CLEANUP_HEIGHT_DEFAULT) > 0;
    boolean failedTaskAttemptDelete =
        isTezShuffle && conf.getBoolean(TezConfiguration.TEZ_AM_TASK_ATTEMPT_CLEANUP_ON_FAILURE,
            TezConfiguration.TEZ_AM_TASK_ATTEMPT_CLEANUP_ON_FAILURE_DEFAULT);

    if (dagDelete || vertexDelete || failedTaskAttemptDelete) {
      String deletionTrackerClassName = conf.get(TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS,
          TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS_DEFAULT);
      try {
        deletionTracker = ReflectionUtils.createClazzInstance(deletionTrackerClassName,
            new Class[]{Configuration.class}, new Object[]{conf});
      } catch (org.apache.tez.dag.api.TezReflectionException e) {
        throw new TezUncheckedException("Failed to instantiate DeletionTracker", e);
      }
    }
  }

  @Override
  public void shutdown() {
    if (deletionTracker != null) {
      deletionTracker.shutdown();
    }
  }

  /** This method is no-op it's just informing AM to change container state */
  @Override
  public void launchContainer(ContainerLaunchRequest launchRequest) {
    LOG.info("Container launch is externally managed for container: {}", launchRequest.getContainerId());

    if (deletionTracker != null) {
      int shufflePort = launchRequest.getNodeId().getPort();
      deletionTracker.addNodeShufflePort(launchRequest.getNodeId(), shufflePort);
    }

    // Immediately tell AM that the container is "launched"
    getContext().containerLaunched(launchRequest.getContainerId());
  }

  /** This method is no-op it's just informing AM to change container state */
  @Override
  public void stopContainer(ContainerStopRequest stopRequest) {
    LOG.info("Container stop is externally managed for container: {}", stopRequest.getContainerId());
    getContext().containerStopRequested(stopRequest.getContainerId());
  }

  @Override
  public void dagComplete(TezDAGID dag, JobTokenSecretManager jobTokenSecretManager) {
    if (deletionTracker != null) {
      deletionTracker.dagComplete(dag, jobTokenSecretManager);
    }
  }

  @Override
  public void vertexComplete(TezVertexID vertex, JobTokenSecretManager jobTokenSecretManager, Set<NodeId> nodeIdList) {
    if (deletionTracker != null) {
      deletionTracker.vertexComplete(vertex, jobTokenSecretManager, nodeIdList);
    }
  }

  @Override
  public void taskAttemptFailed(TezTaskAttemptID taskAttemptID, JobTokenSecretManager jobTokenSecretManager, NodeId nodeId) {
    if (deletionTracker != null) {
      deletionTracker.taskAttemptFailed(taskAttemptID, jobTokenSecretManager, nodeId);
    }
  }
}

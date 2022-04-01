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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

class TaskAttemptFailedRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptFailedRunnable.class);
  private final NodeId nodeId;
  private final TezTaskAttemptID taskAttemptID;
  private final JobTokenSecretManager jobTokenSecretManager;
  private final int shufflePort;
  private final HttpConnectionParams httpConnectionParams;

  TaskAttemptFailedRunnable(NodeId nodeId, int shufflePort, TezTaskAttemptID taskAttemptID,
                           HttpConnectionParams httpConnectionParams,
                           JobTokenSecretManager jobTokenSecretMgr) {
    this.nodeId = nodeId;
    this.shufflePort = shufflePort;
    this.taskAttemptID = taskAttemptID;
    this.httpConnectionParams = httpConnectionParams;
    this.jobTokenSecretManager = jobTokenSecretMgr;
  }

  @Override
  public void run() {
    BaseHttpConnection httpConnection = null;
    try {
      URL baseURL = TezRuntimeUtils.constructBaseURIForShuffleHandlerTaskAttemptFailed(
          nodeId.getHost(), shufflePort, taskAttemptID.getTaskID().getVertexID().getDAGId().
              getApplicationId().toString(), taskAttemptID.getTaskID().getVertexID().getDAGId().getId(),
          taskAttemptID.toString(), httpConnectionParams.isSslShuffle());
      httpConnection = TezRuntimeUtils.getHttpConnection(true, baseURL, httpConnectionParams,
          "FailedTaskAttemptDelete", jobTokenSecretManager);
      httpConnection.connect();
      httpConnection.getInputStream();
    } catch (Exception e) {
      LOG.warn("Could not setup HTTP Connection to the node " + nodeId.getHost() +
          " for failed task attempt delete. ", e);
    } finally {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(true);
        }
      } catch (IOException ioe) {
        LOG.warn("Encountered IOException for " + nodeId.getHost() + " during close. ", ioe);
      }
    }
  }

  @Override
  public String toString() {
    return "TaskAttemptFailedRunnable nodeId=" + nodeId + ", shufflePort=" + shufflePort + ", taskAttemptId=" +
        taskAttemptID.toString();
  }
}

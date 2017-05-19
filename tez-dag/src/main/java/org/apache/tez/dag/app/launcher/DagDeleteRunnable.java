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
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

class DagDeleteRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DagDeleteRunnable.class);
  final NodeId nodeId;
  final TezDAGID dag;
  final JobTokenSecretManager jobTokenSecretManager;
  final int shufflePort;
  final HttpConnectionParams httpConnectionParams;

  public DagDeleteRunnable(NodeId nodeId, int shufflePort, TezDAGID currentDag,
                           HttpConnectionParams httpConnectionParams,
                           JobTokenSecretManager jobTokenSecretMgr) {
    this.nodeId = nodeId;
    this.shufflePort = shufflePort;
    this.dag = currentDag;
    this.httpConnectionParams = httpConnectionParams;
    this.jobTokenSecretManager = jobTokenSecretMgr;
  }

  @Override
  public void run() {
    BaseHttpConnection httpConnection = null;
    try {
      URL baseURL = TezRuntimeUtils.constructBaseURIForShuffleHandlerDagComplete(
          nodeId.getHost(), shufflePort,
          dag.getApplicationId().toString(), dag.getId(), false);
      httpConnection = TezRuntimeUtils.getHttpConnection(true, baseURL, httpConnectionParams,
          "DAGDelete", jobTokenSecretManager);
      httpConnection.connect();
      httpConnection.getInputStream();
    } catch (Exception e) {
      LOG.warn("Could not setup HTTP Connection to the node " + nodeId.getHost() + " for dag delete. ", e);
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
    return "DagDeleteRunnable nodeId=" + nodeId + ", shufflePort=" + shufflePort + ", dagId=" + dag.toString();
  }
}

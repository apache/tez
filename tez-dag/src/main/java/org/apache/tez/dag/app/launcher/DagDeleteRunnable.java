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
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;

import java.net.URL;

class DagDeleteRunnable implements Runnable {
  final NodeId nodeId;
  final DAG dag;
  final JobTokenSecretManager jobTokenSecretManager;
  final String tezDefaultComponentName;
  final int shufflePort;

  public DagDeleteRunnable(NodeId nodeId, int shufflePort, DAG currentDag,
                           JobTokenSecretManager jobTokenSecretMgr, String tezDefaultComponent) {
    this.nodeId = nodeId;
    this.shufflePort = shufflePort;
    this.dag = currentDag;
    this.jobTokenSecretManager = jobTokenSecretMgr;
    this.tezDefaultComponentName = tezDefaultComponent;
  }

  @Override
  public void run() {
    try {
      URL baseURL = ShuffleUtils.constructBaseURIForShuffleHandlerDagComplete(
          nodeId.getHost(), shufflePort,
          dag.getID().getApplicationId().toString(), dag.getID().getId(), false);
      BaseHttpConnection httpConnection = ShuffleUtils.getHttpConnection(true, baseURL,
          ShuffleUtils.getHttpConnectionParams(dag.getConf()), "DAGDelete", jobTokenSecretManager);
      httpConnection.connect();
      httpConnection.getInputStream();
    } catch (Exception e) {
      TezContainerLauncherImpl.LOG.warn("Could not setup HTTP Connection to the node " + nodeId.getHost() + " for dag delete "
          + e);
    }
  }
}

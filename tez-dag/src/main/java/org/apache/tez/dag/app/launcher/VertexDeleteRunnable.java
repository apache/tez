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

package org.apache.tez.dag.app.launcher;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

public class VertexDeleteRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(VertexDeleteRunnable.class);
  final private TezVertexID vertex;
  final private JobTokenSecretManager jobTokenSecretManager;
  final private NodeId nodeId;
  final private int shufflePort;
  final private String vertexId;
  final private HttpConnectionParams httpConnectionParams;

  VertexDeleteRunnable(TezVertexID vertex, JobTokenSecretManager jobTokenSecretManager,
                       NodeId nodeId, int shufflePort, String vertexId,
                       HttpConnectionParams httpConnectionParams) {
    this.vertex = vertex;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.nodeId = nodeId;
    this.shufflePort = shufflePort;
    this.vertexId = vertexId;
    this.httpConnectionParams = httpConnectionParams;
  }

  @Override
  public void run() {
    BaseHttpConnection httpConnection = null;
    try {
      URL baseURL = TezRuntimeUtils.constructBaseURIForShuffleHandlerVertexComplete(
          nodeId.getHost(), shufflePort,
          vertex.getDAGID().getApplicationId().toString(), vertex.getDAGID().getId(), vertexId,
          httpConnectionParams.isSslShuffle());
      httpConnection = TezRuntimeUtils.getHttpConnection(true, baseURL, httpConnectionParams,
          "VertexDelete", jobTokenSecretManager);
      httpConnection.connect();
      httpConnection.getInputStream();
    } catch (Exception e) {
      LOG.warn("Could not setup HTTP Connection to the node %s " + nodeId.getHost() +
          " for vertex shuffle delete. ", e);
    } finally {
      try {
        if (httpConnection != null) {
          httpConnection.cleanup(true);
        }
      } catch (IOException e) {
        LOG.warn("Encountered IOException for " + nodeId.getHost() + " during close. ", e);
      }
    }
  }

  @Override
  public String toString() {
    return "VertexDeleteRunnable nodeId=" + nodeId + ", shufflePort=" + shufflePort + ", vertexId=" + vertexId;
  }
}

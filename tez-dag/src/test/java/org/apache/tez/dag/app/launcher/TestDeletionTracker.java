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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.Assert;
import org.junit.Test;

public class TestDeletionTracker {

  @Test
  public void testNodeIdShufflePortMap() throws Exception {
    DeletionTrackerImpl deletionTracker = new DeletionTrackerImpl(new Configuration());
    // test NodeId
    NodeId nodeId = new NodeId() {
      @Override
      public String getHost() {
        return "testHost";
      }

      @Override
      protected void setHost(String s) {

      }

      @Override
      public int getPort() {
        return 1234;
      }

      @Override
      protected void setPort(int i) {

      }

      @Override
      protected void build() {

      }
    };
    // test shuffle port for the nodeId
    int shufflePort = 9999;
    deletionTracker.addNodeShufflePort(nodeId, shufflePort);
    Assert.assertEquals("Unexpected number of entries in NodeIdShufflePortMap!",
        1, deletionTracker.getNodeIdShufflePortMap().size());
    deletionTracker.addNodeShufflePort(nodeId, shufflePort);
    Assert.assertEquals("Unexpected number of entries in NodeIdShufflePortMap!",
        1, deletionTracker.getNodeIdShufflePortMap().size());
    deletionTracker.dagComplete(new TezDAGID(), new JobTokenSecretManager());
    Assert.assertEquals("Unexpected number of entries in NodeIdShufflePortMap after dagComplete!",
        1, deletionTracker.getNodeIdShufflePortMap().size());
  }
}

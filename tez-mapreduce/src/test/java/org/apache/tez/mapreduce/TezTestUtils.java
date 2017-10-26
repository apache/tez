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
package org.apache.tez.mapreduce;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.InputInitializerContext;

import java.io.IOException;
import java.util.Set;

public class TezTestUtils {

  public static TezTaskAttemptID getMockTaskAttemptId(
      int jobId, int vertexId, int taskId, int taskAttemptId) {
    return TezTaskAttemptID.getInstance(
        TezTaskID.getInstance(
            TezVertexID.getInstance(
                TezDAGID.getInstance(
                    ApplicationId.newInstance(0, jobId), jobId),
                    vertexId),
                    taskId)
        , taskAttemptId);
  }
  
  public static TezTaskID getMockTaskId(int jobId,
      int vertexId, int taskId) {
    return TezTaskID.getInstance(
        TezVertexID.getInstance(TezDAGID.getInstance(
            ApplicationId.newInstance(0, jobId),
            jobId), vertexId),
            taskId);
  }

  public static class TezRootInputInitializerContextForTest implements
      InputInitializerContext {

    private final ApplicationId appId;
    private final UserPayload payload;

    public TezRootInputInitializerContextForTest(UserPayload payload) throws IOException {
      appId = ApplicationId.newInstance(1000, 200);
      this.payload = payload == null ? UserPayload.create(null) : payload;
    }

    @Override
    public ApplicationId getApplicationId() {
      return appId;
    }

    @Override
    public String getDAGName() {
      return "FakeDAG";
    }

    @Override
    public String getInputName() {
      return "MRInput";
    }

    @Override
    public UserPayload getInputUserPayload() {
      return payload;
    }

    @Override
    public int getNumTasks() {
      return 100;
    }

    @Override
    public Resource getVertexTaskResource() {
      return Resource.newInstance(1024, 1);
    }

    @Override
    public Resource getTotalAvailableResource() {
      return Resource.newInstance(10240, 10);
    }

    @Override
    public int getNumClusterNodes() {
      return 10;
    }

    @Override
    public int getDAGAttemptNumber() {
      return 1;
    }

    @Override
    public int getVertexNumTasks(String vertexName) {
      throw new UnsupportedOperationException("getVertexNumTasks not implemented in this mock");
    }

    @Override
    public void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      throw new UnsupportedOperationException("getVertexNumTasks not implemented in this mock");
    }

    @Override
    public void addCounters(TezCounters tezCounters) {
      throw new UnsupportedOperationException("addCounters not implemented in this mock");
    }

    @Override
    public UserPayload getUserPayload() {
      throw new UnsupportedOperationException("getUserPayload not implemented in this mock");
    }

  }

}

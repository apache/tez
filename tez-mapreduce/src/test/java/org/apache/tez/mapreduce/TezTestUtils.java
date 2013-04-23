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

import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.tez.engine.records.TezDAGID;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;
import org.apache.tez.engine.records.TezVertexID;

public class TezTestUtils {

  public static TezTaskAttemptID getMockTaskAttemptId(
      int jobId, int vertexId, int taskId, int taskAttemptId) {
    return new TezTaskAttemptID(
        new TezTaskID(
            new TezVertexID(
                new TezDAGID(
                    BuilderUtils.newApplicationId(0, jobId), jobId),
                    vertexId),
                    taskId)
        , taskAttemptId);
  }
  
  public static TezTaskID getMockTaskId(int jobId,
      int vertexId, int taskId) {
    return new TezTaskID(
        new TezVertexID(new TezDAGID(
            BuilderUtils.newApplicationId(0, jobId),
            jobId), vertexId),
            taskId);
  }
  
  public static TezDAGID getMockJobId(int jobId) {
    return new TezDAGID(
        BuilderUtils.newApplicationId(0, jobId), jobId);
  }
  
  public static TezVertexID getMockVertexId(int jobId, int vId) {
    return new TezVertexID(
        new TezDAGID(
            BuilderUtils.newApplicationId(0, jobId), jobId),
            vId);
  }
}

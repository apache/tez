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
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

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
}

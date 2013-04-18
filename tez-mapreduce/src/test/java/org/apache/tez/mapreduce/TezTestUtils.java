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
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.engine.records.TezDAGID;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskID;
import org.apache.tez.engine.records.TezVertexID;
import org.apache.tez.mapreduce.hadoop.MRTaskType;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TezTestUtils {

  public static TezTaskAttemptID getMockTaskAttemptId(
      int jobId, int taskId, int taskAttemptId, MRTaskType type) {
    TezTaskAttemptID taskAttemptID = mock(TezTaskAttemptID.class);
    TezTaskID taskID = getMockTaskId(jobId, taskId, type);
    when(taskAttemptID.getTaskID()).thenReturn(taskID);
    when(taskAttemptID.getId()).thenReturn(taskAttemptId);
    when(taskAttemptID.toString()).thenReturn(
        "attempt_tez_" + Integer.toString(jobId) + "_" + 
        ((type == MRTaskType.MAP) ? "m" : "r") + "_" + 
        Integer.toString(taskId) + "_" + Integer.toString(taskAttemptId)
        );
    return taskAttemptID;
  }
  
  public static TezTaskID getMockTaskId(int jobId, int taskId, MRTaskType type) {
    TezVertexID vertexID = getMockVertexId(jobId, type);
    TezTaskID taskID = mock(TezTaskID.class);
    when(taskID.getVertexID()).thenReturn(vertexID);
    when(taskID.getId()).thenReturn(taskId);
    return taskID;
  }
  
  public static TezDAGID getMockJobId(int jobId) {
    TezDAGID jobID = mock(TezDAGID.class);
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(0L);
    appId.setId(jobId);
    when(jobID.getId()).thenReturn(jobId);
    when(jobID.getApplicationId()).thenReturn(appId);
    return jobID;
  }
  
  public static TezVertexID getMockVertexId(int jobId, MRTaskType type) {
    TezVertexID vertexID = mock(TezVertexID.class);
    when(vertexID.getDAGId()).thenReturn(getMockJobId(jobId));
    when(vertexID.getId()).thenReturn(type == MRTaskType.MAP ? 0 : 1);
    return vertexID;
  }
}

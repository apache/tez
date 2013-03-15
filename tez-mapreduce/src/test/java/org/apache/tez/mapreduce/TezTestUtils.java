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

import org.apache.tez.mapreduce.hadoop.MRTaskType;
import org.apache.tez.records.TezJobID;
import org.apache.tez.records.TezTaskAttemptID;
import org.apache.tez.records.TezTaskID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TezTestUtils {

  public static TezTaskAttemptID getMockTaskAttemptId(
      int jobId, int taskId, int taskAttemptId, MRTaskType type) {
    TezTaskAttemptID taskAttemptID = mock(TezTaskAttemptID.class);
    TezTaskID taskID = getMockTaskId(jobId, taskId, type);
    TezJobID jobID = taskID.getJobID();
    when(taskAttemptID.getJobID()).thenReturn(jobID);
    when(taskAttemptID.getTaskID()).thenReturn(taskID);
    when(taskAttemptID.getId()).thenReturn(taskAttemptId);
    when(taskAttemptID.getTaskType()).thenReturn(type.toString());
    when(taskAttemptID.toString()).thenReturn(
        "attempt_tez_" + Integer.toString(jobId) + "_" + 
        ((type == MRTaskType.MAP) ? "m" : "r") + "_" + 
        Integer.toString(taskId) + "_" + Integer.toString(taskAttemptId)
        );
    return taskAttemptID;
  }
  
  public static TezTaskID getMockTaskId(int jobId, int taskId, MRTaskType type) {
    TezJobID jobID = getMockJobId(jobId);
    TezTaskID taskID = mock(TezTaskID.class);
    when(taskID.getTaskType()).thenReturn(type.toString());
    when(taskID.getId()).thenReturn(taskId);
    when(taskID.getJobID()).thenReturn(jobID);
    return taskID;
  }
  
  public static TezJobID getMockJobId(int jobId) {
    TezJobID jobID = mock(TezJobID.class);
    when(jobID.getId()).thenReturn(jobId);
    when(jobID.getJtIdentifier()).thenReturn("mock");
    return jobID;
  }
}

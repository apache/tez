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

package org.apache.tez.dag.utils;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.api.oldrecords.AMInfo;
import org.apache.tez.dag.app.dag.DAGReport;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

public final class TezBuilderUtils {

  private TezBuilderUtils() {}

  public static TezVertexID newVertexID(TezDAGID dagId, int vertexId) {
    return TezVertexID.getInstance(dagId, vertexId);
  }

  public static TezTaskAttemptID newTaskAttemptId(TezTaskID taskId, int id) {
    return TezTaskAttemptID.getInstance(taskId, id);
  }
  
  public static DAGReport newDAGReport() {
    return null;
  }

  public static AMInfo newAMInfo(ApplicationAttemptId appAttemptID, 
      long startTime, ContainerId containerID, String nmHost, 
      int nmPort, int nmHttpPort) {
    return null;
  }

  public static TezTaskID newTaskId(TezDAGID dagId, int vertexId, int taskId) {
    return TezTaskID.getInstance(newVertexID(dagId, vertexId), taskId);
  }

}

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

package org.apache.tez.dag.api.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProtoOrBuilder;
import org.apache.tez.dag.api.records.DAGProtos.StringProgressPairProto;
import org.apache.tez.dag.api.TezUncheckedException;

public class DAGStatus {

  public enum State {
    SUBMITTED,
    INITING,
    RUNNING,
    SUCCEEDED,
    KILLED,
    FAILED,
    ERROR,
  };
  
  DAGStatusProtoOrBuilder proxy = null;
  Progress progress = null;
  Map<String, Progress> vertexProgress = null;
  
  public DAGStatus(DAGStatusProtoOrBuilder proxy) {
    this.proxy = proxy;
  }
  
  public State getState() {
    switch(proxy.getState()) {
    case DAG_SUBMITTED:
      return DAGStatus.State.SUBMITTED;
    case DAG_INITING:
      return DAGStatus.State.INITING;
    case DAG_TERMINATING: // For simplicity, DAG_TERMINATING is presented to user as 'still running'
    case DAG_RUNNING:
      return DAGStatus.State.RUNNING;
    case DAG_SUCCEEDED:
      return DAGStatus.State.SUCCEEDED;
    case DAG_FAILED:
      return DAGStatus.State.FAILED;
    case DAG_KILLED:
      return DAGStatus.State.KILLED;
    case DAG_ERROR:
      return DAGStatus.State.ERROR;
    default:
      throw new TezUncheckedException("Unsupported value for DAGStatus.State : " + 
                              proxy.getState());
    }
  }
  
  public boolean isCompleted() {
    State state = getState();
    return (state == State.SUCCEEDED ||
             state == State.FAILED ||
             state == State.KILLED ||
             state == State.ERROR);
  }

  public List<String> getDiagnostics() {
    return proxy.getDiagnosticsList();
  }

  /**
   * Gets overall progress value of the DAG.
   * 
   * @return Progress of the DAG. Maybe null when the DAG is not running. Maybe
   *         null when the DAG is running and the application master cannot be
   *         reached - e.g. when the execution platform has restarted the
   *         application master.
   * @see Progress
   */
  public Progress getDAGProgress() {
    if(progress == null && proxy.hasDAGProgress()) {
      progress = new Progress(proxy.getDAGProgress());
    }
    return progress;
  }

  /**
   * Get the progress of a vertex in the DAG
   * 
   * @return Progress of the vertex. May be null when the DAG is not running.
   *         Maybe null when the DAG is running and the application master
   *         cannot be reached - e.g. when the execution platform has restarted
   *         the application master.
   * @see Progress
   */
  public Map<String, Progress> getVertexProgress() {
    if(vertexProgress == null) {
      if(proxy.getVertexProgressList() != null) {
        List<StringProgressPairProto> kvList = proxy.getVertexProgressList();
        vertexProgress = new HashMap<String, Progress>(kvList.size());        
        for(StringProgressPairProto kv : kvList){
          vertexProgress.put(kv.getKey(), new Progress(kv.getProgress()));
        }
      }
    }
    return vertexProgress;
  }

}

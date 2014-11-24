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

package org.apache.tez.dag.app.dag;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptReport;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

/**
 * Read only view of TaskAttempt.
 */
public interface TaskAttempt {

  public static class TaskAttemptStatus {
    public TaskAttemptState state;
    public DAGCounter localityCounter;
    public float progress;
    public TezCounters counters;

    // insert these counters till they come natively from the task itself.
    // HDFS-5098
    private AtomicBoolean localitySet = new AtomicBoolean(false);
    public void setLocalityCounter(DAGCounter localityCounter) {
      if (!localitySet.get()) {
        localitySet.set(true);
        if (counters == null) {
          counters = new TezCounters();
        }
        if (localityCounter != null) {
          counters.findCounter(localityCounter).increment(1);
          // TODO Maybe validate that the correct value is being set.
        }
      }
    }
  }
  
  TezTaskAttemptID getID();
  TezTaskID getTaskID();
  TezVertexID getVertexID();
  TezDAGID getDAGID();
  
  TaskAttemptReport getReport();
  List<String> getDiagnostics();
  TaskAttemptTerminationCause getTerminationCause();
  TezCounters getCounters();
  float getProgress();
  TaskAttemptState getState();
  TaskAttemptState getStateNoLock();

  /** 
   * Has attempt reached the final state or not.
   * @return true if it has finished, else false
   */
  boolean isFinished();

  /**
   * @return the container ID if a container is assigned, otherwise null.
   */
  ContainerId getAssignedContainerID();
  
  /**
   * @return the container if assigned, otherwise null
   */
  Container getAssignedContainer();

  /**
   * @return container mgr address if a container is assigned, otherwise null.
   */
  String getAssignedContainerMgrAddress();
  
  /**
   * @return node's id if a container is assigned, otherwise null.
   */
  NodeId getNodeId();
  
  /**
   * @return node's http address if a container is assigned, otherwise null.
   */
  String getNodeHttpAddress();
  
  /**
   * @return node's rack name if a container is assigned, otherwise null.
   */
  String getNodeRackName();

  /** 
   * @return time at which container is launched. If container is not launched
   * yet, returns 0.
   */
  long getLaunchTime();

  /** 
   * @return attempt's finish time. If attempt is not finished
   *  yet, returns 0.
   */
  long getFinishTime();
  
  public Task getTask();
  
  TaskAttemptState restoreFromEvent(HistoryEvent event);

}

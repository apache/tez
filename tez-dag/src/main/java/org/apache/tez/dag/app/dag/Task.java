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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;

/**
 * Read only view of Task.
 */
public interface Task {
  TezTaskID getTaskId();
  TaskReport getReport();
  TaskState getState();
  TezCounters getCounters();
  float getProgress();
  Map<TezTaskAttemptID, TaskAttempt> getAttempts();
  TaskAttempt getAttempt(TezTaskAttemptID attemptID);
  TaskAttempt getSuccessfulAttempt();
  /** Has Task reached the final state or not.
   */
  boolean isFinished();

  /**
   * Can the output of the taskAttempt be committed. Note that once the task
   * gives a go for a commit, further canCommit requests from any other attempts
   * should return false.
   * 
   * @param taskAttemptID
   * @return whether the attempt's output can be committed or not.
   */
  boolean canCommit(TezTaskAttemptID taskAttemptID);
  
  public Vertex getVertex();
  
  public ArrayList<TezEvent> getTaskAttemptTezEvents(TezTaskAttemptID attemptID,
      int fromEventId, int maxEvents);
  
  public List<String> getDiagnostics();

  public void registerTezEvent(TezEvent tezEvent);
  
  public TaskSpec getBaseTaskSpec();
  
  public TaskLocationHint getTaskLocationHint();

  long getFirstAttemptStartTime();

  long getFinishTime();

  /**
   * @return set of nodes on which previous attempts were running on, at the time
   * of latest attempt being scheduled.
   */
  Set<NodeId> getNodesWithRunningAttempts();
}

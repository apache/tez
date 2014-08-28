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
import java.util.Map;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskReport;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
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

  
  /**
   * Do the running tasks need to stick around after they're done processing and
   * generating output. Required for tasks which have custom output handling
   * such as in-memory shuffle.
   * 
   * @return whether the task needs to stick around.
   */
  boolean needsWaitAfterOutputConsumable();
  
  /**
   * Get the attempt id which has reported in as output ready. null if not
   * applicable.
   * 
   * @return the attempt id which has reported in as output ready. null if not
   * applicable.
   */
  TezTaskAttemptID getOutputConsumableAttempt();
  
  public Vertex getVertex();
  
  public List<TezEvent> getTaskAttemptTezEvents(TezTaskAttemptID attemptID,
      int fromEventId, int maxEvents);
  
  public List<String> getDiagnostics();

  TaskState restoreFromEvent(HistoryEvent historyEvent);

  public void registerTezEvent(TezEvent tezEvent);
}

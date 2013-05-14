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

package org.apache.tez.dag.app.dag.event;

import java.util.List;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.records.TaskAttemptState;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskAttemptEventStatusUpdate extends TaskAttemptEvent {

  private TaskAttemptStatus reportedTaskAttemptStatus;

  public TaskAttemptEventStatusUpdate(TezTaskAttemptID id,
      TaskAttemptStatus taskAttemptStatus) {
    super(id, TaskAttemptEventType.TA_STATUS_UPDATE);
    this.reportedTaskAttemptStatus = taskAttemptStatus;
  }

  public TaskAttemptStatus getReportedTaskAttemptStatus() {
    return reportedTaskAttemptStatus;
  }

  /**
   * The internal TaskAttemptStatus object corresponding to remote Task status.
   * 
   */
  public static class TaskAttemptStatus {
    public TezTaskAttemptID id;
    public float progress;
    public TezCounters counters;
    public String stateString;
    //public Phase phase;
    public long outputSize;
    public List<TezTaskAttemptID> fetchFailedMaps;
    public long mapFinishTime;
    public long shuffleFinishTime;
    public long sortFinishTime;
    public TaskAttemptState taskState;
  }
}

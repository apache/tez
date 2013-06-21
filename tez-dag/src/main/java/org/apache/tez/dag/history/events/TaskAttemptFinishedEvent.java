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

package org.apache.tez.dag.history.events;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.history.avro.TaskAttemptFinished;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskAttemptFinishedEvent implements HistoryEvent {

  private TaskAttemptFinished datum = new TaskAttemptFinished();
  // FIXME remove this when we have a proper history
  private final TezCounters tezCounters;

  public TaskAttemptFinishedEvent(TezTaskAttemptID taId,
      String vertexName,
      long startTime,
      long finishTime,
      TaskAttemptState state,
      String diagnostics,
      TezCounters counters) {
    datum.taskAttemptId = taId.toString();
    datum.vertexName = vertexName;
    datum.startTime = startTime;
    datum.finishTime = finishTime;
    datum.status = state.name();
    datum.diagnostics = diagnostics;
    tezCounters = counters;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.TASK_ATTEMPT_FINISHED;
  }

  @Override
  public Object getBlob() {
    // TODO Auto-generated method stub
    return this.toString();
  }

  @Override
  public void setBlob(Object blob) {
    this.datum = (TaskAttemptFinished) blob;
  }

  @Override
  public String toString() {
    return "vertexName=" + datum.vertexName
        + ", taskAttemptId=" + datum.taskAttemptId
        + ", startTime=" + datum.startTime
        + ", finishTime=" + datum.finishTime
        + ", timeTaken=" + (datum.finishTime - datum.startTime)
        + ", status=" + datum.status
        + ", diagnostics=" + datum.diagnostics
        + ", counters="
        + tezCounters.toString()
            .replaceAll("\\n", ", ").replaceAll("\\s+", " ");
  }
}

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

import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class VertexEventTaskAttemptStatusUpdate extends VertexEvent {
  final TezTaskAttemptID id;
  final TaskAttemptState state;
  final long timestamp;
  final boolean justStarted;
  
  public VertexEventTaskAttemptStatusUpdate(TezTaskAttemptID taId, TaskAttemptState state,
      long timestamp) {
    this(taId, state, timestamp, false);
  }
  
  public VertexEventTaskAttemptStatusUpdate(TezTaskAttemptID taId, TaskAttemptState state,
      long timestamp, boolean justStarted) {
    super(taId.getTaskID().getVertexID(), VertexEventType.V_TASK_ATTEMPT_STATUS_UPDATE);
    this.id = taId;
    this.state = state;
    this.timestamp = timestamp;
    this.justStarted = justStarted;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public TezTaskAttemptID getAttemptId() {
    return id;
  }
  
  public boolean hasJustStarted() {
    return justStarted;
  }
  
  public TaskAttemptState getTaskAttemptState() {
    return state;
  }

}

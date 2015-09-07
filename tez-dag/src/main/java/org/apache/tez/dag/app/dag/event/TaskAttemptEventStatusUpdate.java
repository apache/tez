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

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;

public class TaskAttemptEventStatusUpdate extends TaskAttemptEvent {
  
  private TaskStatusUpdateEvent taskAttemptStatus;
  private boolean readErrorReported = false;
  
  public TaskAttemptEventStatusUpdate(TezTaskAttemptID id,
      TaskStatusUpdateEvent statusEvent) {
    super(id, TaskAttemptEventType.TA_STATUS_UPDATE);
    this.taskAttemptStatus = statusEvent;
  }
  
  public TaskStatusUpdateEvent getStatusEvent() {
    return this.taskAttemptStatus;
  }
  
  public void setReadErrorReported(boolean value) {
    readErrorReported = value;
  }
  
  public boolean getReadErrorReported() {
    return readErrorReported;
  }
}

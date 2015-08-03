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

import org.apache.tez.common.TezAbstractEvent;
import org.apache.tez.dag.records.TezTaskAttemptID;

@SuppressWarnings("rawtypes")
public class TaskEventTAUpdate extends TaskEvent {

  private TezTaskAttemptID attemptID;
  private TezAbstractEvent causalEvent;

  public TaskEventTAUpdate(TezTaskAttemptID id, TaskEventType type) {
    this(id, type, null);
  }

  public TaskEventTAUpdate(TezTaskAttemptID id, TaskEventType type, TezAbstractEvent causalEvent) {
    super(id.getTaskID(), type);
    this.attemptID = id;
    this.causalEvent = causalEvent;
  }
  
  public TezTaskAttemptID getTaskAttemptID() {
    return attemptID;
  }
  
  public TezAbstractEvent getCausalEvent() {
    return causalEvent;
  }

}

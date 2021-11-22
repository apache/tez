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
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

/**
 * This class encapsulates task attempt related events.
 *
 */
public class TaskAttemptEvent extends TezAbstractEvent<TaskAttemptEventType> {

  private TezTaskAttemptID attemptID;
  
  /**
   * Create a new TaskAttemptEvent.
   * @param id the id of the task attempt
   * @param type the type of event that happened.
   */
  public TaskAttemptEvent(TezTaskAttemptID id, TaskAttemptEventType type) {
    super(type);
    this.attemptID = id;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return attemptID;
  }

  public TezDAGID getDAGId() {
    return attemptID.getDAGId();
  }

  public TezVertexID getVertexID() {
    return attemptID.getVertexID();
  }

  public TezTaskID getTaskID() {
    return attemptID.getTaskID();
  }
  
  @Override
  public int getSerializingHash() {
    return attemptID.getTaskID().getSerializingHash();
  }
}

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

import org.apache.tez.dag.records.TezTaskAttemptID;

public class VertexEventTaskAttemptFetchFailure extends VertexEvent {

  private final TezTaskAttemptID target;
  private final List<TezTaskAttemptID> sources;

  public VertexEventTaskAttemptFetchFailure(TezTaskAttemptID reduce, 
      List<TezTaskAttemptID> maps) {
    super(
        reduce.getTaskID().getVertexID(), 
        VertexEventType.V_TASK_ATTEMPT_FETCH_FAILURE);
    this.target = reduce;
    this.sources = maps;
  }

  public List<TezTaskAttemptID> getSources() {
    return sources;
  }

  public TezTaskAttemptID getTarget() {
    return target;
  }

}

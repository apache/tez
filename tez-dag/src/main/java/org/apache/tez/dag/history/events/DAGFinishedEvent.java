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

import org.apache.tez.dag.api.impl.DAGStatus;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.avro.DAGFinished;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.records.TezDAGID;

public class DAGFinishedEvent implements HistoryEvent {

  private DAGFinished datum = new DAGFinished();

  public DAGFinishedEvent(TezDAGID dagId,
      long finishTime, DAGStatus.State state,
      String diagnostics) {
    datum.dagId = dagId.toString();
    datum.finishTime = finishTime;
    datum.status = state.name();
    datum.diagnostics = diagnostics;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.DAG_FINISHED;
  }

  @Override
  public Object getBlob() {
    // TODO Auto-generated method stub
    return this.toString();
  }

  @Override
  public void setBlob(Object blob) {
    this.datum = (DAGFinished) blob;
  }

  @Override
  public String toString() {
    return "dagId=" + datum.dagId
        + ", finishTime=" + datum.finishTime
        + ", status=" + datum.status
        + ", diagnostics=" + datum.diagnostics;
  }
}

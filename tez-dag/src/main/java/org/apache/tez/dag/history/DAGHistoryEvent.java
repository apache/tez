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

package org.apache.tez.dag.history;

import org.apache.tez.dag.records.TezDAGID;

public class DAGHistoryEvent {

  private final HistoryEvent historyEvent;
  private final TezDAGID dagID;

  public DAGHistoryEvent(TezDAGID dagID,
      HistoryEvent historyEvent) {
    this.dagID = dagID;
    this.historyEvent = historyEvent;
  }

  public DAGHistoryEvent(HistoryEvent historyEvent) {
    this(null, historyEvent);
  }

  public HistoryEvent getHistoryEvent() {
    return historyEvent;
  }

  public TezDAGID getDagID() {
    return this.dagID;
  }

}

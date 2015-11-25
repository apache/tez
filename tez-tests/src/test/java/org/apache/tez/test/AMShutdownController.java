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
package org.apache.tez.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.recovery.RecoveryService;

public abstract class AMShutdownController {

  private List<DAGHistoryEvent> historyEvents = new ArrayList<DAGHistoryEvent>();
  
  protected AppContext appContext;
  protected RecoveryService recoveryService;
  
  public AMShutdownController(AppContext appContext, RecoveryService recoveryService) {
    this.appContext = appContext;
    this.recoveryService = recoveryService;
  }

  public void preHandleHistoryEvent(DAGHistoryEvent event) {
    historyEvents.add(event);
    if (shouldShutdownPreEvent(event, historyEvents)) {
      System.exit(1);
    }
  }

  public void postHandleHistoryEvent(DAGHistoryEvent event) {
    if (shouldShutdownPostEvent(event, historyEvents)) {
      System.exit(1);
    }
  }

  protected abstract boolean shouldShutdownPreEvent(DAGHistoryEvent curEvent,
      List<DAGHistoryEvent> historyEvents);

  protected abstract boolean shouldShutdownPostEvent(DAGHistoryEvent curEvent,
      List<DAGHistoryEvent> historyEvents);
}

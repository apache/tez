/*
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

package org.apache.tez.state;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.tez.dag.records.TezID;

public class StateMachineTez<STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT, OPERAND>
    implements StateMachine<STATE, EVENTTYPE, EVENT> {

  private final Map<STATE, OnStateChangedCallback> callbackMap =
      new HashMap<STATE, OnStateChangedCallback>();
  private final OPERAND operand;

  private final StateMachine<STATE, EVENTTYPE, EVENT> realStatemachine;

  public StateMachineTez(StateMachine sm, OPERAND operand) {
    this.realStatemachine = sm;
    this.operand = operand;
  }

  public StateMachineTez registerStateEnteredCallback(STATE state,
                                                      OnStateChangedCallback callback) {
    callbackMap.put(state, callback);
    return this;
  }

  @Override
  public STATE getCurrentState() {
    return realStatemachine.getCurrentState();
  }

  @Override
  public STATE doTransition(EVENTTYPE eventType, EVENT event) throws
      InvalidStateTransitonException {
    STATE oldState = realStatemachine.getCurrentState();
    STATE newState = realStatemachine.doTransition(eventType, event);
    if (newState != oldState) {
      OnStateChangedCallback callback = callbackMap.get(newState);
      if (callback != null) {
        callback.onStateChanged(operand, newState);
      }
    }
    return newState;
  }
}
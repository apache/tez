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

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.dag.records.TezDAGID;

public class DAGEventCounterUpdate extends DAGEvent {

  List<CounterIncrementalUpdate> counterUpdates = null;
  
  public DAGEventCounterUpdate(TezDAGID dagId) {
    super(dagId, DAGEventType.DAG_COUNTER_UPDATE);
    counterUpdates = new ArrayList<DAGEventCounterUpdate.CounterIncrementalUpdate>();
  }

  public void addCounterUpdate(Enum<?> key, long incrValue) {
    counterUpdates.add(new CounterIncrementalUpdate(key, incrValue));
  }
  
  public List<CounterIncrementalUpdate> getCounterUpdates() {
    return counterUpdates;
  }
  
  public static class CounterIncrementalUpdate {
    Enum<?> key;
    long incrValue;
    
    public CounterIncrementalUpdate(Enum<?> key, long incrValue) {
      this.key = key;
      this.incrValue = incrValue;
    }
    
    public Enum<?> getCounterKey() {
      return key;
    }

    public long getIncrementValue() {
      return incrValue;
    }
  }
}

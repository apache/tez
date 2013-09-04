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

package org.apache.tez.engine.newapi.impl;

import java.util.List;

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.TezProcessorContext;

public class TezProcessorContextImpl extends TezTaskContextImpl
  implements TezProcessorContext {

  private final byte[] userPayload;

  public TezProcessorContextImpl(TezConfiguration tezConf, String vertexName,
      TezTaskAttemptID taskAttemptID, TezCounters counters,
      byte[] userPayload) {
    super(tezConf, vertexName, taskAttemptID, counters);
    this.userPayload = userPayload;
  }

  @Override
  public void sendEvents(List<Event> events) {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getUserPayload() {
    return userPayload;
  }

  @Override
  public void setProgress(float progress) {
    // TODO Auto-generated method stub

  }

}

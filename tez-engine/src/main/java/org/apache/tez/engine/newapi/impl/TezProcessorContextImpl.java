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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.TezProcessorContext;
import org.apache.tez.engine.newapi.impl.EventMetaData.EventGenerator;

public class TezProcessorContextImpl extends TezTaskContextImpl
  implements TezProcessorContext {

  private final byte[] userPayload;
  private final TezUmbilical tezUmbilical;
  private final EventMetaData sourceInfo;

  public TezProcessorContextImpl(Configuration tezConf,
      TezUmbilical tezUmbilical, String vertexName,
      TezTaskAttemptID taskAttemptID, TezCounters counters,
      byte[] userPayload) {
    super(tezConf, vertexName, taskAttemptID, counters);
    this.userPayload = userPayload;
    this.tezUmbilical = tezUmbilical;
    this.sourceInfo = new EventMetaData(EventGenerator.PROCESSOR,
        taskVertexName, "", taskAttemptID);
  }

  @Override
  public void sendEvents(List<Event> events) {
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent tEvt = new TezEvent(e, sourceInfo);
      tezEvents.add(tEvt);
    }
    tezUmbilical.addEvents(tezEvents);
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

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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.impl.EventMetaData.EventGenerator;

public class TezOutputContextImpl extends TezTaskContextImpl
    implements TezOutputContext {

  private final byte[] userPayload;
  private final String destinationVertexName;
  private final TezUmbilical tezUmbilical;
  private final EventMetaData sourceInfo;

  @Private
  public TezOutputContextImpl(Configuration conf,
      TezUmbilical tezUmbilical, String taskVertexName,
      String destinationVertexName,
      TezTaskAttemptID taskAttemptID, TezCounters counters,
      byte[] userPayload) {
    super(conf, taskVertexName, taskAttemptID, counters);
    this.userPayload = userPayload;
    this.destinationVertexName = destinationVertexName;
    this.tezUmbilical = tezUmbilical;
    this.sourceInfo = new EventMetaData(EventGenerator.OUTPUT, taskVertexName,
        destinationVertexName, taskAttemptID);
    this.uniqueIdentifier = String.format("%s_%s_%6d_%2d_%s", taskAttemptID
        .getTaskID().getVertexID().getDAGId().toString(), taskVertexName,
        getTaskIndex(), getAttemptNumber(), destinationVertexName);
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
  public String getDestinationVertexName() {
    return destinationVertexName;
  }

}

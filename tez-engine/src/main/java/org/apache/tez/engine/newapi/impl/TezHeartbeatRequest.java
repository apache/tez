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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.records.TezTaskAttemptID;


public class TezHeartbeatRequest implements Writable {

  private List<TezEvent> events;
  private TezTaskAttemptID currentTaskAttemptID;
  private int startIndex;
  private int maxEvents;

  public TezHeartbeatRequest() {
  }

  public TezHeartbeatRequest(List<TezEvent> events,
      TezTaskAttemptID taskAttemptID,
      int startIndex, int maxEvents) {
    this.events = Collections.unmodifiableList(events);
    this.startIndex = startIndex;
    this.maxEvents = maxEvents;
    this.currentTaskAttemptID = taskAttemptID;
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getMaxEvents() {
    return maxEvents;
  }

  public TezTaskAttemptID getCurrentTaskAttemptID() {
    return currentTaskAttemptID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(events.size());
    for (TezEvent e : events) {
      e.write(out);
    }
    if (currentTaskAttemptID != null) {
      out.writeBoolean(true);
      currentTaskAttemptID.write(out);
    } else {
      out.writeBoolean(false);
    }
    out.writeInt(startIndex);
    out.writeInt(maxEvents);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int eventsCount = in.readInt();
    events = new ArrayList<TezEvent>(eventsCount);
    for (int i = 0; i < eventsCount; ++i) {
      TezEvent e = new TezEvent();
      e.readFields(in);
      events.add(e);
    }
    if (in.readBoolean()) {
      currentTaskAttemptID = new TezTaskAttemptID();
      currentTaskAttemptID.readFields(in);
    } else {
      currentTaskAttemptID = null;
    }
    startIndex = in.readInt();
    maxEvents = in.readInt();
  }

}

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

package org.apache.tez.runtime.api.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class TezHeartbeatResponse implements Writable {

  private long lastRequestId;
  private boolean shouldDie = false;
  private List<TezEvent> events;
  private int nextFromEventId;

  public TezHeartbeatResponse() {
  }

  public TezHeartbeatResponse(List<TezEvent> events) {
    this.events = Collections.unmodifiableList(events);
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public boolean shouldDie() {
    return shouldDie;
  }

  public long getLastRequestId() {
    return lastRequestId;
  }
  
  public int getNextFromEventId() {
    return nextFromEventId;
  }

  public void setEvents(List<TezEvent> events) {
    this.events = Collections.unmodifiableList(events);
  }

  public void setLastRequestId(long lastRequestId ) {
    this.lastRequestId = lastRequestId;
  }

  public void setShouldDie() {
    this.shouldDie = true;
  }
  
  public void setNextFromEventId(int nextFromEventId) {
    this.nextFromEventId = nextFromEventId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(lastRequestId);
    out.writeBoolean(shouldDie);
    out.writeInt(nextFromEventId);
    if(events != null) {
      out.writeBoolean(true);
      out.writeInt(events.size());
      for (TezEvent e : events) {
        e.write(out);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    lastRequestId = in.readLong();
    shouldDie = in.readBoolean();
    nextFromEventId = in.readInt();
    if(in.readBoolean()) {
      int eventCount = in.readInt();
      events = new ArrayList<TezEvent>(eventCount);
      for (int i = 0; i < eventCount; ++i) {
        TezEvent e = new TezEvent();
        e.readFields(in);
        events.add(e);
      }
    }
  }

  @Override
  public String toString() {
    return "{ "
        + " lastRequestId=" + lastRequestId
        + ", shouldDie=" + shouldDie
        + ", nextFromEventId=" + nextFromEventId
        + ", eventCount=" + (events != null ? events.size() : 0)
        + " }";
  }
}

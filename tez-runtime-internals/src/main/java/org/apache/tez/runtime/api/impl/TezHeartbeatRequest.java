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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.records.TezTaskAttemptID;


public class TezHeartbeatRequest implements Writable {

  private String containerIdentifier;
  private List<TezEvent> events;
  private TezTaskAttemptID currentTaskAttemptID;
  private int startIndex;
  private int preRoutedStartIndex;
  private int maxEvents;
  private long requestId;

  public TezHeartbeatRequest() {
  }

  public TezHeartbeatRequest(long requestId, List<TezEvent> events,
      int preRoutedStartIndex, String containerIdentifier,
      TezTaskAttemptID taskAttemptID, int startIndex, int maxEvents) {
    this.containerIdentifier = containerIdentifier;
    this.requestId = requestId;
    this.events = Collections.unmodifiableList(events);
    this.startIndex = startIndex;
    this.preRoutedStartIndex = preRoutedStartIndex;
    this.maxEvents = maxEvents;
    this.currentTaskAttemptID = taskAttemptID;
  }

  public String getContainerIdentifier() {
    return containerIdentifier;
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getPreRoutedStartIndex() {
    return preRoutedStartIndex;
  }

  public int getMaxEvents() {
    return maxEvents;
  }

  public long getRequestId() {
    return requestId;
  }

  public TezTaskAttemptID getCurrentTaskAttemptID() {
    return currentTaskAttemptID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (events != null) {
      out.writeBoolean(true);
      out.writeInt(events.size());
      for (TezEvent e : events) {
        e.write(out);
      }
    } else {
      out.writeBoolean(false);
    }
    if (currentTaskAttemptID != null) {
      out.writeBoolean(true);
      currentTaskAttemptID.write(out);
    } else {
      out.writeBoolean(false);
    }
    out.writeInt(startIndex);
    out.writeInt(preRoutedStartIndex);
    out.writeInt(maxEvents);
    out.writeLong(requestId);
    Text.writeString(out, containerIdentifier);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      int eventsCount = in.readInt();
      events = new ArrayList<TezEvent>(eventsCount);
      for (int i = 0; i < eventsCount; ++i) {
        TezEvent e = new TezEvent();
        e.readFields(in);
        events.add(e);
      }
    }
    if (in.readBoolean()) {
      currentTaskAttemptID = TezTaskAttemptID.readTezTaskAttemptID(in);
    } else {
      currentTaskAttemptID = null;
    }
    startIndex = in.readInt();
    preRoutedStartIndex = in.readInt();
    maxEvents = in.readInt();
    requestId = in.readLong();
    containerIdentifier = Text.readString(in);
  }

  @Override
  public String toString() {
    return "{ "
        + " containerId=" + containerIdentifier
        + ", requestId=" + requestId
        + ", startIndex=" + startIndex
        + ", preRoutedStartIndex=" + preRoutedStartIndex
        + ", maxEventsToGet=" + maxEvents
        + ", taskAttemptId=" + currentTaskAttemptID
        + ", eventCount=" + (events != null ? events.size() : 0)
        + " }";
  }
}

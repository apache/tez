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

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.engine.api.events.EventProtos.DataMovementEventProto;
import org.apache.tez.engine.api.events.EventProtos.InputDataErrorEventProto;
import org.apache.tez.engine.api.events.SystemEventProtos.TaskFailedEventProto;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.events.DataMovementEvent;
import org.apache.tez.engine.newapi.events.InputDataErrorEvent;
import org.apache.tez.engine.newapi.events.TaskFailedEvent;

import com.google.protobuf.ByteString;

public class TezEvent implements Writable {

  private EventType eventType;

  private Event event;

  private EventMetaData sourceInfo;

  private EventMetaData destinationInfo;

  public TezEvent() {
  }

  public TezEvent(Event event, EventMetaData sourceInfo) {
    this.event = event;
    this.setSourceInfo(sourceInfo);
    if (event instanceof DataMovementEvent) {
      eventType = EventType.DATA_MOVEMENT_EVENT;
    } else if (event instanceof InputDataErrorEvent) {
      eventType = EventType.INPUT_DATA_ERROR_EVENT;
    } else if (event instanceof TaskFailedEvent) {
      eventType = EventType.TASK_FAILED_EVENT;
    } else {
      throw new TezUncheckedException("Unknown event, event="
          + event.getClass().getName());
    }
  }

  public Event getEvent() {
    return event;
  }

  public EventMetaData getSourceInfo() {
    return sourceInfo;
  }

  public void setSourceInfo(EventMetaData sourceInfo) {
    this.sourceInfo = sourceInfo;
  }

  public EventMetaData getDestinationInfo() {
    return destinationInfo;
  }

  public void setDestinationInfo(EventMetaData destinationInfo) {
    this.destinationInfo = destinationInfo;
  }

  public EventType getEventType() {
    return eventType;
  }

  private void serializeEvent(DataOutput out) throws IOException {
    if (event == null) {
      out.writeBoolean(false);
      return;
    }
    out.writeBoolean(true);
    byte[] eventBytes = null;
    switch (eventType) {
    case DATA_MOVEMENT_EVENT:
      DataMovementEvent dmEvt = (DataMovementEvent) event;
      eventBytes = DataMovementEventProto.newBuilder()
        .setSourceIndex(dmEvt.getSourceIndex())
        .setTargetIndex(dmEvt.getTargetIndex())
        .setUserPayload(ByteString.copyFrom(dmEvt.getUserPayload()))
        .build().toByteArray();
      break;
    case INPUT_DATA_ERROR_EVENT:
      InputDataErrorEvent ideEvt = (InputDataErrorEvent) event;
      eventBytes = InputDataErrorEventProto.newBuilder()
          .setIndex(ideEvt.getIndex())
          .setDiagnostics(ideEvt.getDiagnostics())
          .build().toByteArray();
      break;
    case TASK_FAILED_EVENT:
      TaskFailedEvent tfEvt = (TaskFailedEvent) event;
      eventBytes = TaskFailedEventProto.newBuilder()
          .setDiagnostics(tfEvt.getDiagnostics())
          .build().toByteArray();
      break;
    }
    out.writeInt(eventType.ordinal());
    out.writeInt(eventBytes.length);
    out.write(eventBytes);
  }

  private void deserializeEvent(DataInput in) throws IOException {
    if (!in.readBoolean()) {
      event = null;
      return;
    }
    eventType = EventType.values()[in.readInt()];
    int eventBytesLen = in.readInt();
    byte[] eventBytes = new byte[eventBytesLen];
    in.readFully(eventBytes);
    switch (eventType) {
    case DATA_MOVEMENT_EVENT:
      DataMovementEventProto dmProto = DataMovementEventProto.parseFrom(eventBytes);
      event = new DataMovementEvent(dmProto.getSourceIndex(),
          dmProto.getTargetIndex(),
          dmProto.getUserPayload().toByteArray());
      break;
    case INPUT_DATA_ERROR_EVENT:
      InputDataErrorEventProto ideProto =
          InputDataErrorEventProto.parseFrom(eventBytes);
      event = new InputDataErrorEvent(ideProto.getDiagnostics(),
          ideProto.getIndex(), ideProto.getVersion());
      break;
    case TASK_FAILED_EVENT:
      TaskFailedEventProto tfProto =
          TaskFailedEventProto.parseFrom(eventBytes);
      event = new TaskFailedEvent(tfProto.getDiagnostics());
      break;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    serializeEvent(out);
    if (sourceInfo != null) {
      out.writeBoolean(true);
      sourceInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (destinationInfo != null) {
      out.writeBoolean(true);
      destinationInfo.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    deserializeEvent(in);
    if (in.readBoolean()) {
      sourceInfo.readFields(in);
    }
    if (in.readBoolean()) {
      destinationInfo.readFields(in);
    }
  }

}

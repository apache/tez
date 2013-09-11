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
import org.apache.tez.engine.api.events.EventProtos.InputFailedEventProto;
import org.apache.tez.engine.api.events.EventProtos.InputInformationEventProto;
import org.apache.tez.engine.api.events.EventProtos.InputReadErrorEventProto;
import org.apache.tez.engine.api.events.SystemEventProtos.TaskAttemptCompletedEventProto;
import org.apache.tez.engine.api.events.SystemEventProtos.TaskAttemptFailedEventProto;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.events.DataMovementEvent;
import org.apache.tez.engine.newapi.events.InputFailedEvent;
import org.apache.tez.engine.newapi.events.InputInformationEvent;
import org.apache.tez.engine.newapi.events.InputReadErrorEvent;
import org.apache.tez.engine.newapi.events.TaskAttemptCompletedEvent;
import org.apache.tez.engine.newapi.events.TaskAttemptFailedEvent;

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
    } else if (event instanceof InputReadErrorEvent) {
      eventType = EventType.INPUT_READ_ERROR_EVENT;
    } else if (event instanceof TaskAttemptFailedEvent) {
      eventType = EventType.TASK_ATTEMPT_FAILED_EVENT;
    } else if (event instanceof TaskAttemptCompletedEvent) {
      eventType = EventType.TASK_ATTEMPT_COMPLETED_EVENT;
    } else if (event instanceof InputInformationEvent) {
      eventType = EventType.INTPUT_INFORMATION_EVENT;
    } else if (event instanceof InputFailedEvent) {
      eventType = EventType.INPUT_FAILED_EVENT;
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
    case INPUT_READ_ERROR_EVENT:
      InputReadErrorEvent ideEvt = (InputReadErrorEvent) event;
      eventBytes = InputReadErrorEventProto.newBuilder()
          .setIndex(ideEvt.getIndex())
          .setDiagnostics(ideEvt.getDiagnostics())
          .build().toByteArray();
      break;
    case TASK_ATTEMPT_FAILED_EVENT:
      TaskAttemptFailedEvent tfEvt = (TaskAttemptFailedEvent) event;
      eventBytes = TaskAttemptFailedEventProto.newBuilder()
          .setDiagnostics(tfEvt.getDiagnostics())
          .build().toByteArray();
      break;
    case TASK_ATTEMPT_COMPLETED_EVENT:
      eventBytes = TaskAttemptCompletedEventProto.newBuilder()
          .build().toByteArray();
      break;
    case INPUT_FAILED_EVENT:
      InputFailedEvent ifEvt = (InputFailedEvent) event;
      eventBytes = InputFailedEventProto.newBuilder()
          .setSourceIndex(ifEvt.getSourceIndex())
          .setTargetIndex(ifEvt.getTargetIndex())
          .setVersion(ifEvt.getVersion()).build().toByteArray();
    case INTPUT_INFORMATION_EVENT:
      InputInformationEvent iEvt = (InputInformationEvent) event;
      eventBytes = InputInformationEventProto.newBuilder()
          .setUserPayload(ByteString.copyFrom(iEvt.getUserPayload()))
          .build().toByteArray();
    default:
      throw new TezUncheckedException("Unknown TezEvent"
         + ", type=" + eventType);
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
    case INPUT_READ_ERROR_EVENT:
      InputReadErrorEventProto ideProto =
          InputReadErrorEventProto.parseFrom(eventBytes);
      event = new InputReadErrorEvent(ideProto.getDiagnostics(),
          ideProto.getIndex(), ideProto.getVersion());
      break;
    case TASK_ATTEMPT_FAILED_EVENT:
      TaskAttemptFailedEventProto tfProto =
          TaskAttemptFailedEventProto.parseFrom(eventBytes);
      event = new TaskAttemptFailedEvent(tfProto.getDiagnostics());
      break;
    case TASK_ATTEMPT_COMPLETED_EVENT:
      event = new TaskAttemptCompletedEvent();
      break;
    case INPUT_FAILED_EVENT:
      InputFailedEventProto ifProto =
          InputFailedEventProto.parseFrom(eventBytes);
      event = new InputFailedEvent(ifProto.getSourceIndex(),
          ifProto.getTargetIndex(), ifProto.getVersion());
      break;
    case INTPUT_INFORMATION_EVENT:
      InputInformationEventProto infoProto =
          InputInformationEventProto.parseFrom(eventBytes);
      event = new InputInformationEvent(
          infoProto.getUserPayload().toByteArray());
      break;
    default:
      throw new TezUncheckedException("Unknown TezEvent"
         + ", type=" + eventType);
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
      sourceInfo = new EventMetaData();
      sourceInfo.readFields(in);
    }
    if (in.readBoolean()) {
      destinationInfo = new EventMetaData();
      destinationInfo.readFields(in);
    }
  }

}

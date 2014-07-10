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

import org.apache.hadoop.io.Writable;
import org.apache.tez.common.ProtoConverters;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.EventProtos;
import org.apache.tez.runtime.api.events.EventProtos.CompositeEventProto;
import org.apache.tez.runtime.api.events.EventProtos.DataMovementEventProto;
import org.apache.tez.runtime.api.events.EventProtos.InputFailedEventProto;
import org.apache.tez.runtime.api.events.EventProtos.InputReadErrorEventProto;
import org.apache.tez.runtime.api.events.EventProtos.RootInputDataInformationEventProto;
import org.apache.tez.runtime.api.events.EventProtos.VertexManagerEventProto;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputInitializerEvent;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskAttemptCompletedEventProto;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskAttemptFailedEventProto;

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
    } else if (event instanceof CompositeDataMovementEvent) {
      eventType = EventType.COMPOSITE_DATA_MOVEMENT_EVENT;
    } else if (event instanceof VertexManagerEvent) {
      eventType = EventType.VERTEX_MANAGER_EVENT;
    } else if (event instanceof InputReadErrorEvent) {
      eventType = EventType.INPUT_READ_ERROR_EVENT;
    } else if (event instanceof TaskAttemptFailedEvent) {
      eventType = EventType.TASK_ATTEMPT_FAILED_EVENT;
    } else if (event instanceof TaskAttemptCompletedEvent) {
      eventType = EventType.TASK_ATTEMPT_COMPLETED_EVENT;
    } else if (event instanceof InputFailedEvent) {
      eventType = EventType.INPUT_FAILED_EVENT;
    } else if (event instanceof TaskStatusUpdateEvent) {
      eventType = EventType.TASK_STATUS_UPDATE_EVENT;
    } else if (event instanceof RootInputDataInformationEvent) {
      eventType = EventType.ROOT_INPUT_DATA_INFORMATION_EVENT;
    } else if (event instanceof RootInputInitializerEvent) {
      eventType = EventType.ROOT_INPUT_INITIALIZER_EVENT;
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
    out.writeInt(eventType.ordinal());
    if (eventType.equals(EventType.TASK_STATUS_UPDATE_EVENT)) {
      // TODO NEWTEZ convert to PB
      TaskStatusUpdateEvent sEvt = (TaskStatusUpdateEvent) event;
      sEvt.write(out);
    } else {
      byte[] eventBytes = null;
      switch (eventType) {
      case DATA_MOVEMENT_EVENT:
        eventBytes =
            ProtoConverters.convertDataMovementEventToProto(
                (DataMovementEvent) event).toByteArray();
        break;
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        eventBytes =
            ProtoConverters.convertCompositeDataMovementEventToProto(
                (CompositeDataMovementEvent) event).toByteArray();
        break;
      case VERTEX_MANAGER_EVENT:
        VertexManagerEvent vmEvt = (VertexManagerEvent) event;
        VertexManagerEventProto.Builder vmBuilder = VertexManagerEventProto.newBuilder();
        vmBuilder.setTargetVertexName(vmEvt.getTargetVertexName());
        if (vmEvt.getUserPayload() != null) {
          vmBuilder.setUserPayload(ByteString.copyFrom(vmEvt.getUserPayload()));
        }
        eventBytes = vmBuilder.build().toByteArray();
        break;
      case INPUT_READ_ERROR_EVENT:
        InputReadErrorEvent ideEvt = (InputReadErrorEvent) event;
        eventBytes = InputReadErrorEventProto.newBuilder()
            .setIndex(ideEvt.getIndex())
            .setDiagnostics(ideEvt.getDiagnostics())
            .setVersion(ideEvt.getVersion())
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
            .setTargetIndex(ifEvt.getTargetIndex())
            .setVersion(ifEvt.getVersion()).build().toByteArray();
        break;
      case ROOT_INPUT_DATA_INFORMATION_EVENT:
        eventBytes = ProtoConverters.convertRootInputDataInformationEventToProto(
            (RootInputDataInformationEvent) event).toByteArray();
        break;
      case ROOT_INPUT_INITIALIZER_EVENT:
        eventBytes = ProtoConverters
            .convertRootInputInitializerEventToProto((RootInputInitializerEvent) event)
            .toByteArray();
        break;
      default:
        throw new TezUncheckedException("Unknown TezEvent"
           + ", type=" + eventType);
      }
      out.writeInt(eventBytes.length);
      out.write(eventBytes);
    }
  }

  private void deserializeEvent(DataInput in) throws IOException {
    if (!in.readBoolean()) {
      event = null;
      return;
    }
    eventType = EventType.values()[in.readInt()];
    if (eventType.equals(EventType.TASK_STATUS_UPDATE_EVENT)) {
      // TODO NEWTEZ convert to PB
      event = new TaskStatusUpdateEvent();
      ((TaskStatusUpdateEvent)event).readFields(in);
    } else {
      int eventBytesLen = in.readInt();
      byte[] eventBytes = new byte[eventBytesLen];
      in.readFully(eventBytes);
      switch (eventType) {
      case DATA_MOVEMENT_EVENT:
        DataMovementEventProto dmProto =
            DataMovementEventProto.parseFrom(eventBytes);
        event = ProtoConverters.convertDataMovementEventFromProto(dmProto);
        break;
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        CompositeEventProto cProto = CompositeEventProto.parseFrom(eventBytes);
        event = ProtoConverters.convertCompositeDataMovementEventFromProto(cProto);
        break;
      case VERTEX_MANAGER_EVENT:
        VertexManagerEventProto vmProto =
            VertexManagerEventProto.parseFrom(eventBytes);
        event = new VertexManagerEvent(vmProto.getTargetVertexName(),
            vmProto.getUserPayload() != null ? vmProto.getUserPayload().toByteArray() : null);
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
        event = new InputFailedEvent(ifProto.getTargetIndex(), ifProto.getVersion());
        break;
      case ROOT_INPUT_DATA_INFORMATION_EVENT:
        RootInputDataInformationEventProto difProto = RootInputDataInformationEventProto
            .parseFrom(eventBytes);
        event = ProtoConverters.convertRootInputDataInformationEventFromProto(difProto);
        break;
      case ROOT_INPUT_INITIALIZER_EVENT:
        EventProtos.RootInputInitializerEventProto riiProto = EventProtos.RootInputInitializerEventProto.parseFrom(eventBytes);
        event = ProtoConverters.convertRootInputInitializerEventFromProto(riiProto);
        break;
      default:
        // RootInputUpdatePayload event not wrapped in a TezEvent.
        throw new TezUncheckedException("Unexpected TezEvent"
           + ", type=" + eventType);
      }
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

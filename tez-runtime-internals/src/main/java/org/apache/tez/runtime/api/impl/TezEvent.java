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
import java.io.OutputStream;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.tez.common.ProtoConverters;
import org.apache.tez.common.TezConverterUtils;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.CompositeRoutedDataMovementEvent;
import org.apache.tez.runtime.api.events.EventProtos;
import org.apache.tez.runtime.api.events.EventProtos.CompositeEventProto;
import org.apache.tez.runtime.api.events.EventProtos.DataMovementEventProto;
import org.apache.tez.runtime.api.events.EventProtos.CompositeRoutedDataMovementEventProto;
import org.apache.tez.runtime.api.events.EventProtos.InputFailedEventProto;
import org.apache.tez.runtime.api.events.EventProtos.InputReadErrorEventProto;
import org.apache.tez.runtime.api.events.EventProtos.RootInputDataInformationEventProto;
import org.apache.tez.runtime.api.events.EventProtos.VertexManagerEventProto;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptKilledEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskAttemptCompletedEventProto;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskAttemptFailedEventProto;
import org.apache.tez.runtime.internals.api.events.SystemEventProtos.TaskAttemptKilledEventProto;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import static org.apache.tez.runtime.api.events.EventProtos.*;

public class TezEvent implements Writable {

  private EventType eventType;

  private Event event;

  private EventMetaData sourceInfo;

  private EventMetaData destinationInfo;

  private long eventReceivedTime;

  public TezEvent() {
  }

  public TezEvent(Event event, EventMetaData sourceInfo) {
    this(event, sourceInfo, System.currentTimeMillis());
  }

  public TezEvent(Event event, EventMetaData sourceInfo, long time) {
    this.event = event;
    this.eventReceivedTime = time;
    this.setSourceInfo(sourceInfo);
    if (event instanceof DataMovementEvent) {
      eventType = EventType.DATA_MOVEMENT_EVENT;
    } else if (event instanceof CustomProcessorEvent) {
      eventType = EventType.CUSTOM_PROCESSOR_EVENT;
    } else if (event instanceof CompositeDataMovementEvent) {
      eventType = EventType.COMPOSITE_DATA_MOVEMENT_EVENT;
    } else if (event instanceof CompositeRoutedDataMovementEvent) {
      eventType = EventType.COMPOSITE_ROUTED_DATA_MOVEMENT_EVENT;
    } else if (event instanceof VertexManagerEvent) {
      eventType = EventType.VERTEX_MANAGER_EVENT;
    } else if (event instanceof InputReadErrorEvent) {
      eventType = EventType.INPUT_READ_ERROR_EVENT;
    } else if (event instanceof TaskAttemptFailedEvent) {
      eventType = EventType.TASK_ATTEMPT_FAILED_EVENT;
    } else if (event instanceof TaskAttemptKilledEvent) {
      eventType = EventType.TASK_ATTEMPT_KILLED_EVENT;
    } else if (event instanceof TaskAttemptCompletedEvent) {
      eventType = EventType.TASK_ATTEMPT_COMPLETED_EVENT;
    } else if (event instanceof InputFailedEvent) {
      eventType = EventType.INPUT_FAILED_EVENT;
    } else if (event instanceof TaskStatusUpdateEvent) {
      eventType = EventType.TASK_STATUS_UPDATE_EVENT;
    } else if (event instanceof InputDataInformationEvent) {
      eventType = EventType.ROOT_INPUT_DATA_INFORMATION_EVENT;
    } else if (event instanceof InputInitializerEvent) {
      eventType = EventType.ROOT_INPUT_INITIALIZER_EVENT;
    } else {
      throw new TezUncheckedException("Unknown event, event="
          + event.getClass().getName());
    }
  }

  public Event getEvent() {
    return event;
  }

  public void setEventReceivedTime(long eventReceivedTime) { // TODO save
    this.eventReceivedTime = eventReceivedTime;
  }

  public long getEventReceivedTime() {
    return eventReceivedTime;
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
    out.writeLong(eventReceivedTime);
    if (eventType.equals(EventType.TASK_STATUS_UPDATE_EVENT)) {
      // TODO NEWTEZ convert to PB
      TaskStatusUpdateEvent sEvt = (TaskStatusUpdateEvent) event;
      sEvt.write(out);
    } else {
      AbstractMessage message;
      switch (eventType) {
      case CUSTOM_PROCESSOR_EVENT:
        message =
            ProtoConverters.convertCustomProcessorEventToProto(
                (CustomProcessorEvent) event);
        break;
      case DATA_MOVEMENT_EVENT:
        message =
            ProtoConverters.convertDataMovementEventToProto(
                (DataMovementEvent) event);
        break;
      case COMPOSITE_ROUTED_DATA_MOVEMENT_EVENT:
          message =
            ProtoConverters.convertCompositeRoutedDataMovementEventToProto(
                (CompositeRoutedDataMovementEvent) event);
      break;
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        message =
            ProtoConverters.convertCompositeDataMovementEventToProto(
                (CompositeDataMovementEvent) event);
        break;
      case VERTEX_MANAGER_EVENT:
        message = ProtoConverters.convertVertexManagerEventToProto((VertexManagerEvent) event);
        break;
      case INPUT_READ_ERROR_EVENT:
        InputReadErrorEvent ideEvt = (InputReadErrorEvent) event;
        message = InputReadErrorEventProto.newBuilder()
            .setIndex(ideEvt.getIndex())
            .setDiagnostics(ideEvt.getDiagnostics())
            .setVersion(ideEvt.getVersion())
            .build();
        break;
      case TASK_ATTEMPT_FAILED_EVENT:
        TaskAttemptFailedEvent tfEvt = (TaskAttemptFailedEvent) event;
        message = TaskAttemptFailedEventProto.newBuilder()
            .setDiagnostics(tfEvt.getDiagnostics())
            .setTaskFailureType(TezConverterUtils.failureTypeToProto(tfEvt.getTaskFailureType()))
            .build();
        break;
        case TASK_ATTEMPT_KILLED_EVENT:
          TaskAttemptKilledEvent tkEvent = (TaskAttemptKilledEvent) event;
          message = TaskAttemptKilledEventProto.newBuilder()
              .setDiagnostics(tkEvent.getDiagnostics()).build();
          break;
      case TASK_ATTEMPT_COMPLETED_EVENT:
        message = TaskAttemptCompletedEventProto.newBuilder()
            .build();
        break;
      case INPUT_FAILED_EVENT:
        InputFailedEvent ifEvt = (InputFailedEvent) event;
        message = InputFailedEventProto.newBuilder()
            .setTargetIndex(ifEvt.getTargetIndex())
            .setVersion(ifEvt.getVersion()).build();
        break;
      case ROOT_INPUT_DATA_INFORMATION_EVENT:
        message = ProtoConverters.convertRootInputDataInformationEventToProto(
            (InputDataInformationEvent) event);
        break;
      case ROOT_INPUT_INITIALIZER_EVENT:
        message = ProtoConverters
            .convertRootInputInitializerEventToProto((InputInitializerEvent) event);
        break;
      default:
        throw new TezUncheckedException("Unknown TezEvent"
           + ", type=" + eventType);
      }
      if (out instanceof OutputStream) { //DataOutputBuffer extends DataOutputStream
        int serializedSize = message.getSerializedSize();
        out.writeInt(serializedSize);
        int buffersize = serializedSize < CodedOutputStream.DEFAULT_BUFFER_SIZE ? serializedSize
            : CodedOutputStream.DEFAULT_BUFFER_SIZE;
        CodedOutputStream codedOut = CodedOutputStream.newInstance(
            (OutputStream) out, buffersize);
        message.writeTo(codedOut);
        codedOut.flush();
      } else {
        byte[] eventBytes = message.toByteArray();
        out.writeInt(eventBytes.length);
        out.write(eventBytes);
      }

    }
  }

  private void deserializeEvent(DataInput in) throws IOException {
    if (!in.readBoolean()) {
      event = null;
      return;
    }
    eventType = EventType.values()[in.readInt()];
    eventReceivedTime = in.readLong();
    if (eventType.equals(EventType.TASK_STATUS_UPDATE_EVENT)) {
      // TODO NEWTEZ convert to PB
      event = new TaskStatusUpdateEvent();
      ((TaskStatusUpdateEvent)event).readFields(in);
    } else {
      int eventBytesLen = in.readInt();
      byte[] eventBytes;
      CodedInputStream input;
      int startOffset = 0;
      if (in instanceof DataInputBuffer) {
        eventBytes = ((DataInputBuffer)in).getData();
        startOffset = ((DataInputBuffer) in).getPosition();
      } else {
        eventBytes = new byte[eventBytesLen];
        in.readFully(eventBytes);
      }
      input = CodedInputStream.newInstance(eventBytes, startOffset, eventBytesLen);
      switch (eventType) {
      case CUSTOM_PROCESSOR_EVENT:
        CustomProcessorEventProto cpProto =
            CustomProcessorEventProto.parseFrom(input);
        event = ProtoConverters.convertCustomProcessorEventFromProto(cpProto);
        break;
      case DATA_MOVEMENT_EVENT:
        DataMovementEventProto dmProto =
            DataMovementEventProto.parseFrom(input);
        event = ProtoConverters.convertDataMovementEventFromProto(dmProto);
        break;
      case COMPOSITE_ROUTED_DATA_MOVEMENT_EVENT:
        CompositeRoutedDataMovementEventProto edmProto =
            CompositeRoutedDataMovementEventProto.parseFrom(eventBytes);
      event = ProtoConverters.convertCompositeRoutedDataMovementEventFromProto(edmProto);
      break;
      case COMPOSITE_DATA_MOVEMENT_EVENT:
        CompositeEventProto cProto = CompositeEventProto.parseFrom(input);
        event = ProtoConverters.convertCompositeDataMovementEventFromProto(cProto);
        break;
      case VERTEX_MANAGER_EVENT:
        VertexManagerEventProto vmProto = VertexManagerEventProto.parseFrom(input);
        event = ProtoConverters.convertVertexManagerEventFromProto(vmProto);
        break;
      case INPUT_READ_ERROR_EVENT:
        InputReadErrorEventProto ideProto =
            InputReadErrorEventProto.parseFrom(input);
        event = InputReadErrorEvent.create(ideProto.getDiagnostics(),
            ideProto.getIndex(), ideProto.getVersion());
        break;
      case TASK_ATTEMPT_FAILED_EVENT:
        TaskAttemptFailedEventProto tfProto =
            TaskAttemptFailedEventProto.parseFrom(input);
        event = new TaskAttemptFailedEvent(tfProto.getDiagnostics(),
            TezConverterUtils.failureTypeFromProto(tfProto.getTaskFailureType()));
        break;
      case TASK_ATTEMPT_KILLED_EVENT:
        TaskAttemptKilledEventProto tkProto = TaskAttemptKilledEventProto.parseFrom(input);
        event = new TaskAttemptKilledEvent(tkProto.getDiagnostics());
        break;
      case TASK_ATTEMPT_COMPLETED_EVENT:
        event = new TaskAttemptCompletedEvent();
        break;
      case INPUT_FAILED_EVENT:
        InputFailedEventProto ifProto =
            InputFailedEventProto.parseFrom(input);
        event = InputFailedEvent.create(ifProto.getTargetIndex(), ifProto.getVersion());
        break;
      case ROOT_INPUT_DATA_INFORMATION_EVENT:
        RootInputDataInformationEventProto difProto = RootInputDataInformationEventProto
            .parseFrom(input);
        event = ProtoConverters.convertRootInputDataInformationEventFromProto(difProto);
        break;
      case ROOT_INPUT_INITIALIZER_EVENT:
        EventProtos.RootInputInitializerEventProto riiProto = EventProtos.RootInputInitializerEventProto.parseFrom(input);
        event = ProtoConverters.convertRootInputInitializerEventFromProto(riiProto);
        break;
      default:
        // RootInputUpdatePayload event not wrapped in a TezEvent.
        throw new TezUncheckedException("Unexpected TezEvent"
           + ", type=" + eventType);
      }
      if (in instanceof DataInputBuffer) {
        // Skip so that position is updated
        int skipped = in.skipBytes(eventBytesLen);
        if (skipped != eventBytesLen) {
          throw new TezUncheckedException("Expected to skip " + eventBytesLen + " bytes. Actually skipped = " + skipped);
        }
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

  @Override
  public String toString() {
    return "TezEvent{" +
        "eventType=" + eventType +
        ", sourceInfo=" + sourceInfo +
        ", destinationInfo=" + destinationInfo +
        '}';
  }
}

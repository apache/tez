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

package org.apache.tez.dag.history.events;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.common.ProtoConverters;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TezDataMovementEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexDataMovementEventsGeneratedProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;

public class VertexDataMovementEventsGeneratedEvent implements HistoryEvent {

  private static final Log LOG = LogFactory.getLog(
      VertexDataMovementEventsGeneratedEvent.class);
  private List<TezEvent> events;
  private TezVertexID vertexID;

  public VertexDataMovementEventsGeneratedEvent(TezVertexID vertexID,
      List<TezEvent> events) {
    this.vertexID = vertexID;
    this.events = Lists.newArrayListWithCapacity(events.size());
    for (TezEvent event : events) {
      if (EnumSet.of(EventType.DATA_MOVEMENT_EVENT,
          EventType.COMPOSITE_DATA_MOVEMENT_EVENT,
          EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)
              .contains(event.getEventType())) {
        this.events.add(event);
      }
    }
    if (events.isEmpty()) {
      throw new RuntimeException("Invalid creation of VertexDataMovementEventsGeneratedEvent"
        + ", no data movement/information events provided");
    }
  }

  public VertexDataMovementEventsGeneratedEvent() {
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_DATA_MOVEMENT_EVENTS_GENERATED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    return null;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return false;
  }

  static RecoveryProtos.EventMetaDataProto convertEventMetaDataToProto(
      EventMetaData eventMetaData) {
    RecoveryProtos.EventMetaDataProto.Builder builder =
        RecoveryProtos.EventMetaDataProto.newBuilder()
        .setProducerConsumerType(eventMetaData.getEventGenerator().ordinal())
        .setEdgeVertexName(eventMetaData.getEdgeVertexName())
        .setTaskVertexName(eventMetaData.getTaskVertexName());
    if (eventMetaData.getTaskAttemptID() != null) {
        builder.setTaskAttemptId(eventMetaData.getTaskAttemptID().toString());
    }
    return builder.build();
  }

  static EventMetaData convertEventMetaDataFromProto(
      RecoveryProtos.EventMetaDataProto proto) {
    TezTaskAttemptID attemptID = null;
    if (proto.hasTaskAttemptId()) {
      attemptID = TezTaskAttemptID.fromString(proto.getTaskAttemptId());
    }
    return new EventMetaData(
        EventMetaData.EventProducerConsumerType.values()[proto.getProducerConsumerType()],
        proto.getTaskVertexName(),
        proto.getEdgeVertexName(),
        attemptID);
  }

  public VertexDataMovementEventsGeneratedProto toProto() {
    List<TezDataMovementEventProto> tezEventProtos = null;
    if (events != null) {
      tezEventProtos = Lists.newArrayListWithCapacity(events.size());
      for (TezEvent event : events) {
        TezDataMovementEventProto.Builder evtBuilder =
            TezDataMovementEventProto.newBuilder();
        if (event.getEventType().equals(EventType.COMPOSITE_DATA_MOVEMENT_EVENT)) {
          evtBuilder.setCompositeDataMovementEvent(
              ProtoConverters.convertCompositeDataMovementEventToProto(
                  (CompositeDataMovementEvent) event.getEvent()));
        } else if (event.getEventType().equals(EventType.DATA_MOVEMENT_EVENT)) {
          evtBuilder.setDataMovementEvent(
              ProtoConverters.convertDataMovementEventToProto(
                  (DataMovementEvent) event.getEvent()));
        } else if (event.getEventType().equals(EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)) {
          evtBuilder.setRootInputDataInformationEvent(
              ProtoConverters.convertRootInputDataInformationEventToProto(
                  (RootInputDataInformationEvent) event.getEvent()));
        }
        if (event.getSourceInfo() != null) {
          evtBuilder.setSourceInfo(convertEventMetaDataToProto(event.getSourceInfo()));
        }
        if (event.getDestinationInfo() != null) {
          evtBuilder.setDestinationInfo(convertEventMetaDataToProto(event.getDestinationInfo()));
        }
        tezEventProtos.add(evtBuilder.build());
      }
    }
    return VertexDataMovementEventsGeneratedProto.newBuilder()
        .setVertexId(vertexID.toString())
        .addAllTezDataMovementEvent(tezEventProtos)
        .build();
  }

  public void fromProto(VertexDataMovementEventsGeneratedProto proto) {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    int eventCount = proto.getTezDataMovementEventCount();
    if (eventCount > 0) {
      this.events = Lists.newArrayListWithCapacity(eventCount);
    }
    for (TezDataMovementEventProto eventProto :
        proto.getTezDataMovementEventList()) {
      Event evt = null;
      if (eventProto.hasCompositeDataMovementEvent()) {
        evt = ProtoConverters.convertCompositeDataMovementEventFromProto(
            eventProto.getCompositeDataMovementEvent());
      } else if (eventProto.hasDataMovementEvent()) {
        evt = ProtoConverters.convertDataMovementEventFromProto(
            eventProto.getDataMovementEvent());
      } else if (eventProto.hasRootInputDataInformationEvent()) {
        evt = ProtoConverters.convertRootInputDataInformationEventFromProto(
            eventProto.getRootInputDataInformationEvent());
      }
      EventMetaData sourceInfo = null;
      EventMetaData destinationInfo = null;
      if (eventProto.hasSourceInfo()) {
        sourceInfo = convertEventMetaDataFromProto(eventProto.getSourceInfo());
      }
      if (eventProto.hasDestinationInfo()) {
        destinationInfo = convertEventMetaDataFromProto(eventProto.getDestinationInfo());
      }
      TezEvent tezEvent = new TezEvent(evt, sourceInfo);
      tezEvent.setDestinationInfo(destinationInfo);
      this.events.add(tezEvent);
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    VertexDataMovementEventsGeneratedProto proto =
        VertexDataMovementEventsGeneratedProto.parseDelimitedFrom(inputStream);
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexId=" + vertexID.toString()
        + ", eventCount=" + (events != null ? events.size() : "null");

  }

  public TezVertexID getVertexID() {
    return this.vertexID;
  }

  public List<TezEvent> getTezEvents() {
    return this.events;
  }

}

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
package org.apache.tez.dag.history.utils;

import java.io.IOException;
import org.apache.tez.common.ProtoConverters;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TezEventProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;

public class TezEventUtils {

  public static TezEventProto toProto(TezEvent event) throws IOException {
    TezEventProto.Builder evtBuilder =
        TezEventProto.newBuilder();
    if (event.getEventType().equals(EventType.COMPOSITE_DATA_MOVEMENT_EVENT)) {
      evtBuilder.setCompositeDataMovementEvent(
          ProtoConverters.convertCompositeDataMovementEventToProto(
              (CompositeDataMovementEvent) event.getEvent()));
    } else if (event.getEventType().equals(EventType.DATA_MOVEMENT_EVENT)) {
      evtBuilder.setDataMovementEvent(
          ProtoConverters.convertDataMovementEventToProto(
              (DataMovementEvent) event.getEvent()));
    } else if (event.getEventType().equals(EventType.ROOT_INPUT_INITIALIZER_EVENT)) {
      evtBuilder.setInputInitializerEvent(ProtoConverters
          .convertRootInputInitializerEventToProto((InputInitializerEvent) event.getEvent()));
    } else if (event.getEventType().equals(EventType.VERTEX_MANAGER_EVENT)) {
      evtBuilder.setVmEvent(ProtoConverters
          .convertVertexManagerEventToProto((VertexManagerEvent)event.getEvent()));
    } else if (event.getEventType().equals(EventType.ROOT_INPUT_DATA_INFORMATION_EVENT)) {
      evtBuilder.setRootInputDataInformationEvent(
          ProtoConverters.convertRootInputDataInformationEventToProto(
              (InputDataInformationEvent) event.getEvent()));
    } else {
      throw new IOException("Unsupported TezEvent type:" + event.getEventType());
    }

    if (event.getSourceInfo() != null) {
      evtBuilder.setSourceInfo(convertEventMetaDataToProto(event.getSourceInfo()));
    }
    if (event.getDestinationInfo() != null) {
      evtBuilder.setDestinationInfo(convertEventMetaDataToProto(event.getDestinationInfo()));
    }
    evtBuilder.setEventTime(event.getEventReceivedTime());
    return evtBuilder.build();
  }

  public static TezEvent fromProto(TezEventProto eventProto) throws IOException {
    Event evt = null;
    if (eventProto.hasCompositeDataMovementEvent()) {
      evt = ProtoConverters.convertCompositeDataMovementEventFromProto(
          eventProto.getCompositeDataMovementEvent());
    } else if (eventProto.hasDataMovementEvent()) {
      evt = ProtoConverters.convertDataMovementEventFromProto(
          eventProto.getDataMovementEvent());
    } else if (eventProto.hasInputInitializerEvent()) {
      evt = ProtoConverters.convertRootInputInitializerEventFromProto(
          eventProto.getInputInitializerEvent());
    } else if (eventProto.hasVmEvent()) {
      evt = ProtoConverters.convertVertexManagerEventFromProto(
          eventProto.getVmEvent());
    } else if (eventProto.hasRootInputDataInformationEvent()) {
      evt = ProtoConverters.convertRootInputDataInformationEventFromProto(
          eventProto.getRootInputDataInformationEvent());
    } else {
      throw new IOException("Unsupported TezEvent type");
    }

    EventMetaData sourceInfo = null;
    EventMetaData destinationInfo = null;
    if (eventProto.hasSourceInfo()) {
      sourceInfo = convertEventMetaDataFromProto(eventProto.getSourceInfo());
    }
    if (eventProto.hasDestinationInfo()) {
      destinationInfo = convertEventMetaDataFromProto(eventProto.getDestinationInfo());
    }
    TezEvent tezEvent = new TezEvent(evt, sourceInfo, eventProto.getEventTime());
    tezEvent.setDestinationInfo(destinationInfo);
    return tezEvent;
  }
  
  public static RecoveryProtos.EventMetaDataProto convertEventMetaDataToProto(
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

  public static EventMetaData convertEventMetaDataFromProto(
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
}

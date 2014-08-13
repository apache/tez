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

package org.apache.tez.common;

import com.google.protobuf.ByteString;

import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.EventProtos;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.events.EventProtos.VertexManagerEventProto;

public class ProtoConverters {

  public static EventProtos.DataMovementEventProto convertDataMovementEventToProto(
      DataMovementEvent event) {
    EventProtos.DataMovementEventProto.Builder builder =
        EventProtos.DataMovementEventProto.newBuilder();
    builder.setSourceIndex(event.getSourceIndex()).
        setTargetIndex(event.getTargetIndex()).setVersion(event.getVersion());
    if (event.getUserPayload() != null) {
      builder.setUserPayload(ByteString.copyFrom(event.getUserPayload()));
    }
    return builder.build();
  }

  public static DataMovementEvent convertDataMovementEventFromProto(
      EventProtos.DataMovementEventProto proto) {
    return new DataMovementEvent(proto.getSourceIndex(),
        proto.getTargetIndex(),
        proto.getVersion(),
        proto.getUserPayload() != null ?
            proto.getUserPayload().toByteArray() : null);
  }

  public static EventProtos.CompositeEventProto convertCompositeDataMovementEventToProto(
      CompositeDataMovementEvent event) {
    EventProtos.CompositeEventProto.Builder builder =
        EventProtos.CompositeEventProto.newBuilder();
    builder.setStartIndex(event.getSourceIndexStart());
    builder.setCount(event.getCount());
    if (event.getUserPayload() != null) {
      builder.setUserPayload(ByteString.copyFrom(event.getUserPayload()));
    }
    return builder.build();
  }

  public static CompositeDataMovementEvent convertCompositeDataMovementEventFromProto(
      EventProtos.CompositeEventProto proto) {
    return new CompositeDataMovementEvent(proto.getStartIndex(),
        proto.getCount(),
        proto.hasUserPayload() ? proto.getUserPayload().toByteArray() : null);
  }
  
  public static EventProtos.VertexManagerEventProto convertVertexManagerEventToProto(
      VertexManagerEvent event) {
    EventProtos.VertexManagerEventProto.Builder vmBuilder = VertexManagerEventProto.newBuilder();
    vmBuilder.setTargetVertexName(event.getTargetVertexName());
    if (event.getUserPayload() != null) {
      vmBuilder.setUserPayload(ByteString.copyFrom(event.getUserPayload()));
    }
    return vmBuilder.build();
  }
  
  public static VertexManagerEvent convertVertexManagerEventFromProto(
      EventProtos.VertexManagerEventProto vmProto) {
    return new VertexManagerEvent(vmProto.getTargetVertexName(),
        vmProto.hasUserPayload() ? vmProto.getUserPayload().toByteArray() : null);
  }

  public static EventProtos.RootInputDataInformationEventProto
      convertRootInputDataInformationEventToProto(InputDataInformationEvent event) {
    EventProtos.RootInputDataInformationEventProto.Builder builder =
        EventProtos.RootInputDataInformationEventProto.newBuilder();
    builder.setSourceIndex(event.getSourceIndex());
    builder.setTargetIndex(event.getTargetIndex());
    if (event.getUserPayload() != null) {
      builder.setUserPayload(ByteString.copyFrom(event.getUserPayload()));
    }
    return builder.build();
  }

  public static InputDataInformationEvent
      convertRootInputDataInformationEventFromProto(
      EventProtos.RootInputDataInformationEventProto proto) {
    InputDataInformationEvent diEvent = new InputDataInformationEvent(
        proto.getSourceIndex(), proto.hasUserPayload() ? proto.getUserPayload().toByteArray() : null);
    diEvent.setTargetIndex(proto.getTargetIndex());
    return diEvent;
  }

  public static EventProtos.RootInputInitializerEventProto convertRootInputInitializerEventToProto(
      InputInitializerEvent event) {
    EventProtos.RootInputInitializerEventProto.Builder builder =
        EventProtos.RootInputInitializerEventProto.newBuilder();
    builder.setTargetVertexName(event.getTargetVertexName());
    builder.setTargetInputName(event.getTargetInputName());
    builder.setVersion(event.getVersion());
    if (event.getUserPayload() != null) {
      builder.setUserPayload(ByteString.copyFrom(event.getUserPayload()));
    }
    return builder.build();
  }

  public static InputInitializerEvent convertRootInputInitializerEventFromProto(
      EventProtos.RootInputInitializerEventProto proto) {
    InputInitializerEvent event =
        new InputInitializerEvent(proto.getTargetVertexName(), proto.getTargetInputName(),
            (proto.hasUserPayload() ? proto.getUserPayload().toByteArray() : null),
            proto.getVersion());
    return event;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tez.common;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EntityDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos.TezCountersProto;
import org.apache.tez.dag.api.records.DAGProtos.TezEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezUserPayloadProto;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.IOStatistics;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.ContainerTaskProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.EventMetaDataProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.EventProducerConsumerTypeProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.EventTypeProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.GroupInputSpecProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.HeartbeatRequestProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.HeartbeatResponseProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.IOStatisticsProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.InputSpecProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.OutputSpecProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TaskSpecProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TaskStatisticsProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TaskStatusUpdateEventProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezDAGIDProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezEventProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezLocalResourceProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezTaskAttemptIDProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezTaskIDProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolProtos.TezVertexIDProto;
import org.apache.tez.runtime.internals.protocolPB.TezTaskUmbilicalProtocolUtils;

/**
 * Utility class for converting between Tez Java domain objects and their Protobuf counterparts used
 * in the TaskUmbilicalProtocol.
 */
@Private
public final class TezPBConverters {

  private TezPBConverters() {}

  // --- ID Converters ---

  public static TezDAGIDProto convertToProto(TezDAGID id) {
    return TezDAGIDProto.newBuilder()
        .setClusterTimestamp(id.getApplicationId().getClusterTimestamp())
        .setAppId(id.getApplicationId().getId())
        .setId(id.getId())
        .build();
  }

  public static TezDAGID convertFromProto(TezDAGIDProto proto) {
    return TezDAGID.getInstance(
        ApplicationId.newInstance(proto.getClusterTimestamp(), proto.getAppId()), proto.getId());
  }

  public static TezVertexIDProto convertToProto(TezVertexID id) {
    return TezVertexIDProto.newBuilder()
        .setDagId(convertToProto(id.getDAGID()))
        .setId(id.getId())
        .build();
  }

  public static TezVertexID convertFromProto(TezVertexIDProto proto) {
    return TezVertexID.getInstance(convertFromProto(proto.getDagId()), proto.getId());
  }

  public static TezTaskIDProto convertToProto(TezTaskID id) {
    return TezTaskIDProto.newBuilder()
        .setVertexId(convertToProto(id.getVertexID()))
        .setId(id.getId())
        .build();
  }

  public static TezTaskID convertFromProto(TezTaskIDProto proto) {
    return TezTaskID.getInstance(convertFromProto(proto.getVertexId()), proto.getId());
  }

  public static TezTaskAttemptIDProto convertToProto(TezTaskAttemptID id) {
    return TezTaskAttemptIDProto.newBuilder()
        .setTaskId(convertToProto(id.getTaskID()))
        .setId(id.getId())
        .build();
  }

  public static TezTaskAttemptID convertFromProto(TezTaskAttemptIDProto proto) {
    return TezTaskAttemptID.getInstance(convertFromProto(proto.getTaskId()), proto.getId());
  }

  // --- Entity Converters ---

  public static TezEntityDescriptorProto convertToProto(EntityDescriptor<?> descriptor) {
    TezEntityDescriptorProto.Builder builder = TezEntityDescriptorProto.newBuilder();
    builder.setClassName(descriptor.getClassName());
    if (descriptor.getUserPayload() != null) {
      TezUserPayloadProto.Builder payloadBuilder = TezUserPayloadProto.newBuilder();
      if (descriptor.getUserPayload().getPayload() != null) {
        payloadBuilder.setUserPayload(
            ByteString.copyFrom(descriptor.getUserPayload().getPayload()));
      }
      payloadBuilder.setVersion(descriptor.getUserPayload().getVersion());
      builder.setTezUserPayload(payloadBuilder.build());
    }
    if (descriptor.getHistoryText() != null) {
      builder.setHistoryText(ByteString.copyFromUtf8(descriptor.getHistoryText()));
    }
    return builder.build();
  }

  private static void fillDescriptorFromProto(
      EntityDescriptor<?> descriptor, TezEntityDescriptorProto proto) {
    if (proto.hasTezUserPayload()) {
      descriptor.setUserPayload(
          UserPayload.create(
              !proto.getTezUserPayload().getUserPayload().isEmpty()
                  ? proto.getTezUserPayload().getUserPayload().asReadOnlyByteBuffer()
                  : null,
              proto.getTezUserPayload().getVersion()));
    }
    if (!proto.getHistoryText().isEmpty()) {
      descriptor.setHistoryText(proto.getHistoryText().toStringUtf8());
    }
  }

  public static ProcessorDescriptor convertProcessorDescriptorFromProto(
      TezEntityDescriptorProto proto) {
    ProcessorDescriptor descriptor = ProcessorDescriptor.create(proto.getClassName());
    fillDescriptorFromProto(descriptor, proto);
    return descriptor;
  }

  public static InputDescriptor convertInputDescriptorFromProto(TezEntityDescriptorProto proto) {
    InputDescriptor descriptor = InputDescriptor.create(proto.getClassName());
    fillDescriptorFromProto(descriptor, proto);
    return descriptor;
  }

  public static OutputDescriptor convertOutputDescriptorFromProto(TezEntityDescriptorProto proto) {
    OutputDescriptor descriptor = OutputDescriptor.create(proto.getClassName());
    fillDescriptorFromProto(descriptor, proto);
    return descriptor;
  }

  // --- Spec Converters ---

  public static InputSpecProto convertToProto(InputSpec spec) {
    return InputSpecProto.newBuilder()
        .setSourceVertexName(spec.getSourceVertexName())
        .setPhysicalEdgeCount(spec.getPhysicalEdgeCount())
        .setInputDescriptor(convertToProto(spec.getInputDescriptor()))
        .build();
  }

  public static InputSpec convertFromProto(InputSpecProto proto) {
    return new InputSpec(
        proto.getSourceVertexName(),
        convertInputDescriptorFromProto(proto.getInputDescriptor()),
        proto.getPhysicalEdgeCount());
  }

  public static OutputSpecProto convertToProto(OutputSpec spec) {
    return OutputSpecProto.newBuilder()
        .setDestinationVertexName(spec.getDestinationVertexName())
        .setPhysicalEdgeCount(spec.getPhysicalEdgeCount())
        .setOutputDescriptor(convertToProto(spec.getOutputDescriptor()))
        .build();
  }

  public static OutputSpec convertFromProto(OutputSpecProto proto) {
    return new OutputSpec(
        proto.getDestinationVertexName(),
        convertOutputDescriptorFromProto(proto.getOutputDescriptor()),
        proto.getPhysicalEdgeCount());
  }

  public static GroupInputSpecProto convertToProto(GroupInputSpec spec) {
    return GroupInputSpecProto.newBuilder()
        .setGroupName(spec.getGroupName())
        .addAllGroupVertices(spec.getGroupVertices())
        .setMergedInputDescriptor(convertToProto(spec.getMergedInputDescriptor()))
        .build();
  }

  public static GroupInputSpec convertFromProto(GroupInputSpecProto proto) {
    return new GroupInputSpec(
        proto.getGroupName(),
        new ArrayList<>(proto.getGroupVerticesList()),
        convertInputDescriptorFromProto(proto.getMergedInputDescriptor()));
  }

  public static TaskSpecProto convertToProto(TaskSpec spec) {
    TaskSpecProto.Builder builder =
        TaskSpecProto.newBuilder()
            .setTaskAttemptId(convertToProto(spec.getTaskAttemptID()))
            .setDagName(spec.getDAGName())
            .setVertexName(spec.getVertexName())
            .setVertexParallelism(spec.getVertexParallelism())
            .setProcessorDescriptor(convertToProto(spec.getProcessorDescriptor()));
    for (InputSpec inputSpec : spec.getInputs()) {
      builder.addInputSpecs(convertToProto(inputSpec));
    }
    for (OutputSpec outputSpec : spec.getOutputs()) {
      builder.addOutputSpecs(convertToProto(outputSpec));
    }
    if (spec.getGroupInputs() != null) {
      for (GroupInputSpec group : spec.getGroupInputs()) {
        builder.addGroupInputSpecs(convertToProto(group));
      }
    }
    if (spec.getTaskConf() != null) {
      try {
        builder.setTaskConfBytes(
            TezTaskUmbilicalProtocolUtils.serializeToByteString(spec.getTaskConf()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return builder.build();
  }

  public static TaskSpec convertFromProto(TaskSpecProto proto) {
    List<InputSpec> inputs = new ArrayList<>(proto.getInputSpecsCount());
    for (InputSpecProto isp : proto.getInputSpecsList()) {
      inputs.add(convertFromProto(isp));
    }
    List<OutputSpec> outputs = new ArrayList<>(proto.getOutputSpecsCount());
    for (OutputSpecProto osp : proto.getOutputSpecsList()) {
      outputs.add(convertFromProto(osp));
    }
    List<GroupInputSpec> groups = null;
    if (proto.getGroupInputSpecsCount() > 0) {
      groups = new ArrayList<>(proto.getGroupInputSpecsCount());
      for (GroupInputSpecProto gsp : proto.getGroupInputSpecsList()) {
        groups.add(convertFromProto(gsp));
      }
    }
    Configuration conf = null;
    if (!proto.getTaskConfBytes().isEmpty()) {
      conf = new Configuration(false);
      try {
        TezTaskUmbilicalProtocolUtils.deserializeFromByteString(proto.getTaskConfBytes(), conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new TaskSpec(
        convertFromProto(proto.getTaskAttemptId()),
        proto.getDagName(),
        proto.getVertexName(),
        proto.getVertexParallelism(),
        convertProcessorDescriptorFromProto(proto.getProcessorDescriptor()),
        inputs,
        outputs,
        groups,
        conf);
  }

  public static TezLocalResourceProto convertToProto(TezLocalResource lr) {
    return TezLocalResourceProto.newBuilder()
        .setUri(lr.getUri().toString())
        .setSize(lr.getSize())
        .setTimestamp(lr.getTimestamp())
        .build();
  }

  public static TezLocalResource convertFromProto(TezLocalResourceProto proto) {
    try {
      return new TezLocalResource(new URI(proto.getUri()), proto.getSize(), proto.getTimestamp());
    } catch (java.net.URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static ContainerTaskProto convertToProto(ContainerTask task) {
    ContainerTaskProto.Builder builder =
        ContainerTaskProto.newBuilder()
            .setShouldDie(task.shouldDie())
            .setCredentialsChanged(task.haveCredentialsChanged());
    if (task.getTaskSpec() != null) {
      builder.setTaskSpec(convertToProto(task.getTaskSpec()));
    }
    if (task.getAdditionalResources() != null) {
      for (Map.Entry<String, TezLocalResource> entry : task.getAdditionalResources().entrySet()) {
        builder.putAdditionalResources(entry.getKey(), convertToProto(entry.getValue()));
      }
    }
    if (task.getCredentials() != null) {
      builder.setCredentialsBinary(
          DagTypeConverters.convertCredentialsToProto(task.getCredentials()));
    }
    return builder.build();
  }

  public static ContainerTask convertFromProto(ContainerTaskProto proto) {
    Map<String, TezLocalResource> resources = new HashMap<>();
    for (Map.Entry<String, TezLocalResourceProto> entry :
        proto.getAdditionalResourcesMap().entrySet()) {
      resources.put(entry.getKey(), convertFromProto(entry.getValue()));
    }
    return new ContainerTask(
        proto.hasTaskSpec() ? convertFromProto(proto.getTaskSpec()) : null,
        proto.getShouldDie(),
        resources,
        DagTypeConverters.convertByteStringToCredentials(proto.getCredentialsBinary()),
        proto.getCredentialsChanged());
  }

  // --- Heartbeat & Event Converters ---

  public static EventProducerConsumerTypeProto convertToProto(EventProducerConsumerType type) {
    return EventProducerConsumerTypeProto.valueOf("EPC_" + type.name());
  }

  public static EventProducerConsumerType convertFromProto(EventProducerConsumerTypeProto proto) {
    return EventProducerConsumerType.valueOf(proto.name().substring(4));
  }

  public static EventMetaDataProto convertToProto(EventMetaData meta) {
    EventMetaDataProto.Builder builder =
        EventMetaDataProto.newBuilder()
            .setProducerConsumerType(convertToProto(meta.getEventGenerator()))
            .setTaskVertexName(meta.getTaskVertexName());
    if (meta.getEdgeVertexName() != null) {
      builder.setEdgeVertexName(meta.getEdgeVertexName());
    }
    if (meta.getTaskAttemptID() != null) {
      builder.setTaskAttemptId(convertToProto(meta.getTaskAttemptID()));
    }
    return builder.build();
  }

  public static EventMetaData convertFromProto(EventMetaDataProto proto) {
    return new EventMetaData(
        convertFromProto(proto.getProducerConsumerType()),
        proto.getTaskVertexName(),
        proto.getEdgeVertexName().isEmpty() ? null : proto.getEdgeVertexName(),
        proto.hasTaskAttemptId() ? convertFromProto(proto.getTaskAttemptId()) : null);
  }

  public static IOStatisticsProto convertToProto(IOStatistics stats) {
    return IOStatisticsProto.newBuilder()
        .setDataSize(stats.getDataSize())
        .setItemsProcessed(stats.getItemsProcessed())
        .build();
  }

  public static IOStatistics convertFromProto(IOStatisticsProto proto) {
    IOStatistics stats = new IOStatistics();
    stats.setDataSize(proto.getDataSize());
    stats.setItemsProcessed(proto.getItemsProcessed());
    return stats;
  }

  public static TaskStatisticsProto convertToProto(TaskStatistics stats) {
    TaskStatisticsProto.Builder builder = TaskStatisticsProto.newBuilder();
    for (Map.Entry<String, IOStatistics> entry : stats.getIOStatistics().entrySet()) {
      builder.putIoStatistics(entry.getKey(), convertToProto(entry.getValue()));
    }
    return builder.build();
  }

  public static TaskStatistics convertFromProto(TaskStatisticsProto proto) {
    TaskStatistics stats = new TaskStatistics();
    for (Map.Entry<String, IOStatisticsProto> entry : proto.getIoStatisticsMap().entrySet()) {
      stats.addIO(entry.getKey(), convertFromProto(entry.getValue()));
    }
    return stats;
  }

  public static TezCountersProto convertTezCountersToProto(TezCounters counters) {
    return DagTypeConverters.convertTezCountersToProto(counters);
  }

  public static TezCounters convertTezCountersFromProto(TezCountersProto proto) {
    return DagTypeConverters.convertTezCountersFromProto(proto);
  }

  public static TaskStatusUpdateEventProto convertToProto(TaskStatusUpdateEvent event) {
    TaskStatusUpdateEventProto.Builder builder =
        TaskStatusUpdateEventProto.newBuilder()
            .setProgress(event.getProgress())
            .setProgressNotified(event.getProgressNotified());
    if (event.getCounters() != null) {
      builder.setCounters(convertTezCountersToProto(event.getCounters()));
    }
    if (event.getStatistics() != null) {
      builder.setStatistics(convertToProto(event.getStatistics()));
    }
    return builder.build();
  }

  public static TaskStatusUpdateEvent convertFromProto(TaskStatusUpdateEventProto proto) {
    return new TaskStatusUpdateEvent(
        proto.hasCounters() ? convertTezCountersFromProto(proto.getCounters()) : null,
        proto.getProgress(),
        proto.hasStatistics() ? convertFromProto(proto.getStatistics()) : null,
        proto.getProgressNotified());
  }

  public static TezEventProto convertToProto(TezEvent event) {
    TezEventProto.Builder builder =
        TezEventProto.newBuilder()
            .setEventType(EventTypeProto.valueOf(event.getEventType().name()))
            .setEventReceivedTime(event.getEventReceivedTime());
    if (event.getSourceInfo() != null) {
      builder.setSourceInfo(convertToProto(event.getSourceInfo()));
    }
    if (event.getDestinationInfo() != null) {
      builder.setDestinationInfo(convertToProto(event.getDestinationInfo()));
    }

    // Payload handling
    if (event.getEventType() == EventType.TASK_STATUS_UPDATE_EVENT) {
      builder.setEventPayload(
          convertToProto((TaskStatusUpdateEvent) event.getEvent()).toByteString());
    } else {
      try {
        builder.setEventPayload(TezTaskUmbilicalProtocolUtils.serializeToByteString(event));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return builder.build();
  }

  public static TezEvent convertFromProto(TezEventProto proto) {
    TezEvent event = new TezEvent();
    event.setEventReceivedTime(proto.getEventReceivedTime());
    event.setEventType(EventType.valueOf(proto.getEventType().name()));
    if (proto.hasSourceInfo()) {
      event.setSourceInfo(convertFromProto(proto.getSourceInfo()));
    }
    if (proto.hasDestinationInfo()) {
      event.setDestinationInfo(convertFromProto(proto.getDestinationInfo()));
    }

    if (!proto.getEventPayload().isEmpty()) {
      if (proto.getEventType() == EventTypeProto.TASK_STATUS_UPDATE_EVENT) {
        try {
          TaskStatusUpdateEventProto payloadProto =
              TaskStatusUpdateEventProto.parseFrom(proto.getEventPayload());
          event.setEvent(convertFromProto(payloadProto));
        } catch (org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      } else {
        try {
          TezTaskUmbilicalProtocolUtils.deserializeFromByteString(proto.getEventPayload(), event);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return event;
  }

  // --- Heartbeat Converters ---

  public static HeartbeatRequestProto convertToProto(TezHeartbeatRequest request) {
    HeartbeatRequestProto.Builder builder =
        HeartbeatRequestProto.newBuilder()
            .setRequestId(request.getRequestId())
            .setPreRoutedStartIndex(request.getPreRoutedStartIndex())
            .setContainerIdentifier(request.getContainerIdentifier())
            .setStartIndex(request.getStartIndex())
            .setMaxEvents(request.getMaxEvents())
            .setUsedMemory(request.getUsedMemory());
    if (request.getCurrentTaskAttemptID() != null) {
      builder.setCurrentTaskAttemptId(convertToProto(request.getCurrentTaskAttemptID()));
    }
    if (request.getEvents() != null) {
      for (TezEvent event : request.getEvents()) {
        builder.addEvents(convertToProto(event));
      }
    }
    return builder.build();
  }

  public static TezHeartbeatRequest convertFromProto(HeartbeatRequestProto proto) {
    List<TezEvent> events = new ArrayList<>(proto.getEventsCount());
    for (TezEventProto ep : proto.getEventsList()) {
      events.add(convertFromProto(ep));
    }
    return new TezHeartbeatRequest(
        proto.getRequestId(),
        events,
        proto.getPreRoutedStartIndex(),
        proto.getContainerIdentifier(),
        proto.hasCurrentTaskAttemptId() ? convertFromProto(proto.getCurrentTaskAttemptId()) : null,
        proto.getStartIndex(),
        proto.getMaxEvents(),
        proto.getUsedMemory());
  }

  public static HeartbeatResponseProto convertToProto(TezHeartbeatResponse response) {
    HeartbeatResponseProto.Builder builder =
        HeartbeatResponseProto.newBuilder()
            .setLastRequestId(response.getLastRequestId())
            .setShouldDie(response.shouldDie())
            .setNextFromEventId(response.getNextFromEventId())
            .setNextPreRoutedEventId(response.getNextPreRoutedEventId());
    if (response.getEvents() != null) {
      for (TezEvent event : response.getEvents()) {
        builder.addEvents(convertToProto(event));
      }
    }
    return builder.build();
  }

  public static TezHeartbeatResponse convertFromProto(HeartbeatResponseProto proto) {
    List<TezEvent> events = new ArrayList<>(proto.getEventsCount());
    for (TezEventProto ep : proto.getEventsList()) {
      events.add(convertFromProto(ep));
    }
    TezHeartbeatResponse response = new TezHeartbeatResponse(events);
    response.setLastRequestId(proto.getLastRequestId());
    if (proto.getShouldDie()) {
      response.setShouldDie();
    }
    response.setNextFromEventId(proto.getNextFromEventId());
    response.setNextPreRoutedEventId(proto.getNextPreRoutedEventId());
    return response;
  }
}

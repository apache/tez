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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.app.dag.impl.ServicePluginInfo;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.utils.TezEventUtils;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.records.VertexIDAware;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.TezEventProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexInitializedProto;
import org.apache.tez.runtime.api.impl.TezEvent;

import com.google.common.collect.Lists;

public class VertexInitializedEvent implements HistoryEvent, VertexIDAware {

  private TezVertexID vertexID;
  private String vertexName;
  private long initRequestedTime;
  private long initedTime;
  private int numTasks;
  private String processorName;
  private Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> additionalInputs;
  private List<TezEvent> initGeneratedEvents;
  private ServicePluginInfo servicePluginInfo;

  public VertexInitializedEvent() {
  }

  public VertexInitializedEvent(TezVertexID vertexId,
      String vertexName, long initRequestedTime, long initedTime,
      int numTasks, String processorName,
      Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> additionalInputs,
      List<TezEvent> initGeneratedEvents, ServicePluginInfo servicePluginInfo) {
    this.vertexName = vertexName;
    this.vertexID = vertexId;
    this.initRequestedTime = initRequestedTime;
    this.initedTime = initedTime;
    this.numTasks = numTasks;
    this.processorName = processorName;
    this.additionalInputs = additionalInputs;
    this.initGeneratedEvents = initGeneratedEvents;
    this.servicePluginInfo = servicePluginInfo;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_INITIALIZED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public RecoveryProtos.VertexInitializedProto toProto() throws IOException {
    VertexInitializedProto.Builder builder = VertexInitializedProto.newBuilder();
    if (additionalInputs != null
      && !additionalInputs.isEmpty()) {
      for (RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input :
        additionalInputs.values()) {
        RootInputLeafOutputProto.Builder inputBuilder
            = RootInputLeafOutputProto.newBuilder();
        inputBuilder.setName(input.getName());
        if (input.getControllerDescriptor() != null) {
          inputBuilder.setControllerDescriptor(DagTypeConverters
              .convertToDAGPlan(input.getControllerDescriptor()));
        }
        inputBuilder.setIODescriptor(
            DagTypeConverters.convertToDAGPlan(input.getIODescriptor()));
        builder.addInputs(inputBuilder.build());
      }
    }
    if (initGeneratedEvents != null && !initGeneratedEvents.isEmpty()) {
      for (TezEvent event : initGeneratedEvents) {
        builder.addInitGeneratedEvents(TezEventUtils.toProto(event));
      }
    }
    return builder.setVertexId(vertexID.toString())
        .setVertexName(vertexName)
        .setInitRequestedTime(initRequestedTime)
        .setInitTime(initedTime)
        .setNumTasks(numTasks)
        .build();
  }

  public void fromProto(RecoveryProtos.VertexInitializedProto proto) throws IOException {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.vertexName = proto.getVertexName();
    this.initRequestedTime = proto.getInitRequestedTime();
    this.initedTime = proto.getInitTime();
    this.numTasks = proto.getNumTasks();
    if (proto.getInputsCount() > 0) {
      this.additionalInputs =
          new LinkedHashMap<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>>();
      for (RootInputLeafOutputProto inputProto : proto.getInputsList()) {
        RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> input =
            new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>(
                inputProto.getName(),
                DagTypeConverters.convertInputDescriptorFromDAGPlan(
                    inputProto.getIODescriptor()),
                inputProto.hasControllerDescriptor() ? DagTypeConverters
                    .convertInputInitializerDescriptorFromDAGPlan(inputProto
                        .getControllerDescriptor()) : null);
        additionalInputs.put(input.getName(), input);
      }
    }
    int eventCount = proto.getInitGeneratedEventsCount();
    if (eventCount > 0) {
      this.initGeneratedEvents = Lists.newArrayListWithCapacity(eventCount);
    }
    for (TezEventProto eventProto :
        proto.getInitGeneratedEventsList()) {
      this.initGeneratedEvents.add(TezEventUtils.fromProto(eventProto));
    }
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    VertexInitializedProto proto =
        inputStream.readMessage(VertexInitializedProto.PARSER, ExtensionRegistry.getEmptyRegistry());
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexName=" + vertexName
        + ", vertexId=" + vertexID
        + ", initRequestedTime=" + initRequestedTime
        + ", initedTime=" + initedTime
        + ", numTasks=" + numTasks
        + ", processorName=" + processorName
        + ", additionalInputsCount="
        + (additionalInputs != null ? additionalInputs.size() : 0)
        + ", initGeneratedEventsCount="
        + (initGeneratedEvents != null ? initGeneratedEvents.size() : 0)
        + ", servicePluginInfo="
        + (servicePluginInfo != null ? servicePluginInfo : "null");
  }

  @Override
  public TezVertexID getVertexID() {
    return vertexID;
  }

  public long getInitRequestedTime() {
    return initRequestedTime;
  }

  public long getInitedTime() {
    return initedTime;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public Map<String, RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>> 
    getAdditionalInputs() {
    return additionalInputs;
  }

  public String getProcessorName() {
    return processorName;
  }

  public String getVertexName() {
    return vertexName;
  }

  public List<TezEvent> getInitGeneratedEvents() {
    return initGeneratedEvents;
  }

  public ServicePluginInfo getServicePluginInfo() {
    return servicePluginInfo;
  }

}

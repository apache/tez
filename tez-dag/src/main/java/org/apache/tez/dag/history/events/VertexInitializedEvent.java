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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.records.DAGProtos.RootInputLeafOutputProto;
import org.apache.tez.dag.app.dag.impl.RootInputLeafOutputDescriptor;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexInitializedProto;

public class VertexInitializedEvent implements HistoryEvent {

  private static final Log LOG = LogFactory.getLog(VertexInitializedEvent.class);

  private TezVertexID vertexID;
  private String vertexName;
  private long initRequestedTime;
  private long initedTime;
  private int numTasks;
  private String processorName;
  private Map<String, RootInputLeafOutputDescriptor<InputDescriptor>> additionalInputs;

  public VertexInitializedEvent() {
  }

  public VertexInitializedEvent(TezVertexID vertexId,
      String vertexName, long initRequestedTime, long initedTime,
      int numTasks, String processorName,
      Map<String, RootInputLeafOutputDescriptor<InputDescriptor>> additionalInputs) {
    this.vertexName = vertexName;
    this.vertexID = vertexId;
    this.initRequestedTime = initRequestedTime;
    this.initedTime = initedTime;
    this.numTasks = numTasks;
    this.processorName = processorName;
    this.additionalInputs = additionalInputs;
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

  public RecoveryProtos.VertexInitializedProto toProto() {
    VertexInitializedProto.Builder builder = VertexInitializedProto.newBuilder();
    if (additionalInputs != null
      && !additionalInputs.isEmpty()) {
      for (RootInputLeafOutputDescriptor<InputDescriptor> input :
        additionalInputs.values()) {
        RootInputLeafOutputProto.Builder inputBuilder
            = RootInputLeafOutputProto.newBuilder();
        inputBuilder.setName(input.getEntityName());
        if (input.getInitializerClassName() != null) {
          inputBuilder.setInitializerClassName(input.getInitializerClassName());
        }
        inputBuilder.setEntityDescriptor(
            DagTypeConverters.convertToDAGPlan(input.getDescriptor()));
        builder.addInputs(inputBuilder.build());
      }
    }
    return builder.setVertexId(vertexID.toString())
        .setVertexName(vertexName)
        .setInitRequestedTime(initRequestedTime)
        .setInitTime(initedTime)
        .setNumTasks(numTasks)
        .build();
  }

  public void fromProto(RecoveryProtos.VertexInitializedProto proto) {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.vertexName = proto.getVertexName();
    this.initRequestedTime = proto.getInitRequestedTime();
    this.initedTime = proto.getInitTime();
    this.numTasks = proto.getNumTasks();
    if (proto.getInputsCount() > 0) {
      this.additionalInputs =
          new LinkedHashMap<String, RootInputLeafOutputDescriptor<InputDescriptor>>();
      for (RootInputLeafOutputProto inputProto : proto.getInputsList()) {
        RootInputLeafOutputDescriptor<InputDescriptor> input =
            new RootInputLeafOutputDescriptor<InputDescriptor>(
                inputProto.getName(),
                DagTypeConverters.convertInputDescriptorFromDAGPlan(
                    inputProto.getEntityDescriptor()),
                inputProto.hasInitializerClassName() ?
                    inputProto.getInitializerClassName() : null);
        additionalInputs.put(input.getEntityName(), input);
      }
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    RecoveryProtos.VertexInitializedProto proto =
        RecoveryProtos.VertexInitializedProto.parseDelimitedFrom(inputStream);
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
        + (additionalInputs != null ? additionalInputs.size() : 0);
  }

  public TezVertexID getVertexID() {
    return this.vertexID;
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

  public Map<String, RootInputLeafOutputDescriptor<InputDescriptor>> getAdditionalInputs() {
    return additionalInputs;
  }

  public String getProcessorName() {
    return processorName;
  }

  public String getVertexName() {
    return vertexName;
  }

}

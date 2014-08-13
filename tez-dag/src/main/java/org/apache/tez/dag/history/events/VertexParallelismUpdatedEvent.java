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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.EdgeManagerDescriptorProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.RootInputSpecUpdateProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexParallelismUpdatedProto;
import org.apache.tez.runtime.api.InputSpecUpdate;

import com.google.common.collect.Maps;

public class VertexParallelismUpdatedEvent implements HistoryEvent {

  private TezVertexID vertexID;
  private int numTasks;
  private VertexLocationHint vertexLocationHint;
  private Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers;
  private Map<String, InputSpecUpdate> rootInputSpecUpdates;

  public VertexParallelismUpdatedEvent() {
  }

  public VertexParallelismUpdatedEvent(TezVertexID vertexID,
      int numTasks, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerPluginDescriptor> sourceEdgeManagers,
      Map<String, InputSpecUpdate> rootInputSpecUpdates) {
    this.vertexID = vertexID;
    this.numTasks = numTasks;
    this.vertexLocationHint = vertexLocationHint;
    this.sourceEdgeManagers = sourceEdgeManagers;
    this.rootInputSpecUpdates = rootInputSpecUpdates;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_PARALLELISM_UPDATED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return false;
  }

  public VertexParallelismUpdatedProto toProto() {
    VertexParallelismUpdatedProto.Builder builder =
        VertexParallelismUpdatedProto.newBuilder();
    builder.setVertexId(vertexID.toString())
        .setNumTasks(numTasks);
    if (vertexLocationHint != null) {
      builder.setVertexLocationHint(DagTypeConverters.convertVertexLocationHintToProto(
            this.vertexLocationHint));
    }
    if (sourceEdgeManagers != null) {
      for (Entry<String, EdgeManagerPluginDescriptor> entry :
          sourceEdgeManagers.entrySet()) {
        EdgeManagerDescriptorProto.Builder edgeMgrBuilder =
            EdgeManagerDescriptorProto.newBuilder();
        edgeMgrBuilder.setEdgeName(entry.getKey());
        edgeMgrBuilder.setEntityDescriptor(
            DagTypeConverters.convertToDAGPlan(entry.getValue()));
        builder.addEdgeManagerDescriptors(edgeMgrBuilder.build());
      }
    }
    if (rootInputSpecUpdates != null) {
      for (Entry<String, InputSpecUpdate> entry : rootInputSpecUpdates.entrySet()) {
        RootInputSpecUpdateProto.Builder rootInputSpecUpdateBuilder = RootInputSpecUpdateProto
            .newBuilder();
        rootInputSpecUpdateBuilder.setInputName(entry.getKey());
        rootInputSpecUpdateBuilder.setForAllWorkUnits(entry.getValue().isForAllWorkUnits());
        rootInputSpecUpdateBuilder.addAllNumPhysicalInputs(entry.getValue()
            .getAllNumPhysicalInputs());
        builder.addRootInputSpecUpdates(rootInputSpecUpdateBuilder.build());
      }
    }
    return builder.build();
  }

  public void fromProto(VertexParallelismUpdatedProto proto) {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.numTasks = proto.getNumTasks();
    if (proto.hasVertexLocationHint()) {
      this.vertexLocationHint = DagTypeConverters.convertVertexLocationHintFromProto(
          proto.getVertexLocationHint());
    }
    if (proto.getEdgeManagerDescriptorsCount() > 0) {
      this.sourceEdgeManagers = new HashMap<String, EdgeManagerPluginDescriptor>(
          proto.getEdgeManagerDescriptorsCount());
      for (EdgeManagerDescriptorProto edgeManagerProto :
        proto.getEdgeManagerDescriptorsList()) {
        EdgeManagerPluginDescriptor edgeManagerDescriptor =
            DagTypeConverters.convertEdgeManagerPluginDescriptorFromDAGPlan(
                edgeManagerProto.getEntityDescriptor());
        sourceEdgeManagers.put(edgeManagerProto.getEdgeName(),
            edgeManagerDescriptor);
      }
    }
    if (proto.getRootInputSpecUpdatesCount() > 0) {
      this.rootInputSpecUpdates = Maps.newHashMap();
      for (RootInputSpecUpdateProto rootInputSpecUpdateProto : proto.getRootInputSpecUpdatesList()) {
        InputSpecUpdate specUpdate;
        if (rootInputSpecUpdateProto.getForAllWorkUnits()) {
          specUpdate = InputSpecUpdate
              .createAllTaskInputSpecUpdate(rootInputSpecUpdateProto.getNumPhysicalInputs(0));
        } else {
          specUpdate = InputSpecUpdate
              .createPerTaskInputSpecUpdate(rootInputSpecUpdateProto.getNumPhysicalInputsList());
        }
        this.rootInputSpecUpdates.put(rootInputSpecUpdateProto.getInputName(), specUpdate);
      }
    }
  }

  @Override
  public void toProtoStream(OutputStream outputStream) throws IOException {
    toProto().writeDelimitedTo(outputStream);
  }

  @Override
  public void fromProtoStream(InputStream inputStream) throws IOException {
    VertexParallelismUpdatedProto proto = VertexParallelismUpdatedProto.parseDelimitedFrom(inputStream);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexId=" + vertexID
        + ", numTasks=" + numTasks
        + ", vertexLocationHint=" +
        (vertexLocationHint == null? "null" : vertexLocationHint)
        + ", edgeManagersCount=" +
        (sourceEdgeManagers == null? "null" : sourceEdgeManagers.size()
        + ", rootInputSpecUpdateCount="
        + (rootInputSpecUpdates == null ? "null" : rootInputSpecUpdates.size()));
  }

  public TezVertexID getVertexID() {
    return this.vertexID;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public VertexLocationHint getVertexLocationHint() {
    return vertexLocationHint;
  }

  public Map<String, EdgeManagerPluginDescriptor> getSourceEdgeManagers() {
    return sourceEdgeManagers;
  }
  
  public Map<String, InputSpecUpdate> getRootInputSpecUpdates() {
    return rootInputSpecUpdates;
  }
}

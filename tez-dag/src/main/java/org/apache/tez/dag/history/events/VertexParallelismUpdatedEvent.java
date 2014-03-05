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

import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.EdgeManagerDescriptorProto;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexParallelismUpdatedProto;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class VertexParallelismUpdatedEvent implements HistoryEvent {

  private TezVertexID vertexID;
  private int numTasks;
  private VertexLocationHint vertexLocationHint;
  private Map<String, EdgeManagerDescriptor> sourceEdgeManagers;

  public VertexParallelismUpdatedEvent() {
  }

  public VertexParallelismUpdatedEvent(TezVertexID vertexID,
      int numTasks, VertexLocationHint vertexLocationHint,
      Map<String, EdgeManagerDescriptor> sourceEdgeManagers) {
    this.vertexID = vertexID;
    this.numTasks = numTasks;
    this.vertexLocationHint = vertexLocationHint;
    this.sourceEdgeManagers = sourceEdgeManagers;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_PARALLELISM_UPDATED;
  }

  @Override
  public JSONObject convertToATSJSON() throws JSONException {
    throw new UnsupportedOperationException("VertexParallelismUpdatedEvent"
        + " not a History event");
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
      for (Entry<String, EdgeManagerDescriptor> entry :
          sourceEdgeManagers.entrySet()) {
        EdgeManagerDescriptorProto.Builder edgeMgrBuilder =
            EdgeManagerDescriptorProto.newBuilder();
        edgeMgrBuilder.setEdgeName(entry.getKey());
        edgeMgrBuilder.setEntityDescriptor(
            DagTypeConverters.convertToDAGPlan(entry.getValue()));
        builder.addEdgeManagerDescriptors(edgeMgrBuilder.build());
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
      this.sourceEdgeManagers = new HashMap<String, EdgeManagerDescriptor>(
          proto.getEdgeManagerDescriptorsCount());
      for (EdgeManagerDescriptorProto edgeManagerProto :
        proto.getEdgeManagerDescriptorsList()) {
        EdgeManagerDescriptor edgeManagerDescriptor =
            DagTypeConverters.convertEdgeManagerDescriptorFromDAGPlan(
                edgeManagerProto.getEntityDescriptor());
        sourceEdgeManagers.put(edgeManagerProto.getEdgeName(),
            edgeManagerDescriptor);
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
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexId=" + vertexID
        + ", numTasks=" + numTasks
        + ", vertexLocationHint=" +
        (vertexLocationHint == null? "null" : vertexLocationHint)
        + ", edgeManagersCount=" +
        (sourceEdgeManagers == null? "null" : sourceEdgeManagers.size());
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

  public Map<String, EdgeManagerDescriptor> getSourceEdgeManagers() {
    return sourceEdgeManagers;
  }
}

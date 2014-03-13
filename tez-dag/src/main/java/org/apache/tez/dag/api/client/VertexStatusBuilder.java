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

package org.apache.tez.dag.api.client;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusProto.Builder;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusStateProto;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.dag.VertexState;

public class VertexStatusBuilder extends VertexStatus {

  public VertexStatusBuilder() {
    super(VertexStatusProto.newBuilder());
  }

  public void setState(VertexState state) {
    getBuilder().setState(getProtoState(state));
  }

  public void setDiagnostics(List<String> diagnostics) {
    Builder builder = getBuilder();
    builder.clearDiagnostics();
    builder.addAllDiagnostics(diagnostics);
  }

  public void setProgress(ProgressBuilder progress) {
    getBuilder().setProgress(progress.getProto());
  }

  public void setVertexCounters(TezCounters counters) {
    getBuilder().setVertexCounters(
      DagTypeConverters.convertTezCountersToProto(counters));
  }

  public VertexStatusProto getProto() {
    return getBuilder().build();
  }

  @VisibleForTesting
  static VertexStatusStateProto getProtoState(VertexState state) {
    switch(state) {
      case NEW:
        return VertexStatusStateProto.VERTEX_NEW;
      case INITIALIZING:
        return VertexStatusStateProto.VERTEX_INITIALIZING;
      case RECOVERING:
        return VertexStatusStateProto.VERTEX_NEW;
      case INITED:
        return VertexStatusStateProto.VERTEX_INITED;
      case RUNNING:
        return VertexStatusStateProto.VERTEX_RUNNING;
      case SUCCEEDED:
        return VertexStatusStateProto.VERTEX_SUCCEEDED;
      case FAILED:
        return VertexStatusStateProto.VERTEX_FAILED;
      case KILLED:
        return VertexStatusStateProto.VERTEX_KILLED;
      case TERMINATING:
        return VertexStatusStateProto.VERTEX_TERMINATING;
      case ERROR:
        return VertexStatusStateProto.VERTEX_ERROR;
      default:
        throw new TezUncheckedException("Unsupported value for VertexState : " + state);
    }
  }

  private VertexStatusProto.Builder getBuilder() {
    return (Builder) this.proxy;
  }
}

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

import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusStateProto;
import org.apache.tez.dag.api.records.DAGProtos.StringProgressPairProto;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProto.Builder;
import org.apache.tez.dag.app.dag.DAGState;

public class DAGStatusBuilder extends DAGStatus {

  public DAGStatusBuilder() {
    super(DAGStatusProto.newBuilder(), null);
  }

  public void setState(DAGState state) {
    getBuilder().setState(getProtoState(state));
  }

  public void setDiagnostics(List<String> diagnostics) {
    Builder builder = getBuilder();
    builder.clearDiagnostics();
    builder.addAllDiagnostics(diagnostics);
  }

  public void setDAGProgress(ProgressBuilder progress) {
    getBuilder().setDAGProgress(progress.getProto());
  }

  public void setDAGCounters(TezCounters counters) {
    getBuilder().setDagCounters(
        DagTypeConverters.convertTezCountersToProto(counters));
  }

  public void addVertexProgress(String name, ProgressBuilder progress) {
    StringProgressPairProto.Builder builder = StringProgressPairProto.newBuilder();
    builder.setKey(name);
    builder.setProgress(progress.getProto());
    getBuilder().addVertexProgress(builder.build());
  }

  public DAGStatusProto getProto() {
    return getBuilder().build();
  }

  private DAGStatusStateProto getProtoState(DAGState state) {
    switch(state) {
    case NEW:
    case INITED:
      return DAGStatusStateProto.DAG_INITING;
    case RUNNING:
      return DAGStatusStateProto.DAG_RUNNING;
    case SUCCEEDED:
      return DAGStatusStateProto.DAG_SUCCEEDED;
    case FAILED:
      return DAGStatusStateProto.DAG_FAILED;
    case KILLED:
      return DAGStatusStateProto.DAG_KILLED;
    case TERMINATING:
      return DAGStatusStateProto.DAG_TERMINATING;
    case ERROR:
      return DAGStatusStateProto.DAG_ERROR;
    default:
      throw new TezUncheckedException("Unsupported value for DAGState : " + state);
    }
  }

  private DAGStatusProto.Builder getBuilder() {
    return (Builder) this.proxy;
  }
}

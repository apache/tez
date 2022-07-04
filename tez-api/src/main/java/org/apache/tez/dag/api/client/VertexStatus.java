/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api.client;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.VertexStatusProtoOrBuilder;
import org.apache.tez.dag.api.TezUncheckedException;

/**
 * Describes the status of the {@link Vertex}
 */
@Public
public class VertexStatus {

  public enum State {
    NEW,
    INITIALIZING,
    INITED,
    RUNNING,
    COMMITTING,
    SUCCEEDED,
    FAILED,
    KILLED,
    ERROR,
    TERMINATING
  }

  VertexStatusProtoOrBuilder proxy = null;
  Progress progress = null;
  TezCounters vertexCounters = null;
  private AtomicBoolean countersInitialized = new AtomicBoolean(false);

  public VertexStatus(VertexStatusProtoOrBuilder proxy) {
    this.proxy = proxy;
  }

  public String getId() {
    return proxy.getId();
  }

  public State getState() {
    return getState(proxy.getState());
  }

  @VisibleForTesting
  static State getState(DAGProtos.VertexStatusStateProto stateProto) {
    switch (stateProto) {
      case VERTEX_NEW:
        return VertexStatus.State.NEW;
      case VERTEX_INITIALIZING:
        return VertexStatus.State.INITIALIZING;
      case VERTEX_INITED:
        return VertexStatus.State.INITED;
      case VERTEX_RUNNING:
        return VertexStatus.State.RUNNING;
      case VERTEX_COMMITTING:
        return VertexStatus.State.COMMITTING;
      case VERTEX_SUCCEEDED:
        return VertexStatus.State.SUCCEEDED;
      case VERTEX_FAILED:
        return VertexStatus.State.FAILED;
      case VERTEX_KILLED:
        return VertexStatus.State.KILLED;
      case VERTEX_ERROR:
        return VertexStatus.State.ERROR;
      case VERTEX_TERMINATING:
        return VertexStatus.State.TERMINATING;
      default:
        throw new TezUncheckedException(
            "Unsupported value for VertexStatus.State : " + stateProto);
    }
  }

  public List<String> getDiagnostics() {
    return proxy.getDiagnosticsList();
  }

  public Progress getProgress() {
    if (progress == null && proxy.hasProgress()) {
      progress = new Progress(proxy.getProgress());
    }
    return progress;
  }

  public TezCounters getVertexCounters() {
    if (countersInitialized.get()) {
      return vertexCounters;
    }
    if (proxy.hasVertexCounters()) {
      vertexCounters = DagTypeConverters.convertTezCountersFromProto(
          proxy.getVertexCounters());
    }
    countersInitialized.set(true);
    return vertexCounters;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof VertexStatus) {
      VertexStatus other = (VertexStatus) obj;
      return getState().equals(other.getState())
          && getDiagnostics().equals(other.getDiagnostics())
          && getProgress().equals(other.getProgress())
          &&
          ((getVertexCounters() == null && other.getVertexCounters() == null)
              || getVertexCounters().equals(other.getVertexCounters()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 46021;
    int result = 1;
    result = prime +
        getState().hashCode();

    List<String> diagnostics = getDiagnostics();
    Progress vProgress = getProgress();
    TezCounters counters = getVertexCounters();

    result = prime * result +
        ((diagnostics == null) ? 0 : diagnostics.hashCode());
    result = prime * result +
        ((vProgress == null) ? 0 : vProgress.hashCode());
    result = prime * result +
        ((counters == null) ? 0 : counters.hashCode());

    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("status=" + getState()
        + ", progress=" + getProgress()
        + ", counters="
        + (getVertexCounters() == null ? "null" : getVertexCounters().toString()));
    return sb.toString();
  }
}

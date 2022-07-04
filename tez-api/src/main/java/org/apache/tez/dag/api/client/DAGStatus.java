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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.records.DAGProtos.DAGStatusProtoOrBuilder;
import org.apache.tez.dag.api.records.DAGProtos.StringProgressPairProto;
import org.apache.tez.dag.api.TezUncheckedException;

/**
 * Describes the status of the {@link DAG}
 */
@Public
public class DAGStatus {

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  public enum State {
    SUBMITTED, // Returned from the RM only
    INITING, // This is currently never returned. DAG_INITING is treated as RUNNING.
    RUNNING,
    SUCCEEDED,
    KILLED,
    FAILED,
    ERROR,
  }

  final DagStatusSource source;
  DAGStatusProtoOrBuilder proxy = null;
  Progress progress = null;
  // use LinkedHashMap to ensure the vertex order (TEZ-1065)
  LinkedHashMap<String, Progress> vertexProgress = null;
  TezCounters dagCounters = null;
  AtomicBoolean countersInitialized = new AtomicBoolean(false);

  @InterfaceAudience.Private
  public DAGStatus(DAGStatusProtoOrBuilder proxy, DagStatusSource source) {
    this.proxy = proxy;
    this.source = source;
  }

  public State getState() {
    switch (proxy.getState()) {
      case DAG_SUBMITTED:
        return DAGStatus.State.SUBMITTED;
      // For simplicity, initing/terminating states are presented as running
      case DAG_INITING:
      case DAG_TERMINATING:
      case DAG_RUNNING:
      case DAG_COMMITTING:
        return DAGStatus.State.RUNNING;
      case DAG_SUCCEEDED:
        return DAGStatus.State.SUCCEEDED;
      case DAG_FAILED:
        return DAGStatus.State.FAILED;
      case DAG_KILLED:
        return DAGStatus.State.KILLED;
      case DAG_ERROR:
        return DAGStatus.State.ERROR;
      default:
        throw new TezUncheckedException("Unsupported value for DAGStatus.State : " +
            proxy.getState());
    }
  }

  public boolean isCompleted() {
    State state = getState();
    return (state == State.SUCCEEDED ||
        state == State.FAILED ||
        state == State.KILLED ||
        state == State.ERROR);
  }

  public List<String> getDiagnostics() {
    return proxy.getDiagnosticsList();
  }

  /**
   * Gets overall progress value of the DAG.
   *
   * @return Progress of the DAG. Maybe null when the DAG is not running. Maybe
   *         null when the DAG is running and the application master cannot be
   *         reached - e.g. when the execution platform has restarted the
   *         application master.
   * @see Progress
   */
  public Progress getDAGProgress() {
    if (progress == null && proxy.hasDAGProgress()) {
      progress = new Progress(proxy.getDAGProgress());
    }
    return progress;
  }

  /**
   * Get the progress of a vertex in the DAG
   *
   * @return Progress of the vertex. May be null when the DAG is not running.
   *         Maybe null when the DAG is running and the application master
   *         cannot be reached - e.g. when the execution platform has restarted
   *         the application master.
   * @see Progress
   */
  public Map<String, Progress> getVertexProgress() {
    if (vertexProgress == null) {
      if (proxy.getVertexProgressList() != null) {
        List<StringProgressPairProto> kvList = proxy.getVertexProgressList();
        vertexProgress = new LinkedHashMap<String, Progress>(kvList.size());
        for (StringProgressPairProto kv : kvList) {
          vertexProgress.put(kv.getKey(), new Progress(kv.getProgress()));
        }
      }
    }
    return vertexProgress;
  }

  public TezCounters getDAGCounters() {
    if (countersInitialized.get()) {
      return dagCounters;
    }
    if (proxy.hasDagCounters()) {
      dagCounters = DagTypeConverters.convertTezCountersFromProto(
          proxy.getDagCounters());
    }
    countersInitialized.set(true);
    return dagCounters;
  }

  public long getMemoryUsedByAM() {
    return proxy.getMemoryUsedByAM();
  }

  public long getMemoryUsedByTasks() {
    return proxy.getMemoryUsedByTasks();
  }

  @InterfaceAudience.Private
  DagStatusSource getSource() {
    return this.source;
  }

  @Override
  public boolean equals(Object obj) {
    // Source explicitly exclude from equals
    if (obj instanceof DAGStatus) {
      DAGStatus other = (DAGStatus) obj;
      return getState() == other.getState()
          && getDiagnostics().equals(other.getDiagnostics())
          && getDAGProgress().equals(other.getDAGProgress())
          && getVertexProgress().equals(other.getVertexProgress())
          &&
          ((getDAGCounters() == null && other.getDAGCounters() == null)
              || getDAGCounters().equals(other.getDAGCounters()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    // Source explicitly exclude from hashCode
    final int prime = 44017;
    int result = 1;
    result = prime +
        getState().hashCode();

    List<String> diagnostics = getDiagnostics();
    Progress dagProgress = getDAGProgress();
    Map<String, Progress> vProgress = getVertexProgress();
    TezCounters counters = getDAGCounters();

    result = prime * result +
        ((diagnostics == null) ? 0 : diagnostics.hashCode());
    result = prime * result +
        ((dagProgress == null) ? 0 : dagProgress.hashCode());
    result = prime * result +
        ((vProgress == null) ? 0 : vProgress.hashCode());
    result = prime * result +
        ((counters == null) ? 0 : counters.hashCode());

    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("status=" + getState());
    sb.append(", progress=" + getDAGProgress());
    sb.append(", diagnostics=" + StringUtils.join(getDiagnostics(), LINE_SEPARATOR));
    sb.append(", memoryUsedByAM=").append(proxy.getMemoryUsedByAM());
    sb.append(", memoryUsedByTasks=").append(proxy.getMemoryUsedByTasks());
    sb.append(", counters=" + (getDAGCounters() == null ? "null" : getDAGCounters().toString()));
    return sb.toString();
  }
}

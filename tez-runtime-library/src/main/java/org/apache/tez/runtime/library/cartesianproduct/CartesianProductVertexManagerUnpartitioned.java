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
package org.apache.tez.runtime.library.cartesianproduct;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

class CartesianProductVertexManagerUnpartitioned extends CartesianProductVertexManagerReal {
  List<String> sourceVertices;
  private int parallelism = 1;
  private boolean vertexStarted = false;
  private boolean vertexReconfigured = false;
  private int numSourceVertexConfigured = 0;
  private int[] numTasks;
  private Queue<TaskAttemptIdentifier> pendingCompletedSrcTask = new LinkedList<>();
  private Map<String, BitSet> sourceTaskCompleted = new HashMap<>();
  private BitSet scheduledTasks = new BitSet();
  private CartesianProductConfig config;
  private int numSrcHasCompletedTask = 0;

  public CartesianProductVertexManagerUnpartitioned(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductVertexManagerConfig config) throws Exception {
    sourceVertices = config.getSourceVertices();
    numTasks = new int[sourceVertices.size()];
    for (String vertex : sourceVertices) {
      sourceTaskCompleted.put(vertex, new BitSet());
    }
    for (String vertex : sourceVertices) {
      getContext().registerForVertexStateUpdates(vertex, EnumSet.of(VertexState.CONFIGURED));
    }
    this.config = config;
    getContext().vertexReconfigurationPlanned();
  }

  private void reconfigureVertex() throws IOException {
    for (int numTask : numTasks) {
      parallelism *= numTask;
    }

    UserPayload payload = null;
    Map<String, EdgeProperty> edgeProperties = getContext().getInputVertexEdgeProperties();
    for (EdgeProperty edgeProperty : edgeProperties.values()) {
      EdgeManagerPluginDescriptor descriptor = edgeProperty.getEdgeManagerDescriptor();
      if (payload == null) {
        CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
        builder.setIsPartitioned(false).addAllNumTasks(Ints.asList(numTasks))
          .addAllSourceVertices(config.getSourceVertices());
        payload = UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray()));
      }
      descriptor.setUserPayload(payload);
    }
    getContext().reconfigureVertex(parallelism, null, edgeProperties);
    vertexReconfigured = true;
    getContext().doneReconfiguringVertex();
  }

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions)
    throws Exception {
    vertexStarted = true;
    // if vertex is already reconfigured, we can handle pending completions immediately
    // otherwise we have to wait until vertex is reconfigured
    if (vertexReconfigured) {
      Preconditions.checkArgument(pendingCompletedSrcTask.size() == 0,
        "Unexpected pending source completion on vertex start after vertex reconfiguration");
      for (TaskAttemptIdentifier taId : completions) {
        handleCompletedSrcTask(taId);
      }
    } else {
      pendingCompletedSrcTask.addAll(completions);
    }
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws IOException {
    Preconditions.checkArgument(stateUpdate.getVertexState() == VertexState.CONFIGURED);
    String vertex = stateUpdate.getVertexName();
    numTasks[sourceVertices.indexOf(vertex)] = getContext().getVertexNumTasks(vertex);
    // reconfigure vertex when all source vertices are CONFIGURED
    if (++numSourceVertexConfigured == sourceVertices.size()) {
      reconfigureVertex();
      // handle pending source completions when vertex is started and reconfigured
      if (vertexStarted) {
        while (!pendingCompletedSrcTask.isEmpty()) {
          handleCompletedSrcTask(pendingCompletedSrcTask.poll());
        }
      }
    }
  }

  @Override
  public synchronized void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws Exception {
    if (numSourceVertexConfigured < sourceVertices.size()) {
      pendingCompletedSrcTask.add(attempt);
      return;
    }
    Preconditions.checkArgument(pendingCompletedSrcTask.size() == 0,
      "Unexpected pending src completion on source task completed after vertex reconfiguration");
    handleCompletedSrcTask(attempt);
  }

  private void handleCompletedSrcTask(TaskAttemptIdentifier attempt) {
    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();
    if (sourceTaskCompleted.get(vertex).get(taskId)) {
      return;
    }

    if (sourceTaskCompleted.get(vertex).isEmpty()) {
      numSrcHasCompletedTask++;
    }
    sourceTaskCompleted.get(vertex).set(taskId);
    if (numSrcHasCompletedTask != sourceVertices.size()) {
      return;
    }

    List<ScheduleTaskRequest> requests = new ArrayList<>();
    CartesianProductCombination combination = new CartesianProductCombination(numTasks, sourceVertices.indexOf(vertex));
    combination.firstTaskWithFixedPartition(taskId);
    do {
      List<Integer> list = combination.getCombination();
      boolean readyToSchedule = true;
      for (int i = 0; i < list.size(); i++) {
        if (!sourceTaskCompleted.get(sourceVertices.get(i)).get(list.get(i))) {
          readyToSchedule = false;
          break;
        }
      }
      if (readyToSchedule && !scheduledTasks.get(combination.getTaskId())) {
        requests.add(ScheduleTaskRequest.create(combination.getTaskId(), null));
        scheduledTasks.set(combination.getTaskId());
      }
    } while (combination.nextTaskWithFixedPartition());
    if (!requests.isEmpty()) {
      getContext().scheduleTasks(requests);
    }
  }
}
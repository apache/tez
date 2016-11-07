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

import com.google.common.primitives.Ints;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;
import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

class CartesianProductVertexManagerUnpartitioned extends CartesianProductVertexManagerReal {
  List<String> sourceVertices;
  private int parallelism = 1;
  private boolean vertexReconfigured = false;
  private boolean vertexStarted = false;
  private boolean vertexStartSchedule = false;
  private int numCPSrcNotInConfigureState = 0;
  private int numBroadcastSrcNotInRunningState = 0;
  private int[] numTasks;

  private Queue<TaskAttemptIdentifier> completedSrcTaskToProcess = new LinkedList<>();
  private Map<String, RoaringBitmap> sourceTaskCompleted = new HashMap<>();
  private RoaringBitmap scheduledTasks = new RoaringBitmap();
  private CartesianProductConfig config;

  public CartesianProductVertexManagerUnpartitioned(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductVertexManagerConfig config) throws Exception {
    sourceVertices = config.getSourceVertices();
    numTasks = new int[sourceVertices.size()];

    for (String vertex : getContext().getInputVertexEdgeProperties().keySet()) {
      if (sourceVertices.indexOf(vertex) != -1) {
        sourceTaskCompleted.put(vertex, new RoaringBitmap());
        getContext().registerForVertexStateUpdates(vertex, EnumSet.of(VertexState.CONFIGURED));
        numCPSrcNotInConfigureState++;
      } else {
        getContext().registerForVertexStateUpdates(vertex, EnumSet.of(VertexState.RUNNING));
        numBroadcastSrcNotInRunningState++;
      }
    }
    this.config = config;
    getContext().vertexReconfigurationPlanned();
  }

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions)
    throws Exception {
    vertexStarted = true;
    if (completions != null) {
      for (TaskAttemptIdentifier attempt : completions) {
        addCompletedSrcTaskToProcess(attempt);
      }
    }
    tryScheduleTasks();
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws IOException {
    String vertex = stateUpdate.getVertexName();
    VertexState state = stateUpdate.getVertexState();

    if (state == VertexState.CONFIGURED) {
      numTasks[sourceVertices.indexOf(vertex)] = getContext().getVertexNumTasks(vertex);
      numCPSrcNotInConfigureState--;
    } else if (state == VertexState.RUNNING) {
      numBroadcastSrcNotInRunningState--;
    }
    tryScheduleTasks();
  }

  @Override
  public synchronized void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws Exception {
    addCompletedSrcTaskToProcess(attempt);
    tryScheduleTasks();
  }

  private void addCompletedSrcTaskToProcess(TaskAttemptIdentifier attempt) {
    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();
    if (sourceVertices.indexOf(vertex) == -1) {
      return;
    }
    if (sourceTaskCompleted.get(vertex).contains(taskId)) {
      return;
    }
    sourceTaskCompleted.get(vertex).add(taskId);
    completedSrcTaskToProcess.add(attempt);
  }

  private boolean tryStartSchedule() {
    if (!vertexReconfigured || !vertexStarted || numBroadcastSrcNotInRunningState > 0) {
      return false;
    }
    for (RoaringBitmap bitmap: sourceTaskCompleted.values()) {
      if (bitmap.isEmpty()) {
        return false;
      }
    }
    vertexStartSchedule = true;
    return true;
  }

  private boolean tryReconfigure() throws IOException {
    if (numCPSrcNotInConfigureState > 0) {
      return false;
    }

    for (int numTask : numTasks) {
      parallelism *= numTask;
    }

    UserPayload payload = null;
    Map<String, EdgeProperty> edgeProperties = getContext().getInputVertexEdgeProperties();
    Iterator<Map.Entry<String,EdgeProperty>> iter = edgeProperties.entrySet().iterator();
    while (iter.hasNext()) {
      EdgeProperty edgeProperty = iter.next().getValue();
      if (edgeProperty.getDataMovementType() != CUSTOM) {
        iter.remove();
        continue;
      }
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
    return true;
  }

  private void tryScheduleTasks() throws IOException {
    if (!vertexReconfigured && !tryReconfigure()) {
      return;
    }
    if (!vertexStartSchedule && !tryStartSchedule()) {
      return;
    }

    while (!completedSrcTaskToProcess.isEmpty()) {
      scheduledTasksDependOnCompletion(completedSrcTaskToProcess.poll());
    }
  }

  private void scheduledTasksDependOnCompletion(TaskAttemptIdentifier attempt) {
    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();

    List<ScheduleTaskRequest> requests = new ArrayList<>();
    CartesianProductCombination combination =
      new CartesianProductCombination(numTasks, sourceVertices.indexOf(vertex));
    combination.firstTaskWithFixedPartition(taskId);
    do {
      List<Integer> list = combination.getCombination();
      boolean readyToSchedule = true;
      for (int i = 0; i < list.size(); i++) {
        if (!sourceTaskCompleted.get(sourceVertices.get(i)).contains(list.get(i))) {
          readyToSchedule = false;
          break;
        }
      }
      if (readyToSchedule && !scheduledTasks.contains(combination.getTaskId())) {
        requests.add(ScheduleTaskRequest.create(combination.getTaskId(), null));
        scheduledTasks.add(combination.getTaskId());
      }
    } while (combination.nextTaskWithFixedPartition());
    if (!requests.isEmpty()) {
      getContext().scheduleTasks(requests);
    }
  }
}
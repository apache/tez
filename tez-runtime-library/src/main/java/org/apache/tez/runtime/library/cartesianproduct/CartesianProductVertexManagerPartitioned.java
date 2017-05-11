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
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Starts scheduling tasks when number of completed source tasks crosses
 * min fraction and schedules all task when max fraction is reached
 */
class CartesianProductVertexManagerPartitioned extends CartesianProductVertexManagerReal {
  private List<String> sourceVertices;
  private int[] numPartitions;
  private float minFraction, maxFraction;
  private int parallelism = 0;
  private boolean vertexStarted = false;
  private boolean vertexReconfigured = false;
  private int numCPSrcNotInConfiguredState = 0;
  private int numBroadcastSrcNotInRunningState = 0;

  private CartesianProductFilter filter;
  private Map<String, BitSet> sourceTaskCompleted = new HashMap<>();
  private int numFinishedSrcTasks = 0;
  private int totalNumSrcTasks = 0;
  private int lastScheduledTaskId = -1;
  private static final Logger LOG =
    LoggerFactory.getLogger(CartesianProductVertexManagerPartitioned.class);

  public CartesianProductVertexManagerPartitioned(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductConfigProto config) throws TezReflectionException {
    this.sourceVertices = config.getSourcesList();
    this.numPartitions = Ints.toArray(config.getNumPartitionsList());
    this.minFraction = config.hasMinFraction() ? config.getMinFraction()
      : CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MIN_FRACTION_DEFAULT;
    this.maxFraction = config.hasMaxFraction() ? config.getMaxFraction()
      : CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_SLOW_START_MAX_FRACTION_DEFAULT;

    if (config.hasFilterClassName()) {
      UserPayload userPayload = config.hasFilterUserPayload()
        ? UserPayload.create(ByteBuffer.wrap(config.getFilterUserPayload().toByteArray())) : null;
      try {
        filter = ReflectionUtils.createClazzInstance(config.getFilterClassName(),
          new Class[]{UserPayload.class}, new UserPayload[]{userPayload});
      } catch (TezReflectionException e) {
        LOG.error("Creating filter failed");
        throw e;
      }
    }

    for (String sourceVertex : sourceVertices) {
      sourceTaskCompleted.put(sourceVertex, new BitSet());
    }
    for (String vertex : getContext().getInputVertexEdgeProperties().keySet()) {
      if (sourceVertices.indexOf(vertex) != -1) {
        getContext().registerForVertexStateUpdates(vertex, EnumSet.of(VertexState.CONFIGURED));
        numCPSrcNotInConfiguredState++;
      } else {
        getContext().registerForVertexStateUpdates(vertex, EnumSet.of(VertexState.RUNNING));
        numBroadcastSrcNotInRunningState++;
      }
    }
    getContext().vertexReconfigurationPlanned();
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws IOException {}

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions)
    throws Exception {
    vertexStarted = true;
    if (completions != null) {
      for (TaskAttemptIdentifier attempt : completions) {
        onSourceTaskCompleted(attempt);
      }
    }
    // try schedule because there may be no more vertex state update and source completions
    tryScheduleTask();
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws IOException{
    VertexState state = stateUpdate.getVertexState();

    if (state == VertexState.CONFIGURED) {
      if (!vertexReconfigured) {
        reconfigureVertex();
      }
      numCPSrcNotInConfiguredState--;
      totalNumSrcTasks += getContext().getVertexNumTasks(stateUpdate.getVertexName());
    } else if (state == VertexState.RUNNING){
      numBroadcastSrcNotInRunningState--;
    }
    // try schedule because there may be no more vertex start and source completions
    tryScheduleTask();
  }

  @Override
  public synchronized void onSourceTaskCompleted(TaskAttemptIdentifier attempt) throws Exception {
    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();

    if (!sourceTaskCompleted.containsKey(vertex)) {
      return;
    }

    BitSet bitSet = this.sourceTaskCompleted.get(vertex);
    if (!bitSet.get(taskId)) {
      bitSet.set(taskId);
      numFinishedSrcTasks++;
      tryScheduleTask();
    }
  }

  private void reconfigureVertex() throws IOException {
    // try all combinations, check against filter and get final parallelism
    Map<String, Integer> vertexPartitionMap = new HashMap<>();

    CartesianProductCombination combination =
      new CartesianProductCombination(numPartitions);
    combination.firstTask();
    do {
      for (int i = 0; i < sourceVertices.size(); i++) {
        vertexPartitionMap.put(sourceVertices.get(i), combination.getCombination().get(i));
      }
      if (filter == null || filter.isValidCombination(vertexPartitionMap)) {
        parallelism++;
      }
    } while (combination.nextTask());
    // no need to reconfigure EM because EM already has all necessary information via config object
    getContext().reconfigureVertex(parallelism, null, null);
    vertexReconfigured = true;
    getContext().doneReconfiguringVertex();
  }

  /**
   * schedule task as the ascending order of id. Slow start has same behavior as ShuffleVertexManager
   */
  private void tryScheduleTask() {
    // only schedule task when vertex is already started and all source vertices are configured
    if (!vertexStarted || numCPSrcNotInConfiguredState > 0 || numBroadcastSrcNotInRunningState > 0) {
      return;
    }
    // determine the destination task with largest id to schedule
    float percentFinishedSrcTask = numFinishedSrcTasks*1f/totalNumSrcTasks;
    int numTaskToSchedule;
    if (percentFinishedSrcTask < minFraction) {
      numTaskToSchedule = 0;
    } else if (minFraction <= percentFinishedSrcTask &&
        percentFinishedSrcTask <= maxFraction) {
      numTaskToSchedule = (int) ((percentFinishedSrcTask - minFraction)
        /(maxFraction - minFraction) * parallelism);
    } else {
      numTaskToSchedule = parallelism;
    }
    // schedule tasks if there are more we can schedule
    if (numTaskToSchedule-1 > lastScheduledTaskId) {
      List<ScheduleTaskRequest> scheduleTaskRequests = new ArrayList<>();
      for (int i = lastScheduledTaskId + 1; i < numTaskToSchedule; i++) {
        scheduleTaskRequests.add(ScheduleTaskRequest.create(i, null));
      }
      lastScheduledTaskId = numTaskToSchedule-1;
      getContext().scheduleTasks(scheduleTaskRequests);
    }
  }
}
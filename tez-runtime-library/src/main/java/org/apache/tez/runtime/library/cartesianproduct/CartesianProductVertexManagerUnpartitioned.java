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
import com.google.protobuf.ByteString;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;
import org.apache.tez.runtime.library.utils.Grouper;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;
import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

/**
 * In unpartitioned case, we have one destination task for each source chunk combination. A source
 * is a source vertex or a source vertex group. A chunk is one source task (without auto grouping)
 * or a group of source tasks (with auto grouping). A chunk may contains multiple tasks across
 * vertices. The mapping from source chunk to destination task id is done by
 * {@link <CartesianProductCombination>}.
 *
 * If auto grouping is enabled, this vertex manager will estimate output size of each source and
 * group source tasks of each source in chunk according to desired grouping size configured by user.
 *
 *
 */
class CartesianProductVertexManagerUnpartitioned extends CartesianProductVertexManagerReal {
  /**
   * a cartesian product source
   */
  static class Source {
    // list of source vertices of this source
    List<SrcVertex> srcVertices = new ArrayList<>();
    // position of this source in all sources
    int position;
    // name of source vertex or vertex group
    String name;

    // total number of chunks in this source
    public int getNumChunk() {
      int numChunk = 0;
      for (SrcVertex srcV : srcVertices) {
        numChunk += srcV.numChunk;
      }
      return numChunk;
    }

    // whether this source has any task completed
    public boolean hasTaskCompleted() {
      for (SrcVertex srcV : srcVertices) {
        if (!srcV.taskCompleted.isEmpty()) {
          return true;
        }
      }
      return false;
    }

    public String toString(boolean afterReconfigure) {
      StringBuilder sb = new StringBuilder();
      sb.append("Source at position ");
      sb.append(position);
      if (name != null) {
        sb.append(", ");
        sb.append("vertex group ");
        sb.append(name);

      }
      sb.append(": {");
      for (SrcVertex srcV : srcVertices) {
        sb.append("[");
        sb.append(srcV.toString(afterReconfigure));
        sb.append("], ");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.setCharAt(sb.length() - 1, '}');
      return sb.toString();
    }
  }

  /**
   * a cartesian product source vertex
   */
  class SrcVertex {
    // which source this vertex belongs to
    Source source;
    // vertex name
    String name;
    int numTask;
    // num chunks of this source vertex
    int numChunk;
    // offset of chunk id in vertex group
    // we need sequence chunks in the vertex group to make them look like from single vertex
    int chunkIdOffset = 0;
    RoaringBitmap taskCompleted = new RoaringBitmap();
    RoaringBitmap taskWithVMEvent = new RoaringBitmap();
    long outputBytes;

    public void doGrouping() {
      numChunk = numTask;
      if (config.enableAutoGrouping) {
        outputBytes = outputBytes * numTask / taskWithVMEvent.getCardinality();
        numChunk = Math.min(numChunk,
          (int) ((outputBytes + config.desiredBytesPerChunk - 1) / config.desiredBytesPerChunk));
      }
    }

    public String toString(boolean afterReconfigure) {
      StringBuilder sb = new StringBuilder();
      sb.append("vertex ").append(name).append(", ");
      if (afterReconfigure) {
        sb.append("estimated output ").append(outputBytes).append(" bytes, ");
        sb.append(numChunk).append(" chunks");
      } else {
        sb.append(numTask).append(" tasks, ");
        sb.append(taskWithVMEvent.getCardinality()).append(" VMEvents, ");
        sb.append("output ").append(outputBytes).append(" bytes");
      }
      return sb.toString();
    }
  }

  private static final Logger LOG =
    org.slf4j.LoggerFactory.getLogger(CartesianProductVertexManagerUnpartitioned.class);

  CartesianProductVertexManagerConfig config;
  Map<String, Source> sourcesByName = new HashMap<>();
  Map<String, SrcVertex> srcVerticesByName = new HashMap<>();

  private boolean vertexReconfigured = false;
  private boolean vertexStarted = false;
  private boolean vertexStartSchedule = false;
  private int numCPSrcNotInConfigureState = 0;
  private int numBroadcastSrcNotInRunningState = 0;
  private Queue<TaskAttemptIdentifier> completedSrcTaskToProcess = new LinkedList<>();
  private RoaringBitmap scheduledTasks = new RoaringBitmap();

  /* auto reduce related */
  // num of chunks of source at the corresponding position in source list
  private int[] numChunksPerSrc;
  private Set<String> vertexSentVME = new HashSet<>();
  private Grouper grouper = new Grouper();

  public CartesianProductVertexManagerUnpartitioned(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductVertexManagerConfig config) throws Exception {
    for (Map.Entry<String, EdgeProperty> e : getContext().getInputVertexEdgeProperties().entrySet()) {
      if (e.getValue().getDataMovementType() == CUSTOM
        && e.getValue().getEdgeManagerDescriptor().getClassName()
          .equals(CartesianProductEdgeManager.class.getName())) {
        srcVerticesByName.put(e.getKey(), new SrcVertex());
        srcVerticesByName.get(e.getKey()).name = e.getKey();
        getContext().registerForVertexStateUpdates(e.getKey(), EnumSet.of(VertexState.CONFIGURED));
        numCPSrcNotInConfigureState++;
      } else {
        getContext().registerForVertexStateUpdates(e.getKey(), EnumSet.of(VertexState.RUNNING));
        numBroadcastSrcNotInRunningState++;
      }
    }

    Map<String, List<String>> srcGroups = getContext().getInputVertexGroups();
    for (int i = 0; i < config.getSourceVertices().size(); i++) {
      String srcName = config.getSourceVertices().get(i);
      Source source = new Source();
      source.position = i;
      if (srcGroups.containsKey(srcName)) {
        source.name = srcName;
        for (String srcVName : srcGroups.get(srcName)) {
          source.srcVertices.add(srcVerticesByName.get(srcVName));
          srcVerticesByName.get(srcVName).source = source;
        }
      } else {
        source.srcVertices.add(srcVerticesByName.get(srcName));
        srcVerticesByName.get(srcName).source = source;
      }
      sourcesByName.put(srcName, source);
    }

    numChunksPerSrc = new int[sourcesByName.size()];
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
      srcVerticesByName.get(vertex).numTask = getContext().getVertexNumTasks(vertex);
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
    SrcVertex srcV = srcVerticesByName.get(vertex);
    if (srcV != null && !srcV.taskCompleted.contains(taskId)) {
      srcV.taskCompleted.add(taskId);
      completedSrcTaskToProcess.add(attempt);
    }
  }

  private boolean tryStartSchedule() {
    if (!vertexReconfigured || !vertexStarted || numBroadcastSrcNotInRunningState > 0) {
      return false;
    }

    for (Source src : sourcesByName.values()) {
      if (!src.hasTaskCompleted()) {
        return false;
      }
    }
    vertexStartSchedule = true;
    return true;
  }

  public synchronized void onVertexManagerEventReceived(VertexManagerEvent vmEvent)
    throws IOException {
    /* vmEvent after reconfigure doesn't matter */
    if (vertexReconfigured) {
      return;
    }

    if (vmEvent.getUserPayload() != null) {
      String srcVertex =
        vmEvent.getProducerAttemptIdentifier().getTaskIdentifier().getVertexIdentifier().getName();
      SrcVertex srcV = srcVerticesByName.get(srcVertex);

      // vmEvent from non-cp vertex doesn't matter
      if (srcV == null) {
        return;
      }

      VertexManagerEventPayloadProto proto =
        VertexManagerEventPayloadProto.parseFrom(ByteString.copyFrom(vmEvent.getUserPayload()));
      srcV.outputBytes += proto.getOutputSize();
      srcV.taskWithVMEvent.add(vmEvent.getProducerAttemptIdentifier().getTaskIdentifier().getIdentifier());
      vertexSentVME.add(srcVertex);
    }

    tryScheduleTasks();
  }

  private boolean tryReconfigure() throws IOException {
    if (numCPSrcNotInConfigureState > 0) {
      return false;
    }
    if (config.enableAutoGrouping) {
      if (vertexSentVME.size() != srcVerticesByName.size()) {
        return false;
      }
      // every src v must output at least one chunk size
      for (SrcVertex srcV : srcVerticesByName.values()) {
        if (srcV.outputBytes < config.desiredBytesPerChunk
          && srcV.taskWithVMEvent.getCardinality() < srcV.numTask) {
          return false;
        }
      }
    }

    LOG.info("Start reconfigure, grouping: " + config.enableAutoGrouping
      + ", chunk size: " + config.desiredBytesPerChunk + " bytes.");
    for (String srcName : config.getSourceVertices()) {
      LOG.info(sourcesByName.get(srcName).toString(false));
    }

    for (Source src : sourcesByName.values()) {
      for (int i = 0; i < src.srcVertices.size(); i++) {
        src.srcVertices.get(i).doGrouping();
        if (i > 0) {
          src.srcVertices.get(i).chunkIdOffset += src.srcVertices.get(i-1).numChunk;
        }
      }
      numChunksPerSrc[src.position] = src.getNumChunk();
    }

    int parallelism = 1;
    for (Source src : sourcesByName.values()) {
      parallelism *= src.getNumChunk();
    }

    LOG.info("After reconfigure, ");
    for (String srcName : config.getSourceVertices()) {
      LOG.info(sourcesByName.get(srcName).toString(true));
    }
    LOG.info("Final parallelism: " + parallelism);

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder();
    for (int i = 0; i < numChunksPerSrc.length; i++) {
      numChunksPerSrc[i] = sourcesByName.get(config.getSourceVertices().get(i)).getNumChunk();
    }
    builder.setIsPartitioned(false).addAllSources(config.getSourceVertices())
      .addAllNumChunks(Ints.asList(this.numChunksPerSrc));

    Map<String, EdgeProperty> edgeProperties = getContext().getInputVertexEdgeProperties();
    Iterator<Map.Entry<String,EdgeProperty>> iter = edgeProperties.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, EdgeProperty> e = iter.next();
      if (e.getValue().getDataMovementType() != CUSTOM) {
        iter.remove();
      } else {
        SrcVertex srcV = srcVerticesByName.get(e.getKey());
        builder.setNumChunk(srcV.numChunk).setChunkIdOffset(srcV.chunkIdOffset);
        e.getValue().getEdgeManagerDescriptor()
          .setUserPayload(UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray())));
      }
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
      scheduleTasksDependOnCompletion(completedSrcTaskToProcess.poll());
    }
  }

  private void scheduleTasksDependOnCompletion(TaskAttemptIdentifier attempt) {
    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();
    SrcVertex srcV = srcVerticesByName.get(vertex);
    Source src = srcV.source;

    List<ScheduleTaskRequest> requests = new ArrayList<>();
    CartesianProductCombination combination =
      new CartesianProductCombination(numChunksPerSrc, src.position);
    grouper.init(srcV.numTask, srcV.numChunk);
    combination.firstTaskWithFixedChunk(grouper.getGroupId(taskId) + srcV.chunkIdOffset);
    do {
      List<Integer> list = combination.getCombination();

      if (scheduledTasks.contains(combination.getChunkId())) {
        continue;
      }
      boolean readyToSchedule = true;
      for (int i = 0; i < list.size(); i++) {
        int chunkId = list.get(i);
        SrcVertex srcVHasGroup = null;
        for (SrcVertex v : sourcesByName.get(config.getSourceVertices().get(i)).srcVertices) {
          if (v.chunkIdOffset <= chunkId && chunkId < v.chunkIdOffset + v.numChunk) {
            srcVHasGroup = v;
            break;
          }
        }
        assert srcVHasGroup != null;
        grouper.init(srcVHasGroup.numTask, srcVHasGroup.numChunk);
        chunkId -= srcVHasGroup.chunkIdOffset;
        for (int j = grouper.getFirstTaskInGroup(chunkId); j <= grouper.getLastTaskInGroup(chunkId); j++) {
          if (!srcVHasGroup.taskCompleted.contains(j)) {
            readyToSchedule = false;
            break;
          }
        }
        if (!readyToSchedule) {
          break;
        }
      }

      if (readyToSchedule) {
        requests.add(ScheduleTaskRequest.create(combination.getChunkId(), null));
        scheduledTasks.add(combination.getChunkId());
      }
    } while (combination.nextTaskWithFixedChunk());
    if (!requests.isEmpty()) {
      getContext().scheduleTasks(requests);
    }
  }
}
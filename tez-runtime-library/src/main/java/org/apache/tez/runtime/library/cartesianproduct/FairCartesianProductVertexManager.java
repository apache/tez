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

import com.google.common.math.LongMath;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.tez.dag.api.EdgeProperty.DataMovementType.CUSTOM;
import static org.apache.tez.runtime.library.cartesianproduct.CartesianProductUserPayload.CartesianProductConfigProto;

/**
 * In fair cartesian product case, we have one destination task for each source chunk combination.
 * A source is a source vertex or a source vertex group. A chunk is a part of source output. The
 * mapping from source chunk to destination task id is done by {@link <CartesianProductCombination>}
 *
 * This requires source output to be partitioned with a round robin partitioner, even data is
 * unpartitioned intrinsically. By doing this, we can achieve arbitrary parallelism.
 *
 * It tries to distribute work evenly by having each task to perform similar number of cartesian
 * product operations. To achieve this, it estimate #record from each source and total # ops.
 *
 * The parallelism is decided based on estimated total #ops and two configurations, max allowed
 * parallelism and min-ops-per-worker. The max parallelism will be tried first and used if resulting
 * #ops-per-worker is no less than min-ops-per-worker. Otherwise, parallelism will be total # ops
 * divided by #ops-per-worker.
 *
 * To reduce shuffle overhead, we try to group output from same task first. A chunk from a source
 * vertex contains continuous physical output from a task or its neighboring task.
 *
 * Vertex group is supported. Chunk i of a source group contains chunk i of every vertex in this
 * group.
 */
class FairCartesianProductVertexManager extends CartesianProductVertexManagerReal {
  /**
   * a cartesian product source.
   * Chunk i of a source contains chunk i of every vertex in this source
   */
  static class Source {
    // list of source vertices of this source
    List<SrcVertex> srcVertices = new ArrayList<>();
    // position of this source in all sources
    int position;
    // name of source vertex or vertex group
    String name;

    // total number of chunks in this source
    // each vertex in this source has same numChunk
    int numChunk;
    // total number of acknowledged output record (before reconfiguration)
    // or estimated total number of output record (after reconfiguration)
    long numRecord;

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Source at position ");
      sb.append(position);
      if (name != null) {
        sb.append(", ");
        sb.append("name ");
        sb.append(name);

      }
      sb.append(", num chunk ").append(numChunk);
      sb.append(": {");
      for (SrcVertex srcV : srcVertices) {
        sb.append("[");
        sb.append(srcV.toString());
        sb.append("], ");
      }
      sb.deleteCharAt(sb.length() - 1);
      sb.setCharAt(sb.length() - 1, '}');
      return sb.toString();
    }

    // estimate total number of output record from all vertices in this group
    public long estimateNumRecord() {
      long estimation = 0;
      for (SrcVertex srcV : srcVertices) {
        estimation += srcV.estimateNumRecord();
      }
      return estimation;
    }

    private boolean isChunkCompleted(int chunkId) {
      // a chunk is completed only if its corresponding chunk in each vertex is completed
      for (SrcVertex srcV : srcVertices) {
        if (!srcV.isChunkCompleted(chunkId)) {
          return false;
        }
      }
      return true;
    }

    public int getNumTask() {
      int numTask = 0;
      for (SrcVertex srcV : srcVertices) {
        numTask += srcV.numTask;
      }
      return numTask;
    }

    public SrcVertex getSrcVertexWithMostOutput() {
      SrcVertex srcVWithMaxOutput = null;
      for (SrcVertex srcV : srcVertices) {
        if (srcVWithMaxOutput == null || srcV.numRecord > srcVWithMaxOutput.numRecord) {
          srcVWithMaxOutput = srcV;
        }
      }
      return srcVWithMaxOutput;
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

    RoaringBitmap taskCompleted = new RoaringBitmap();
    RoaringBitmap taskWithVMEvent = new RoaringBitmap();
    // total number of acknowledged output record (before reconfiguration)
    // or estimated total number of output record (after reconfiguration)
    long numRecord;

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("vertex ").append(name).append(", ");
      sb.append(numTask).append(" tasks, ");
      sb.append(taskWithVMEvent.getCardinality()).append(" VMEvents, ");
      sb.append("numRecord ").append(numRecord).append(", ");
      sb.append("estimated # output records ").append(estimateNumRecord());
      return sb.toString();
    }

    public long estimateNumRecord() {
      if (taskWithVMEvent.isEmpty()) {
        return 0;
      } else {
        return numRecord * numTask / taskWithVMEvent.getCardinality();
      }
    }

    public boolean isChunkCompleted(int chunkId) {
      grouper.init(numTask * numPartitions, source.numChunk);
      int firstRelevantTask = grouper.getFirstItemInGroup(chunkId) / numPartitions;
      int lastRelevantTask = grouper.getLastItemInGroup(chunkId) / numPartitions;
      for (int relevantTask = firstRelevantTask; relevantTask <= lastRelevantTask; relevantTask++) {
        if (!taskCompleted.contains(relevantTask)) {
          return false;
        }
      }
      return true;
    }
  }

  private static final Logger LOG =
    org.slf4j.LoggerFactory.getLogger(FairCartesianProductVertexManager.class);

  private CartesianProductConfigProto config;
  private List<String> sourceList;
  private Map<String, Source> sourcesByName = new HashMap<>();
  private Map<String, SrcVertex> srcVerticesByName = new HashMap<>();
  private boolean enableGrouping;
  private int maxParallelism;
  private int numPartitions;
  private long minOpsPerWorker;

  private long minNumRecordForEstimation;
  private boolean vertexReconfigured = false;
  private boolean vertexStarted = false;
  private boolean vertexStartSchedule = false;
  private int numCPSrcNotInConfigureState = 0;
  private int numBroadcastSrcNotInRunningState = 0;
  private Queue<TaskAttemptIdentifier> completedSrcTaskToProcess = new LinkedList<>();
  private RoaringBitmap scheduledTasks = new RoaringBitmap();

  private int parallelism;

  /* auto reduce related */
  // num of chunks of source at the corresponding position in source list
  private int[] numChunksPerSrc;
  private Grouper grouper = new Grouper();

  public FairCartesianProductVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void initialize(CartesianProductConfigProto config) throws Exception {
    this.config = config;
    maxParallelism = config.hasMaxParallelism() ? config.getMaxParallelism()
      :CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MAX_PARALLELISM_DEFAULT;
    enableGrouping = config.hasEnableGrouping() ? config.getEnableGrouping()
      :CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_ENABLE_GROUPING_DEFAULT;
    minOpsPerWorker = config.hasMinOpsPerWorker() ? config.getMinOpsPerWorker()
      : CartesianProductVertexManager.TEZ_CARTESIAN_PRODUCT_MIN_OPS_PER_WORKER_DEFAULT;
    sourceList = config.getSourcesList();
    if (config.hasNumPartitionsForFairCase()) {
      numPartitions = config.getNumPartitionsForFairCase();
    } else {
      numPartitions = (int) Math.pow(maxParallelism, 1.0 / sourceList.size());
    }

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
    for (int i = 0; i < sourceList.size(); i++) {
      String srcName = sourceList.get(i);
      Source source = new Source();
      source.position = i;
      if (srcGroups.containsKey(srcName)) {
        source.name = srcName;
        for (String srcVName : srcGroups.get(srcName)) {
          source.srcVertices.add(srcVerticesByName.get(srcVName));
          srcVerticesByName.get(srcVName).source = source;
        }
      } else {
        source.name = srcName;
        source.srcVertices.add(srcVerticesByName.get(srcName));
        srcVerticesByName.get(srcName).source = source;
      }
      sourcesByName.put(srcName, source);
    }

    minNumRecordForEstimation =
      (long) Math.pow(minOpsPerWorker * maxParallelism, 1.0 / sourceList.size());

    numChunksPerSrc = new int[sourcesByName.size()];
    getContext().vertexReconfigurationPlanned();
  }

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions)
    throws Exception {
    vertexStarted = true;
    if (completions != null) {
      LOG.info("OnVertexStarted with " + completions.size() + " completed source task");
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
    vertexStartSchedule =
      (vertexReconfigured && vertexStarted && numBroadcastSrcNotInRunningState == 0);
    return vertexStartSchedule;
  }

  @Override
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
      srcV.numRecord += proto.getNumRecord();
      srcV.taskWithVMEvent.add(
        vmEvent.getProducerAttemptIdentifier().getTaskIdentifier().getIdentifier());
    }

    tryScheduleTasks();
  }

  private void reconfigureWithZeroTask() {
    getContext().reconfigureVertex(0, null, null);
    vertexReconfigured = true;
    getContext().doneReconfiguringVertex();
  }

  private boolean tryReconfigure() throws IOException {
    if (numCPSrcNotInConfigureState > 0) {
      return false;
    }

    for (Source src : sourcesByName.values()) {
      if (src.getNumTask() == 0) {
        parallelism = 0;
        reconfigureWithZeroTask();
        return true;
      }
    }

    if (config.hasGroupingFraction() && config.getGroupingFraction() > 0) {
      // every src vertex must complete a certain number of task before we do estimation
      for (SrcVertex srcV : srcVerticesByName.values()) {
        if (srcV.taskCompleted.getCardinality() < srcV.numTask
          && (srcV.numTask * config.getGroupingFraction() > srcV.taskCompleted.getCardinality()
          || srcV.numRecord == 0)) {
          return false;
        }
      }
    } else {
      // every src vertex must generate enough output records before we do estimation
      // or all its tasks already finish but we cannot get enough result for estimation
      for (SrcVertex srcV : srcVerticesByName.values()) {
        if (srcV.numRecord < minNumRecordForEstimation
          && srcV.taskWithVMEvent.getCardinality() < srcV.numTask) {
          return false;
        }
      }
    }

    LOG.info("Start reconfiguring vertex " + getContext().getVertexName()
      + ", max parallelism: " + maxParallelism
      + ", min-ops-per-worker: " + minOpsPerWorker
      + ", num partition: " + numPartitions);
    for (Source src : sourcesByName.values()) {
      LOG.info(src.toString());
    }

    long totalOps = 1;
    for (Source src : sourcesByName.values()) {
      src.numRecord = src.estimateNumRecord();
      if (src.numRecord == 0) {
        LOG.info("Set parallelism to 0 because source " + src.name + " has 0 output recorc");
        reconfigureWithZeroTask();
        return true;
      }

      try {
        totalOps  = LongMath.checkedMultiply(totalOps, src.numRecord);
      } catch (ArithmeticException e) {
        LOG.info("totalOps exceeds " + Long.MAX_VALUE + ", capping to " + Long.MAX_VALUE);
        totalOps = Long.MAX_VALUE;
      }
    }

    // determine initial parallelism
    if (totalOps / minOpsPerWorker >= maxParallelism) {
      parallelism = maxParallelism;
    } else {
      parallelism = (int) ((totalOps + minOpsPerWorker - 1) / minOpsPerWorker);
    }
    LOG.info("Total ops " + totalOps + ", initial parallelism " + parallelism);

    if (enableGrouping) {
      determineNumChunks(sourcesByName, parallelism);
    } else {
      for (Source src : sourcesByName.values()) {
        src.numChunk = src.getSrcVertexWithMostOutput().numTask;
      }
    }

    // final parallelism will be product of all #chunk
    parallelism = 1;
    for (Source src : sourcesByName.values()) {
      parallelism *= src.numChunk;
    }

    LOG.info("After reconfigure, final parallelism " + parallelism);
    for (Source src : sourcesByName.values()) {
      LOG.info(src.toString());
    }

    for (int i = 0; i < numChunksPerSrc.length; i++) {
      numChunksPerSrc[i] = sourcesByName.get(sourceList.get(i)).numChunk;
    }

    CartesianProductConfigProto.Builder builder = CartesianProductConfigProto.newBuilder(config);
    builder.addAllNumChunks(Ints.asList(this.numChunksPerSrc));

    Map<String, EdgeProperty> edgeProperties = getContext().getInputVertexEdgeProperties();
    Iterator<Map.Entry<String, EdgeProperty>> iter = edgeProperties.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, EdgeProperty> e = iter.next();
      if (e.getValue().getDataMovementType() != CUSTOM) {
        iter.remove();
      }
    }

    // send out vertex group info for computing physical input id of destination task
    for (Source src : sourcesByName.values()) {
      builder.clearNumTaskPerVertexInGroup();
      for (int i = 0; i < src.srcVertices.size(); i++) {
        SrcVertex srcV = src.srcVertices.get(i);
        builder.setPositionInGroup(i);
        edgeProperties.get(srcV.name).getEdgeManagerDescriptor()
          .setUserPayload(UserPayload.create(ByteBuffer.wrap(builder.build().toByteArray())));
        builder.addNumTaskPerVertexInGroup(srcV.numTask);
      }
    }
    getContext().reconfigureVertex(parallelism, null, edgeProperties);
    vertexReconfigured = true;
    getContext().doneReconfiguringVertex();
    return true;
  }

  /**
   * determine num chunk for each source by weighted factorization of initial parallelism
   **/
  private void determineNumChunks(Map<String, Source> sourcesByName, int parallelism) {
    // first round: set numChunk to 1 if source output is too small
    double k = Math.log10(parallelism);
    for (Source src : sourcesByName.values()) {
      k -= Math.log10(src.numRecord);
    }
    k = Math.pow(10, k / sourcesByName.size());

    for (Source src : sourcesByName.values()) {
      if (src.numRecord * k < 2) {
        src.numChunk = 1;
      }
    }

    // second round: weighted factorization
    k = Math.log10(parallelism);
    int numLargeSrc = 0;
    for (Source src : sourcesByName.values()) {
      if (src.numChunk != 1) {
        k -= Math.log10(src.numRecord);
        numLargeSrc++;
      }
    }
    k = Math.pow(10, k / numLargeSrc);

    for (Source src : sourcesByName.values()) {
      if (src.numChunk != 1) {
        src.numChunk = Math.min(maxParallelism,
          Math.min(src.getSrcVertexWithMostOutput().numTask * numPartitions,
            Math.max(1, (int) (src.numRecord * k))));
      }
    }
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
    if (parallelism == 0) {
      return;
    }

    int taskId = attempt.getTaskIdentifier().getIdentifier();
    String vertex = attempt.getTaskIdentifier().getVertexIdentifier().getName();
    SrcVertex srcV = srcVerticesByName.get(vertex);
    Source src = srcV.source;

    List<ScheduleTaskRequest> requests = new ArrayList<>();
    CartesianProductCombination combination =
      new CartesianProductCombination(numChunksPerSrc, src.position);

    grouper.init(srcV.numTask * numPartitions, src.numChunk);
    int firstRelevantChunk = grouper.getGroupId(taskId * numPartitions);
    int lastRelevantChunk = grouper.getGroupId(taskId * numPartitions + numPartitions - 1);
    for (int chunkId = firstRelevantChunk; chunkId <= lastRelevantChunk; chunkId++) {
      combination.firstTaskWithFixedChunk(chunkId);
      do {
        List<Integer> list = combination.getCombination();

        if (scheduledTasks.contains(combination.getTaskId())) {
          continue;
        }

        // a task is ready for schedule only if all its src chunk has been completed
        boolean readyToSchedule = src.isChunkCompleted(list.get(src.position));
        for (int srcId = 0; readyToSchedule && srcId < list.size(); srcId++) {
          if (srcId != src.position){
            readyToSchedule =
              sourcesByName.get(sourceList.get(srcId)).isChunkCompleted(list.get(srcId));
          }
        }

        if (readyToSchedule) {
          requests.add(ScheduleTaskRequest.create(combination.getTaskId(), null));
          scheduledTasks.add(combination.getTaskId());
        }
      } while (combination.nextTaskWithFixedChunk());
    }

    if (!requests.isEmpty()) {
      getContext().scheduleTasks(requests);
    }
  }
}
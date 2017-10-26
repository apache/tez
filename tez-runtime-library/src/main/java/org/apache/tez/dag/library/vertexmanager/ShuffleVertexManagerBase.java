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

package org.apache.tez.dag.library.vertexmanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.library.utils.DATA_RANGE_IN_MB;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;


import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Inflater;

/**
 * It provides common functions used by ShuffleVertexManager and
 * FairShuffleVertexManager.
 */
@Private
@Evolving
abstract class ShuffleVertexManagerBase extends VertexManagerPlugin {
  static long MB = 1024l * 1024l;

  private static final Logger LOG =
     LoggerFactory.getLogger(ShuffleVertexManagerBase.class);

  ComputeRoutingAction computeRoutingAction = ComputeRoutingAction.WAIT;

  int totalNumBipartiteSourceTasks = 0;
  int numBipartiteSourceTasksCompleted = 0;
  int numVertexManagerEventsReceived = 0;
  List<VertexManagerEvent> pendingVMEvents = Lists.newLinkedList();
  AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);

  private Set<TaskIdentifier> taskWithVmEvents = Sets.newHashSet();

  //Track source vertex and its finished tasks
  private final Map<String, SourceVertexInfo> srcVertexInfo = Maps.newConcurrentMap();
  boolean sourceVerticesScheduled = false;
  @VisibleForTesting
  int bipartiteSources = 0;
  long completedSourceTasksOutputSize = 0;
  List<VertexStateUpdate> pendingStateUpdates = Lists.newArrayList();
  List<PendingTaskInfo> pendingTasks = Lists.newLinkedList();
  int totalTasksToSchedule = 0;

  @VisibleForTesting
  Configuration conf;
  ShuffleVertexManagerBaseConfig config;
  // requires synchronized access
  final Inflater inflater;

  /**
   * Used when automatic parallelism is enabled
   * Initially the vertex manager will start in WAIT state.
   * After it gathers enough data, it will compute the new
   * parallelism. In some special cases, it will skip the parallelism
   * computation.
   */
  enum ComputeRoutingAction {
    WAIT, // not enough data yet.
    SKIP, // skip the routing computation
    COMPUTE; // compute the new routing

    public boolean determined() {
      return this != WAIT;
    }
  }

  static class SourceVertexInfo {
    final EdgeProperty edgeProperty;
    boolean vertexIsConfigured;
    final BitSet finishedTaskSet;
    int numTasks;
    int numVMEventsReceived;
    long outputSize;
    int[] statsInMB;
    EdgeManagerPluginDescriptor newDescriptor;

    SourceVertexInfo(final EdgeProperty edgeProperty,
       int totalTasksToSchedule) {
      this.edgeProperty = edgeProperty;
      this.finishedTaskSet = new BitSet();
      this.statsInMB = new int[totalTasksToSchedule];
    }

    int getNumTasks() {
      return numTasks;
    }

    int getNumCompletedTasks() {
      return finishedTaskSet.cardinality();
    }

    BigInteger getExpectedStatsAtIndex(int index) {
      return (numVMEventsReceived == 0) ?
         BigInteger.ZERO :
         BigInteger.valueOf(statsInMB[index]).
           multiply(BigInteger.valueOf(numTasks)).
           divide(BigInteger.valueOf(numVMEventsReceived)).
           multiply(BigInteger.valueOf(MB));
    }
  }

  SourceVertexInfo createSourceVertexInfo(EdgeProperty edgeProperty,
      int numTasks) {
    return new SourceVertexInfo(edgeProperty, numTasks);
  }

  static class PendingTaskInfo {
    final private int index;
    private int inputStats;

    public PendingTaskInfo(int index) {
      this.index = index;
    }

    public String toString() {
      return "[index=" + index + ", inputStats=" + inputStats + "]";
    }
    public int getIndex() {
      return index;
    }
    public int getInputStats() {
      return inputStats;
    }
    // return true if stat is set.
    public boolean setInputStats(int inputStats) {
      if (inputStats > 0 && this.inputStats != inputStats) {
        this.inputStats = inputStats;
        return true;
      } else {
        return false;
      }
    }
  }

  static class ReconfigVertexParams {
    final private int finalParallelism;
    final private VertexLocationHint locationHint;

    public ReconfigVertexParams(final int finalParallelism,
        final VertexLocationHint locationHint) {
      this.finalParallelism = finalParallelism;
      this.locationHint = locationHint;
    }

    public int getFinalParallelism() {
      return finalParallelism;
    }
    public VertexLocationHint getLocationHint() {
      return locationHint;
    }
  }

  public ShuffleVertexManagerBase(VertexManagerPluginContext context) {
    super(context);
    inflater = TezCommonUtils.newInflater();
  }

  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions) {
    // examine edges after vertex started because until then these may not have been defined
    Map<String, EdgeProperty> inputs = getContext().getInputVertexEdgeProperties();
    for(Map.Entry<String, EdgeProperty> entry : inputs.entrySet()) {
      srcVertexInfo.put(entry.getKey(), createSourceVertexInfo(entry.getValue(),
          getContext().getVertexNumTasks(getContext().getVertexName())));
      // TODO what if derived class has already called this
      // register for status update from all source vertices
      getContext().registerForVertexStateUpdates(entry.getKey(),
          EnumSet.of(VertexState.CONFIGURED));
      if (entry.getValue().getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        bipartiteSources++;
      }
    }
    onVertexStartedCheck();

    for (VertexStateUpdate stateUpdate : pendingStateUpdates) {
      handleVertexStateUpdate(stateUpdate);
    }
    pendingStateUpdates.clear();

    // track the tasks in this vertex
    updatePendingTasks();

    for (VertexManagerEvent vmEvent : pendingVMEvents) {
      handleVertexManagerEvent(vmEvent);
    }
    pendingVMEvents.clear();

    LOG.info("OnVertexStarted vertex: {} with {} source tasks and {} pending" +
        " tasks", getContext().getVertexName(), totalNumBipartiteSourceTasks,
        totalTasksToSchedule);

    if (completions != null) {
      for (TaskAttemptIdentifier attempt : completions) {
        onSourceTaskCompleted(attempt);
      }
    }
    onVertexStartedDone.set(true);
    // for the special case when source has 0 tasks or min fraction == 0
    processPendingTasks(null);
  }

  protected void onVertexStartedCheck() {
    if(bipartiteSources == 0) {
      throw new TezUncheckedException("At least 1 bipartite source should exist");
    }
  }

  @Override
  public synchronized void onSourceTaskCompleted(TaskAttemptIdentifier attempt) {
    String srcVertexName = attempt.getTaskIdentifier().getVertexIdentifier().getName();
    int srcTaskId = attempt.getTaskIdentifier().getIdentifier();
    SourceVertexInfo srcInfo = srcVertexInfo.get(srcVertexName);
    if (srcInfo.vertexIsConfigured) {
      Preconditions.checkState(srcTaskId < srcInfo.numTasks,
          "Received completion for srcTaskId " + srcTaskId + " but Vertex: " + srcVertexName +
          " has only " + srcInfo.numTasks + " tasks");
    }
    //handle duplicate events and count task completions from all source vertices
    BitSet completedSourceTasks = srcInfo.finishedTaskSet;
    // duplicate notifications tracking
    if (!completedSourceTasks.get(srcTaskId)) {
      completedSourceTasks.set(srcTaskId);
      // source task has completed
      if (srcInfo.edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        numBipartiteSourceTasksCompleted++;
      }
    }
    processPendingTasks(attempt);
  }

  @VisibleForTesting
  void parsePartitionStats(SourceVertexInfo srcInfo,
      RoaringBitmap partitionStats) {
    Preconditions.checkState(srcInfo.statsInMB != null,
        "Stats should be initialized");
    Iterator<Integer> it = partitionStats.iterator();
    final DATA_RANGE_IN_MB[] RANGES = DATA_RANGE_IN_MB.values();
    final int RANGE_LEN = RANGES.length;
    while (it.hasNext()) {
      int pos = it.next();
      int index = ((pos) / RANGE_LEN);
      int rangeIndex = ((pos) % RANGE_LEN);
      //Add to aggregated stats and normalize to DATA_RANGE_IN_MB.
      if (RANGES[rangeIndex].getSizeInMB() > 0) {
        srcInfo.statsInMB[index] += RANGES[rangeIndex].getSizeInMB();
      }
    }
  }

  void parseDetailedPartitionStats(SourceVertexInfo srcInfo,
      List<Integer> partitionStats) {
    for (int i=0; i<partitionStats.size(); i++) {
      srcInfo.statsInMB[i] += partitionStats.get(i);
    }
  }

  @Override
  public synchronized void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    if (onVertexStartedDone.get()) {
      // internal data structures have been initialized - so handle the events directly
      handleVertexManagerEvent(vmEvent);
    } else {
      // save this event for processing after vertex starts
      pendingVMEvents.add(vmEvent);
    }
  }

  private void handleVertexManagerEvent(VertexManagerEvent vmEvent) {
    // currently events from multiple attempts of the same task can be ignored because
    // their output will be the same.
    TaskIdentifier producerTask = vmEvent.getProducerAttemptIdentifier().getTaskIdentifier();
    if (!taskWithVmEvents.add(producerTask)) {
      LOG.info("Ignoring vertex manager event from: {}", producerTask);
      return;
    }

    String vName = producerTask.getVertexIdentifier().getName();
    SourceVertexInfo srcInfo = srcVertexInfo.get(vName);
    Preconditions.checkState(srcInfo != null,
        "Unknown vmEvent from " + producerTask);

    numVertexManagerEventsReceived++;

    long sourceTaskOutputSize = 0;
    if (vmEvent.getUserPayload() != null) {
      // save output size
      VertexManagerEventPayloadProto proto;
      try {
        proto = VertexManagerEventPayloadProto.parseFrom(
            ByteString.copyFrom(vmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException(e);
      }
      sourceTaskOutputSize = proto.getOutputSize();

      if (proto.hasPartitionStats()) {
        try {
          RoaringBitmap partitionStats = new RoaringBitmap();
          ByteString compressedPartitionStats = proto.getPartitionStats();
          byte[] rawData = TezCommonUtils.decompressByteStringToByteArray(
              compressedPartitionStats, inflater);
          NonSyncByteArrayInputStream bin = new NonSyncByteArrayInputStream(rawData);
          partitionStats.deserialize(new DataInputStream(bin));

          parsePartitionStats(srcInfo, partitionStats);

        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
      } else if (proto.hasDetailedPartitionStats()) {
        List<Integer> detailedPartitionStats =
            proto.getDetailedPartitionStats().getSizeInMbList();
        parseDetailedPartitionStats(srcInfo, detailedPartitionStats);
      }
      srcInfo.numVMEventsReceived++;
      srcInfo.outputSize += sourceTaskOutputSize;
      completedSourceTasksOutputSize += sourceTaskOutputSize;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("For attempt: {} received info of output size: {}"
                      + " vertex numEventsReceived: {} vertex output size: {}"
                      + " total numEventsReceived: {} total output size: {}",
              vmEvent.getProducerAttemptIdentifier(), sourceTaskOutputSize,
              srcInfo.numVMEventsReceived, srcInfo.outputSize,
              numVertexManagerEventsReceived, completedSourceTasksOutputSize);
    }
  }

  void updatePendingTasks() {
    int tasks = getContext().getVertexNumTasks(getContext().getVertexName());
    if (tasks == pendingTasks.size() || tasks <= 0) {
      return;
    }
    pendingTasks.clear();
    for (int i = 0; i < tasks; ++i) {
      pendingTasks.add(new PendingTaskInfo(i));
    }
    totalTasksToSchedule = pendingTasks.size();
  }

  /**
   * Beginning of functions related to how new parallelism is determined.
   * ShuffleVertexManagerBase implements common functionality used by
   * VertexManagerPlugin, while each VertexManagerPlugin implements its own
   * routing policy.
   */
  private ComputeRoutingAction getComputeRoutingAction(
      float minSourceVertexCompletedTaskFraction) {
    if (getNumOfTasksToSchedule(minSourceVertexCompletedTaskFraction) <= 0 &&
        numBipartiteSourceTasksCompleted != totalNumBipartiteSourceTasks) {
      // Wait when there aren't enough completed tasks
      return ComputeRoutingAction.WAIT;
    } else if (numVertexManagerEventsReceived == 0 &&
      totalNumBipartiteSourceTasks > 0) {
      // When source tasks don't have output data,
      // there will be no VME.
      return ComputeRoutingAction.SKIP;
    } else if (
        completedSourceTasksOutputSize < config.getDesiredTaskInputDataSize()
        && (minSourceVertexCompletedTaskFraction < config.getMaxFraction())) {
      /**
       * When overall completed output size is not even equal to
       * desiredTaskInputSize, we can wait for some more data to be available to
       * determine better parallelism until max.fraction is reached.
       * min.fraction is just a hint to the framework and need not be
       * honored strictly in this case.
       */
      LOG.info("Defer scheduling tasks; vertex = {}"
          + ", totalNumBipartiteSourceTasks = {}"
          + ", completedSourceTasksOutputSize = {}"
          + ", numVertexManagerEventsReceived = {}"
          + ", numBipartiteSourceTasksCompleted = {}"
          + ", minSourceVertexCompletedTaskFraction = {}",
          getContext().getVertexName(), totalNumBipartiteSourceTasks,
          completedSourceTasksOutputSize, numVertexManagerEventsReceived,
          numBipartiteSourceTasksCompleted,
          minSourceVertexCompletedTaskFraction);
       return ComputeRoutingAction.WAIT;
    } else {
      return ComputeRoutingAction.COMPUTE;
    }
  }

  BigInteger getExpectedTotalBipartiteSourceTasksOutputSize() {
    BigInteger expectedTotalSourceTasksOutputSize = BigInteger.ZERO;
    for (Map.Entry<String, SourceVertexInfo> vInfo : getBipartiteInfo()) {
      SourceVertexInfo srcInfo = vInfo.getValue();
      if (srcInfo.numTasks > 0 && srcInfo.numVMEventsReceived > 0) {
        // this assumes that 1 vmEvent is received per completed task - TEZ-2961
        // Estimate total size by projecting based on the current average size per event
        BigInteger srcOutputSize = BigInteger.valueOf(srcInfo.outputSize);
        BigInteger srcNumTasks = BigInteger.valueOf(srcInfo.numTasks);
        BigInteger srcNumVMEventsReceived = BigInteger.valueOf(srcInfo.numVMEventsReceived);
        BigInteger expectedSrcOutputSize = srcOutputSize.multiply(
            srcNumTasks).divide(srcNumVMEventsReceived);
        expectedTotalSourceTasksOutputSize =
            expectedTotalSourceTasksOutputSize.add(expectedSrcOutputSize);
      }
    }
    return expectedTotalSourceTasksOutputSize;
  }

  int getCurrentlyKnownStatsAtIndex(int index) {
    int stats = 0;
    for(SourceVertexInfo entry : getAllSourceVertexInfo()) {
      stats += entry.statsInMB[index];
    }
    return stats;
  }

  long getExpectedStatsAtIndex(int index) {
    BigInteger stats = BigInteger.ZERO;
    for(SourceVertexInfo entry : getAllSourceVertexInfo()) {
      stats = stats.add(entry.getExpectedStatsAtIndex(index));
    }
    if (stats.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      LOG.warn("Partition {}'s size {} exceeded Long.MAX_VALUE", index, stats);
      return Long.MAX_VALUE;
    } else {
      return stats.longValue();
    }
  }

  /**
   * Subclass might return null to indicate there is no new routing.
   */
  abstract ReconfigVertexParams computeRouting();

  abstract void postReconfigVertex();

  /**
   * Compute optimal parallelism needed for the job
   * @return true (if parallelism is determined), false otherwise
   */
  @VisibleForTesting
  boolean determineParallelismAndApply(
      float minSourceVertexCompletedTaskFraction) {
    if (computeRoutingAction.equals(ComputeRoutingAction.WAIT)) {
      ComputeRoutingAction computeRoutingAction = getComputeRoutingAction(
          minSourceVertexCompletedTaskFraction);
      if (computeRoutingAction.equals(computeRoutingAction.COMPUTE)) {
        ReconfigVertexParams params = computeRouting();
        if (params != null) {
          reconfigVertex(params.getFinalParallelism());
          updatePendingTasks();
          postReconfigVertex();
        }
      }
      if (!computeRoutingAction.equals(ComputeRoutingAction.WAIT)) {
        getContext().doneReconfiguringVertex();
      }
      this.computeRoutingAction = computeRoutingAction;
    }
    return this.computeRoutingAction.determined();
  }

  private boolean determineParallelismAndApply() {
    return determineParallelismAndApply(
        getMinSourceVertexCompletedTaskFraction());
  }
  /**
   * End of functions related to how new parallelism is determined.
   */


  /**
   * Subclass might return null or empty list to indicate no tasks
   * to schedule at this point.
   * @param completedSourceAttempt the completed source task attempt
   * @return the list of tasks to schedule.
   */
  abstract List<ScheduleTaskRequest> getTasksToSchedule(
      TaskAttemptIdentifier completedSourceAttempt);

  abstract void processPendingTasks();

  private void schedulePendingTasks(
      TaskAttemptIdentifier completedSourceAttempt) {
    List<ScheduleTaskRequest> scheduledTasks =
        getTasksToSchedule(completedSourceAttempt);
    if (scheduledTasks != null && scheduledTasks.size() > 0) {
      getContext().scheduleTasks(scheduledTasks);
    }
  }

  Iterable<SourceVertexInfo> getAllSourceVertexInfo() {
    return srcVertexInfo.values();
  }

  SourceVertexInfo getSourceVertexInfo(String vertextName) {
    return srcVertexInfo.get(vertextName);
  }

  Iterable<Map.Entry<String, SourceVertexInfo>> getBipartiteInfo() {
    return Iterables.filter(srcVertexInfo.entrySet(),
        new Predicate<Map.Entry<String,SourceVertexInfo>>() {
      public boolean apply(Map.Entry<String, SourceVertexInfo> input) {
        return (input.getValue().edgeProperty.getDataMovementType() ==
            DataMovementType.SCATTER_GATHER);
      }
    });
  }

  /**
   * Verify whether each of the source vertices have completed at least 1 task
   *
   * @return boolean
   */
  private boolean canScheduleTasks() {
    for(Map.Entry<String, SourceVertexInfo> entry : srcVertexInfo.entrySet()) {
      // need to check for vertex configured because until that we dont know
      // if numTask s== 0 is valid
      if (!entry.getValue().vertexIsConfigured) { // isConfigured
        // vertex not scheduled tasks
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for vertex: {} in vertex: {}", entry.getKey(),
              getContext().getVertexName());
        }
        return false;
      }
    }
    sourceVerticesScheduled = true;
    return sourceVerticesScheduled;
  }

  int getNumOfTasksToScheduleAndLog(float minFraction) {
    int numTasksToSchedule = getNumOfTasksToSchedule(minFraction);
    if (numTasksToSchedule > 0) {
      // numTasksToSchedule can be -ve if minFraction
      // is less than slowStartMinSrcCompletionFraction.
      LOG.info("Scheduling {} tasks for vertex: {} with totalTasks: {}. " +
          "{} source tasks completed out of {}. " +
          "MinSourceTaskCompletedFraction: {} min: {} max: {}",
          numTasksToSchedule, getContext().getVertexName(),
          totalTasksToSchedule, numBipartiteSourceTasksCompleted,
          totalNumBipartiteSourceTasks, minFraction,
          config.getMinFraction(), config.getMaxFraction());
    }
    return numTasksToSchedule;
  }

  int getNumOfTasksToSchedule(float minSourceVertexCompletedTaskFraction) {
    int numPendingTasks = pendingTasks.size();
    if (numBipartiteSourceTasksCompleted == totalNumBipartiteSourceTasks) {
      LOG.info("All source tasks completed. Ramping up {} remaining tasks" +
          " for vertex: {}", numPendingTasks, getContext().getVertexName());
      return numPendingTasks;
    }

    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1;
    float percentRange =
        config.getMaxFraction() - config.getMinFraction();
    if (percentRange > 0) {
      tasksFractionToSchedule =
          (minSourceVertexCompletedTaskFraction -
              config.getMinFraction()) / percentRange;
    } else {
      // min and max are equal. schedule 100% on reaching min
      if(minSourceVertexCompletedTaskFraction <
          config.getMinFraction()) {
        tasksFractionToSchedule = 0;
      }
    }

    tasksFractionToSchedule =
        Math.max(0, Math.min(1, tasksFractionToSchedule));

    // round up to avoid the corner case that single task cannot be scheduled
    // until src completed fraction reach max
    return ((int)(Math.ceil(tasksFractionToSchedule * totalTasksToSchedule)) -
        (totalTasksToSchedule - numPendingTasks));
  }

  float getMinSourceVertexCompletedTaskFraction() {
    float minSourceVertexCompletedTaskFraction = 1f;

    if (numBipartiteSourceTasksCompleted != totalNumBipartiteSourceTasks) {
      for (Map.Entry<String, SourceVertexInfo> vInfo : getBipartiteInfo()) {
        SourceVertexInfo srcInfo = vInfo.getValue();
        // canScheduleTasks check has already verified all sources are configured
        Preconditions.checkState(srcInfo.vertexIsConfigured,
            "Vertex: " + vInfo.getKey());
        if (srcInfo.numTasks > 0) {
          int numCompletedTasks = srcInfo.getNumCompletedTasks();
          float completedFraction =
              (float) numCompletedTasks / srcInfo.numTasks;
          if (minSourceVertexCompletedTaskFraction > completedFraction) {
            minSourceVertexCompletedTaskFraction = completedFraction;
          }
        }
      }
    }
    return minSourceVertexCompletedTaskFraction;
  }


  private boolean preconditionsSatisfied() {
    if (!onVertexStartedDone.get()) {
      // vertex not started yet
      return false;
    }

    if (!sourceVerticesScheduled && !canScheduleTasks()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Defer scheduling tasks for vertex: {} as one task needs " +
            "to be completed per source vertex", getContext().getVertexName());
      }
      return false;
    }
    return true;
  }

  /**
   * Process pending tasks when a source task has completed.
   * The processing goes through 4 steps.
   * Step 1: Precondition check such as whether the vertex has started.
   * Step 2: Determine new parallelism if possible.
   * Step 3: Process pending tasks such as sorting based on size.
   * Step 4: Schedule the pending tasks.
   * @param completedSourceAttempt
   */
  private void processPendingTasks(TaskAttemptIdentifier completedSourceAttempt) {
    if (!preconditionsSatisfied()) {
      return;
    }

    // determine parallelism before scheduling the first time
    // this is the latest we can wait before determining parallelism.
    // currently this depends on task completion and so this is the best time
    // to do this. This is the max time we have until we have to launch tasks
    // as specified by the user. If/When we move to some other method of
    // calculating parallelism or change parallelism while tasks are already
    // running then we can create other parameters to trigger this calculation.
    if(config.isAutoParallelismEnabled()) {
      if (!determineParallelismAndApply()) {
        //try to determine parallelism later when more info is available.
        return;
      }
    }
    processPendingTasks();
    schedulePendingTasks(completedSourceAttempt);
  }

  static class ShuffleVertexManagerBaseConfig {
    final private boolean enableAutoParallelism;
    final private long desiredTaskInputDataSize;
    final private float slowStartMinFraction;
    final private float slowStartMaxFraction;
    public ShuffleVertexManagerBaseConfig(final boolean enableAutoParallelism,
        final long desiredTaskInputDataSize, final float slowStartMinFraction,
        final float slowStartMaxFraction) {
      if (slowStartMinFraction < 0 || slowStartMaxFraction > 1
          || slowStartMaxFraction < slowStartMinFraction) {
        throw new IllegalArgumentException(
            "Invalid values for slowStartMinFraction"
                + "/slowStartMaxFraction. Min "
                + "cannot be < 0, max cannot be > 1, and max cannot be < min."
                + ", configuredMin=" + slowStartMinFraction
                + ", configuredMax=" + slowStartMaxFraction);
      }

      this.enableAutoParallelism = enableAutoParallelism;
      this.desiredTaskInputDataSize = desiredTaskInputDataSize;
      this.slowStartMinFraction = slowStartMinFraction;
      this.slowStartMaxFraction = slowStartMaxFraction;

      LOG.info("Settings minFrac: {} maxFrac: {} auto: {} desiredTaskIput: {}",
          slowStartMinFraction, slowStartMaxFraction, enableAutoParallelism,
          desiredTaskInputDataSize);
    }

    public boolean isAutoParallelismEnabled() {
      return this.enableAutoParallelism;
    }
    public long getDesiredTaskInputDataSize() {
      return this.desiredTaskInputDataSize;
    }
    public float getMinFraction() {
      return this.slowStartMinFraction;
    }
    public float getMaxFraction() {
      return this.slowStartMaxFraction;
    }
  }

  abstract ShuffleVertexManagerBaseConfig initConfiguration();

  @Override
  public void initialize() {
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    config = initConfiguration();
    updatePendingTasks();
    if (config.isAutoParallelismEnabled()) {
      getContext().vertexReconfigurationPlanned();
    }
    // dont track the source tasks here since those tasks may themselves be
    // dynamically changed as the DAG progresses.
  }

  private void handleVertexStateUpdate(VertexStateUpdate stateUpdate) {
    Preconditions.checkArgument(stateUpdate.getVertexState() == VertexState.CONFIGURED,
        "Received incorrect state notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    Preconditions.checkArgument(srcVertexInfo.containsKey(stateUpdate.getVertexName()),
        "Received incorrect vertex notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    SourceVertexInfo vInfo = srcVertexInfo.get(stateUpdate.getVertexName());
    Preconditions.checkState(vInfo.vertexIsConfigured == false);
    vInfo.vertexIsConfigured = true;
    vInfo.numTasks = getContext().getVertexNumTasks(stateUpdate.getVertexName());
    if (vInfo.edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER) {
      totalNumBipartiteSourceTasks += vInfo.numTasks;
    }
    LOG.info("Received configured notification : {}" + " for vertex: {} in" +
        " vertex: {}" + " numBipartiteSourceTasks: {}",
        stateUpdate.getVertexState(), stateUpdate.getVertexName(),
        getContext().getVertexName(), totalNumBipartiteSourceTasks);
    processPendingTasks(null);
  }

  @Override
  public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    if (stateUpdate.getVertexState() == VertexState.CONFIGURED) {
      // we will not register for updates until our vertex starts.
      // derived classes can make other update requests for other states that we should
      // ignore. However that will not be allowed until the state change notified supports
      // multiple registers for the same vertex
      if (onVertexStartedDone.get()) {
        // normally this if check will always be true because we register after vertex
        // start.
        handleVertexStateUpdate(stateUpdate);
      } else {
        // normally this code will not trigger since we are the ones who register for
        // the configured states updates and that will happen after vertex starts.
        // So this code will only trigger if a derived class also registers for updates
        // for the same vertices but multiple registers to the same vertex is currently
        // not supported by the state change notifier code. This is just future proofing
        // when that is supported
        // vertex not started yet. So edge info may not have been defined correctly yet.
        pendingStateUpdates.add(stateUpdate);
      }
    }
  }

  @Override
  public synchronized void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    // Not allowing this for now. Nothing to do.
  }

  private void reconfigVertex(final int finalTaskParallelism) {
    Map<String, EdgeProperty> edgeProperties =
        new HashMap<String, EdgeProperty>(bipartiteSources);
    Iterable<Map.Entry<String, SourceVertexInfo>> bipartiteItr = getBipartiteInfo();
    for(Map.Entry<String, SourceVertexInfo> entry : bipartiteItr) {
      String vertex = entry.getKey();
      EdgeProperty oldEdgeProp = entry.getValue().edgeProperty;
      EdgeProperty newEdgeProp = EdgeProperty.create(entry.getValue().newDescriptor,
          oldEdgeProp.getDataSourceType(), oldEdgeProp.getSchedulingType(),
          oldEdgeProp.getEdgeSource(), oldEdgeProp.getEdgeDestination());
      edgeProperties.put(vertex, newEdgeProp);
    }
    getContext().reconfigureVertex(finalTaskParallelism, null, edgeProperties);
  }
}

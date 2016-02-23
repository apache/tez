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
import org.apache.tez.runtime.library.utils.DATA_RANGE_IN_MB;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.ShuffleEdgeManagerConfigPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts scheduling tasks when number of completed source tasks crosses 
 * <code>slowStartMinSrcCompletionFraction</code> and schedules all tasks 
 *  when <code>slowStartMaxSrcCompletionFraction</code> is reached
 */
@Public
@Evolving
public class ShuffleVertexManager extends VertexManagerPlugin {
  
  /**
   * In case of a ScatterGather connection, the fraction of source tasks which
   * should complete before tasks for the current vertex are scheduled
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION = 
                                    "tez.shuffle-vertex-manager.min-src-fraction";
  public static final float TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT = 0.25f;

  /**
   * In case of a ScatterGather connection, once this fraction of source tasks
   * have completed, all tasks on the current vertex can be scheduled. Number of
   * tasks ready for scheduling on the current vertex scales linearly between
   * min-fraction and max-fraction. Defaults to the greater of the default value
   * or tez.shuffle-vertex-manager.min-src-fraction.
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION = 
                                      "tez.shuffle-vertex-manager.max-src-fraction";
  public static final float TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT = 0.75f;
  
  /**
   * Enables automatic parallelism determination for the vertex. Based on input data
   * statisitics the parallelism is decreased to a desired level.
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL = 
                                      "tez.shuffle-vertex-manager.enable.auto-parallel";
  public static final boolean
    TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT = false;
  
  /**
   * The desired size of input per task. Parallelism will be changed to meet this criteria
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE = 
                                     "tez.shuffle-vertex-manager.desired-task-input-size";
  public static final long
    TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT = 1024*1024*100L;

  /**
   * Automatic parallelism determination will not decrease parallelism below this value
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM = 
                                    "tez.shuffle-vertex-manager.min-task-parallelism";
  public static final int TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT = 1;

  
  private static final Logger LOG = 
                   LoggerFactory.getLogger(ShuffleVertexManager.class);

  float slowStartMinSrcCompletionFraction;
  float slowStartMaxSrcCompletionFraction;
  long desiredTaskInputDataSize = 1024*1024*100L;
  int minTaskParallelism = 1;
  boolean enableAutoParallelism = false;
  boolean parallelismDetermined = false;

  int totalNumBipartiteSourceTasks = 0;
  int numBipartiteSourceTasksCompleted = 0;
  int numVertexManagerEventsReceived = 0;
  List<PendingTaskInfo> pendingTasks = Lists.newLinkedList();
  List<VertexManagerEvent> pendingVMEvents = Lists.newLinkedList();
  int totalTasksToSchedule = 0;
  private AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);
  
  private Set<TaskIdentifier> taskWithVmEvents = Sets.newHashSet();
  
  //Track source vertex and its finished tasks
  private final Map<String, SourceVertexInfo> srcVertexInfo = Maps.newConcurrentMap();
  boolean sourceVerticesScheduled = false;
  @VisibleForTesting
  int bipartiteSources = 0;
  long completedSourceTasksOutputSize = 0;
  List<VertexStateUpdate> pendingStateUpdates = Lists.newArrayList();

  private int[][] targetIndexes;
  private int basePartitionRange;
  private int remainderRangeForLastShuffler;
  @VisibleForTesting
  long[] stats; //approximate amount of data to be fetched

  static class SourceVertexInfo {
    EdgeProperty edgeProperty;
    boolean vertexIsConfigured;
    BitSet finishedTaskSet;
    int numTasks;
    int numVMEventsReceived;
    long outputSize;

    SourceVertexInfo(EdgeProperty edgeProperty) {
      this.edgeProperty = edgeProperty;
      finishedTaskSet = new BitSet();
    }
    
    int getNumTasks() {
      return numTasks;
    }
    
    int getNumCompletedTasks() {
      return finishedTaskSet.cardinality();
    }
  }

  static class PendingTaskInfo {
    private int index;
    private long outputStats;

    public PendingTaskInfo(int index) {
      this.index = index;
    }

    public String toString() {
      return "[index=" + index + ", outputStats=" + outputStats + "]";
    }
  }

  public ShuffleVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  static int[] createIndices(int partitionRange, int taskIndex, int offSetPerTask) {
    int startIndex = taskIndex * offSetPerTask;
    int[] indices = new int[partitionRange];
    for (int currentIndex = 0; currentIndex < partitionRange; ++currentIndex) {
      indices[currentIndex] = (startIndex + currentIndex);
    }
    return indices;
  }

  public static class CustomShuffleEdgeManager extends EdgeManagerPluginOnDemand {
    int numSourceTaskOutputs;
    int numDestinationTasks;
    int basePartitionRange;
    int remainderRangeForLastShuffler;
    int numSourceTasks;
    
    int[][] sourceIndices;
    int[][] targetIndices;

    public CustomShuffleEdgeManager(EdgeManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      // Nothing to do. This class isn't currently designed to be used at the DAG API level.
      UserPayload userPayload = getContext().getUserPayload();
      if (userPayload == null || userPayload.getPayload() == null ||
          userPayload.getPayload().limit() == 0) {
        throw new RuntimeException("Could not initialize CustomShuffleEdgeManager"
            + " from provided user payload");
      }
      CustomShuffleEdgeManagerConfig config;
      try {
        config = CustomShuffleEdgeManagerConfig.fromUserPayload(userPayload);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not initialize CustomShuffleEdgeManager"
            + " from provided user payload", e);
      }
      this.numSourceTaskOutputs = config.numSourceTaskOutputs;
      this.numDestinationTasks = config.numDestinationTasks;
      this.basePartitionRange = config.basePartitionRange;
      this.remainderRangeForLastShuffler = config.remainderRangeForLastShuffler;
      this.numSourceTasks = getContext().getSourceVertexNumTasks();
      Preconditions.checkState(this.numDestinationTasks == getContext().getDestinationVertexNumTasks());
    }

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      return numSourceTasks * partitionRange;
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
      return numSourceTaskOutputs;
    }
    
    @Override
    public void routeDataMovementEventToDestination(DataMovementEvent event,
        int sourceTaskIndex, int sourceOutputIndex, 
        Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
      int sourceIndex = event.getSourceIndex();
      int destinationTaskIndex = sourceIndex/basePartitionRange;
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }

      // all inputs from a source task are next to each other in original order
      int targetIndex = 
          sourceTaskIndex * partitionRange 
          + sourceIndex % partitionRange;

      destinationTaskAndInputIndices.put(destinationTaskIndex, Collections.singletonList(targetIndex));
    }

    @Override
    public EventRouteMetadata routeDataMovementEventToDestination(
        int sourceTaskIndex, int sourceOutputIndex, int destTaskIndex) throws Exception {
      int sourceIndex = sourceOutputIndex;
      int destinationTaskIndex = sourceIndex/basePartitionRange;
      if (destinationTaskIndex != destTaskIndex) {
        return null;
      }
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      
      // all inputs from a source task are next to each other in original order
      int targetIndex = 
          sourceTaskIndex * partitionRange 
          + sourceIndex % partitionRange;
      return EventRouteMetadata.create(1, new int[]{targetIndex});
    }
    

    
    @Override
    public void prepareForRouting() throws Exception {
      // target indices derive from num src tasks
      int numSourceTasks = getContext().getSourceVertexNumTasks();
      targetIndices = new int[numSourceTasks][];
      for (int srcTaskIndex=0; srcTaskIndex<numSourceTasks; ++srcTaskIndex) {
        targetIndices[srcTaskIndex] = createIndices(basePartitionRange, srcTaskIndex,
            basePartitionRange);
      }
      
      // source indices derive from num dest tasks (==partitions)
      int numTargetTasks = getContext().getDestinationVertexNumTasks();
      sourceIndices = new int[numTargetTasks][];
      for (int destTaskIndex=0; destTaskIndex<numTargetTasks; ++destTaskIndex) {
        int partitionRange = basePartitionRange;
        if (destTaskIndex == (numTargetTasks-1)) {
          partitionRange = remainderRangeForLastShuffler;
        }
        // skip the basePartitionRange per destination task
        sourceIndices[destTaskIndex] = createIndices(partitionRange, destTaskIndex,
            basePartitionRange);
      }
    }

    private int[] createTargetIndicesForRemainder(int srcTaskIndex) {
      // for the last task just generate on the fly instead of doubling the memory
      return createIndices(remainderRangeForLastShuffler, srcTaskIndex,
          remainderRangeForLastShuffler);
    }
    
    @Override
    public @Nullable EventRouteMetadata routeCompositeDataMovementEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex)
        throws Exception {
      int[] targetIndicesToSend;
      int partitionRange;
      if(destinationTaskIndex == (numDestinationTasks-1)) {
        if (remainderRangeForLastShuffler != basePartitionRange) {
          targetIndicesToSend = createTargetIndicesForRemainder(sourceTaskIndex);
        } else {
          targetIndicesToSend = targetIndices[sourceTaskIndex];
        }
        partitionRange = remainderRangeForLastShuffler;
      } else {
        targetIndicesToSend = targetIndices[sourceTaskIndex];
        partitionRange = basePartitionRange;
      }

      return EventRouteMetadata.create(partitionRange, targetIndicesToSend, 
          sourceIndices[destinationTaskIndex]);
    }

    @Override
    public EventRouteMetadata routeInputSourceTaskFailedEventToDestination(
        int sourceTaskIndex, int destinationTaskIndex) throws Exception {
      int partitionRange = basePartitionRange;
      if (destinationTaskIndex == (numDestinationTasks-1)) {
        partitionRange = remainderRangeForLastShuffler;
      }
      int startOffset = sourceTaskIndex * partitionRange;        
      int[] targetIndices = new int[partitionRange];
      for (int i=0; i<partitionRange; ++i) {
        targetIndices[i] = (startOffset + i);
      }
      return EventRouteMetadata.create(partitionRange, targetIndices);
    }

    @Override
    public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex, 
        Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
      if (remainderRangeForLastShuffler < basePartitionRange) {
        int startOffset = sourceTaskIndex * basePartitionRange;
        List<Integer> allIndices = Lists.newArrayListWithCapacity(basePartitionRange);
        for (int i=0; i<basePartitionRange; ++i) {
          allIndices.add(startOffset + i);
        }
        List<Integer> inputIndices = Collections.unmodifiableList(allIndices);
        for (int i=0; i<numDestinationTasks-1; ++i) {
          destinationTaskAndInputIndices.put(i, inputIndices);
        }
        
        
        startOffset = sourceTaskIndex * remainderRangeForLastShuffler;
        allIndices = Lists.newArrayListWithCapacity(remainderRangeForLastShuffler);
        for (int i=0; i<remainderRangeForLastShuffler; ++i) {
          allIndices.add(startOffset+i);
        }
        inputIndices = Collections.unmodifiableList(allIndices);
        destinationTaskAndInputIndices.put(numDestinationTasks-1, inputIndices);
      } else {
        // all tasks have same pattern
        int startOffset = sourceTaskIndex * basePartitionRange;        
        List<Integer> allIndices = Lists.newArrayListWithCapacity(basePartitionRange);
        for (int i=0; i<basePartitionRange; ++i) {
          allIndices.add(startOffset + i);
        }
        List<Integer> inputIndices = Collections.unmodifiableList(allIndices);
        for (int i=0; i<numDestinationTasks; ++i) {
          destinationTaskAndInputIndices.put(i, inputIndices);
        }
      }
    }

    @Override
    public int routeInputErrorEventToSource(InputReadErrorEvent event,
        int destinationTaskIndex, int destinationFailedInputIndex) {
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      return destinationFailedInputIndex/partitionRange;
    }

    @Override
    public int routeInputErrorEventToSource(int destinationTaskIndex,
        int destinationFailedInputIndex) {
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      return destinationFailedInputIndex/partitionRange;
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
      return numDestinationTasks;
    }
   }

  private static class CustomShuffleEdgeManagerConfig {
    int numSourceTaskOutputs;
    int numDestinationTasks;
    int basePartitionRange;
    int remainderRangeForLastShuffler;

    private CustomShuffleEdgeManagerConfig(int numSourceTaskOutputs,
        int numDestinationTasks,
        int basePartitionRange,
        int remainderRangeForLastShuffler) {
      this.numSourceTaskOutputs = numSourceTaskOutputs;
      this.numDestinationTasks = numDestinationTasks;
      this.basePartitionRange = basePartitionRange;
      this.remainderRangeForLastShuffler = remainderRangeForLastShuffler;
    }

    public UserPayload toUserPayload() {
      return UserPayload.create(
          ByteBuffer.wrap(ShuffleEdgeManagerConfigPayloadProto.newBuilder()
              .setNumSourceTaskOutputs(numSourceTaskOutputs)
              .setNumDestinationTasks(numDestinationTasks)
              .setBasePartitionRange(basePartitionRange)
              .setRemainderRangeForLastShuffler(remainderRangeForLastShuffler)
              .build().toByteArray()));
    }

    public static CustomShuffleEdgeManagerConfig fromUserPayload(
        UserPayload payload) throws InvalidProtocolBufferException {
      ShuffleEdgeManagerConfigPayloadProto proto =
          ShuffleEdgeManagerConfigPayloadProto.parseFrom(ByteString.copyFrom(payload.getPayload()));
      return new CustomShuffleEdgeManagerConfig(
          proto.getNumSourceTaskOutputs(),
          proto.getNumDestinationTasks(),
          proto.getBasePartitionRange(),
          proto.getRemainderRangeForLastShuffler());

    }
  }

  
  @Override
  public synchronized void onVertexStarted(List<TaskAttemptIdentifier> completions) {
    // examine edges after vertex started because until then these may not have been defined
    Map<String, EdgeProperty> inputs = getContext().getInputVertexEdgeProperties();
    for(Map.Entry<String, EdgeProperty> entry : inputs.entrySet()) {
      srcVertexInfo.put(entry.getKey(), new SourceVertexInfo(entry.getValue()));
      // TODO what if derived class has already called this
      // register for status update from all source vertices
      getContext().registerForVertexStateUpdates(entry.getKey(),
          EnumSet.of(VertexState.CONFIGURED));
      if (entry.getValue().getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        bipartiteSources++;
      }
    }
    if(bipartiteSources == 0) {
      throw new TezUncheckedException("Atleast 1 bipartite source should exist");
    }

    for (VertexStateUpdate stateUpdate : pendingStateUpdates) {
      handleVertexStateUpdate(stateUpdate);
    }
    pendingStateUpdates.clear();

    for (VertexManagerEvent vmEvent : pendingVMEvents) {
      handleVertexManagerEvent(vmEvent);
    }
    pendingVMEvents.clear();
    
    // track the tasks in this vertex
    updatePendingTasks();
    
    LOG.info("OnVertexStarted vertex: " + getContext().getVertexName() +
             " with " + totalNumBipartiteSourceTasks + " source tasks and " +
             totalTasksToSchedule + " pending tasks");
    
    if (completions != null) {
      for (TaskAttemptIdentifier attempt : completions) {
        onSourceTaskCompleted(attempt);
      }
    }
    onVertexStartedDone.set(true);
    // for the special case when source has 0 tasks or min fraction == 0
    schedulePendingTasks();
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
    schedulePendingTasks();
  }

  @VisibleForTesting
  void parsePartitionStats(RoaringBitmap partitionStats) {
    Preconditions.checkState(stats != null, "Stats should be initialized");
    Iterator<Integer> it = partitionStats.iterator();
    final DATA_RANGE_IN_MB[] RANGES = DATA_RANGE_IN_MB.values();
    final int RANGE_LEN = RANGES.length;
    while (it.hasNext()) {
      int pos = it.next();
      int index = ((pos) / RANGE_LEN);
      int rangeIndex = ((pos) % RANGE_LEN);
      //Add to aggregated stats and normalize to DATA_RANGE_IN_MB.
      if (RANGES[rangeIndex].getSizeInMB() > 0) {
        stats[index] += RANGES[rangeIndex].getSizeInMB();
      }
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
    // their output will be the same. However, with pipelined events that may not hold.
    TaskIdentifier producerTask = vmEvent.getProducerAttemptIdentifier().getTaskIdentifier();
    if (!taskWithVmEvents.add(producerTask)) {
      LOG.info("Ignoring vertex manager event from: " + producerTask);
      return;
    }

    String vName = producerTask.getVertexIdentifier().getName();
    SourceVertexInfo srcInfo = srcVertexInfo.get(vName);
    Preconditions.checkState(srcInfo != null, "Unknown vmEvent from " + producerTask);

    numVertexManagerEventsReceived++;

    long sourceTaskOutputSize = 0;
    if (vmEvent.getUserPayload() != null) {
      // save output size
      VertexManagerEventPayloadProto proto;
      try {
        proto = VertexManagerEventPayloadProto.parseFrom(ByteString.copyFrom(vmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException(e);
      }
      sourceTaskOutputSize = proto.getOutputSize();

      if (proto.hasPartitionStats()) {
        try {
          RoaringBitmap partitionStats = new RoaringBitmap();
          ByteString compressedPartitionStats = proto.getPartitionStats();
          byte[] rawData = TezCommonUtils.decompressByteStringToByteArray(compressedPartitionStats);
          ByteArrayInputStream bin = new ByteArrayInputStream(rawData);
          partitionStats.deserialize(new DataInputStream(bin));

          parsePartitionStats(partitionStats);
        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
      }
      srcInfo.numVMEventsReceived++;
      srcInfo.outputSize += sourceTaskOutputSize;
      completedSourceTasksOutputSize += sourceTaskOutputSize;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("For attempt: " + vmEvent.getProducerAttemptIdentifier()
          + " received info of output size: " + sourceTaskOutputSize
          + " vertex numEventsReceived: " + srcInfo.numVMEventsReceived
          + " vertex output size: " + srcInfo.outputSize
          + " total numEventsReceived: " + numVertexManagerEventsReceived
          + " total output size: " + completedSourceTasksOutputSize);
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
    if (stats == null) {
      stats = new long[totalTasksToSchedule]; // TODO lost previous data
    }
  }

  Iterable<Map.Entry<String, SourceVertexInfo>> getBipartiteInfo() {
    return Iterables.filter(srcVertexInfo.entrySet(), new Predicate<Map.Entry<String,SourceVertexInfo>>() {
      public boolean apply(Map.Entry<String, SourceVertexInfo> input) {
        return (input.getValue().edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER);
      }
    });
  }

  /**
   * Compute optimal parallelism needed for the job
   * @return true (if parallelism is determined), false otherwise
   */
  @VisibleForTesting
  boolean determineParallelismAndApply(float minSourceVertexCompletedTaskFraction) {
    if(numVertexManagerEventsReceived == 0) {
      if (totalNumBipartiteSourceTasks > 0) {
        return true;
      }
    }
    
    int currentParallelism = pendingTasks.size();
    /**
     * When overall completed output size is not even equal to
     * desiredTaskInputSize, we can wait for some more data to be available to determine
     * better parallelism until max.fraction is reached.  min.fraction is just a hint to the
     * framework and need not be honored strictly in this case.
     */
    boolean canDetermineParallelismLater = (completedSourceTasksOutputSize <
        desiredTaskInputDataSize)
        && (minSourceVertexCompletedTaskFraction < slowStartMaxSrcCompletionFraction);
    if (canDetermineParallelismLater) {
      LOG.info("Defer scheduling tasks; vertex=" + getContext().getVertexName()
          + ", totalNumBipartiteSourceTasks=" + totalNumBipartiteSourceTasks
          + ", completedSourceTasksOutputSize=" + completedSourceTasksOutputSize
          + ", numVertexManagerEventsReceived=" + numVertexManagerEventsReceived
          + ", numBipartiteSourceTasksCompleted=" + numBipartiteSourceTasksCompleted
          + ", minSourceVertexCompletedTaskFraction=" + minSourceVertexCompletedTaskFraction);
      return false;
    }

    // Change this to use per partition stats for more accuracy TEZ-2962.
    // Instead of aggregating overall size and then dividing equally - coalesce partitions until 
    // desired per partition size is achieved.
    long expectedTotalSourceTasksOutputSize = 0;
    for (Map.Entry<String, SourceVertexInfo> vInfo : getBipartiteInfo()) {
      SourceVertexInfo srcInfo = vInfo.getValue();
      if (srcInfo.numTasks > 0 && srcInfo.numVMEventsReceived > 0) {
        // this assumes that 1 vmEvent is received per completed task - TEZ-2961
        expectedTotalSourceTasksOutputSize += 
            (srcInfo.numTasks * srcInfo.outputSize) / srcInfo.numVMEventsReceived;
      }
    }

    LOG.info("Expected output: " + expectedTotalSourceTasksOutputSize + " based on actual output: "
        + completedSourceTasksOutputSize + " from " + numVertexManagerEventsReceived + " vertex manager events. "
        + " desiredTaskInputSize: " + desiredTaskInputDataSize + " max slow start tasks:"
        + (totalNumBipartiteSourceTasks * slowStartMaxSrcCompletionFraction) + " num sources completed:"
        + numBipartiteSourceTasksCompleted);

    int desiredTaskParallelism = 
        (int)(
            (expectedTotalSourceTasksOutputSize+desiredTaskInputDataSize-1)/
            desiredTaskInputDataSize);
    if(desiredTaskParallelism < minTaskParallelism) {
      desiredTaskParallelism = minTaskParallelism;
    }

    if(desiredTaskParallelism >= currentParallelism) {
      LOG.info("Not reducing auto parallelism for vertex: " + getContext().getVertexName()
          + " since the desired parallelism of " + desiredTaskParallelism
          + " is greater than or equal to the current parallelism of " + pendingTasks.size());
      return true;
    }

    // most shufflers will be assigned this range
    basePartitionRange = currentParallelism/desiredTaskParallelism;
    
    if (basePartitionRange <= 1) {
      // nothing to do if range is equal 1 partition. shuffler does it by default
      LOG.info("Not reducing auto parallelism for vertex: " + getContext().getVertexName()
          + " by less than half since combining two inputs will potentially break the desired task input size of "
          + desiredTaskInputDataSize);
      return true;
    }
    
    int numShufflersWithBaseRange = currentParallelism / basePartitionRange;
    remainderRangeForLastShuffler = currentParallelism % basePartitionRange;
    
    int finalTaskParallelism = (remainderRangeForLastShuffler > 0) ?
          (numShufflersWithBaseRange + 1) : (numShufflersWithBaseRange);

    LOG.info("Reducing auto parallelism for vertex: " + getContext().getVertexName()
        + " from " + pendingTasks.size() + " to " + finalTaskParallelism);

    if(finalTaskParallelism < currentParallelism) {
      // final parallelism is less than actual parallelism
      Map<String, EdgeProperty> edgeProperties =
          new HashMap<String, EdgeProperty>(bipartiteSources);
      Iterable<Map.Entry<String, SourceVertexInfo>> bipartiteItr = getBipartiteInfo();
      for(Map.Entry<String, SourceVertexInfo> entry : bipartiteItr) {
        String vertex = entry.getKey();
        EdgeProperty oldEdgeProp = entry.getValue().edgeProperty;
        // use currentParallelism for numSourceTasks to maintain original state
        // for the source tasks
        CustomShuffleEdgeManagerConfig edgeManagerConfig =
            new CustomShuffleEdgeManagerConfig(
                currentParallelism, finalTaskParallelism, basePartitionRange,
                ((remainderRangeForLastShuffler > 0) ?
                    remainderRangeForLastShuffler : basePartitionRange));
        EdgeManagerPluginDescriptor edgeManagerDescriptor =
            EdgeManagerPluginDescriptor.create(CustomShuffleEdgeManager.class.getName());
        edgeManagerDescriptor.setUserPayload(edgeManagerConfig.toUserPayload());
        EdgeProperty newEdgeProp = EdgeProperty.create(edgeManagerDescriptor,
            oldEdgeProp.getDataSourceType(), oldEdgeProp.getSchedulingType(), 
            oldEdgeProp.getEdgeSource(), oldEdgeProp.getEdgeDestination());
        edgeProperties.put(vertex, newEdgeProp);
      }

      getContext().reconfigureVertex(finalTaskParallelism, null, edgeProperties);
      updatePendingTasks();
      configureTargetMapping(finalTaskParallelism);
    }
    return true;
  }

  void configureTargetMapping(int tasks) {
    targetIndexes = new int[tasks][];
    for (int idx = 0; idx < tasks; ++idx) {
      int partitionRange = basePartitionRange;
      if (idx == (tasks - 1)) {
        partitionRange = ((remainderRangeForLastShuffler > 0)
            ? remainderRangeForLastShuffler : basePartitionRange);
      }
      // skip the basePartitionRange per destination task
      targetIndexes[idx] = createIndices(partitionRange, idx, basePartitionRange);
      if (LOG.isDebugEnabled()) {
        LOG.debug("targetIdx[" + idx + "] to " + Arrays.toString(targetIndexes[idx]));
      }
    }
  }

  void schedulePendingTasks(int numTasksToSchedule, float minSourceVertexCompletedTaskFraction) {
    // determine parallelism before scheduling the first time
    // this is the latest we can wait before determining parallelism.
    // currently this depends on task completion and so this is the best time
    // to do this. This is the max time we have until we have to launch tasks 
    // as specified by the user. If/When we move to some other method of 
    // calculating parallelism or change parallelism while tasks are already
    // running then we can create other parameters to trigger this calculation.
    if(enableAutoParallelism && !parallelismDetermined) {
      parallelismDetermined = determineParallelismAndApply(minSourceVertexCompletedTaskFraction);
      if (!parallelismDetermined) {
        //try to determine parallelism later when more info is available.
        return;
      }
      getContext().doneReconfiguringVertex();
    }
    if (totalNumBipartiteSourceTasks > 0) {
      //Sort in case partition stats are available
      sortPendingTasksBasedOnDataSize();
    }
    List<ScheduleTaskRequest> scheduledTasks = Lists.newArrayListWithCapacity(numTasksToSchedule);

    while(!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
      numTasksToSchedule--;
      Integer taskIndex = pendingTasks.get(0).index;
      scheduledTasks.add(ScheduleTaskRequest.create(taskIndex, null));
      pendingTasks.remove(0);
    }

    getContext().scheduleTasks(scheduledTasks);
    if (pendingTasks.size() == 0) {
      // done scheduling all tasks
      // TODO TEZ-1714 locking issues. getContext().vertexManagerDone();
    }
  }

  private void sortPendingTasksBasedOnDataSize() {
    //Get partition sizes from all source vertices
    boolean statsUpdated = computePartitionSizes();

    if (statsUpdated) {
      //Order the pending tasks based on task size in reverse order
      Collections.sort(pendingTasks, new Comparator<PendingTaskInfo>() {
        @Override
        public int compare(PendingTaskInfo left, PendingTaskInfo right) {
          return (left.outputStats > right.outputStats) ? -1 :
              ((left.outputStats == right.outputStats) ? 0 : 1);
        }
      });

      if (LOG.isDebugEnabled()) {
        for (PendingTaskInfo pendingTask : pendingTasks) {
          LOG.debug("Pending task:" + pendingTask.toString());
        }
      }
    }
  }

  /**
   * Compute partition sizes in case statistics are available in vertex.
   *
   * @return boolean indicating whether stats are computed
   */
  private synchronized boolean computePartitionSizes() {
    boolean computedPartitionSizes = false;
    for (PendingTaskInfo taskInfo : pendingTasks) {
      int index = taskInfo.index;
      if (targetIndexes != null) { //parallelism has changed.
        Preconditions.checkState(index < targetIndexes.length,
            "index=" + index +", targetIndexes length=" + targetIndexes.length);
        int[] mapping = targetIndexes[index];
        long totalStats = 0;
        for (int i : mapping) {
          totalStats += stats[i];
        }
        if ((totalStats > 0) && (taskInfo.outputStats != totalStats)) {
          computedPartitionSizes = true;
          taskInfo.outputStats = totalStats;
        }
      } else {
        if ((stats[index] > 0) && (stats[index] != taskInfo.outputStats)) {
          computedPartitionSizes = true;
          taskInfo.outputStats = stats[index];
        }
      }
    }
    return computedPartitionSizes;
  }

  /**
   * Verify whether each of the source vertices have completed at least 1 task
   *
   * @return boolean
   */
  boolean canScheduleTasks() {
    for(Map.Entry<String, SourceVertexInfo> entry : srcVertexInfo.entrySet()) {
      // need to check for vertex configured because until that we dont know if numTasks==0 is valid
      if (!entry.getValue().vertexIsConfigured) { // isConfigured
        // vertex not scheduled tasks
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for vertex: " + entry.getKey() + " in vertex: "
              + getContext().getVertexName());
        }
        return false;
      }
    }
    sourceVerticesScheduled = true;
    return sourceVerticesScheduled;
  }

  void schedulePendingTasks() {
    if (!onVertexStartedDone.get()) {
      // vertex not started yet
      return;
    }
    int numPendingTasks = pendingTasks.size();
    if (numPendingTasks == 0) {
      return;
    }

    if (!sourceVerticesScheduled && !canScheduleTasks()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Defer scheduling tasks for vertex:" + getContext().getVertexName()
            + " as one task needs to be completed per source vertex");
      }
      return;
    }

    if (numBipartiteSourceTasksCompleted == totalNumBipartiteSourceTasks && numPendingTasks > 0) {
      LOG.info("All source tasks assigned. " +
          "Ramping up " + numPendingTasks + 
          " remaining tasks for vertex: " + getContext().getVertexName());
      schedulePendingTasks(numPendingTasks, 1);
      return;
    }

    float minSourceVertexCompletedTaskFraction = 1f;
    String minCompletedVertexName = "";
    for (Map.Entry<String, SourceVertexInfo> vInfo : getBipartiteInfo()) {
      SourceVertexInfo srcInfo = vInfo.getValue();
      // canScheduleTasks check has already verified all sources are configured
      Preconditions.checkState(srcInfo.vertexIsConfigured, "Vertex: " + vInfo.getKey());
      if (srcInfo.numTasks > 0) {
        int numCompletedTasks = srcInfo.getNumCompletedTasks();
        float completedFraction = (float) numCompletedTasks / srcInfo.numTasks;
        if (minSourceVertexCompletedTaskFraction > completedFraction) {
          minSourceVertexCompletedTaskFraction = completedFraction;
          minCompletedVertexName = vInfo.getKey();
        }
      }
    }

    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are 
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1;
    float percentRange = slowStartMaxSrcCompletionFraction - slowStartMinSrcCompletionFraction;
    if (percentRange > 0) {
      tasksFractionToSchedule = 
            (minSourceVertexCompletedTaskFraction - slowStartMinSrcCompletionFraction)/
            percentRange;
    } else {
      // min and max are equal. schedule 100% on reaching min
      if(minSourceVertexCompletedTaskFraction < slowStartMinSrcCompletionFraction) {
        tasksFractionToSchedule = 0;
      }
    }
    
    tasksFractionToSchedule = Math.max(0, Math.min(1, tasksFractionToSchedule));

    int numTasksToSchedule = 
        ((int)(tasksFractionToSchedule * totalTasksToSchedule) - 
         (totalTasksToSchedule - numPendingTasks));
    
    if (numTasksToSchedule > 0) {
      // numTasksToSchedule can be -ve if numBipartiteSourceTasksCompleted does not
      // does not increase monotonically
      LOG.info("Scheduling " + numTasksToSchedule + " tasks for vertex: " + 
               getContext().getVertexName() + " with totalTasks: " +
               totalTasksToSchedule + ". " + numBipartiteSourceTasksCompleted +
               " source tasks completed out of " + totalNumBipartiteSourceTasks +
               ". MinSourceTaskCompletedFraction: " + minSourceVertexCompletedTaskFraction +
               " in Vertex: " + minCompletedVertexName +
               " min: " + slowStartMinSrcCompletionFraction + 
               " max: " + slowStartMaxSrcCompletionFraction);
      schedulePendingTasks(numTasksToSchedule, minSourceVertexCompletedTaskFraction);
    }
  }

  @Override
  public void initialize() {
    Configuration conf;
    try {
      conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }

    this.slowStartMinSrcCompletionFraction = conf
        .getFloat(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    float defaultSlowStartMaxSrcFraction = ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT;
    if (slowStartMinSrcCompletionFraction > defaultSlowStartMaxSrcFraction) {
      defaultSlowStartMaxSrcFraction = slowStartMinSrcCompletionFraction;
    }
    this.slowStartMaxSrcCompletionFraction = conf
        .getFloat(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            defaultSlowStartMaxSrcFraction);

    if (slowStartMinSrcCompletionFraction < 0 || slowStartMaxSrcCompletionFraction > 1
        || slowStartMaxSrcCompletionFraction < slowStartMinSrcCompletionFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinSrcCompletionFraction"
              + "/slowStartMaxSrcCompletionFraction. Min cannot be < 0, max cannot be > 1,"
              + " and max cannot be < min."
              + ", configuredMin=" + slowStartMinSrcCompletionFraction
              + ", configuredMax=" + slowStartMaxSrcCompletionFraction);
    }

    enableAutoParallelism = conf
        .getBoolean(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT);
    desiredTaskInputDataSize = conf
        .getLong(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    minTaskParallelism = Math.max(1, conf
        .getInt(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT));
    LOG.info("Shuffle Vertex Manager: settings" + " minFrac:"
        + slowStartMinSrcCompletionFraction + " maxFrac:"
        + slowStartMaxSrcCompletionFraction + " auto:" + enableAutoParallelism
        + " desiredTaskIput:" + desiredTaskInputDataSize + " minTasks:"
        + minTaskParallelism);

    updatePendingTasks();
    if (enableAutoParallelism) {
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
    LOG.info("Received configured notification : " + stateUpdate.getVertexState() + " for vertex: "
      + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName() + 
      " numBipartiteSourceTasks: " + totalNumBipartiteSourceTasks);
    schedulePendingTasks();
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
  
  /**
   * Create a {@link VertexManagerPluginDescriptor} builder that can be used to
   * configure the plugin.
   * 
   * @param conf
   *          {@link Configuration} May be modified in place. May be null if the
   *          configuration parameters are to be set only via code. If
   *          configuration values may be changed at runtime via a config file
   *          then pass in a {@link Configuration} that is initialized from a
   *          config file. The parameters that are not overridden in code will
   *          be derived from the Configuration object.
   * @return {@link org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager.ShuffleVertexManagerConfigBuilder}
   */
  public static ShuffleVertexManagerConfigBuilder createConfigBuilder(@Nullable Configuration conf) {
    return new ShuffleVertexManagerConfigBuilder(conf);
  }

  /**
   * Helper class to configure ShuffleVertexManager
   */
  public static final class ShuffleVertexManagerConfigBuilder {
    private final Configuration conf;

    private ShuffleVertexManagerConfigBuilder(@Nullable Configuration conf) {
      if (conf == null) {
        this.conf = new Configuration(false);
      } else {
        this.conf = conf;
      }
    }

    public ShuffleVertexManagerConfigBuilder setAutoReduceParallelism(boolean enabled) {
      conf.setBoolean(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, enabled);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setSlowStartMinSrcCompletionFraction(float minFraction) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, minFraction);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setSlowStartMaxSrcCompletionFraction(float maxFraction) {
      conf.setFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, maxFraction);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setDesiredTaskInputSize(long desiredTaskInputSize) {
      conf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          desiredTaskInputSize);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setMinTaskParallelism(int minTaskParallelism) {
      conf.setInt(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
          minTaskParallelism);
      return this;
    }

    public VertexManagerPluginDescriptor build() {
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(ShuffleVertexManager.class.getName());

      try {
        return desc.setUserPayload(TezUtils.createUserPayloadFromConf(this.conf));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }
}

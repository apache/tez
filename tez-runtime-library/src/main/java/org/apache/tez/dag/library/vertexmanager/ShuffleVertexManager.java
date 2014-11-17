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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPlugin;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.ShuffleEdgeManagerConfigPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
   * min-fraction and max-fraction
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

  
  private static final Log LOG = 
                   LogFactory.getLog(ShuffleVertexManager.class);

  float slowStartMinSrcCompletionFraction;
  float slowStartMaxSrcCompletionFraction;
  long desiredTaskInputDataSize = 1024*1024*100L;
  int minTaskParallelism = 1;
  boolean enableAutoParallelism = false;
  boolean parallelismDetermined = false;

  int totalNumBipartiteSourceTasks = 0;
  int numBipartiteSourceTasksCompleted = 0;
  int numVertexManagerEventsReceived = 0;
  List<Integer> pendingTasks = Lists.newLinkedList();
  int totalTasksToSchedule = 0;
  private AtomicBoolean onVertexStartedDone = new AtomicBoolean(false);
  
  //Track source vertex and its finished tasks
  private final Map<String, SourceVertexInfo> srcVertexInfo = Maps.newConcurrentMap();
  boolean sourceVerticesScheduled = false;
  @VisibleForTesting
  int bipartiteSources = 0;
  long completedSourceTasksOutputSize = 0;

  class SourceVertexInfo {
    EdgeProperty edgeProperty;
    boolean vertexIsConfigured;
    BitSet finishedTaskSet;

    SourceVertexInfo(EdgeProperty edgeProperty) {
      this.edgeProperty = edgeProperty;
      if (edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        finishedTaskSet = new BitSet();
      }
    }
  }

  public ShuffleVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  public static class CustomShuffleEdgeManager extends EdgeManagerPlugin {
    int numSourceTaskOutputs;
    int numDestinationTasks;
    int basePartitionRange;
    int remainderRangeForLastShuffler;
    int numSourceTasks;

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
      
      destinationTaskAndInputIndices.put(new Integer(destinationTaskIndex),
          Collections.singletonList(new Integer(targetIndex)));
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
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    // track the tasks in this vertex
    updatePendingTasks();
    updateSourceTaskCount();
    
    LOG.info("OnVertexStarted vertex: " + getContext().getVertexName() +
             " with " + totalNumBipartiteSourceTasks + " source tasks and " +
             totalTasksToSchedule + " pending tasks");
    
    if (completions != null) {
      for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
        for (Integer taskId : entry.getValue()) {
          onSourceTaskCompleted(entry.getKey(), taskId);
        }
      }
    }
    onVertexStartedDone.set(true);
    // for the special case when source has 0 tasks or min fraction == 0
    schedulePendingTasks();
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer srcTaskId) {
    updateSourceTaskCount();
    SourceVertexInfo srcInfo = srcVertexInfo.get(srcVertexName);

    if (srcInfo.edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER) {
      //handle duplicate events for bipartite sources
      BitSet completedSourceTasks = srcInfo.finishedTaskSet;
      if (completedSourceTasks != null) {
        // duplicate notifications tracking
        if (!completedSourceTasks.get(srcTaskId)) {
          completedSourceTasks.set(srcTaskId);
          // source task has completed
          ++numBipartiteSourceTasksCompleted;
        }
      }
    }
    schedulePendingTasks();
  }
  
  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    // TODO handle duplicates from retries
    if (enableAutoParallelism) {
      // save output size
      VertexManagerEventPayloadProto proto;
      try {
        proto = VertexManagerEventPayloadProto.parseFrom(ByteString.copyFrom(vmEvent.getUserPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new TezUncheckedException(e);
      }
      long sourceTaskOutputSize = proto.getOutputSize();
      numVertexManagerEventsReceived++;
      completedSourceTasksOutputSize += sourceTaskOutputSize;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received info of output size: " + sourceTaskOutputSize 
            + " numInfoReceived: " + numVertexManagerEventsReceived
            + " total output size: " + completedSourceTasksOutputSize);
      }
    }
    
  }
  
  void updatePendingTasks() {
    pendingTasks.clear();
    for (int i=0; i<getContext().getVertexNumTasks(getContext().getVertexName()); ++i) {
      pendingTasks.add(new Integer(i));
    }
    totalTasksToSchedule = pendingTasks.size();
  }

  Iterable<Map.Entry<String, SourceVertexInfo>> getBipartiteInfo() {
    return Iterables.filter(srcVertexInfo.entrySet(), new Predicate<Map.Entry<String,SourceVertexInfo>>() {
      public boolean apply(Map.Entry<String, SourceVertexInfo> input) {
        return (input.getValue().edgeProperty.getDataMovementType() == DataMovementType.SCATTER_GATHER);
      }
    });
  }

  void updateSourceTaskCount() {
    // track source vertices
    int numSrcTasks = 0;
    Iterable<Map.Entry<String, SourceVertexInfo>> bipartiteItr = getBipartiteInfo();
    for(Map.Entry<String, SourceVertexInfo> entry : bipartiteItr) {
      numSrcTasks += getContext().getVertexNumTasks(entry.getKey());
    }
    totalNumBipartiteSourceTasks = numSrcTasks;
  }

  /**
   * Compute optimal parallelism needed for the job
   * @return true (if parallelism is determined), false otherwise
   */
  @VisibleForTesting
  boolean determineParallelismAndApply() {
    if(numBipartiteSourceTasksCompleted == 0) {
      return true;
    }
    
    if(numVertexManagerEventsReceived == 0) {
      return true;
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
        && (numBipartiteSourceTasksCompleted < (totalNumBipartiteSourceTasks * slowStartMaxSrcCompletionFraction));
    if (canDetermineParallelismLater) {
      LOG.info("Defer scheduling tasks; vertex=" + getContext().getVertexName()
          + ", totalNumBipartiteSourceTasks=" + totalNumBipartiteSourceTasks
          + ", completedSourceTasksOutputSize=" + completedSourceTasksOutputSize
          + ", numVertexManagerEventsReceived=" + numVertexManagerEventsReceived
          + ", numBipartiteSourceTasksCompleted=" + numBipartiteSourceTasksCompleted + ", maxThreshold="
          + (totalNumBipartiteSourceTasks * slowStartMaxSrcCompletionFraction));
      return false;
    }

    long expectedTotalSourceTasksOutputSize =
        (totalNumBipartiteSourceTasks * completedSourceTasksOutputSize) / numVertexManagerEventsReceived;

    int desiredTaskParallelism = 
        (int)(
            (expectedTotalSourceTasksOutputSize+desiredTaskInputDataSize-1)/
            desiredTaskInputDataSize);
    if(desiredTaskParallelism < minTaskParallelism) {
      desiredTaskParallelism = minTaskParallelism;
    }
    
    if(desiredTaskParallelism >= currentParallelism) {
      return true;
    }
    
    // most shufflers will be assigned this range
    int basePartitionRange = currentParallelism/desiredTaskParallelism;
    
    if (basePartitionRange <= 1) {
      // nothing to do if range is equal 1 partition. shuffler does it by default
      return true;
    }
    
    int numShufflersWithBaseRange = currentParallelism / basePartitionRange;
    int remainderRangeForLastShuffler = currentParallelism % basePartitionRange; 
    
    int finalTaskParallelism = (remainderRangeForLastShuffler > 0) ?
          (numShufflersWithBaseRange + 1) : (numShufflersWithBaseRange);

    LOG.info("Reduce auto parallelism for vertex: " + getContext().getVertexName()
        + " to " + finalTaskParallelism + " from " + pendingTasks.size() 
        + " . Expected output: " + expectedTotalSourceTasksOutputSize 
        + " based on actual output: " + completedSourceTasksOutputSize
        + " from " + numVertexManagerEventsReceived + " vertex manager events. "
        + " desiredTaskInputSize: " + desiredTaskInputDataSize + " max slow start tasks:" +
        (totalNumBipartiteSourceTasks * slowStartMaxSrcCompletionFraction) + " num sources completed:" +
        numBipartiteSourceTasksCompleted);
          
    if(finalTaskParallelism < currentParallelism) {
      // final parallelism is less than actual parallelism
      Map<String, EdgeManagerPluginDescriptor> edgeManagers =
          new HashMap<String, EdgeManagerPluginDescriptor>(bipartiteSources);
      Iterable<Map.Entry<String, SourceVertexInfo>> bipartiteItr = getBipartiteInfo();
      for(Map.Entry<String, SourceVertexInfo> entry : bipartiteItr) {
        String vertex = entry.getKey();
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
        edgeManagers.put(vertex, edgeManagerDescriptor);
      }
      
      getContext().setVertexParallelism(finalTaskParallelism, null, edgeManagers, null);
      updatePendingTasks();
    }
    return true;
  }
  
  void schedulePendingTasks(int numTasksToSchedule) {
    // determine parallelism before scheduling the first time
    // this is the latest we can wait before determining parallelism.
    // currently this depends on task completion and so this is the best time
    // to do this. This is the max time we have until we have to launch tasks 
    // as specified by the user. If/When we move to some other method of 
    // calculating parallelism or change parallelism while tasks are already
    // running then we can create other parameters to trigger this calculation.
    if(enableAutoParallelism && !parallelismDetermined) {
      parallelismDetermined = determineParallelismAndApply();
      if (!parallelismDetermined) {
        //try to determine parallelism later when more info is available.
        return;
      }
      getContext().doneReconfiguringVertex();
    }
    List<TaskWithLocationHint> scheduledTasks = Lists.newArrayListWithCapacity(numTasksToSchedule);
    while(!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
      numTasksToSchedule--;
      scheduledTasks.add(new TaskWithLocationHint(pendingTasks.get(0), null));
      pendingTasks.remove(0);
    }
    getContext().scheduleVertexTasks(scheduledTasks);
    if (pendingTasks.size() == 0) {
      // done scheduling all tasks
      // TODO TEZ-1714 locking issues. getContext().vertexManagerDone();
    }
  }

  /**
   * Verify whether each of the source vertices have completed at least 1 task
   *
   * @return boolean
   */
  boolean canScheduleTasks() {
    for(Map.Entry<String, SourceVertexInfo> entry : srcVertexInfo.entrySet()) {
      String sourceVertex = entry.getKey();
      int numSourceTasks = getContext().getVertexNumTasks(sourceVertex);
      if (numSourceTasks > 0 && !entry.getValue().vertexIsConfigured) {
        // vertex not configured
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
      schedulePendingTasks(numPendingTasks);
      return;
    }

    float completedSourceTaskFraction = 0f;
    if (totalNumBipartiteSourceTasks != 0) { // support for 0 source tasks
      completedSourceTaskFraction = (float) numBipartiteSourceTasksCompleted / totalNumBipartiteSourceTasks;
    } else {
      completedSourceTaskFraction = 1;
    }

    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are 
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1; 
    float percentRange = slowStartMaxSrcCompletionFraction - slowStartMinSrcCompletionFraction;
    if (percentRange > 0) {
      tasksFractionToSchedule = 
            (completedSourceTaskFraction - slowStartMinSrcCompletionFraction)/
            percentRange;
    } else {
      // min and max are equal. schedule 100% on reaching min
      if(completedSourceTaskFraction < slowStartMinSrcCompletionFraction) {
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
               ". SourceTaskCompletedFraction: " + completedSourceTaskFraction + 
               " min: " + slowStartMinSrcCompletionFraction + 
               " max: " + slowStartMaxSrcCompletionFraction);
      schedulePendingTasks(numTasksToSchedule);
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
    this.slowStartMaxSrcCompletionFraction = conf
        .getFloat(
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);

    if (slowStartMinSrcCompletionFraction < 0
        || slowStartMaxSrcCompletionFraction < slowStartMinSrcCompletionFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinSrcCompletionFraction"
              + "/slowStartMaxSrcCompletionFraction. Min cannot be < 0 and "
              + "max cannot be < min.");
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
    
    Map<String, EdgeProperty> inputs = getContext().getInputVertexEdgeProperties();
    for(Map.Entry<String, EdgeProperty> entry : inputs.entrySet()) {
      srcVertexInfo.put(entry.getKey(), new SourceVertexInfo(entry.getValue()));
      getContext().registerForVertexStateUpdates(entry.getKey(),
          EnumSet.of(VertexState.CONFIGURED));
      if (entry.getValue().getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        bipartiteSources++;
      }
    }
    if(bipartiteSources == 0) {
      throw new TezUncheckedException("Atleast 1 bipartite source should exist");
    }
    
    if (enableAutoParallelism) {
      getContext().vertexReconfigurationPlanned();
    }
    // dont track the source tasks here since those tasks may themselves be
    // dynamically changed as the DAG progresses.

  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
    Preconditions.checkArgument(stateUpdate.getVertexState() == VertexState.CONFIGURED,
        "Received incorrect state notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    Preconditions.checkArgument(srcVertexInfo.containsKey(stateUpdate.getVertexName()),
        "Received incorrect vertex notification : " + stateUpdate.getVertexState() + " for vertex: "
            + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    SourceVertexInfo vInfo = srcVertexInfo.get(stateUpdate.getVertexName()); 
    Preconditions.checkState(vInfo.vertexIsConfigured == false);
    vInfo.vertexIsConfigured = true;
    LOG.info("Received configured notification : " + stateUpdate.getVertexState() + " for vertex: "
      + stateUpdate.getVertexName() + " in vertex: " + getContext().getVertexName());
    schedulePendingTasks();
  }
  
  @Override
  public void onRootVertexInitialized(String inputName,
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

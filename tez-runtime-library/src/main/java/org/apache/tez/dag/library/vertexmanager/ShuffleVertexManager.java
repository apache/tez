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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManager;
import org.apache.tez.dag.api.EdgeManagerContext;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.ShuffleEdgeManagerConfigPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Starts scheduling tasks when number of completed source tasks crosses 
 * <code>slowStartMinSrcCompletionFraction</code> and schedules all tasks 
 *  when <code>slowStartMaxSrcCompletionFraction</code> is reached
 */
public class ShuffleVertexManager extends VertexManagerPlugin {
  
  private static final String TEZ_AM_PREFIX = "tez.am.";
  
  /**
   * In case of a ScatterGather connection, the fraction of source tasks which
   * should complete before tasks for the current vertex are scheduled
   */
  public static final String
  TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION = TEZ_AM_PREFIX
  + "shuffle-vertex-manager.min-src-fraction";
  public static final float
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT = 0.25f;
  
  /**
   * In case of a ScatterGather connection, once this fraction of source tasks
   * have completed, all tasks on the current vertex can be scheduled. Number of
   * tasks ready for scheduling on the current vertex scales linearly between
   * min-fraction and max-fraction
   */
  public static final String
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION = TEZ_AM_PREFIX
    + "shuffle-vertex-manager.max-src-fraction";
  public static final float
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT = 0.75f;
  
  public static final String
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL = TEZ_AM_PREFIX +
    "shuffle-vertex-manager.enable.auto-parallel";
  public static final boolean
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT = false;
  
  public static final String
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE = TEZ_AM_PREFIX +
    "shuffle-vertex-manager.desired-task-input-size";
  public static final long
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT =
    1024*1024*100L;
  
  public static final String
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM = TEZ_AM_PREFIX +
    "shuffle-vertex-manager.min-task-parallelism";
  public static final int
    TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT = 1;

  
  private static final Log LOG = 
                   LogFactory.getLog(ShuffleVertexManager.class);

  VertexManagerPluginContext context;
  float slowStartMinSrcCompletionFraction;
  float slowStartMaxSrcCompletionFraction;
  long desiredTaskInputDataSize = 1024*1024*100L;
  int minTaskParallelism = 1;
  boolean enableAutoParallelism = false;
  boolean parallelismDetermined = false;
  
  int totalNumSourceTasks = 0;
  int numSourceTasksCompleted = 0;
  int numVertexManagerEventsReceived = 0;
  List<Integer> pendingTasks;
  int totalTasksToSchedule = 0;
  
  Map<String, Set<Integer>> bipartiteSources = Maps.newHashMap();
  long completedSourceTasksOutputSize = 0;
  
  public ShuffleVertexManager() {
  }
  
  
  public static class CustomShuffleEdgeManager extends EdgeManager {
    int numSourceTaskOutputs;
    int numDestinationTasks;
    int basePartitionRange;
    int remainderRangeForLastShuffler;
    int numSourceTasks;

    public CustomShuffleEdgeManager() {
    }

    @Override
    public void initialize(EdgeManagerContext edgeManagerContext) {
      // Nothing to do. This class isn't currently designed to be used at the DAG API level.
      byte[] userPayload = edgeManagerContext.getUserPayload();
      if (userPayload == null
        || userPayload.length == 0) {
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
      this.numSourceTasks = config.numSourceTasks;
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
    int numSourceTasks;

    private CustomShuffleEdgeManagerConfig(int numSourceTaskOutputs,
        int numDestinationTasks,
        int numSourceTasks,
        int basePartitionRange,
        int remainderRangeForLastShuffler) {
      this.numSourceTaskOutputs = numSourceTaskOutputs;
      this.numDestinationTasks = numDestinationTasks;
      this.basePartitionRange = basePartitionRange;
      this.remainderRangeForLastShuffler = remainderRangeForLastShuffler;
      this.numSourceTasks = numSourceTasks;
    }

    public byte[] toUserPayload() {
      return ShuffleEdgeManagerConfigPayloadProto.newBuilder()
          .setNumSourceTaskOutputs(numSourceTaskOutputs)
          .setNumDestinationTasks(numDestinationTasks)
          .setBasePartitionRange(basePartitionRange)
          .setRemainderRangeForLastShuffler(remainderRangeForLastShuffler)
          .setNumSourceTasks(numSourceTasks)
          .build().toByteArray();
    }

    public static CustomShuffleEdgeManagerConfig fromUserPayload(
        byte[] userPayload) throws InvalidProtocolBufferException {
      ShuffleEdgeManagerConfigPayloadProto proto =
          ShuffleEdgeManagerConfigPayloadProto.parseFrom(userPayload);
      return new CustomShuffleEdgeManagerConfig(
          proto.getNumSourceTaskOutputs(),
          proto.getNumDestinationTasks(),
          proto.getNumSourceTasks(),
          proto.getBasePartitionRange(),
          proto.getRemainderRangeForLastShuffler());

    }
  }

  
  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    pendingTasks = Lists.newArrayListWithCapacity(
        context.getVertexNumTasks(context.getVertexName()));
    // track the tasks in this vertex
    updatePendingTasks();
    updateSourceTaskCount();
    
    LOG.info("OnVertexStarted vertex: " + context.getVertexName() + 
             " with " + totalNumSourceTasks + " source tasks and " + 
             totalTasksToSchedule + " pending tasks");
    
    if (completions != null) {
      for (Map.Entry<String, List<Integer>> entry : completions.entrySet()) {
        for (Integer taskId : entry.getValue()) {
          onSourceTaskCompleted(entry.getKey(), taskId);
        }
      }
    }
    // for the special case when source has 0 tasks or min fraction == 0
    schedulePendingTasks();
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer srcTaskId) {
    updateSourceTaskCount();
    Set<Integer> completedSourceTasks = bipartiteSources.get(srcVertexName);
    if (completedSourceTasks != null) {
      // duplicate notifications tracking
      if (completedSourceTasks.add(srcTaskId)) {
        // source task has completed
        ++numSourceTasksCompleted;
      }
      schedulePendingTasks();
    }
  }
  
  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    // TODO handle duplicates from retries
    if (enableAutoParallelism) {
      // save output size
      VertexManagerEventPayloadProto proto;
      try {
        proto = VertexManagerEventPayloadProto.parseFrom(vmEvent.getUserPayload());
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
    for (int i=0; i<context.getVertexNumTasks(context.getVertexName()); ++i) {
      pendingTasks.add(new Integer(i));
    }
    totalTasksToSchedule = pendingTasks.size();
  }
  
  void updateSourceTaskCount() {
    // track source vertices
    int numSrcTasks = 0;
    for(String vertex : bipartiteSources.keySet()) {
      numSrcTasks += context.getVertexNumTasks(vertex);
    }
    totalNumSourceTasks = numSrcTasks;
  }

  void determineParallelismAndApply() {
    if(numSourceTasksCompleted == 0) {
      return;
    }
    
    if(numVertexManagerEventsReceived == 0) {
      return;
    }
    
    int currentParallelism = pendingTasks.size();
    long expectedTotalSourceTasksOutputSize = 
        (totalNumSourceTasks*completedSourceTasksOutputSize)/numVertexManagerEventsReceived;
    int desiredTaskParallelism = 
        (int)(
            (expectedTotalSourceTasksOutputSize+desiredTaskInputDataSize-1)/
            desiredTaskInputDataSize);
    if(desiredTaskParallelism < minTaskParallelism) {
      desiredTaskParallelism = minTaskParallelism;
    }
    
    if(desiredTaskParallelism >= currentParallelism) {
      return;
    }
    
    // most shufflers will be assigned this range
    int basePartitionRange = currentParallelism/desiredTaskParallelism;
    
    if (basePartitionRange <= 1) {
      // nothing to do if range is equal 1 partition. shuffler does it by default
      return;
    }
    
    int numShufflersWithBaseRange = currentParallelism / basePartitionRange;
    int remainderRangeForLastShuffler = currentParallelism % basePartitionRange; 
    
    int finalTaskParallelism = (remainderRangeForLastShuffler > 0) ?
          (numShufflersWithBaseRange + 1) : (numShufflersWithBaseRange);

    LOG.info("Reduce auto parallelism for vertex: " + context.getVertexName() 
        + " to " + finalTaskParallelism + " from " + pendingTasks.size() 
        + " . Expected output: " + expectedTotalSourceTasksOutputSize 
        + " based on actual output: " + completedSourceTasksOutputSize
        + " from " + numVertexManagerEventsReceived + " vertex manager events. "
        + " desiredTaskInputSize: " + desiredTaskInputDataSize);
          
    if(finalTaskParallelism < currentParallelism) {
      // final parallelism is less than actual parallelism
      Map<String, EdgeManagerDescriptor> edgeManagers =
          new HashMap<String, EdgeManagerDescriptor>(bipartiteSources.size());
      for(String vertex : bipartiteSources.keySet()) {
        // use currentParallelism for numSourceTasks to maintain original state
        // for the source tasks
        CustomShuffleEdgeManagerConfig edgeManagerConfig =
            new CustomShuffleEdgeManagerConfig(
                currentParallelism, finalTaskParallelism, 
                context.getVertexNumTasks(vertex), basePartitionRange,
                ((remainderRangeForLastShuffler > 0) ?
                    remainderRangeForLastShuffler : basePartitionRange));
        EdgeManagerDescriptor edgeManagerDescriptor =
            new EdgeManagerDescriptor(CustomShuffleEdgeManager.class.getName());
        edgeManagerDescriptor.setUserPayload(edgeManagerConfig.toUserPayload());
        edgeManagers.put(vertex, edgeManagerDescriptor);
      }
      
      context.setVertexParallelism(finalTaskParallelism, null, edgeManagers, null);
      updatePendingTasks();      
    }
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
      // do this once
      parallelismDetermined = true;
      determineParallelismAndApply();
    }
    List<TaskWithLocationHint> scheduledTasks = Lists.newArrayListWithCapacity(numTasksToSchedule);
    while(!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
      numTasksToSchedule--;
      scheduledTasks.add(new TaskWithLocationHint(pendingTasks.get(0), null));
      pendingTasks.remove(0);
    }
    context.scheduleVertexTasks(scheduledTasks);
  }
  
  void schedulePendingTasks() {
    int numPendingTasks = pendingTasks.size();
    if (numPendingTasks == 0) {
      return;
    }
    
    if (numSourceTasksCompleted == totalNumSourceTasks && numPendingTasks > 0) {
      LOG.info("All source tasks assigned. " +
          "Ramping up " + numPendingTasks + 
          " remaining tasks for vertex: " + context.getVertexName());
      schedulePendingTasks(numPendingTasks);
      return;
    }

    float completedSourceTaskFraction = 0f;
    if (totalNumSourceTasks != 0) { // support for 0 source tasks
      completedSourceTaskFraction = (float)numSourceTasksCompleted/totalNumSourceTasks;
    } else {
      completedSourceTaskFraction = 1;
    }
    
    // start scheduling when source tasks completed fraction is more than min.
    // linearly increase the number of scheduled tasks such that all tasks are 
    // scheduled when source tasks completed fraction reaches max
    float tasksFractionToSchedule = 1; 
    float percentRange = slowStartMaxSrcCompletionFraction - 
                          slowStartMinSrcCompletionFraction;
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
    
    if (tasksFractionToSchedule > 1) {
      tasksFractionToSchedule = 1;
    } else if (tasksFractionToSchedule < 0) {
      tasksFractionToSchedule = 0;
    }
    
    int numTasksToSchedule = 
        ((int)(tasksFractionToSchedule * totalTasksToSchedule) - 
         (totalTasksToSchedule - numPendingTasks));
    
    if (numTasksToSchedule > 0) {
      // numTasksToSchedule can be -ve if numSourceTasksCompleted does not 
      // does not increase monotonically
      LOG.info("Scheduling " + numTasksToSchedule + " tasks for vertex: " + 
               context.getVertexName() + " with totalTasks: " + 
               totalTasksToSchedule + ". " + numSourceTasksCompleted + 
               " source tasks completed out of " + totalNumSourceTasks + 
               ". SourceTaskCompletedFraction: " + completedSourceTaskFraction + 
               " min: " + slowStartMinSrcCompletionFraction + 
               " max: " + slowStartMaxSrcCompletionFraction);
      schedulePendingTasks(numTasksToSchedule);
    }
  }

  @Override
  public void initialize(VertexManagerPluginContext context) {
    Configuration conf;
    try {
      conf = TezUtils.createConfFromUserPayload(context.getUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    
    this.context = context;

    this.slowStartMinSrcCompletionFraction = conf
        .getFloat(
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    this.slowStartMaxSrcCompletionFraction = conf
        .getFloat(
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);

    if (slowStartMinSrcCompletionFraction < 0
        || slowStartMaxSrcCompletionFraction < slowStartMinSrcCompletionFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinSrcCompletionFraction"
              + "/slowStartMaxSrcCompletionFraction. Min cannot be < 0 and "
              + "max cannot be < min.");
    }

    enableAutoParallelism = conf
        .getBoolean(
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT);
    desiredTaskInputDataSize = conf
        .getLong(
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    minTaskParallelism = Math.max(1, conf
        .getInt(
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
            ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT));
    LOG.info("Shuffle Vertex Manager: settings" + " minFrac:"
        + slowStartMinSrcCompletionFraction + " maxFrac:"
        + slowStartMaxSrcCompletionFraction + " auto:" + enableAutoParallelism
        + " desiredTaskIput:" + desiredTaskInputDataSize + " minTasks:"
        + minTaskParallelism);
    
    Map<String, EdgeProperty> inputs = context.getInputVertexEdgeProperties();
    for(Map.Entry<String, EdgeProperty> entry : inputs.entrySet()) {
      if (entry.getValue().getDataMovementType() == DataMovementType.SCATTER_GATHER) {
        String vertex = entry.getKey();
        bipartiteSources.put(vertex, new HashSet<Integer>());
      }
    }
    if(bipartiteSources.isEmpty()) {
      throw new TezUncheckedException("Atleast 1 bipartite source should exist");
    }
    // dont track the source tasks here since those tasks may themselves be
    // dynamically changed as the DAG progresses.

  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    // Not allowing this for now. Nothing to do.
  }

}

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

package org.apache.tez.dag.app.dag.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.app.dag.EdgeManager;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.VertexManager;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Starts scheduling tasks when number of completed source tasks crosses 
 * <code>slowStartMinSrcCompletionFraction</code> and schedules all tasks 
 *  when <code>slowStartMaxSrcCompletionFraction</code> is reached
 */
public class ShuffleVertexManager extends VertexManager {
  
  private static final Log LOG = 
                   LogFactory.getLog(ShuffleVertexManager.class);

  final Vertex managedVertex;
  float slowStartMinSrcCompletionFraction;
  float slowStartMaxSrcCompletionFraction;
  long desiredTaskInputDataSize = 1024*1024*100L;
  int minTaskParallelism = 1;
  boolean enableAutoParallelism = false;
  boolean parallelismDetermined = false;
  
  int numSourceTasks = 0;
  int numSourceTasksCompleted = 0;
  int numVertexManagerEventsReceived = 0;
  ArrayList<TezTaskID> pendingTasks;
  int totalTasksToSchedule = 0;
  HashMap<TezVertexID, Vertex> bipartiteSources = 
                                            new HashMap<TezVertexID, Vertex>();
  
  Set<TezTaskID> completedSourceTasks = new HashSet<TezTaskID>();
  long completedSourceTasksOutputSize = 0;
  
  public ShuffleVertexManager(Vertex managedVertex) {
    this.managedVertex = managedVertex;
    Map<Vertex, Edge> inputs = managedVertex.getInputVertices();
    for(Map.Entry<Vertex, Edge> entry : inputs.entrySet()) {
      if (entry.getValue().getEdgeProperty().getDataMovementType() == 
          DataMovementType.SCATTER_GATHER) {
        Vertex vertex = entry.getKey();
        bipartiteSources.put(vertex.getVertexId(), vertex);
      }
    }
    if(bipartiteSources.isEmpty()) {
      throw new TezUncheckedException("Atleast 1 bipartite source should exist");
    }
    // dont track the source tasks here since those tasks may themselves be
    // dynamically changed as the DAG progresses.
  }
  
  
  public class CustomShuffleEdgeManager extends EdgeManager {
    int numSourceTaskOutputs;
    int numDestinationTasks;
    int basePartitionRange;
    int remainderRangeForLastShuffler;
    
    CustomShuffleEdgeManager(int numSourceTaskOutputs, int numDestinationTasks,
        int basePartitionRange, int remainderPartitionForLastShuffler) {
      this.numSourceTaskOutputs = numSourceTaskOutputs;
      this.numDestinationTasks = numDestinationTasks;
      this.basePartitionRange = basePartitionRange;
      this.remainderRangeForLastShuffler = remainderPartitionForLastShuffler;
    }

    @Override
    public int getNumDestinationTaskInputs(int numSourceTasks, 
        int destinationTaskIndex) {
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      return numSourceTasks * partitionRange;
    }

    @Override
    public int getNumSourceTaskOutputs(int numDestinationTasks, 
        int sourceTaskIndex) {
      return numSourceTaskOutputs;
    }
    
    @Override
    public void routeEventToDestinationTasks(DataMovementEvent event,
        int sourceTaskIndex, int numDestinationTasks, List<Integer> taskIndices) {
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
      
      event.setTargetIndex(targetIndex);
      taskIndices.add(new Integer(destinationTaskIndex));
    }

    @Override
    public void routeEventToDestinationTasks(InputFailedEvent event,
        int sourceTaskIndex, int numDestinationTasks, List<Integer> taskIndices) {
      int sourceIndex = event.getSourceIndex();
      int destinationTaskIndex = sourceIndex/basePartitionRange;
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      int targetIndex = 
          sourceTaskIndex * partitionRange 
          + sourceIndex % partitionRange;
      
      event.setTargetIndex(targetIndex);
      taskIndices.add(new Integer(destinationTaskIndex));
    }

    @Override
    public int routeEventToSourceTasks(int destinationTaskIndex,
        InputReadErrorEvent event) {
      int partitionRange = 1;
      if(destinationTaskIndex < numDestinationTasks-1) {
        partitionRange = basePartitionRange;
      } else {
        partitionRange = remainderRangeForLastShuffler;
      }
      return event.getIndex()/partitionRange;
    }

    @Override
    public int getDestinationConsumerTaskNumber(int sourceTaskIndex,
        int numDestTasks) {
      return numDestTasks;
    }
  }

  
  @Override
  public void onVertexStarted(List<TezTaskAttemptID> completions) {
    pendingTasks = new ArrayList<TezTaskID>(managedVertex.getTotalTasks());
    // track the tasks in this vertex
    updatePendingTasks();
    updateSourceTaskCount();
    
    LOG.info("OnVertexStarted vertex: " + managedVertex.getVertexId() + 
             " with " + numSourceTasks + " source tasks and " + 
             totalTasksToSchedule + " pending tasks");
    
    if (completions != null) {
      for (TezTaskAttemptID srcAttemptId : completions) {
        onSourceTaskCompleted(srcAttemptId);
      }
    }
    // for the special case when source has 0 tasks or min fraction == 0
    schedulePendingTasks();
  }

  @Override
  public void onSourceTaskCompleted(TezTaskAttemptID srcAttemptId) {
    updateSourceTaskCount();
    TezTaskID srcTaskId = srcAttemptId.getTaskID();
    TezVertexID srcVertexId = srcTaskId.getVertexID();
    if (bipartiteSources.containsKey(srcVertexId)) {
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
    pendingTasks.addAll(managedVertex.getTasks().keySet());
    totalTasksToSchedule = pendingTasks.size();
  }
  
  void updateSourceTaskCount() {
    // track source vertices
    int numSrcTasks = 0;
    for(Vertex vertex : bipartiteSources.values()) {
      numSrcTasks += vertex.getTotalTasks();
    }
    numSourceTasks = numSrcTasks;
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
        (numSourceTasks*completedSourceTasksOutputSize)/numVertexManagerEventsReceived;
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

    LOG.info("Reduce auto parallelism for vertex: " + managedVertex.getVertexId() 
        + " to " + finalTaskParallelism + " from " + pendingTasks.size() 
        + " . Expected output: " + expectedTotalSourceTasksOutputSize 
        + " based on actual output: " + completedSourceTasksOutputSize
        + " from " + numVertexManagerEventsReceived + " vertex manager events. "
        + " desiredTaskInputSize: " + desiredTaskInputDataSize);
          
    if(finalTaskParallelism < currentParallelism) {
      // final parallelism is less than actual parallelism
      Map<Vertex, EdgeManager> edgeManagers = new HashMap<Vertex, EdgeManager>(
          bipartiteSources.size());
      for(Vertex vertex : bipartiteSources.values()) {
        // use currentParallelism for numSourceTasks to maintain original state
        // for the source tasks
        edgeManagers.put(vertex, new CustomShuffleEdgeManager(
            currentParallelism, finalTaskParallelism, basePartitionRange,
            ((remainderRangeForLastShuffler > 0) ?
                remainderRangeForLastShuffler : basePartitionRange)));
      }
      
      managedVertex.setParallelism(finalTaskParallelism, edgeManagers);
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
    ArrayList<TezTaskID> scheduledTasks = new ArrayList<TezTaskID>(numTasksToSchedule);
    while(!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
      numTasksToSchedule--;
      scheduledTasks.add(pendingTasks.get(0));
      pendingTasks.remove(0);
    }
    managedVertex.scheduleTasks(scheduledTasks);
  }
  
  void schedulePendingTasks() {    
    int numPendingTasks = pendingTasks.size();
    if (numPendingTasks == 0) {
      return;
    }
    
    if (numSourceTasksCompleted == numSourceTasks && numPendingTasks > 0) {
      LOG.info("All source tasks assigned. " +
          "Ramping up " + numPendingTasks + 
          " remaining tasks for vertex: " + managedVertex.getName());
      schedulePendingTasks(numPendingTasks);
      return;
    }

    float completedSourceTaskFraction = 0f;
    if (numSourceTasks != 0) { // support for 0 source tasks
      completedSourceTaskFraction = (float)numSourceTasksCompleted/numSourceTasks;
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
               managedVertex.getVertexId() + " with totalTasks: " + 
               totalTasksToSchedule + ". " + numSourceTasksCompleted + 
               " source tasks completed out of " + numSourceTasks + 
               ". SourceTaskCompletedFraction: " + completedSourceTaskFraction + 
               " min: " + slowStartMinSrcCompletionFraction + 
               " max: " + slowStartMaxSrcCompletionFraction);
      schedulePendingTasks(numTasksToSchedule);
    }
  }

  @Override
  public void initialize(Configuration conf) {
    this.slowStartMinSrcCompletionFraction = conf
        .getFloat(
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    this.slowStartMaxSrcCompletionFraction = conf
        .getFloat(
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT);
    
    if(slowStartMinSrcCompletionFraction < 0 || 
       slowStartMaxSrcCompletionFraction < slowStartMinSrcCompletionFraction) {
      throw new IllegalArgumentException(
          "Invalid values for slowStartMinSrcCompletionFraction" + 
          "/slowStartMaxSrcCompletionFraction. Min cannot be < 0 and " + 
          "max cannot be < min.");
    }
    
    enableAutoParallelism = conf
        .getBoolean(
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT);
    desiredTaskInputDataSize = conf
        .getLong(
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT);
    minTaskParallelism = conf.getInt(
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
            TezConfiguration.TEZ_AM_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT);
    LOG.info("Shuffle Vertex Manager: settings" + 
            " minFrac:" + slowStartMinSrcCompletionFraction +
            " maxFrac:" + slowStartMaxSrcCompletionFraction +
            " auto:" + enableAutoParallelism + 
            " desiredTaskIput:" + desiredTaskInputDataSize +
            " minTasks:" + minTaskParallelism);
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
    // Not allowing this for now. Nothing to do.
  }

}

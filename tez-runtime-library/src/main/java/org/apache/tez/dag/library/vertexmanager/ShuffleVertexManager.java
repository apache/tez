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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeManagerPluginOnDemand;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.ShuffleEdgeManagerConfigPayloadProto;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Starts scheduling tasks when number of completed source tasks crosses 
 * <code>slowStartMinSrcCompletionFraction</code> and schedules all tasks 
 *  when <code>slowStartMaxSrcCompletionFraction</code> is reached
 */
@Public
@Evolving
public class ShuffleVertexManager extends ShuffleVertexManagerBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ShuffleVertexManager.class);

  /**
   * The desired size of input per task. Parallelism will be changed to meet this criteria
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE =
      "tez.shuffle-vertex-manager.desired-task-input-size";
  public static final long
      TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT = 100 * MB;

  /**
   * Enables automatic parallelism determination for the vertex. Based on input data
   * statistics the parallelism is decreased to a desired level.
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL =
      "tez.shuffle-vertex-manager.enable.auto-parallel";
  public static final boolean
      TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT = false;

  /**
   * Automatic parallelism determination will not decrease parallelism below this value
   */
  public static final String TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM =
      "tez.shuffle-vertex-manager.min-task-parallelism";
  public static final int TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT = 1;


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

  ShuffleVertexManagerConfig mgrConfig;

  private int[][] targetIndexes;
  private int basePartitionRange;
  private int remainderRangeForLastShuffler;


  public ShuffleVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  static class ShuffleVertexManagerConfig extends ShuffleVertexManagerBaseConfig {
    final int minTaskParallelism;
    public ShuffleVertexManagerConfig(final boolean enableAutoParallelism,
        final long desiredTaskInputDataSize, final float slowStartMinFraction,
        final float slowStartMaxFraction, final int minTaskParallelism) {
      super(enableAutoParallelism, desiredTaskInputDataSize,
          slowStartMinFraction, slowStartMaxFraction);
      this.minTaskParallelism = minTaskParallelism;
      LOG.info("minTaskParallelism {}", this.minTaskParallelism);
    }
    int getMinTaskParallelism() {
      return minTaskParallelism;
    }
  }

  @Override
  ShuffleVertexManagerBaseConfig initConfiguration() {
    float slowStartMinFraction = conf.getFloat(
        TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
        TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);

    mgrConfig = new ShuffleVertexManagerConfig(
        conf.getBoolean(
            TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT),
        conf.getLong(
            TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
            TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT),
        slowStartMinFraction,
        conf.getFloat(
            TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            Math.max(slowStartMinFraction,
            TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT)),
        Math.max(1, conf
            .getInt(TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
            TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM_DEFAULT)));
    return mgrConfig;
  }

  static int[] createIndices(int partitionRange, int taskIndex,
      int offSetPerTask) {
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

      destinationTaskAndInputIndices.put(
          destinationTaskIndex, Collections.singletonList(targetIndex));
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
    public @Nullable CompositeEventRouteMetadata routeCompositeDataMovementEventToDestination(
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

      return CompositeEventRouteMetadata.create(partitionRange, targetIndicesToSend[0], 
          sourceIndices[destinationTaskIndex][0]);
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

  ReconfigVertexParams computeRouting() {
    int currentParallelism = pendingTasks.size();

    // Change this to use per partition stats for more accuracy TEZ-2962.
    // Instead of aggregating overall size and then dividing equally - coalesce partitions until
    // desired per partition size is achieved.
    BigInteger expectedTotalSourceTasksOutputSize =
        getExpectedTotalBipartiteSourceTasksOutputSize();

    LOG.info("Expected output: {} based on actual output: {} from {} vertex " +
        "manager events. desiredTaskInputSize: {} max slow start tasks: {} " +
        " num sources completed: {}", expectedTotalSourceTasksOutputSize,
        completedSourceTasksOutputSize, numVertexManagerEventsReceived,
        config.getDesiredTaskInputDataSize(),
        (totalNumBipartiteSourceTasks * config.getMaxFraction()),
        numBipartiteSourceTasksCompleted);

    // Calculate number of desired tasks by dividing with rounding up
    BigInteger desiredTaskInputDataSize = BigInteger.valueOf(config.getDesiredTaskInputDataSize());
    BigInteger desiredTaskInputDataSizeMinusOne = BigInteger.valueOf(config.getDesiredTaskInputDataSize() - 1);
    BigInteger bigDesiredTaskParallelism =
        expectedTotalSourceTasksOutputSize.add(desiredTaskInputDataSizeMinusOne).divide(desiredTaskInputDataSize);

    if(bigDesiredTaskParallelism.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
      LOG.info("Not reducing auto parallelism for vertex: {}"
              + " since the desired parallelism of {} is greater than or equal"
              + " to the max parallelism of {}", getContext().getVertexName(),
          bigDesiredTaskParallelism, Integer.MAX_VALUE);
      return null;
    }
    int desiredTaskParallelism = bigDesiredTaskParallelism.intValue();
    if(desiredTaskParallelism < mgrConfig.getMinTaskParallelism()) {
      desiredTaskParallelism = mgrConfig.getMinTaskParallelism();
    }

    if(desiredTaskParallelism >= currentParallelism) {
      LOG.info("Not reducing auto parallelism for vertex: {}"
          + " since the desired parallelism of {} is greater than or equal"
          + " to the current parallelism of {}", getContext().getVertexName(),
          desiredTaskParallelism, pendingTasks.size());
      return null;
    }

    // most shufflers will be assigned this range
    basePartitionRange = currentParallelism/desiredTaskParallelism;
    if (basePartitionRange <= 1) {
      // nothing to do if range is equal 1 partition. shuffler does it by default
      LOG.info("Not reducing auto parallelism for vertex: {} by less than"
          + " half since combining two inputs will potentially break the"
          + " desired task input size of {}", getContext().getVertexName(),
          config.getDesiredTaskInputDataSize());
      return null;
    }
    int numShufflersWithBaseRange = currentParallelism / basePartitionRange;
    remainderRangeForLastShuffler = currentParallelism % basePartitionRange;

    int finalTaskParallelism = (remainderRangeForLastShuffler > 0) ?
        (numShufflersWithBaseRange + 1) : (numShufflersWithBaseRange);

    LOG.info("Reducing auto parallelism for vertex: {} from {} to {}",
        getContext().getVertexName(), pendingTasks.size(),
        finalTaskParallelism);

    if(finalTaskParallelism >= currentParallelism) {
      return null;
    }

    CustomShuffleEdgeManagerConfig edgeManagerConfig =
        new CustomShuffleEdgeManagerConfig(
            currentParallelism, finalTaskParallelism, basePartitionRange,
            ((remainderRangeForLastShuffler > 0) ?
            remainderRangeForLastShuffler : basePartitionRange));
    EdgeManagerPluginDescriptor descriptor =
        EdgeManagerPluginDescriptor.create(CustomShuffleEdgeManager.class.getName());
    descriptor.setUserPayload(edgeManagerConfig.toUserPayload());

    Iterable<Map.Entry<String, SourceVertexInfo>> bipartiteItr = getBipartiteInfo();
    for(Map.Entry<String, SourceVertexInfo> entry : bipartiteItr) {
      entry.getValue().newDescriptor = descriptor;
    }
    ReconfigVertexParams params =
        new ReconfigVertexParams(finalTaskParallelism, null);
    return params;
  }

  @Override
  void postReconfigVertex() {
      configureTargetMapping(pendingTasks.size());
  }

  private void configureTargetMapping(int tasks) {
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
        LOG.debug("targetIdx[{}] to {}", idx,
            Arrays.toString(targetIndexes[idx]));
      }
    }
  }

  /**
   * Get the list of tasks to schedule based on the overall progress.
   * Parameter completedSourceAttempt is part of the base class used by other
   * VertexManagerPlugins; it isn't used here.
   */
  @Override
  List<ScheduleTaskRequest> getTasksToSchedule(
      TaskAttemptIdentifier completedSourceAttempt) {
    float minSourceVertexCompletedTaskFraction =
        getMinSourceVertexCompletedTaskFraction();
    int numTasksToSchedule = getNumOfTasksToScheduleAndLog(
        minSourceVertexCompletedTaskFraction);
    if (numTasksToSchedule > 0) {
      List<ScheduleTaskRequest> tasksToSchedule =
          Lists.newArrayListWithCapacity(numTasksToSchedule);

      while (!pendingTasks.isEmpty() && numTasksToSchedule > 0) {
        numTasksToSchedule--;
        Integer taskIndex = pendingTasks.get(0).getIndex();
        tasksToSchedule.add(ScheduleTaskRequest.create(taskIndex, null));
        pendingTasks.remove(0);
      }
      return tasksToSchedule;
    }
    return null;
  }

  @Override
  void processPendingTasks() {
    if (totalNumBipartiteSourceTasks > 0) {
      //Sort in case partition stats are available
      sortPendingTasksBasedOnDataSize();
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
          return (left.getInputStats() > right.getInputStats()) ? -1 :
              ((left.getInputStats() == right.getInputStats()) ? 0 : 1);
        }
      });

      if (LOG.isDebugEnabled()) {
        for (PendingTaskInfo pendingTask : pendingTasks) {
          LOG.debug("Pending task: {}", pendingTask.toString());
        }
      }
    }
  }

  /**
   * Compute partition sizes in case statistics are available in vertex.
   *
   * @return boolean indicating whether stats are computed
   */
  private boolean computePartitionSizes() {
    boolean computedPartitionSizes = false;
    for (PendingTaskInfo taskInfo : pendingTasks) {
      int index = taskInfo.getIndex();
      if (targetIndexes != null) { //parallelism has changed.
        Preconditions.checkState(index < targetIndexes.length,
            "index=" + index +", targetIndexes length=" + targetIndexes.length);
        int[] mapping = targetIndexes[index];
        int partitionStats = 0;
        for (int i : mapping) {
          partitionStats += getCurrentlyKnownStatsAtIndex(i);
        }
        computedPartitionSizes |= taskInfo.setInputStats(partitionStats);
      } else {
        computedPartitionSizes |= taskInfo.setInputStats(
            getCurrentlyKnownStatsAtIndex(index));
      }
    }
    return computedPartitionSizes;
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
   * @return {@link ShuffleVertexManagerConfigBuilder}
   */
  public static ShuffleVertexManagerConfigBuilder createConfigBuilder(
      @Nullable Configuration conf) {
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

    public ShuffleVertexManagerConfigBuilder setAutoReduceParallelism(
        boolean enabled) {
      conf.setBoolean(TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
          enabled);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder
        setSlowStartMinSrcCompletionFraction(float minFraction) {
      conf.setFloat(TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, minFraction);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder
        setSlowStartMaxSrcCompletionFraction(float maxFraction) {
      conf.setFloat(TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, maxFraction);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setDesiredTaskInputSize(
        long desiredTaskInputSize) {
      conf.setLong(TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          desiredTaskInputSize);
      return this;
    }

    public ShuffleVertexManagerConfigBuilder setMinTaskParallelism(
        int minTaskParallelism) {
      conf.setInt(TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM,
        minTaskParallelism);
      return this;
    }

    public VertexManagerPluginDescriptor build() {
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(
              ShuffleVertexManager.class.getName());

      try {
        return desc.setUserPayload(TezUtils.createUserPayloadFromConf(this.conf));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }
}

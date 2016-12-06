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
import com.google.common.collect.UnmodifiableIterator;

import com.google.common.primitives.Ints;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginContext.ScheduleTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * Fair routing based on partition size distribution to achieve optimal
 * input size for any destination task thus reduce data skewness.
 * By default the feature is turned off and it supports the regular shuffle like
 * ShuffleVertexManager.
 * When the feature is turned on, there are two routing types as defined in
 * {@link FairRoutingType}. One is {@link FairRoutingType#REDUCE_PARALLELISM}
 * which is similar to ShuffleVertexManager's auto reduce functionality.
 * Another one is {@link FairRoutingType#FAIR_PARALLELISM} where each
 * destination task can process a range of consecutive partitions from a range
 * of consecutive source tasks.
 */
@Public
@Evolving
public class FairShuffleVertexManager extends ShuffleVertexManagerBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(FairShuffleVertexManager.class);

  /**
   * The desired size of input per task. Parallelism will be changed to meet
   * this criteria.
   */
  public static final String TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE =
      "tez.fair-shuffle-vertex-manager.desired-task-input-size";
  public static final long
      TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT = 100 * MB;

  /**
   * Enables automatic parallelism determination for the vertex. Based on input data
   * statistics the parallelism is adjusted to a desired level.
   */
  public static final String TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL =
      "tez.fair-shuffle-vertex-manager.enable.auto-parallel";
  public static final String
      TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT =
          FairRoutingType.NONE.getType();

  /**
   * In case of a ScatterGather connection, the fraction of source tasks which
   * should complete before tasks for the current vertex are scheduled
   */
  public static final String TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION =
      "tez.fair-shuffle-vertex-manager.min-src-fraction";
  public static final float TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT = 0.25f;

  /**
   * In case of a ScatterGather connection, once this fraction of source tasks
   * have completed, all tasks on the current vertex can be scheduled. Number of
   * tasks ready for scheduling on the current vertex scales linearly between
   * min-fraction and max-fraction. Defaults to the greater of the default value
   * or tez.fair-shuffle-vertex-manager.min-src-fraction.
   */
  public static final String TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION =
      "tez.fair-shuffle-vertex-manager.max-src-fraction";
  public static final float TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT = 0.75f;

  /**
   * Enables automatic parallelism determination for the vertex. Based on input data
   * statistics the parallelism is adjusted to a desired level.
   */
  public enum FairRoutingType {
    /**
     * Don't do any fair routing.
     */
    NONE("none"),

    /**
     * TEZ-2962 Based on input data statistics the parallelism is decreased
     * to a desired level by having one destination task process multiple
     * consecutive partitions.
     */
    REDUCE_PARALLELISM("reduce_parallelism"),

    /**
     * Based on input data statistics the parallelism is adjusted
     * to a desired level by having one destination task process multiple
     * small partitions and multiple destination tasks process one
     * large partition. Only works when there is one bipartite edge.
     */
    FAIR_PARALLELISM("fair_parallelism");

    private final String type;

    private FairRoutingType(String type) {
      this.type = type;
    }

    public final String getType() {
      return type;
    }

    public boolean reduceParallelismEnabled() {
      return equals(FairRoutingType.REDUCE_PARALLELISM);
    }

    public boolean fairParallelismEnabled() {
      return equals(FairRoutingType.FAIR_PARALLELISM);
    }

    public boolean enabled() {
      return !equals(FairRoutingType.NONE);
    }

    public static FairRoutingType fromString(String type) {
      if (type != null) {
        for (FairRoutingType b : FairRoutingType.values()) {
          if (type.equalsIgnoreCase(b.type)) {
            return b;
          }
        }
      }
      throw new IllegalArgumentException("Invalid type " + type);
    }
  }

  static class FairSourceVertexInfo extends SourceVertexInfo {
    // mapping from destination task id to DestinationTaskInputsProperty
    private final HashMap<Integer, DestinationTaskInputsProperty>
        destinationInputsProperties = new HashMap<>();

    FairSourceVertexInfo(final EdgeProperty edgeProperty,
        int totalTasksToSchedule) {
      super(edgeProperty, totalTasksToSchedule);
    }
    public HashMap<Integer, DestinationTaskInputsProperty>
        getDestinationInputsProperties() {
      return destinationInputsProperties;
    }
  }

  @Override
  SourceVertexInfo createSourceVertexInfo(EdgeProperty edgeProperty,
      int numTasks) {
    return new FairSourceVertexInfo(edgeProperty, numTasks);
  }


  FairShuffleVertexManagerConfig mgrConfig;

  public FairShuffleVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  protected void onVertexStartedCheck() {
    super.onVertexStartedCheck();
    if (bipartiteSources > 1 &&
        (mgrConfig.getFairRoutingType().fairParallelismEnabled())) {
      // TODO TEZ-3500
      throw new TezUncheckedException(
          "Having more than one destination task process same partition(s) " +
              "only works with one bipartite source.");
    }
  }

  static long ceil(long a, long b) {
    return (a + (b - 1)) / b;
  }

  public long[] estimatePartitionSize() {
    boolean partitionStatsReported = false;
    int numOfPartitions = pendingTasks.size();
    long[] estimatedPartitionOutputSize = new long[numOfPartitions];
    for (int i = 0; i < numOfPartitions; i++) {
      if (getCurrentlyKnownStatsAtIndex(i) > 0) {
        partitionStatsReported = true;
        break;
      }
    }

    if (!partitionStatsReported) {
      // partition stats reporting isn't enabled at the source. Use
      // expected source output size and assume all partitions are evenly
      // distributed.
      if (numOfPartitions > 0) {
        long estimatedPerPartitionSize =
                getExpectedTotalBipartiteSourceTasksOutputSize().divide(
                        BigInteger.valueOf(numOfPartitions)).longValue();
        for (int i = 0; i < numOfPartitions; i++) {
          estimatedPartitionOutputSize[i] = estimatedPerPartitionSize;
        }
      }
    } else {
      for (int i = 0; i < numOfPartitions; i++) {
        estimatedPartitionOutputSize[i] =
            MB * getExpectedStatsAtIndex(i);
      }
    }
    return estimatedPartitionOutputSize;
  }

  /*
   * The class calculates how partitions and source tasks should be
   * grouped together. It allows a destination task to fetch a consecutive
   * range of partitions from a consecutive range of source tasks to achieve
   * optimal physical input size specified by desiredTaskInputDataSize.
   * First it estimates the size of each partition at job completion based
   * on the partition and output size of the completed tasks. The estimation
   * is stored in estimatedPartitionOutputSize.
   * Then it walks the partitions starting from beginning.
   * If a partition is not greater than desiredTaskInputDataSize, it keeps
   * accumulating the next partition until it is about to exceed
   * desiredTaskInputDataSize. Then it will create a new destination task to
   * fetch these small partitions in the range of
   * {firstPartitionId, numOfPartitions} to from all source tasks.
   * If a partition is larger than desiredTaskInputDataSize,
   * For FairRoutingType.REDUCE policy, it creates a new destination task to
   * to fetch this large partition from all source tasks.
   * For FairRoutingType.FAIR policy, it will create multiple destination tasks
   * each of which will fetch the large partition from a range
   * of source tasks.
   */
  private class PartitionsGroupingCalculator
      implements Iterable<DestinationTaskInputsProperty> {

    private final FairSourceVertexInfo sourceVertexInfo;

    // Estimated aggregated partition output size when the job is done.
    private long[] estimatedPartitionOutputSize;

    // Intermediate states used to group partitions.

    // Total output size of partitions in current group.
    private long sizeOfPartitions = 0;
    // Total number of partitions in the current group.
    private int numOfPartitions = 0;
    // The first partition id in the current group.
    private int firstPartitionId = 0;
    // The # of source tasks a destination task consumes.
    // When FAIR_PARALLELISM is enabled, there will be multiple destination
    // tasks processing the same partition and each destination task will
    // process a range of source tasks of that partition. For a given
    // partition, the number of source tasks assigned to different destination
    // tasks should differ by one at most and numOfBaseSourceTasks is the
    // smaller value. numOfBaseDestinationTasks is the number of destination tasks that
    // process numOfBaseSourceTasks source tasks.
    // e.g. if 8 source tasks are assigned 3 destination tasks, the number of
    // source tasks assigned to these 3 destination tasks are {2, 3, 3}.
    // numOfBaseDestinationTasks == 1, numOfBaseSourceTasks == 2.
    private int numOfBaseSourceTasks = 0;
    private int numOfBaseDestinationTasks = 0;
    public PartitionsGroupingCalculator(long[] estimatedPartitionOutputSize,
        FairSourceVertexInfo sourceVertexInfo) {
      this.estimatedPartitionOutputSize = estimatedPartitionOutputSize;
      this.sourceVertexInfo = sourceVertexInfo;
    }

    // Start the processing of the next group of partitions
    private void startNextPartitionsGroup() {
      this.firstPartitionId += this.numOfPartitions;
      this.sizeOfPartitions = 0;
      this.numOfPartitions = 0;
      this.numOfBaseSourceTasks = 0;
      this.numOfBaseDestinationTasks = 0;
    }

    private int getNextPartitionId() {
      return this.firstPartitionId + this.numOfPartitions;
    }

    private void addNextPartition() {
      if (hasPartitionsLeft()) {
        this.sizeOfPartitions +=
            estimatedPartitionOutputSize[getNextPartitionId()];
        this.numOfPartitions++;
      }
    }

    private boolean hasPartitionsLeft() {
      return getNextPartitionId() < this.estimatedPartitionOutputSize.length;
    }

    private long getCurrentAndNextPartitionSize() {
      return hasPartitionsLeft() ? this.sizeOfPartitions +
          estimatedPartitionOutputSize[getNextPartitionId()] :
          this.sizeOfPartitions;
    }

    // For the current source output partition(s), decide how
    // source tasks should be grouped.
    private boolean computeSourceTasksGrouping() {
      boolean finalizeCurrentPartitions = true;
      int groupCount = Ints.checkedCast(ceil(getCurrentAndNextPartitionSize(),
          config.getDesiredTaskInputDataSize()));
      if (groupCount <= 1) {
        // There is no enough data so far to reach desiredTaskInputDataSize.
        addNextPartition();
        if (!hasPartitionsLeft()) {
          // We have reached the last partition.
          // Consume from all source tasks.
          this.numOfBaseDestinationTasks = 1;
          this.numOfBaseSourceTasks = this.sourceVertexInfo.numTasks;
        } else {
          finalizeCurrentPartitions = false;
        }
      } else if (numOfPartitions == 0) {
        // The first partition in the current group exceeds
        // desiredTaskInputDataSize.
        addNextPartition();
        if (mgrConfig.getFairRoutingType().reduceParallelismEnabled()) {
          // Consume from all source tasks
          this.numOfBaseDestinationTasks = 1;
          this.numOfBaseSourceTasks = this.sourceVertexInfo.numTasks;
        } else {
          // When groupCount > sourceVertexInfo.numTasks, it means
          // sizeOfPartitions is too big so that even if
          // we just have one destination task fetch from one source task the
          // input size still exceeds desiredTaskInputDataSize.
          if ((this.sourceVertexInfo.numTasks >= groupCount)) {
            this.numOfBaseDestinationTasks = groupCount -
                this.sourceVertexInfo.numTasks % groupCount;
            this.numOfBaseSourceTasks =
                this.sourceVertexInfo.numTasks / groupCount;
          } else {
            this.numOfBaseDestinationTasks = this.sourceVertexInfo.numTasks;
            this.numOfBaseSourceTasks = 1;
          }
        }
      } else {
        // There are existing partitions in the current group. Adding the next
        // partition causes the total size to exceed desiredTaskInputDataSize.
        // Let us process the existing partitions in the current group. The
        // next partition will be processed in the next group.
        this.numOfBaseDestinationTasks = 1;
        this.numOfBaseSourceTasks = this.sourceVertexInfo.numTasks;
      }
      return finalizeCurrentPartitions;
    }

    @Override
    public Iterator<DestinationTaskInputsProperty> iterator() {
      return new UnmodifiableIterator<DestinationTaskInputsProperty>() {
        private int j = 0;
        private boolean visitedAtLeastOnce = false;
        private int groupIndex = 0;

        // Get number of source tasks in the current group.
        private int getNumOfSourceTasks() {
          return groupIndex++ < numOfBaseDestinationTasks ?
              numOfBaseSourceTasks : numOfBaseSourceTasks + 1;
        }

        @Override
        public boolean hasNext() {
          return j < sourceVertexInfo.numTasks || !visitedAtLeastOnce;
        }

        @Override
        public DestinationTaskInputsProperty next() {
          if (hasNext()) {
            visitedAtLeastOnce = true;
            int start = j;
            int numOfSourceTasks = getNumOfSourceTasks();
            j += numOfSourceTasks;
            return new DestinationTaskInputsProperty(firstPartitionId,
                numOfPartitions, start, numOfSourceTasks);
          }
          throw new NoSuchElementException();
        }
      };
    }

    public void compute() {
      int destinationIndex = 0;
      while (hasPartitionsLeft()) {
        if (!computeSourceTasksGrouping()) {
          continue;
        }
        Iterator<DestinationTaskInputsProperty> it = iterator();
        while(it.hasNext()) {
          sourceVertexInfo.getDestinationInputsProperties().put(
              destinationIndex,it.next());
          destinationIndex++;
        }
        startNextPartitionsGroup();
      }
    }
  }

  public ReconfigVertexParams computeRouting() {
    int currentParallelism = pendingTasks.size();
    int finalTaskParallelism = 0;
    long[] estimatedPartitionOutputSize = estimatePartitionSize();
    for (Map.Entry<String, SourceVertexInfo> vInfo : getBipartiteInfo()) {
      FairSourceVertexInfo info = (FairSourceVertexInfo)vInfo.getValue();
      computeParallelism(estimatedPartitionOutputSize, info);
      if (finalTaskParallelism != 0) {
        Preconditions.checkState(
            finalTaskParallelism == info.getDestinationInputsProperties().size(),
                "the parallelism shall be the same for source vertices");
      }
      finalTaskParallelism = info.getDestinationInputsProperties().size();

      FairEdgeConfiguration fairEdgeConfig = new FairEdgeConfiguration(
          currentParallelism, info.getDestinationInputsProperties());
      EdgeManagerPluginDescriptor descriptor =
          EdgeManagerPluginDescriptor.create(
              FairShuffleEdgeManager.class.getName());
      descriptor.setUserPayload(fairEdgeConfig.getBytePayload());
      vInfo.getValue().newDescriptor = descriptor;
    }
    ReconfigVertexParams params = new ReconfigVertexParams(
        finalTaskParallelism, null);

    return params;
  }

  @Override
  void postReconfigVertex() {
  }

  @Override
  void processPendingTasks() {
  }

  private void computeParallelism(long[] estimatedPartitionOutputSize,
      FairSourceVertexInfo sourceVertexInfo) {
    PartitionsGroupingCalculator calculator = new PartitionsGroupingCalculator(
        estimatedPartitionOutputSize, sourceVertexInfo);
    calculator.compute();
  }

  @Override
  List<ScheduleTaskRequest> getTasksToSchedule(
      TaskAttemptIdentifier completedSourceAttempt) {
    float minSourceVertexCompletedTaskFraction =
        getMinSourceVertexCompletedTaskFraction();
    int numTasksToSchedule = getNumOfTasksToScheduleAndLog(
        minSourceVertexCompletedTaskFraction);
    if (numTasksToSchedule > 0) {
      boolean scheduleAll =
          (numTasksToSchedule == pendingTasks.size());
      List<ScheduleTaskRequest> tasksToSchedule =
          Lists.newArrayListWithCapacity(numTasksToSchedule);

      Iterator<PendingTaskInfo> it = pendingTasks.iterator();
      FairSourceVertexInfo srcInfo = null;
      int srcTaskId = 0;
      if (completedSourceAttempt != null) {
        srcTaskId = completedSourceAttempt.getTaskIdentifier().getIdentifier();
        String srcVertexName = completedSourceAttempt.getTaskIdentifier().getVertexIdentifier().getName();
        srcInfo = (FairSourceVertexInfo)getSourceVertexInfo(srcVertexName);
      }
      while (it.hasNext() && numTasksToSchedule > 0) {
        Integer taskIndex = it.next().getIndex();
        // filter out those destination tasks that don't depend on
        // this completed source task.
        // destinationInputsProperties's size could be 0 if routing computation
        // is skipped.
        if (!scheduleAll && config.isAutoParallelismEnabled()
            && srcInfo != null && srcInfo.getDestinationInputsProperties().size() > 0) {
          DestinationTaskInputsProperty property =
              srcInfo.getDestinationInputsProperties().get(taskIndex);
          if (!property.isSourceTaskInRange(srcTaskId)) {
            LOG.debug("completedSourceTaskIndex {} and taskIndex {} don't " +
                "connect.", srcTaskId, taskIndex);
            continue;
          }
        }
        tasksToSchedule.add(ScheduleTaskRequest.create(taskIndex, null));
        it.remove();
        numTasksToSchedule--;
      }
      return tasksToSchedule;
    }
    return null;
  }

  static class FairShuffleVertexManagerConfig extends ShuffleVertexManagerBaseConfig {
    final FairRoutingType fairRoutingType;
    public FairShuffleVertexManagerConfig(final boolean enableAutoParallelism,
        final long desiredTaskInputDataSize, final float slowStartMinFraction,
        final float slowStartMaxFraction, final FairRoutingType fairRoutingType) {
      super(enableAutoParallelism, desiredTaskInputDataSize,
          slowStartMinFraction, slowStartMaxFraction);
      this.fairRoutingType = fairRoutingType;
      LOG.info("fairRoutingType {}", this.fairRoutingType);
    }
    FairRoutingType getFairRoutingType() {
      return fairRoutingType;
    }
  }

  @Override
  ShuffleVertexManagerBaseConfig initConfiguration() {
    float slowStartMinFraction = conf.getFloat(
        TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
        TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION_DEFAULT);
    FairRoutingType fairRoutingType = FairRoutingType.fromString(
        conf.get(TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
            TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL_DEFAULT));

    mgrConfig = new FairShuffleVertexManagerConfig(
        fairRoutingType.enabled(),
        conf.getLong(
            TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
            TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE_DEFAULT),
        slowStartMinFraction,
        conf.getFloat(
            TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
            Math.max(slowStartMinFraction,
            TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION_DEFAULT)),
        fairRoutingType);
    return mgrConfig;
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
   * @return {@link FairShuffleVertexManagerConfigBuilder}
   */
  public static FairShuffleVertexManagerConfigBuilder
      createConfigBuilder(@Nullable Configuration conf) {
    return new FairShuffleVertexManagerConfigBuilder(conf);
  }

  /**
   * Helper class to configure ShuffleVertexManager
   */
  public static final class FairShuffleVertexManagerConfigBuilder {
    private final Configuration conf;

    private FairShuffleVertexManagerConfigBuilder(@Nullable Configuration conf) {
      if (conf == null) {
        this.conf = new Configuration(false);
      } else {
        this.conf = conf;
      }
    }

    public FairShuffleVertexManagerConfigBuilder setAutoParallelism(
        FairRoutingType fairRoutingType) {
      conf.set(TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL,
          fairRoutingType.toString());
      return this;
    }

    public FairShuffleVertexManagerConfigBuilder
        setSlowStartMinSrcCompletionFraction(float minFraction) {
      conf.setFloat(TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION,
          minFraction);
      return this;
    }

    public FairShuffleVertexManagerConfigBuilder
        setSlowStartMaxSrcCompletionFraction(float maxFraction) {
      conf.setFloat(TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION,
          maxFraction);
      return this;
    }

    public FairShuffleVertexManagerConfigBuilder setDesiredTaskInputSize(
        long desiredTaskInputSize) {
      conf.setLong(TEZ_FAIR_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
          desiredTaskInputSize);
      return this;
    }

    public VertexManagerPluginDescriptor build() {
      VertexManagerPluginDescriptor desc =
          VertexManagerPluginDescriptor.create(
              FairShuffleVertexManager.class.getName());

      try {
        return desc.setUserPayload(TezUtils.createUserPayloadFromConf(
            this.conf));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }
}

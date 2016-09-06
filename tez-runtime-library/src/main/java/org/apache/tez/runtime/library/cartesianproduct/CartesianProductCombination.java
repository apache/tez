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

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represent the combination of source partitions or tasks.
 *
 * For example, if we have two source vertices and each generates two partition, we will have 2*2=4
 * destination tasks. The mapping from source partition/task to destination task is like this:
 * <0, 0> -> 0, <0, 1> -> 1, <1, 0> -> 2, <1, 1> -> 3;
 *
 * Basically, it stores the source partition/task combination and can compute corresponding
 * destination task. It can also figure out the source combination from a given destination task.
 * Task id is mapped in the ascending order of combinations, starting from 0. <field>factor</field>
 * is the helper array to computer task id, so task id = (combination) dot-product (factor)
 *
 * You can traverse all combinations with <method>firstTask</method> and <method>nextTask</method>,
 * like <0, 0> -> <0, 1> -> <1, 0> -> <1, 1>.
 *
 * Or you can also traverse all combinations that has one specific partition with
 * <method>firstTaskWithFixedPartition</method> and <method>nextTaskWithFixedPartition</method>,
 * like <0, 1, 0> -> <0, 1, 1> -> <1, 1, 0> -> <1, 1, 1> (all combinations with 2nd vertex's 2nd
 * partition.
 */
class CartesianProductCombination {
  // numPartitions for partitioned case, numTasks for unpartitioned case
  private int[] numPartitionOrTask;
  // at which position (in source vertices array) our vertex is
  private int positionId = -1;
  // The i-th element Ci represents partition/task Ci of source vertex i.
  private final Integer[] combination;
  // the weight of each vertex when computing the task id
  private final Integer[] factor;

  public CartesianProductCombination(int[] numPartitionOrTask) {
    this.numPartitionOrTask = Arrays.copyOf(numPartitionOrTask, numPartitionOrTask.length);
    combination = new Integer[numPartitionOrTask.length];
    factor = new Integer[numPartitionOrTask.length];
    factor[factor.length-1] = 1;
    for (int i = combination.length-2; i >= 0; i--) {
      factor[i] = factor[i+1]*numPartitionOrTask[i+1];
    }
  }

  public CartesianProductCombination(int[] numPartitionOrTask, int positionId) {
    this(numPartitionOrTask);
    this.positionId = positionId;
  }

  /**
   * @return a read only view of current combination
   */
  public List<Integer> getCombination() {
    return Collections.unmodifiableList(Arrays.asList(combination));
  }

  /**
   * first combination with given partition id in current position
   * @param partition
   */
  public void firstTaskWithFixedPartition(int partition) {
    Preconditions.checkArgument(positionId >= 0 && positionId < combination.length);
    Arrays.fill(combination, 0);
    combination[positionId] = partition;
  }

  /**
   * next combination without current partition in current position
   * @return false if there is no next combination
   */
  public boolean nextTaskWithFixedPartition() {
    Preconditions.checkArgument(positionId >= 0 && positionId < combination.length);
    int i;
    for (i = combination.length-1; i >= 0; i--) {
      if (i != positionId && combination[i] != numPartitionOrTask[i]-1) {
        break;
      }
    }

    if (i < 0) {
      return false;
    }

    combination[i]++;

    for (i++; i < combination.length; i++) {
      if (i != positionId) {
        combination[i] = 0;
      }
    }

    return true;
  }

  /**
   * first combination with given partition id in current position
   */
  public void firstTask() {
    Arrays.fill(combination, 0);
  }

  /**
   * next combination without current partition in current position
   * @return false if there is no next combination
   */
  public boolean nextTask() {
    int i;
    for (i = combination.length-1; i >= 0; i--) {
      if (combination[i] != numPartitionOrTask[i]-1) {
        break;
      }
    }

    if (i < 0) {
      return false;
    }

    combination[i]++;
    Arrays.fill(combination, i+1, combination.length, 0);
    return true;
  }

  /**
   * @return corresponding task id for current combination
   */
  public int getTaskId() {
    int taskId = 0;
    for (int i = 0; i < combination.length; i++) {
      taskId += combination[i]*factor[i];
    }
    return taskId;
  }

  public static CartesianProductCombination fromTaskId(int[] numPartitionOrTask,
                                                       int taskId) {
    CartesianProductCombination result = new CartesianProductCombination(numPartitionOrTask);
    for (int i = 0; i < result.combination.length; i++) {
      result.combination[i] = taskId/result.factor[i];
      taskId %= result.factor[i];
    }
    return result;
  }
}

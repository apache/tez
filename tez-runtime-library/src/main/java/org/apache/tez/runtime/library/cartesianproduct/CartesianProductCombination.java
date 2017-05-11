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
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represent the combination of source chunks. A chunk is one or more source tasks or partitions.
 *
 * For example, if we have two source vertices and each generates two chunks, we will have 2*2=4
 * destination tasks. The mapping from source chunks to destination task is like this:
 * <0, 0> -> 0, <0, 1> -> 1, <1, 0> -> 2, <1, 1> -> 3;
 *
 * Basically, it stores the source chunk id combination and can compute corresponding
 * destination task. It can also figure out the source combination from a given destination task.
 * Task id is mapped in the ascending order of combinations, starting from 0.
 *
 * You can traverse all combinations with <method>firstTask</method> and <method>nextTask</method>,
 * like <0, 0> -> <0, 1> -> <1, 0> -> <1, 1>.
 *
 * Or you can also traverse all combinations that has one specific chunk with
 * <method>firstTaskWithFixedChunk</method> and <method>nextTaskWithFixedChunk</method>,
 * like <0, 1, 0> -> <0, 1, 1> -> <1, 1, 0> -> <1, 1, 1> (all combinations with 2nd vertex's 2nd
 * chunk.
 */
class CartesianProductCombination {
  private int[] numChunk;
  // which position (in source vertices array) we care about
  private int positionId = -1;
  // The i-th element Ci represents chunk Ci of source vertex i.
  private final Integer[] combination;
  // helper array to computer task id: task id = (combination) dot-product (factor)
  private final Integer[] factor;

  public CartesianProductCombination(int[] numChunk) {
    Preconditions.checkArgument(!Ints.contains(numChunk, 0),
      "CartesianProductCombination doesn't allow zero chunk");
    this.numChunk = Arrays.copyOf(numChunk, numChunk.length);
    combination = new Integer[numChunk.length];
    factor = new Integer[numChunk.length];
    factor[factor.length-1] = 1;
    for (int i = combination.length-2; i >= 0; i--) {
      factor[i] = factor[i+1]* numChunk[i+1];
    }
  }

  public CartesianProductCombination(int[] numChunk, int positionId) {
    this(numChunk);
    this.positionId = positionId;
  }

  /**
   * @return a read only view of current combination
   */
  public List<Integer> getCombination() {
    return Collections.unmodifiableList(Arrays.asList(combination));
  }

  /**
   * first combination with given chunk id in current position
   * @param chunkId
   */
  public void firstTaskWithFixedChunk(int chunkId) {
    Preconditions.checkArgument(positionId >= 0 && positionId < combination.length);
    Arrays.fill(combination, 0);
    combination[positionId] = chunkId;
  }

  /**
   * next combination without current chunk in current position
   * @return false if there is no next combination
   */
  public boolean nextTaskWithFixedChunk() {
    Preconditions.checkArgument(positionId >= 0 && positionId < combination.length);
    int i;
    for (i = combination.length-1; i >= 0; i--) {
      if (i != positionId && combination[i] != numChunk[i]-1) {
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
   * first combination with given chunk id in current position
   */
  public void firstTask() {
    Arrays.fill(combination, 0);
  }

  /**
   * next combination without current chunk in current position
   * @return false if there is no next combination
   */
  public boolean nextTask() {
    int i;
    for (i = combination.length-1; i >= 0; i--) {
      if (combination[i] != numChunk[i]-1) {
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
    int chunkId = 0;
    for (int i = 0; i < combination.length; i++) {
      chunkId += combination[i]*factor[i];
    }
    return chunkId;
  }

  public static CartesianProductCombination fromTaskId(int[] numChunk,
                                                       int taskId) {
    CartesianProductCombination result = new CartesianProductCombination(numChunk);
    for (int i = 0; i < result.combination.length; i++) {
      result.combination[i] = taskId/result.factor[i];
      taskId %= result.factor[i];
    }
    return result;
  }
}

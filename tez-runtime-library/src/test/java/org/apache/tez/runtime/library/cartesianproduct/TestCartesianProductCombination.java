/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.library.cartesianproduct;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestCartesianProductCombination {
  private void verifyCombination(CartesianProductCombination combination, int[] result, int taskId) {
    assertArrayEquals(result, Ints.toArray(combination.getCombination()));
    assertEquals(taskId, combination.getTaskId());
  }

  private void testCombinationTwoWayVertex0() {
    CartesianProductCombination combination = new CartesianProductCombination(new int[]{2,3}, 0);

    combination.firstTaskWithFixedChunk(1);
    verifyCombination(combination, new int[]{1,0}, 3);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{1,1}, 4);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{1,2}, 5);
    assertFalse(combination.nextTaskWithFixedChunk());
  }

  private void testCombinationTwoWayVertex1() {
    CartesianProductCombination combination = new CartesianProductCombination(new int[]{2,3}, 1);

    combination.firstTaskWithFixedChunk(1);
    verifyCombination(combination, new int[]{0,1}, 1);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{1,1}, 4);

    assertFalse(combination.nextTaskWithFixedChunk());
  }

  private void testCombinationThreeWay() {
    CartesianProductCombination combination = new CartesianProductCombination(new int[]{2,2,2}, 1);

    combination.firstTaskWithFixedChunk(1);
    verifyCombination(combination, new int[]{0,1,0}, 2);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{0,1,1}, 3);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{1,1,0}, 6);
    assertTrue(combination.nextTaskWithFixedChunk());
    verifyCombination(combination, new int[]{1,1,1}, 7);
    assertFalse(combination.nextTaskWithFixedChunk());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testCombinationWithFixedPartition() {
    // two way cartesian product
    testCombinationTwoWayVertex0();
    testCombinationTwoWayVertex1();

    // three way cartesian product
    testCombinationThreeWay();
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testCombination() {
    CartesianProductCombination combination = new CartesianProductCombination(new int[]{2,3});
    List<Integer> list = combination.getCombination();
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        if (i == 0 && j == 0) {
          combination.firstTask();
        } else {
          assertTrue(combination.nextTask());
        }
        assertEquals((int) list.get(0), i);
        assertEquals((int) list.get(1), j);
      }
    }
    assertFalse(combination.nextTask());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testFromTaskId() {
    for (int i = 0; i < 6; i++) {
      List<Integer> list = CartesianProductCombination.fromTaskId(new int[]{2,3}, i)
                                                      .getCombination();
      assertEquals((int) list.get(0), i / 3);
      assertEquals((int) list.get(1), i % 3);
    }
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testRejectZero() {
    int[] numChunk = new int[] {0 ,1};
    try {
      new CartesianProductCombination(numChunk);
      fail();
    } catch (Exception ignored) {}
  }
}

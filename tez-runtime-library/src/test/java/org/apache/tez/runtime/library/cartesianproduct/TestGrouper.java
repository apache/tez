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

import org.apache.tez.runtime.library.utils.Grouper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGrouper {
  private Grouper grouper = new Grouper();

  @Test(timeout = 5000)
  public void testEvenlyGrouping() {
    grouper.init(4, 2);
    assertEquals(0, grouper.getFirstItemInGroup(0));
    assertEquals(2, grouper.getFirstItemInGroup(1));
    assertEquals(2, grouper.getNumItemsInGroup(0));
    assertEquals(2, grouper.getNumItemsInGroup(1));
    assertEquals(1, grouper.getLastItemInGroup(0));
    assertEquals(3, grouper.getLastItemInGroup(1));
    assertEquals(0, grouper.getGroupId(1));
    assertEquals(1, grouper.getGroupId(2));
    assertTrue(grouper.isInGroup(2, 1));
    assertFalse(grouper.isInGroup(2, 0));
  }

  @Test(timeout = 5000)
  public void testUnevenlyGrouping() {
    grouper.init(5, 2);
    assertEquals(0, grouper.getFirstItemInGroup(0));
    assertEquals(2, grouper.getFirstItemInGroup(1));
    assertEquals(2, grouper.getNumItemsInGroup(0));
    assertEquals(3, grouper.getNumItemsInGroup(1));
    assertEquals(1, grouper.getLastItemInGroup(0));
    assertEquals(4, grouper.getLastItemInGroup(1));
    assertEquals(0, grouper.getGroupId(1));
    assertEquals(1, grouper.getGroupId(3));
    assertTrue(grouper.isInGroup(3, 1));
    assertFalse(grouper.isInGroup(3, 0));
  }

  @Test(timeout = 5000)
  public void testSingleGroup() {
    grouper.init(4, 1);
    assertEquals(0, grouper.getFirstItemInGroup(0));
    assertEquals(4, grouper.getNumItemsInGroup(0));
    assertEquals(3, grouper.getLastItemInGroup(0));
    assertEquals(0, grouper.getGroupId(0));
    assertEquals(0, grouper.getGroupId(3));
    assertTrue(grouper.isInGroup(3, 0));
  }

  @Test(timeout = 5000)
  public void testNoGrouping() {
    grouper.init(2, 2);
    assertEquals(0, grouper.getFirstItemInGroup(0));
    assertEquals(1, grouper.getNumItemsInGroup(0));
    assertEquals(0, grouper.getLastItemInGroup(0));
    assertEquals(0, grouper.getGroupId(0));
    assertTrue(grouper.isInGroup(0, 0));
  }
}

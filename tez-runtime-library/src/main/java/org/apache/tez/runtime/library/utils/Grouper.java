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
package org.apache.tez.runtime.library.utils;

import com.google.common.base.Preconditions;

/**
 * This grouper group specified number of items into specified number of groups.
 *
 * If numItem%numGroup is zero, every group has numItem/numGroup items.
 * Otherwise, every group will get numItem/numGroup items first, and remaining items will be
 * distributed in last numItem-numItem%numGroup*numGroup groups (one item for each group).
 * For example, if we group 8 items into 3 groups, each group get {2, 3, 3} items.
 */
public class Grouper {
  private int numGroup;
  private int numItem;
  private int numGroup1;
  private int itemPerGroup1;
  private int numGroup2;
  private int itemPerGroup2;

  public Grouper init(int numItem, int numGroup) {
    Preconditions.checkArgument(numGroup > 0,
      "Number of groups is " + numGroup + ". Should be positive");
    Preconditions.checkArgument(numItem > 0,
      "Number of items is " + numItem + ". Should be positive");
    Preconditions.checkArgument(numItem >= numGroup,
      "Num of groups + " + numGroup + " shouldn't be more than number of items " + numItem);
    this.numItem = numItem;
    this.numGroup = numGroup;
    this.itemPerGroup1 = numItem / numGroup;
    this.itemPerGroup2 = itemPerGroup1 + 1;
    this.numGroup2 = numItem % numGroup;
    this.numGroup1 = numGroup - numGroup2;

    return this;
  }

  public int getFirstItemInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId " + groupId);
    if (groupId < numGroup1) {
      return groupId * itemPerGroup1;
    } else {
      return groupId * itemPerGroup1 + (groupId - numGroup1);
    }
  }

  public int getNumItemsInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    return groupId < numGroup1 ? itemPerGroup1 : itemPerGroup2;
  }

  public int getLastItemInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    return getFirstItemInGroup(groupId) + getNumItemsInGroup(groupId) - 1;
  }

  public int getGroupId(int itemId) {
    Preconditions.checkArgument(0 <= itemId && itemId < numItem, "Invalid itemId" + itemId);
    if (itemId < itemPerGroup1 * numGroup1) {
      return itemId/ itemPerGroup1;
    } else {
      return numGroup1 + (itemId - itemPerGroup1 * numGroup1) / itemPerGroup2;
    }
  }

  public boolean isInGroup(int itemId, int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    Preconditions.checkArgument(0 <= itemId && itemId < numItem, "Invalid itemId" + itemId);
    return getFirstItemInGroup(groupId) <= itemId
      && itemId < getFirstItemInGroup(groupId) + getNumItemsInGroup(groupId);
  }
}

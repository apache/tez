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
 * This grouper group specified number of tasks into specified number of groups.
 *
 * If numTask%numGroup is zero, every group has numTask/numGroup tasks.
 * Otherwise, every group will get numTask/numGroup tasks first, and remaining tasks will be
 * distributed in last numTask-numTask%numGroup*numGroup groups (one task for each group).
 * For example, if we group 8 tasks into 3 groups, each group get {2, 3, 3} tasks.
 */
public class Grouper {
  private int numGroup;
  private int numTask;
  private int numGroup1;
  private int taskPerGroup1;
  private int numGroup2;
  private int taskPerGroup2;

  public Grouper init(int numTask, int numGroup) {
    Preconditions.checkArgument(numGroup > 0,
      "Number of groups is " + numGroup + ". Should be positive");
    Preconditions.checkArgument(numTask > 0,
      "Number of tasks is " + numTask + ". Should be positive");
    Preconditions.checkArgument(numTask >= numGroup,
      "Num of groups + " + numGroup + " shouldn't be more than number of tasks " + numTask);
    this.numTask = numTask;
    this.numGroup = numGroup;
    this.taskPerGroup1 = numTask / numGroup;
    this.taskPerGroup2 = taskPerGroup1 + 1;
    this.numGroup2 = numTask % numGroup;
    this.numGroup1 = numGroup - numGroup2;

    return this;
  }

  public int getFirstTaskInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId " + groupId);
    if (groupId < numGroup1) {
      return groupId * taskPerGroup1;
    } else {
      return groupId * taskPerGroup1 + (groupId - numGroup1);
    }
  }

  public int getNumTasksInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    return groupId < numGroup1 ? taskPerGroup1 : taskPerGroup2;
  }

  public int getLastTaskInGroup(int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    return getFirstTaskInGroup(groupId) + getNumTasksInGroup(groupId) - 1;
  }

  public int getGroupId(int taskId) {
    Preconditions.checkArgument(0 <= taskId && taskId < numTask, "Invalid taskId" + taskId);
    if (taskId < taskPerGroup1 * numGroup1) {
      return taskId/taskPerGroup1;
    } else {
      return numGroup1 + (taskId - taskPerGroup1 * numGroup1) / taskPerGroup2;
    }
  }

  public boolean isInGroup(int taskId, int groupId) {
    Preconditions.checkArgument(0 <= groupId && groupId < numGroup, "Invalid groupId" + groupId);
    Preconditions.checkArgument(0 <= taskId && taskId < numTask, "Invalid taskId" + taskId);
    return getFirstTaskInGroup(groupId) <= taskId
      && taskId < getFirstTaskInGroup(groupId) + getNumTasksInGroup(groupId);
  }
}

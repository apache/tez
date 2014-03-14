/* Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Assert;
import org.junit.Test;

public class TestVertexStats {

  @Test
  public void testBasicStats() {
    VertexStats stats = new VertexStats();
    Assert.assertEquals(-1, stats.firstTaskStartTime);
    Assert.assertEquals(-1, stats.lastTaskFinishTime);
    Assert.assertEquals(-1, stats.minTaskDuration);
    Assert.assertEquals(-1, stats.maxTaskDuration);
    Assert.assertTrue(-1 == stats.avgTaskDuration);
    Assert.assertEquals(0, stats.firstTasksToStart.size());
    Assert.assertEquals(0, stats.lastTasksToFinish.size());
    Assert.assertEquals(0, stats.shortestDurationTasks.size());
    Assert.assertEquals(0, stats.longestDurationTasks.size());

    TezVertexID tezVertexID = TezVertexID.getInstance(
        TezDAGID.getInstance(
            ApplicationId.newInstance(100l, 1), 1), 1);
    TezTaskID tezTaskID1 = TezTaskID.getInstance(tezVertexID, 1);
    TezTaskID tezTaskID2 = TezTaskID.getInstance(tezVertexID, 2);
    TezTaskID tezTaskID3 = TezTaskID.getInstance(tezVertexID, 3);
    TezTaskID tezTaskID4 = TezTaskID.getInstance(tezVertexID, 4);
    TezTaskID tezTaskID5 = TezTaskID.getInstance(tezVertexID, 5);
    TezTaskID tezTaskID6 = TezTaskID.getInstance(tezVertexID, 6);

    stats.updateStats(new TaskReportImpl(tezTaskID1,
        TaskState.SUCCEEDED, 1, 100, 200));
    Assert.assertEquals(100, stats.firstTaskStartTime);
    Assert.assertEquals(200, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(100, stats.maxTaskDuration);
    Assert.assertTrue(100 == stats.avgTaskDuration);
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID1));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID1));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    Assert.assertEquals(1, stats.firstTasksToStart.size());
    Assert.assertEquals(1, stats.lastTasksToFinish.size());
    Assert.assertEquals(1, stats.shortestDurationTasks.size());
    Assert.assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID2,
        TaskState.FAILED, 1, 150, 300));
    Assert.assertEquals(100, stats.firstTaskStartTime);
    Assert.assertEquals(300, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(100, stats.maxTaskDuration);
    Assert.assertTrue(100 == stats.avgTaskDuration);
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID1));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID2));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    Assert.assertEquals(1, stats.firstTasksToStart.size());
    Assert.assertEquals(1, stats.lastTasksToFinish.size());
    Assert.assertEquals(1, stats.shortestDurationTasks.size());
    Assert.assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID3,
        TaskState.RUNNING, 1, 50, 550));
    Assert.assertEquals(50, stats.firstTaskStartTime);
    Assert.assertEquals(550, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(100, stats.maxTaskDuration);
    Assert.assertTrue(100 == stats.avgTaskDuration);
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    Assert.assertEquals(1, stats.firstTasksToStart.size());
    Assert.assertEquals(1, stats.lastTasksToFinish.size());
    Assert.assertEquals(1, stats.shortestDurationTasks.size());
    Assert.assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID4,
        TaskState.SUCCEEDED, 1, 50, 450));
    Assert.assertEquals(50, stats.firstTaskStartTime);
    Assert.assertEquals(550, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(400, stats.maxTaskDuration);
    Assert.assertTrue(250 == stats.avgTaskDuration);
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    Assert.assertEquals(2, stats.firstTasksToStart.size());
    Assert.assertEquals(1, stats.lastTasksToFinish.size());
    Assert.assertEquals(1, stats.shortestDurationTasks.size());
    Assert.assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID5,
        TaskState.SUCCEEDED, 1, 50, 450));
    Assert.assertEquals(50, stats.firstTaskStartTime);
    Assert.assertEquals(550, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(400, stats.maxTaskDuration);
    Assert.assertTrue(300 == stats.avgTaskDuration);
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID5));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID5));
    Assert.assertEquals(3, stats.firstTasksToStart.size());
    Assert.assertEquals(1, stats.lastTasksToFinish.size());
    Assert.assertEquals(1, stats.shortestDurationTasks.size());
    Assert.assertEquals(2, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID6,
        TaskState.SUCCEEDED, 1, 450, 550));
    Assert.assertEquals(50, stats.firstTaskStartTime);
    Assert.assertEquals(550, stats.lastTaskFinishTime);
    Assert.assertEquals(100, stats.minTaskDuration);
    Assert.assertEquals(400, stats.maxTaskDuration);
    Assert.assertTrue(250 == stats.avgTaskDuration);
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID5));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    Assert.assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    Assert.assertTrue(stats.lastTasksToFinish.contains(tezTaskID6));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    Assert.assertTrue(stats.shortestDurationTasks.contains(tezTaskID6));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    Assert.assertTrue(stats.longestDurationTasks.contains(tezTaskID5));
    Assert.assertEquals(3, stats.firstTasksToStart.size());
    Assert.assertEquals(2, stats.lastTasksToFinish.size());
    Assert.assertEquals(2, stats.shortestDurationTasks.size());
    Assert.assertEquals(2, stats.longestDurationTasks.size());
  }

}

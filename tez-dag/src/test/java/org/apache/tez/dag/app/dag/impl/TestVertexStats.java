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
package org.apache.tez.dag.app.dag.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestVertexStats {

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testBasicStats() {
    VertexStats stats = new VertexStats();
    assertEquals(-1, stats.firstTaskStartTime);
    assertEquals(-1, stats.lastTaskFinishTime);
    assertEquals(-1, stats.minTaskDuration);
    assertEquals(-1, stats.maxTaskDuration);
    assertEquals(-1, stats.avgTaskDuration);
    assertEquals(0, stats.getFirstTasksToStart().size());
    assertEquals(0, stats.getLastTasksToFinish().size());
    assertEquals(0, stats.getShortestDurationTasks().size());
    assertEquals(0, stats.getLongestDurationTasks().size());

    TezVertexID tezVertexID = TezVertexID.getInstance(
        TezDAGID.getInstance(
            ApplicationId.newInstance(100L, 1), 1), 1);
    TezTaskID tezTaskID1 = TezTaskID.getInstance(tezVertexID, 1);
    TezTaskID tezTaskID2 = TezTaskID.getInstance(tezVertexID, 2);
    TezTaskID tezTaskID3 = TezTaskID.getInstance(tezVertexID, 3);
    TezTaskID tezTaskID4 = TezTaskID.getInstance(tezVertexID, 4);
    TezTaskID tezTaskID5 = TezTaskID.getInstance(tezVertexID, 5);
    TezTaskID tezTaskID6 = TezTaskID.getInstance(tezVertexID, 6);

    stats.updateStats(new TaskReportImpl(tezTaskID1,
        TaskState.SUCCEEDED, 1, 100, 200));
    assertEquals(100, stats.firstTaskStartTime);
    assertEquals(200, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(100, stats.maxTaskDuration);
    assertEquals(100, stats.avgTaskDuration);
    assertTrue(stats.firstTasksToStart.contains(tezTaskID1));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID1));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    assertEquals(1, stats.firstTasksToStart.size());
    assertEquals(1, stats.lastTasksToFinish.size());
    assertEquals(1, stats.shortestDurationTasks.size());
    assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID2,
        TaskState.FAILED, 1, 150, 300));
    assertEquals(100, stats.firstTaskStartTime);
    assertEquals(300, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(100, stats.maxTaskDuration);
    assertEquals(100, stats.avgTaskDuration);
    assertTrue(stats.firstTasksToStart.contains(tezTaskID1));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID2));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    assertEquals(1, stats.firstTasksToStart.size());
    assertEquals(1, stats.lastTasksToFinish.size());
    assertEquals(1, stats.shortestDurationTasks.size());
    assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID3,
        TaskState.RUNNING, 1, 50, 550));
    assertEquals(50, stats.firstTaskStartTime);
    assertEquals(550, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(100, stats.maxTaskDuration);
    assertEquals(100, stats.avgTaskDuration);
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    assertEquals(1, stats.firstTasksToStart.size());
    assertEquals(1, stats.lastTasksToFinish.size());
    assertEquals(1, stats.shortestDurationTasks.size());
    assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID4,
        TaskState.SUCCEEDED, 1, 50, 450));
    assertEquals(50, stats.firstTaskStartTime);
    assertEquals(550, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(400, stats.maxTaskDuration);
    assertEquals(250, stats.avgTaskDuration);
    assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    assertEquals(2, stats.firstTasksToStart.size());
    assertEquals(1, stats.lastTasksToFinish.size());
    assertEquals(1, stats.shortestDurationTasks.size());
    assertEquals(1, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID5,
        TaskState.SUCCEEDED, 1, 50, 450));
    assertEquals(50, stats.firstTaskStartTime);
    assertEquals(550, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(400, stats.maxTaskDuration);
    assertEquals(300, stats.avgTaskDuration);
    assertTrue(stats.firstTasksToStart.contains(tezTaskID5));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID5));
    assertEquals(3, stats.firstTasksToStart.size());
    assertEquals(1, stats.lastTasksToFinish.size());
    assertEquals(1, stats.shortestDurationTasks.size());
    assertEquals(2, stats.longestDurationTasks.size());

    stats.updateStats(new TaskReportImpl(tezTaskID6,
        TaskState.SUCCEEDED, 1, 450, 550));
    assertEquals(50, stats.firstTaskStartTime);
    assertEquals(550, stats.lastTaskFinishTime);
    assertEquals(100, stats.minTaskDuration);
    assertEquals(400, stats.maxTaskDuration);
    assertEquals(250, stats.avgTaskDuration);
    assertTrue(stats.firstTasksToStart.contains(tezTaskID5));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID4));
    assertTrue(stats.firstTasksToStart.contains(tezTaskID3));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID3));
    assertTrue(stats.lastTasksToFinish.contains(tezTaskID6));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID1));
    assertTrue(stats.shortestDurationTasks.contains(tezTaskID6));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID4));
    assertTrue(stats.longestDurationTasks.contains(tezTaskID5));
    assertEquals(3, stats.firstTasksToStart.size());
    assertEquals(2, stats.lastTasksToFinish.size());
    assertEquals(2, stats.shortestDurationTasks.size());
    assertEquals(2, stats.longestDurationTasks.size());
  }

}

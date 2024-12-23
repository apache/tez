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

package org.apache.tez.common.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

// Per-job counters
// Keep in sync with tez-ui/src/main/webapp/config/default-app-conf.js
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum DAGCounter {
  NUM_FAILED_TASKS, 
  NUM_KILLED_TASKS,
  NUM_SUCCEEDED_TASKS,
  TOTAL_LAUNCHED_TASKS,

  /* The durations of task attempts are categorized based on their final states. The duration of successful tasks
  can serve as a reference when analyzing the durations of failed or killed tasks. This is because solely examining
  failed or killed task durations may be misleading, as these durations are measured from the submission time,
  which does not always correspond to the actual start time of the task attempt on executor nodes
  (e.g., in scenarios involving Hive LLAP).
  These counters align with the duration metrics used for WALL_CLOCK_MILLIS.
  As such, the following relationship applies:
  WALL_CLOCK_MILLIS = DURATION_FAILED_TASKS_MILLIS + DURATION_KILLED_TASKS_MILLIS + DURATION_SUCCEEDED_TASKS_MILLIS
  */

  // Total amount of time spent on running FAILED task attempts. This can be blamed for performance degradation, as a
  // DAG can still finish successfully in the presence of failed attempts.
  DURATION_FAILED_TASKS_MILLIS,

  // Total amount of time spent on running KILLED task attempts.
  DURATION_KILLED_TASKS_MILLIS,

  // Total amount of time spent on running SUCCEEDED task attempts, which can be a reference together with the same for
  // FAILED and KILLED attempts.
  DURATION_SUCCEEDED_TASKS_MILLIS,

  OTHER_LOCAL_TASKS,
  DATA_LOCAL_TASKS,
  RACK_LOCAL_TASKS,
  SLOTS_MILLIS_TASKS,
  FALLOW_SLOTS_MILLIS_TASKS,
  TOTAL_LAUNCHED_UBERTASKS,
  NUM_UBER_SUBTASKS,
  NUM_FAILED_UBERTASKS,
  AM_CPU_MILLISECONDS,
  /** Wall clock time taken by all the tasks. */
  WALL_CLOCK_MILLIS,
  AM_GC_TIME_MILLIS,

  /*
   * Type: # of containers
   * Both allocated and launched containers before DAG start.
   * This is incremented only once when the DAG starts and it's calculated
   * by querying all the held containers from TaskSchedulers.
   */
  INITIAL_HELD_CONTAINERS,

  /*
   * Type: # of containers
   * All containers that have been seen/used in this DAG by task allocation.
   * This counter can be calculated at the end of DAG by simply counting the distinct
   * ContainerIds that have been seen in TaskSchedulerManager.taskAllocated callbacks.
   */
  TOTAL_CONTAINERS_USED,

  /*
   * Type: # of events
   * Number of container allocations during a DAG. This is incremented every time
   * the containerAllocated callback is called in the TaskSchedulerContext.
   * This counter doesn't account for initially held (launched, allocated) containers.
   */
  TOTAL_CONTAINER_ALLOCATION_COUNT,

  /*
   * Type: # of events
   * Number of container launches during a DAG. This is incremented every time
   * the containerLaunched callback is called in the ContainerLauncherContext.
   * This counter doesn't account for initially held (launched, allocated) containers.
   */
  TOTAL_CONTAINER_LAUNCH_COUNT,

  /*
   * Type: # of events
   * Number of container releases during a DAG. This is incremented every time
   * the containerBeingReleased callback is called in the TaskSchedulerContext.
   */
  TOTAL_CONTAINER_RELEASE_COUNT,

  /*
   * Type: # of events
   * Number of container reuses during a DAG. This is incremented every time
   * the containerReused callback is called in the TaskSchedulerContext.
   */
  TOTAL_CONTAINER_REUSE_COUNT,

  /*
   * Number of nodes to which task attempts were assigned in this DAG.
   * Nodes are distinguished by the Yarn NodeId.getHost().
   */
  NODE_USED_COUNT,

  /*
   * Total number of nodes visible to the task scheduler (regardless of
   * task assignments). This is typically exposed by a resource manager
   * client.
   */
  NODE_TOTAL_COUNT
}

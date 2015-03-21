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
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum DAGCounter {
  NUM_FAILED_TASKS, 
  NUM_KILLED_TASKS,
  NUM_SUCCEEDED_TASKS,
  TOTAL_LAUNCHED_TASKS,
  OTHER_LOCAL_TASKS,
  DATA_LOCAL_TASKS,
  RACK_LOCAL_TASKS,
  SLOTS_MILLIS_TASKS,
  FALLOW_SLOTS_MILLIS_TASKS,
  TOTAL_LAUNCHED_UBERTASKS,
  NUM_UBER_SUBTASKS,
  NUM_FAILED_UBERTASKS,
  AM_CPU_MILLISECONDS,
  AM_GC_TIME_MILLIS
}

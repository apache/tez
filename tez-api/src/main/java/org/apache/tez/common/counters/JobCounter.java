/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.counters;

import org.apache.hadoop.classification.InterfaceAudience.Private;

// Per-job counters
@Private
public enum JobCounter {
  NUM_FAILED_MAPS,
  NUM_FAILED_REDUCES,
  NUM_KILLED_MAPS,
  NUM_KILLED_REDUCES,
  TOTAL_LAUNCHED_MAPS,
  TOTAL_LAUNCHED_REDUCES,
  OTHER_LOCAL_MAPS,
  DATA_LOCAL_MAPS,
  RACK_LOCAL_MAPS,
  SLOTS_MILLIS_MAPS,
  SLOTS_MILLIS_REDUCES,
  FALLOW_SLOTS_MILLIS_MAPS,
  FALLOW_SLOTS_MILLIS_REDUCES,
  TOTAL_LAUNCHED_UBERTASKS,
  NUM_UBER_SUBMAPS,
  NUM_UBER_SUBREDUCES,
  NUM_FAILED_UBERTASKS
}

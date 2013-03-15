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
package org.apache.tez.mapreduce.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Enum for map, reduce, job-setup, job-cleanup, task-cleanup task types.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum MRTaskType {

  MAP, REDUCE, JOB_SETUP, JOB_CLEANUP, TASK_CLEANUP;

  public String toString() {
    switch (this) {
      case MAP:
        return "m";
      case REDUCE:
        return "r";
      default:
        return this.name();
    }
  }

  public static MRTaskType fromString(String taskTypeString) {
    if (taskTypeString.equals("m") || taskTypeString.equals(MRTaskType.MAP.toString())) {
      return MRTaskType.MAP;
    } else if (taskTypeString.equals("r") || taskTypeString.equals(MRTaskType.REDUCE.toString())) {
      return MRTaskType.REDUCE;
    } else {
      return MRTaskType.valueOf(taskTypeString);
    }
  }
  
  public String toSerializedString() {
    return this.name();
  }
}

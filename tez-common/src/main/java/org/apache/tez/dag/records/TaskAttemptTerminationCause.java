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

package org.apache.tez.dag.records;

public enum TaskAttemptTerminationCause {
  UNKNOWN_ERROR, // The error cause is unknown. Usually means a gap in error propagation
  
  TERMINATED_BY_CLIENT, // Killed by client command
  TERMINATED_AT_SHUTDOWN, // Killed due execution shutdown
  INTERNAL_PREEMPTION, // Killed by Tez to makes space for higher pri work
  EXTERNAL_PREEMPTION, // Killed by the cluster to make space for other work
  TERMINATED_INEFFECTIVE_SPECULATION, // Killed speculative attempt because original succeeded
  TERMINATED_EFFECTIVE_SPECULATION, // Killed original attempt because speculation succeeded
  TERMINATED_ORPHANED, // Attempt is no longer needed by the task
  
  APPLICATION_ERROR, // Failed due to application code error
  FRAMEWORK_ERROR, // Failed due to code error in Tez code
  INPUT_READ_ERROR, // Failed due to error in reading inputs
  OUTPUT_WRITE_ERROR, // Failed due to error in writing outputs
  OUTPUT_LOST, // Failed because attempts output were reported lost
  TASK_HEARTBEAT_ERROR, // Failed because AM lost connection to the task
  
  CONTAINER_LAUNCH_FAILED, // Failed to launch container
  CONTAINER_EXITED, // Container exited. Indicates gap in specific error propagation from the cluster
  CONTAINER_STOPPED, // Container stopped or released by Tez
  NODE_FAILED, // Node for the container failed
  NODE_DISK_ERROR, // Disk failed on the node runnign the task
  
}

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

package org.apache.tez.dag.app.dag.event;

/**
 * Event types handled by TaskAttempt.
 */
public enum TaskAttemptEventType {

//Producer:Task, Speculator
  TA_SCHEDULE,

//Producer: TaskAttemptListener
  TA_STARTED_REMOTELY,
  TA_STATUS_UPDATE,
  TA_OUTPUT_CONSUMABLE,  // TODO History event to indicate this ?
  TA_DIAGNOSTICS_UPDATE, // REMOVE THIS - UNUSED
  TA_DONE,
  TA_FAILED,
  TA_TIMED_OUT,
  
//Producer: Client, Scheduler, On speculation.
  TA_KILL_REQUEST,

//Producer: Container / Scheduler.
  // Container may be running and is in the process of shutting down.
  TA_CONTAINER_TERMINATING,

  // Container has shut down.
  // In reality, the RM considers the container to be complete. Container has
  // shutdown except for once case - likely when the RM decides to kill the
  // container. TODO: Document the case.
  TA_CONTAINER_TERMINATED,

  // Container has either been preempted or will be preempted
  TA_CONTAINER_TERMINATED_BY_SYSTEM,

  // The node running the task attempt failed.
  TA_NODE_FAILED,
  
  // Producer: consumer destination vertex
  TA_OUTPUT_FAILED,

  // Recovery
  TA_RECOVER,
  
}

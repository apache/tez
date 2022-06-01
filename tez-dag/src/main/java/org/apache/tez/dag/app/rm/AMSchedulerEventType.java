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

package org.apache.tez.dag.app.rm;

public enum AMSchedulerEventType {
  //Producer: TaskAttempt
  S_TA_LAUNCH_REQUEST,
  S_TA_STATE_UPDATED,
  S_TA_ENDED, // Annotated with FAILED/KILLED/SUCCEEDED.

  //Producer: Node
  S_NODE_BLOCKLISTED,
  S_NODE_UNBLOCKLISTED,
  S_NODE_UNHEALTHY,
  S_NODE_HEALTHY,
  // The scheduler should have a way of knowing about unusable nodes. Acting on
  // this information to change requests etc is scheduler specific.
  
  // Producer : AMContainer
  S_CONTAINER_DEALLOCATE
}

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

package org.apache.tez.dag.app.rm.node;

public enum AMNodeEventType {
  //Producer: Scheduler
  N_CONTAINER_ALLOCATED,

  //Producer: Container
  N_CONTAINER_COMPLETED,

  //Producer: TaskSchedulerEventHandler
  N_TA_SUCCEEDED,

  // Producer: TaskSchedulerEventHandler, Task(retroactive failure)
  N_TA_ENDED,

  //Producer: TaskScheduler via TaskSchedulerEventHandler
  N_TURNED_UNHEALTHY,
  N_TURNED_HEALTHY,
  N_NODE_COUNT_UPDATED, // for blacklisting.

  //Producer: AMNodeManager
  N_IGNORE_BLACKLISTING_ENABLED,
  N_IGNORE_BLACKLISTING_DISABLED,

}

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

package org.apache.tez.dag.app.rm.container;

public enum AMContainerEventType {

  //Producer: Scheduler
  C_LAUNCH_REQUEST,
  C_ASSIGN_TA,
  
  //Producer: NMCommunicator
  C_LAUNCHED,
  C_LAUNCH_FAILED,

  //Producer: Scheduler via TA
  C_TA_SUCCEEDED, // maybe change this to C_TA_FINISHED with a status.

  //Producer: RMCommunicator
  C_COMPLETED,
  
  //Producer: RMCommunicator, AMNode
  C_NODE_FAILED,
  
  //TODO ZZZ CREUSE: Consider introducing a new event C_NODE_BLOCKLISTED -> container can take a call on what to do if this event comes in.
  
  //Producer: TA-> Scheduler -> Container (in case of failure etc)
  //          Scheduler -> Container (in case of pre-emption etc)
  //          Node -> Container (in case of Node blocklisted etc)
  C_STOP_REQUEST,
  
  //Producer: NMCommunicator
  C_NM_STOP_FAILED,
  C_NM_STOP_SENT,
  
  //Producer: ContainerHeartbeatHandler
  C_TIMED_OUT,
}

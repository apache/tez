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
 * Event types handled by Task.
 */
public enum VertexEventType {

  //Producer:Client, Job
  V_TERMINATE,

  //Producer:Job
  V_INIT,
  
  //Producer:Vertex
  V_COMPLETED,
  V_START,
  V_SOURCE_TASK_ATTEMPT_COMPLETED,
  V_SOURCE_VERTEX_STARTED,
  
  //Producer:Task
  V_TASK_COMPLETED,
  V_TASK_RESCHEDULED,
  V_TASK_ATTEMPT_COMPLETED,

  V_TASK_LAUNCHED,
  V_TASK_OUTPUT_CONSUMABLE,
  V_TASK_FAILED,
  V_TASK_SUCCEEDED,
  V_ATTEMPT_KILLED,
  
  //Producer:Any component
  V_INTERNAL_ERROR,
  V_COUNTER_UPDATE,
  
  V_ROUTE_EVENT,
  V_ONE_TO_ONE_SOURCE_SPLIT,
  
  //Producer: VertexInputInitializer
  V_ROOT_INPUT_INITIALIZED,
  V_ROOT_INPUT_FAILED,

  // Recover Event, Producer:DAG
  V_RECOVER,

  // Recover Event, Producer:Vertex
  V_SOURCE_VERTEX_RECOVERED

}

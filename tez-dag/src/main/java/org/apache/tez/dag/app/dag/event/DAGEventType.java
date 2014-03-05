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
 * Event types handled by Job.
 */
public enum DAGEventType {

  //Producer:Client
  DAG_KILL,

  //Producer:AM
  DAG_INIT,
  DAG_START,

  //Producer: Vertex
  DAG_VERTEX_COMPLETED,
  DAG_VERTEX_RERUNNING,

  //Producer: TaskImpl
  DAG_SCHEDULER_UPDATE,

  //Producer:Dag
  DAG_COMPLETED,

  //Producer:Any component
  DAG_DIAGNOSTIC_UPDATE,
  INTERNAL_ERROR,
  DAG_COUNTER_UPDATE,

  // Event to trigger recovery
  // Producer:AM
  DAG_RECOVER
}

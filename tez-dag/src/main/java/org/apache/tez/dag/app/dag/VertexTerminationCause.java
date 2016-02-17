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

package org.apache.tez.dag.app.dag;

/**
 * Represents proximate cause of transition to FAILED or KILLED.
 */
public enum VertexTerminationCause {

  /** DAG was killed  */
  DAG_TERMINATED(VertexState.KILLED),

  /** Other vertex failed causing DAG to fail thus killing this vertex  */
  OTHER_VERTEX_FAILURE(VertexState.KILLED),

  /** Initialization failed for one of the root Inputs */
  ROOT_INPUT_INIT_FAILURE(VertexState.FAILED),
  
  /** This vertex failed as its AM usercode (VertexManager/EdgeManager/InputInitializer)
   * throw Exception
   */
  AM_USERCODE_FAILURE(VertexState.FAILED),

  /** One of the tasks for this vertex failed.  */
  OWN_TASK_FAILURE(VertexState.FAILED),

  /** This vertex failed during commit. */
  COMMIT_FAILURE(VertexState.FAILED),

  /** In some cases, vertex could not rerun, e.g. its output been committed as a shared output of vertex group */
  VERTEX_RERUN_AFTER_COMMIT(VertexState.FAILED),

  /** Rerun vertex while it is in committing, it would cause conflict. */
  VERTEX_RERUN_IN_COMMITTING(VertexState.FAILED),

  /** This vertex failed as it had invalid number tasks. */
  INVALID_NUM_OF_TASKS(VertexState.FAILED),

  /** This vertex failed during init. */
  INIT_FAILURE(VertexState.FAILED),
  
  INTERNAL_ERROR(VertexState.ERROR),
  
  /** error when writing recovery log */ 
  RECOVERY_ERROR(VertexState.FAILED),

  /** This vertex failed due to counter limits exceeded. */
  COUNTER_LIMITS_EXCEEDED(VertexState.FAILED);

  private VertexState finishedState;

  private VertexTerminationCause(VertexState finishedState) {
    this.finishedState = finishedState;
  }

  public VertexState getFinishedState() {
    return finishedState;
  }
}

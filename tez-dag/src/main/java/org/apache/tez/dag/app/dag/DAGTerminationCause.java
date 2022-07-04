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

package org.apache.tez.dag.app.dag;

/**
 * Represents proximate cause of a DAG transition to FAILED or KILLED. 
 */
public enum DAGTerminationCause {

  /** DAG was directly killed.   */
  DAG_KILL(DAGState.KILLED),

  /** A service plugin indicated an error */
  SERVICE_PLUGIN_ERROR(DAGState.FAILED),

  /** A vertex failed. */
  VERTEX_FAILURE(DAGState.FAILED),

  /** DAG failed due as it had zero vertices. */
  ZERO_VERTICES(DAGState.FAILED),

  /** DAG failed during init. */
  INIT_FAILURE(DAGState.FAILED),

  /** DAG failed during output commit. */
  COMMIT_FAILURE(DAGState.FAILED),

  /** In some cases, vertex could not rerun, e.g. its output been committed as a shared output of vertex group */
  VERTEX_RERUN_AFTER_COMMIT(DAGState.FAILED),

  VERTEX_RERUN_IN_COMMITTING(DAGState.FAILED),

  /** DAG failed while trying to write recovery events */
  RECOVERY_FAILURE(DAGState.FAILED),

  INTERNAL_ERROR(DAGState.ERROR);

  private DAGState finishedState;

  DAGTerminationCause(DAGState finishedState) {
    this.finishedState = finishedState;
  }

  public DAGState getFinishedState() {
    return finishedState;
  }
}

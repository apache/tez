/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.tez.dag.app.dag.DAG;

public class DAGAppMasterEventDagCleanup extends DAGAppMasterEvent {

  private final DAG dag;

  public DAGAppMasterEventDagCleanup(DAG dag) {
    super(DAGAppMasterEventType.DAG_CLEANUP);
    this.dag = dag;
  }

  public DAG getDag() {
    return this.dag;
  }
}

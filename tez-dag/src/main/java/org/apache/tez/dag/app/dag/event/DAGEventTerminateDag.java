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

import org.apache.tez.dag.app.dag.DAGTerminationCause;
import org.apache.tez.dag.records.TezDAGID;

public class DAGEventTerminateDag extends DAGEvent implements DiagnosableEvent {
  private final String diagMessage;
  private final DAGTerminationCause terminationCause;

  public DAGEventTerminateDag(TezDAGID dagId, DAGTerminationCause terminationCause, String message) {
    super(dagId, DAGEventType.DAG_TERMINATE);
    this.diagMessage = message;
    this.terminationCause = terminationCause;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagMessage;
  }

  public DAGTerminationCause getTerminationCause() {
    return terminationCause;
  }
}

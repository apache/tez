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

package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 *  Fatal exception: thrown by the AM if there is no DAG running when
 *  when a DAG's status is queried. This is different from {@link org.apache.tez.dag.api.DAGNotRunningException}
 *  in a sense that this exception is fatal, in which scenario the client might consider the DAG failed, because
 *  it tries to ask a status from an AM which is not currently running a DAG. This scenario is possible in case
 *  an AM is restarted and the DagClient fails to realize it's asking the status of a possibly lost DAG.
 */
@Private
public class NoCurrentDAGException extends TezException {
  private static final long serialVersionUID = 6337442733802964448L;

  public static final String MESSAGE_PREFIX = "No running DAG at present";

  public NoCurrentDAGException(String dagId) {
    super(MESSAGE_PREFIX + ": " + dagId);
  }
}

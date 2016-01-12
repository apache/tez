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

package org.apache.tez.examples;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;

public class JoinValidateConfigured extends JoinValidate {

  private final VertexExecutionContext defaultExecutionContext;
  private final VertexExecutionContext lhsContext;
  private final VertexExecutionContext rhsContext;
  private final VertexExecutionContext validateContext;
  private final String dagNameSuffix;

  public JoinValidateConfigured(VertexExecutionContext defaultExecutionContext,
                                VertexExecutionContext lhsContext,
                                VertexExecutionContext rhsContext,
                                VertexExecutionContext validateContext, String dagNameSuffix) {
    this.defaultExecutionContext = defaultExecutionContext;
    this.lhsContext = lhsContext;
    this.rhsContext = rhsContext;
    this.validateContext = validateContext;
    this.dagNameSuffix = dagNameSuffix;
  }

  @Override
  protected VertexExecutionContext getDefaultExecutionContext() {
    return this.defaultExecutionContext;
  }

  @Override
  protected VertexExecutionContext getLhsExecutionContext() {
    return this.lhsContext;
  }

  @Override
  protected VertexExecutionContext getRhsExecutionContext() {
    return this.rhsContext;
  }

  @Override
  protected VertexExecutionContext getValidateExecutionContext() {
    return this.validateContext;
  }

  @Override
  protected String getDagName() {
    return "JoinValidate_" + dagNameSuffix;
  }

  public DAG createDag(TezConfiguration tezConf, Path lhs, Path rhs, int numPartitions)
      throws IOException {
    return super.createDag(tezConf, lhs, rhs, numPartitions);
  }
}

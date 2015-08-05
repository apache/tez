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

package org.apache.tez.dag.records;

import org.apache.tez.runtime.api.DagIdentifier;
import org.apache.tez.runtime.api.VertexIdentifier;

public class VertexIdentifierImpl implements VertexIdentifier {

  private final DagIdentifier dagIdentifier;
  private final TezVertexID vertexId;
  private final String vertexName;
  
  public VertexIdentifierImpl(String dagName, String vertexName, TezVertexID vertexId) {
    this.vertexId = vertexId;
    this.vertexName = vertexName;
    this.dagIdentifier = new DagIdentifierImpl(dagName, vertexId.getDAGId());
  }

  @Override
  public String getName() {
    return vertexName;
  }

  @Override
  public int getIdentifier() {
    return vertexId.getId();
  }
  
  @Override
  public DagIdentifier getDagIdentifier() {
    return dagIdentifier;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if(o == null) {
      return false;
    }
    if (o.getClass() == this.getClass()) {
      VertexIdentifierImpl other = (VertexIdentifierImpl) o;
      return this.vertexId.equals(other.vertexId);
    }
    else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return dagIdentifier.toString() + " Vertex: " + vertexName + ":[" + getIdentifier() + "]";
  }
  
  @Override
  public int hashCode() {
    return vertexId.hashCode();
  }
}

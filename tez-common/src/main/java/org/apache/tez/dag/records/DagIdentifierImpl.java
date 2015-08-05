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

public class DagIdentifierImpl implements DagIdentifier {

  private final TezDAGID dagId;
  private final String dagName;
  
  public DagIdentifierImpl(String dagName, TezDAGID dagId) {
    this.dagId = dagId;
    this.dagName = dagName;
  }

  @Override
  public String getName() {
    return dagName;
  }

  @Override
  public int getIdentifier() {
    return dagId.getId();
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
      DagIdentifierImpl other = (DagIdentifierImpl) o;
      return this.dagId.equals(other.dagId);
    }
    else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return "Dag: " + dagName + ":[" + getIdentifier() + "]";
  }
  
  @Override
  public int hashCode() {
    return dagId.hashCode();
  }
}

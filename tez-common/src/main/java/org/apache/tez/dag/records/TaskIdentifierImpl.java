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

import org.apache.tez.runtime.api.TaskIdentifier;
import org.apache.tez.runtime.api.VertexIdentifier;

public class TaskIdentifierImpl implements TaskIdentifier {

  private final VertexIdentifier vertexIdentifier;
  private final TezTaskID taskId;
  
  public TaskIdentifierImpl(String dagName, String vertexName, TezTaskID taskId) {
    this.taskId = taskId;
    this.vertexIdentifier = new VertexIdentifierImpl(dagName, vertexName, taskId.getVertexID());
  }

  @Override
  public int getIdentifier() {
    return taskId.getId();
  }
  
  @Override
  public VertexIdentifier getVertexIdentifier() {
    return vertexIdentifier;
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
      TaskIdentifierImpl other = (TaskIdentifierImpl) o;
      return this.taskId.equals(other.taskId);
    }
    else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return vertexIdentifier.toString() + " Task [" + getIdentifier() + "]";
  }

  @Override
  public int hashCode() {
    return taskId.hashCode();
  }
}

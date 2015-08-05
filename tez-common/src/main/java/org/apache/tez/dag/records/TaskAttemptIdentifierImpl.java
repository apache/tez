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

import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.TaskIdentifier;

public class TaskAttemptIdentifierImpl implements TaskAttemptIdentifier {

  private final TaskIdentifier taskIdentifier;
  private final TezTaskAttemptID attemptId;
  
  public TaskAttemptIdentifierImpl(String dagName, String vertexName, TezTaskAttemptID attemptId) {
    this.attemptId = attemptId;
    this.taskIdentifier = new TaskIdentifierImpl(dagName, vertexName, attemptId.getTaskID());
  }

  @Override
  public int getIdentifier() {
    return attemptId.getId();
  }
  
  @Override
  public TaskIdentifier getTaskIdentifier() {
    return taskIdentifier;
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
      TaskAttemptIdentifierImpl other = (TaskAttemptIdentifierImpl) o;
      return this.attemptId.equals(other.attemptId);
    }
    else {
      return false;
    }
  }
  
  @Override
  public String toString() {
    return taskIdentifier.toString() + " Attempt: [" + getIdentifier() + "]";
  }
  
  @Override
  public int hashCode() {
    return attemptId.hashCode();
  }
}

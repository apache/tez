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

package org.apache.tez.engine.common.shuffle.impl;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * Container for a task number and an attempt number for the task.
 */
@Private
public class TaskAttemptIdentifier {

  private final int taskIndex;
  private final int attemptNumber;
  private String pathComponent;
  
  public TaskAttemptIdentifier(int taskIndex, int attemptNumber) {
    this.taskIndex = taskIndex;
    this.attemptNumber = attemptNumber;
  }
  
  public TaskAttemptIdentifier(int taskIndex, int attemptNumber, String pathComponent) {
    this.taskIndex = taskIndex;
    this.attemptNumber = attemptNumber;
    this.pathComponent = pathComponent;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }
  
  public String getPathComponent() {
    return pathComponent;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + attemptNumber;
    result = prime * result
        + ((pathComponent == null) ? 0 : pathComponent.hashCode());
    result = prime * result + taskIndex;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TaskAttemptIdentifier other = (TaskAttemptIdentifier) obj;
    if (attemptNumber != other.attemptNumber)
      return false;
    if (pathComponent == null) {
      if (other.pathComponent != null)
        return false;
    } else if (!pathComponent.equals(other.pathComponent))
      return false;
    if (taskIndex != other.taskIndex)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "TaskAttemptIdentifier [taskIndex=" + taskIndex + ", attemptNumber="
        + attemptNumber + ", pathComponent=" + pathComponent + "]";
  }


}

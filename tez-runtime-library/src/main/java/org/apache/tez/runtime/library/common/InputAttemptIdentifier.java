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

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.dag.api.TezUncheckedException;

/**
 * Container for a task number and an attempt number for the task.
 */
@Private
public class InputAttemptIdentifier {

  private final InputIdentifier inputIdentifier;
  private final int attemptNumber;
  private final String pathComponent;
  private final boolean shared;

  public static final String PATH_PREFIX = "attempt";

  public InputAttemptIdentifier(int inputIndex, int attemptNumber) {
    this(new InputIdentifier(inputIndex), attemptNumber, null);
  }

  public InputAttemptIdentifier(InputIdentifier inputIdentifier, int attemptNumber, String pathComponent) {
    this(inputIdentifier, attemptNumber, pathComponent, false);
  }

  public InputAttemptIdentifier(InputIdentifier inputIdentifier, int attemptNumber, String pathComponent, boolean shared) {
    this.inputIdentifier = inputIdentifier;
    this.attemptNumber = attemptNumber;
    this.pathComponent = pathComponent;
    this.shared = shared;
    if (pathComponent != null && !pathComponent.startsWith(PATH_PREFIX)) {
      throw new TezUncheckedException(
          "Path component must start with: " + PATH_PREFIX + " " + this);
    }
  }

  public InputAttemptIdentifier(int taskIndex, int attemptNumber, String pathComponent) {
    this(new InputIdentifier(taskIndex), attemptNumber, pathComponent);
  }

  public InputAttemptIdentifier(int taskIndex, int attemptNumber, String pathComponent, boolean shared) {
    this(new InputIdentifier(taskIndex), attemptNumber, pathComponent, shared);
  }

  public InputIdentifier getInputIdentifier() {
    return this.inputIdentifier;
  }

  public int getAttemptNumber() {
    return attemptNumber;
  }
  
  public String getPathComponent() {
    return pathComponent;
  }

  public boolean isShared() {
    return this.shared;
  }

  // PathComponent & shared does not need to be part of the hashCode and equals computation.
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + attemptNumber;
    result = prime * result
        + ((inputIdentifier == null) ? 0 : inputIdentifier.hashCode());
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
    InputAttemptIdentifier other = (InputAttemptIdentifier) obj;
    if (attemptNumber != other.attemptNumber)
      return false;
    if (inputIdentifier == null) {
      if (other.inputIdentifier != null)
        return false;
    } else if (!inputIdentifier.equals(other.inputIdentifier))
      return false;
    // do not compare pathComponent as they may not always be present
    return true;
  }

  @Override
  public String toString() {
    return "InputAttemptIdentifier [inputIdentifier=" + inputIdentifier
        + ", attemptNumber=" + attemptNumber + ", pathComponent="
        + pathComponent + "]";
  }
}

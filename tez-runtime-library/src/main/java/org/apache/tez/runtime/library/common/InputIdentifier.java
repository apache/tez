/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class InputIdentifier {

  private final int inputIndex;

  public InputIdentifier(int srcInputIndex) {
    this.inputIndex = srcInputIndex;
  }

  public int getInputIndex() {
    return this.inputIndex;
  }

  @Override
  public int hashCode() {
    return inputIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InputIdentifier other = (InputIdentifier) obj;
    if (inputIndex != other.inputIndex)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "InputIdentifier [inputIndex=" + inputIndex + "]";
  }
}

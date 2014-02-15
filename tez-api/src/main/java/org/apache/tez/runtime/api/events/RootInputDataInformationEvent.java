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

package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;

/**
 * Events used by {@link TezRootInputInitializer} implementations to provide the
 * byte payload for individual tasks running as part of the Vertex for which an
 * Initial Input has been configured.
 */
public final class RootInputDataInformationEvent extends Event {

  private final int sourceIndex;
  private int targetIndex; // TODO Likely to be multiple at a later point.
  private final byte[] userPayload;
  
  public RootInputDataInformationEvent(int srcIndex, byte[] userPayload) {
    this.sourceIndex = srcIndex;
    this.userPayload = userPayload;
  }

  public int getSourceIndex() {
    return this.sourceIndex;
  }

  public int getTargetIndex() {
    return this.targetIndex;
  }

  public void setTargetIndex(int target) {
    this.targetIndex = target;
  }
  
  public byte[] getUserPayload() {
    return this.userPayload;
  }

  @Override
  public String toString() {
    return "RootInputDataInformationEvent [sourceIndex=" + sourceIndex + ", targetIndex="
        + targetIndex + "]";
  }  
}

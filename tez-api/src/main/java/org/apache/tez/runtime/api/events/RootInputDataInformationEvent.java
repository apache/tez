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

import org.apache.tez.common.TezUserPayload;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TezRootInputInitializer;

/**
 * Events used by {@link TezRootInputInitializer} implementations to provide the
 * user payload for individual tasks running as part of the Vertex for which an
 * Initial Input has been configured.
 * 
 * This event is used by InputInitialziers to configure tasks belonging to a
 * Vertex. The event may be processed by a @link {@link VertexManagerPlugin}
 * before being sent to tasks.
 * 
 * A {@link TezRootInputInitializer} may send Events with or without a
 * serialized user payload.
 * 
 * Events, after being processed by a {@link VertexManagerPlugin,} must
 * contained the payload in a serialized form.
 */
public final class RootInputDataInformationEvent extends Event {

  private final int sourceIndex;
  private int targetIndex; // TODO Likely to be multiple at a later point.
  private final TezUserPayload userPayload;
  private final Object userPayloadObject;
  
  /**
   * Provide a serialzied form of the payload
   * @param srcIndex the src index
   * @param userPayload the serizlied payload
   */
  public RootInputDataInformationEvent(int srcIndex, byte[] userPayload) {
    this.sourceIndex = srcIndex;
    this.userPayload = DagTypeConverters.convertToTezUserPayload(userPayload);
    this.userPayloadObject = null;
  }
  
  public RootInputDataInformationEvent(int srcIndex, Object userPayloadDeserialized) {
    this.sourceIndex = srcIndex;
    this.userPayloadObject = userPayloadDeserialized;
    this.userPayload = DagTypeConverters.convertToTezUserPayload(null);
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
    return userPayload.getPayload();
  }
  
  public Object getDeserializedUserPayload() {
    return this.userPayloadObject;
  }

  @Override
  public String toString() {
    return "RootInputDataInformationEvent [sourceIndex=" + sourceIndex + ", targetIndex="
        + targetIndex + ", serializedUserPayloadExists=" + (userPayload != null)
        + ", deserializedUserPayloadExists=" + (userPayloadObject != null) + "]";
  } 
}

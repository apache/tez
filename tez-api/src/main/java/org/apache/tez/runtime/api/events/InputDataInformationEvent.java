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

package org.apache.tez.runtime.api.events;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;

/**
 * Events used by {@link InputInitializer} implementations to provide the
 * user payload for individual tasks running as part of the Vertex for which an
 * Initial Input has been configured.
 *
 * This event is used by InputInitialziers to configure tasks belonging to a
 * Vertex. The event may be processed by a @link {@link VertexManagerPlugin}
 * before being sent to tasks.
 *
 * A {@link InputInitializer} may send Events with or without a
 * serialized user payload.
 *
 * Events, after being processed by a {@link VertexManagerPlugin}, must
 * contain the payload in a serialized form.
 */
@Unstable
@Public
public final class InputDataInformationEvent extends Event {

  private final int sourceIndex;
  private int targetIndex; // TODO Likely to be multiple at a later point.
  private final ByteBuffer userPayload;
  private final Object userPayloadObject;

  private InputDataInformationEvent(int srcIndex, ByteBuffer userPayload) {
    this.sourceIndex = srcIndex;
    this.userPayload = userPayload;
    this.userPayloadObject = null;
  }

  private InputDataInformationEvent(int srcIndex, Object userPayloadDeserialized, Object sigChanged) {
    this.sourceIndex = srcIndex;
    this.userPayloadObject = userPayloadDeserialized;
    this.userPayload = null;
  }

  /**
   * Provide a serialized form of the payload
   * @param srcIndex the src index
   * @param userPayload the serialized payload
   */
  public static InputDataInformationEvent createWithSerializedPayload(int srcIndex,
                                                                      ByteBuffer userPayload) {
    return new InputDataInformationEvent(srcIndex, userPayload);
  }

  public static InputDataInformationEvent createWithObjectPayload(int srcIndex,
                                                                  Object userPayloadDeserialized) {
    return new InputDataInformationEvent(srcIndex, userPayloadDeserialized, null);
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

  public ByteBuffer getUserPayload() {
    return userPayload == null ? null : userPayload.asReadOnlyBuffer();
  }

  public Object getDeserializedUserPayload() {
    return this.userPayloadObject;
  }

  @Override
  public String toString() {
    return "InputDataInformationEvent [sourceIndex=" + sourceIndex + ", targetIndex="
        + targetIndex + ", serializedUserPayloadExists=" + (userPayload != null)
        + ", deserializedUserPayloadExists=" + (userPayloadObject != null) + "]";
  }
}

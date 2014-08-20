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

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;

import com.google.common.base.Preconditions;

/**
 * Events used by {@link InputInitializer} implementations to update the
 * shared user payload for the Input that is being initialized. </p>
 *
 * This event is specific to an Input, and should only be sent once - ideally
 * before {@link InputDataInformationEvent}s
 */
@Unstable
@Public
public class InputUpdatePayloadEvent extends Event {

  private final ByteBuffer userPayload;

  private InputUpdatePayloadEvent(ByteBuffer userPayload) {
    Preconditions.checkNotNull(userPayload);
    this.userPayload = userPayload;
  }

  public static InputUpdatePayloadEvent create(ByteBuffer userPayload) {
    return new InputUpdatePayloadEvent(userPayload);
  }

  public ByteBuffer getUserPayload() {
    return userPayload == null ? null : userPayload.asReadOnlyBuffer();
  }
}

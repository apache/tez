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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.api.Event;

import java.nio.ByteBuffer;

public class CustomProcessorEvent extends Event {
  private ByteBuffer payload;

  /**
   * Version number to indicate what app attempt generated this Event
   */
  private int version;

  private CustomProcessorEvent(ByteBuffer payload) {
    this(payload, -1);
  }

  private CustomProcessorEvent(ByteBuffer payload, int version) {
    this.payload = payload;
    this.version = version;
  }

  public static CustomProcessorEvent create(ByteBuffer payload) {
    return new CustomProcessorEvent(payload);
  }

  @Private
  public static CustomProcessorEvent create(ByteBuffer payload, int version) {
    return new CustomProcessorEvent(payload, version);
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  @Private
  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
  }
}

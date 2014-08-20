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

import com.google.common.base.Preconditions;

/**
 * Event used to send information from a Task to the VertexManager for a vertex.
 * This may be used to send statistics like samples etc to the VertexManager for
 * automatic plan recofigurations based on observed statistics
 */
@Unstable
@Public
public class VertexManagerEvent extends Event {

  /**
   * Vertex to which the event should be sent 
   */
  private final String targetVertexName;
  
  /**
   * User payload to be sent
   */
  private final ByteBuffer userPayload;

  private VertexManagerEvent(String vertexName, ByteBuffer userPayload) {
    Preconditions.checkArgument(vertexName != null);
    Preconditions.checkArgument(userPayload != null);
    this.targetVertexName = vertexName;
    this.userPayload = userPayload;
  }

  /**
   * Create a new VertexManagerEvent
   * @param vertexName
   * @param userPayload This should not be modified since a reference is kept
   */
  public static VertexManagerEvent create(String vertexName, ByteBuffer userPayload) {
    return new VertexManagerEvent(vertexName, userPayload);
  }

  public String getTargetVertexName() {
    return targetVertexName;
  }
  
  public ByteBuffer getUserPayload() {
    return userPayload == null ? null : userPayload.asReadOnlyBuffer();
  }
}

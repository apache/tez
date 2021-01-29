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
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;

/**
 * Event used to send information from a Task to the VertexManager for a vertex.
 * This may be used to send statistics like samples etc to the VertexManager for
 * automatic plan reconfigurations based on observed statistics
 */
@Unstable
@Public
public class VertexManagerEvent extends Event {

  /**
   * Vertex to which the event should be sent 
   */
  private final String targetVertexName;
  
  private TaskAttemptIdentifier producerAttempt;
  
  /**
   * User payload to be sent
   */
  private final ByteBuffer userPayload;

  /**
   * Constructor.
   *
   * @param vertexName
   * @param userPayload
   * @throws NullPointerException if {@code vertexName} or {@code userPayload}
   *           is {@code null}
   */
  private VertexManagerEvent(String vertexName, ByteBuffer userPayload) {
    this.targetVertexName = Objects.requireNonNull(vertexName);
    this.userPayload = Objects.requireNonNull(userPayload);
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
  
  /**
   * Get metadata about the task attempt that produced the event.
   * This method will provide a valid return value only when invoked in the 
   * {@link VertexManagerPlugin}
   * @return attempt metadata
   */
  public TaskAttemptIdentifier getProducerAttemptIdentifier() {
    return producerAttempt;
  }
  
  @Private
  public void setProducerAttemptIdentifier(TaskAttemptIdentifier producerAttempt) {
    this.producerAttempt = producerAttempt;
  }
}

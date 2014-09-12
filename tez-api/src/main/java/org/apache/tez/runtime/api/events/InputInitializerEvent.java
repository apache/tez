/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.runtime.api.events;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.Event;

/**
 * An event that is routed to the specified {@link InputInitializer}.
 * This can be used to send information/metadata to the {@link InputInitializer}
 * <p/>
 *
 * These events are routed to the InputInitializer, only after the task which generated the event
 * succeeds. Also, the events will only be sent once per task - irrespective of how many attempts
 * were run, or succeeded. An example of this is when a task is retried because the node on which it
 * was running failed. If the Task had succeeded once, the event would already have been sent - and
 * will not be resent when the task reruns and succeeds. </p>
 */
@Unstable
@Public
public class InputInitializerEvent extends Event {

  private String targetVertexName;
  private String targetInputName;

  private ByteBuffer eventPayload;
  private String sourceVertexName;

  private InputInitializerEvent(String targetVertexName, String targetInputName,
                                ByteBuffer eventPayload) {
    Preconditions.checkNotNull(targetVertexName, "TargetVertexName cannot be null");
    Preconditions.checkNotNull(targetInputName, "TargetInputName cannot be null");
    this.targetVertexName = targetVertexName;
    this.targetInputName = targetInputName;
    this.eventPayload = eventPayload;
  }

  /**
   * @param targetVertexName the vertex on which the targeted Input exists
   * @param targetInputName  the name of the input
   * @param eventPayload     the payload for the event. It is advisable to limit the size of the
   *                         payload to a few KB at max
   */
  public static InputInitializerEvent create(String targetVertexName, String targetInputName,
                                             ByteBuffer eventPayload) {
    return new InputInitializerEvent(targetVertexName, targetInputName, eventPayload);
  }

  /**
   * Get the vertex name on which the targeted Input exists
   *
   * @return the vertex name
   */
  public String getTargetVertexName() {
    return this.targetVertexName;
  }

  /**
   * Get the input name to which this event is targeted
   *
   * @return the input name
   */
  public String getTargetInputName() {
    return this.targetInputName;
  }

  /**
   * Get the actual user payload
   *
   * @return a byte representation of the payload
   */
  public ByteBuffer getUserPayload() {
    return eventPayload == null ? null : eventPayload.asReadOnlyBuffer();
  }

  @InterfaceAudience.Private
  public void setSourceVertexName(String srcVertexName) {
    this.sourceVertexName = srcVertexName;
  }

  /**
   * Returns the name of the vertex which generated the event. This will only be populated after
   * the event has been routed by the AM.
   * @return the name of the source vertex
   */
  public String getSourceVertexName() {
    return this.sourceVertexName;
  }
}
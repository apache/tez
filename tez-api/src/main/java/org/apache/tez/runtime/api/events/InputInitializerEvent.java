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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.Event;

/**
 * An event that is routed to the specified {@link InputInitializer}.
 */
@Unstable
public class InputInitializerEvent extends Event {

  private String targetVertexName;
  private String targetInputName;

  private int version;
  private byte[] eventPayload;

  /**
   * @param targetVertexName the vertex on which the targeted Input exists
   * @param targetInputName  the name of the input
   * @param eventPayload     the payload for the event. It is advisable to limit the size of the
   *                         payload to a few KB at max
   * @param version          version of the event. Multiple versions may be generated in case of
   *                         retries
   */
  public InputInitializerEvent(String targetVertexName, String targetInputName,
                                   byte[] eventPayload, int version) {
    Preconditions.checkNotNull(targetVertexName, "TargetVertexName cannot be null");
    Preconditions.checkNotNull(targetInputName, "TargetInputName cannot be null");
    this.targetVertexName = targetVertexName;
    this.targetInputName = targetInputName;
    this.version = version;
    this.eventPayload = eventPayload;
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

  public int getVersion() {
    return this.version;
  }

  /**
   * Get the actual user payload
   *
   * @return a byte representation of the payload
   */
  public byte[] getUserPayload() {
    return this.eventPayload;
  }
}
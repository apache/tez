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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.api.Event;

/**
 * Event used by user code to send information between tasks. An output can
 * generate an Event of this type to sending information regarding output data
 * ( such as URI for file-based output data, port info in case of
 * streaming-based data transfers ) to the Input on the destination vertex.
 */
public final class DataMovementEvent extends Event {

  /**
   * Index(i) of the i-th (physical) Input or Output that generated an Event.
   * For a Processor-generated event, this is ignored.
   */
  private final int sourceIndex;

  /**
   * Index(i) of the i-th (physical) Input or Output that is meant to receive
   * this Event. For a Processor event, this is ignored.
   */
  private int targetIndex;

  /**
   * User Payload for this Event
   */
  private final byte[] userPayload;

  /**
   * Version number to indicate what attempt generated this Event
   */
  private int version;

  /**
   * User Event constructor
   * @param sourceIndex Index to identify the physical edge of the input/output
   * that generated the event
   * @param userPayload User Payload of the User Event
   */
  public DataMovementEvent(int sourceIndex,
      byte[] userPayload) {
    this.userPayload = userPayload;
    this.sourceIndex = sourceIndex;
  }

  @Private
  public DataMovementEvent(int sourceIndex,
      int targetIndex,
      int version,
      byte[] userPayload) {
    this.userPayload = userPayload;
    this.sourceIndex = sourceIndex;
    this.version = version;
    this.targetIndex = targetIndex;
  }

  /**
   * Constructor for Processor-generated User Events
   * @param userPayload
   */
  public DataMovementEvent(byte[] userPayload) {
    this(-1, userPayload);
  }

  public byte[] getUserPayload() {
    return userPayload;
  }

  public int getSourceIndex() {
    return sourceIndex;
  }

  public int getTargetIndex() {
    return targetIndex;
  }

  @Private
  public void setTargetIndex(int targetIndex) {
    this.targetIndex = targetIndex;
  }

  public int getVersion() {
    return version;
  }

  @Private
  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "DataMovementEvent [sourceIndex=" + sourceIndex + ", targetIndex="
        + targetIndex + ", version=" + version + "]";
  }
}

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

package org.apache.tez.engine.newapi;

import java.util.List;

import org.apache.tez.common.counters.TezCounters;

/**
 * Base interface for Context classes used to initialize the Input, Output
 * and Processor instances.
 */
public interface TezTaskContext {


  /**
   * Get the index of this Task
   * @return Task Index
   */
  public int getTaskIndex();

  /**
   * Get the current Task Attempt Number
   * @return Attempt Number
   */
  public int getAttemptNumber();

  /**
   * Get the name of the Vertex
   * @return Vertex Name
   */
  public String getVertexName();


  public TezCounters getCounters();

  /**
   * Send Events to the AM and/or dependent Vertices
   * @param events Events to be sent
   */
  public void sendEvents(List<Event> events);

  /**
   * Get the User Payload for the Input/Output/Processor
   * @return User Payload
   */
  public byte[] getUserPayload();

}

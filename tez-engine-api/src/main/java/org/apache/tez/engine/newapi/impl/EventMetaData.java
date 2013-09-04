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

package org.apache.tez.engine.newapi.impl;

/**
 * Class that encapsulates all the information to identify the unique
 * object that either generated an Event or is the recipient of an Event.
 */
public class EventMetaData {

  public static enum EventGenerator {
    INPUT,
    PROCESSOR,
    OUTPUT
  }

  /**
   * Source Type ( one of Input/Output/Processor ) that generated the Event.
   */
  private final EventGenerator generator;

  /**
   * Name of the vertex where the event was generated.
   */
  private final String vertexName;

  public EventMetaData(EventGenerator idType,
      String vertexName) {
    this.generator = idType;
    this.vertexName = vertexName;
  }

  public EventGenerator getIDType() {
    return generator;
  }

  public String getVertexName() {
    return vertexName;
  }

}

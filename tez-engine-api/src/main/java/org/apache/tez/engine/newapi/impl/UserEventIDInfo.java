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

import org.apache.tez.engine.newapi.TezInputContext;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.TezProcessorContext;

/**
 * Class that encapsulates all the information to identify the unique
 * object that either generated an Event or is the recipient of an Event.
 */
public class UserEventIDInfo {

  public static enum UserEventIDType {
    INPUT,
    PROCESSOR,
    OUTPUT
  }

  /**
   * Source Type ( one of Input/Output/Processor ) that generated the Event.
   */
  private final UserEventIDType sourceType;

  /**
   * Name of the vertex where the event was generated.
   */
  private final String sourceVertexName;

  /**
   * Index(i) of the i-th (physical) Input or Output that generated an Event.
   * For a Processor-generated event, this is ignored.
   */
  private final int index;

  private UserEventIDInfo(UserEventIDType sourceType,
      String sourceVertexName,
      int index) {
    this.sourceType = sourceType;
    this.sourceVertexName = sourceVertexName;
    this.index = index;
  }

  public UserEventIDInfo(TezInputContext inputContext, int index) {
    // TODO
    this(UserEventIDType.INPUT,
        inputContext.getVertexName(),
        index);
  }

  public UserEventIDInfo(TezOutputContext outputContext, int index) {
    // TODO
    this(UserEventIDType.OUTPUT,
        outputContext.getVertexName(),
        index);
  }

  public UserEventIDInfo(TezProcessorContext processorContext) {
    // TODO
    this(UserEventIDType.PROCESSOR,
        processorContext.getVertexName(),
        0);
  }

  public UserEventIDType getSourceType() {
    return sourceType;
  }

  public String getSourceVertexName() {
    return sourceVertexName;
  }

  public int getIndex() {
    return index;
  }

}

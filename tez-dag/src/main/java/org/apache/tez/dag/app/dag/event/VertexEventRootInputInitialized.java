/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.dag.event;

import java.util.Collections;
import java.util.List;

import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.Event;

public class VertexEventRootInputInitialized extends VertexEvent {

  private final String inputName;
  private final List<Event> events;

  public VertexEventRootInputInitialized(TezVertexID vertexId, String inputName, List<Event> events) {
    super(vertexId, VertexEventType.V_ROOT_INPUT_INITIALIZED);
    this.inputName = inputName;
    if (events == null) {
      this.events = Collections.emptyList();
    } else {
      this.events = events;
    }
  }

  public List<Event> getEvents() {
    return events;
  }

  public String getInputName() {
    return this.inputName;
  }
}

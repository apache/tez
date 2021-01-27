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

package org.apache.tez.runtime.internals.api;

import org.apache.tez.common.TezAbstractEvent;
import org.apache.tez.runtime.api.impl.TezEvent;

import java.util.List;

import static org.apache.tez.runtime.internals.api.TezTrapEventType.TRAP_EVENT_TYPE;

/**
 * Event sent when no more events should be sent to the AM.
 */
public class TezTrapEvent extends TezAbstractEvent<TezTrapEventType> {
  /**
   * Events that were reported.
   */
  private final List<TezEvent> tezEvents;

  /**
   * Create a tez trap event.
   * @param events events tried to be sent to the AM.
   */
  public TezTrapEvent(final List<TezEvent> events) {
    super(TRAP_EVENT_TYPE);
    this.tezEvents = events;
  }

  /**
   * @return events.
   */
  public final List<TezEvent> getTezEvents() {
    return tezEvents;
  }
}
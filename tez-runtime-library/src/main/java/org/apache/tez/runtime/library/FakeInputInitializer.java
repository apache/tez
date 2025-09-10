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
package org.apache.tez.runtime.library;

import java.util.ArrayList;
import java.util.List;

import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

public class FakeInputInitializer extends InputInitializer {

  private static final int SRC_PARALLELISM = 1;

  /**
   * Constructs an instance of {@code InputInitializer}.
   * <p>
   * Classes extending this one to create a custom {@code InputInitializer} must provide
   * the same constructor signature so that Tez can instantiate the class at runtime.
   * </p>
   *
   * @param initializerContext  the context that provides access to the payload,
   *                            vertex properties, and other initialization data
   */
  public FakeInputInitializer(InputInitializerContext initializerContext) {
    super(initializerContext);
  }

  @Override
  public List<Event> initialize() throws Exception {
    List<org.apache.tez.runtime.api.Event> list = new ArrayList<>();
    list.add(InputConfigureVertexTasksEvent.create(SRC_PARALLELISM, null, null));
    for (int i = 0; i < SRC_PARALLELISM; i++) {
      list.add(InputDataInformationEvent.createWithObjectPayload(i, null));
    }
    return list;
  }

  @Override
  public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
  }
}

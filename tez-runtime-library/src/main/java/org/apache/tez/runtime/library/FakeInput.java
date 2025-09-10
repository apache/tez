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

import java.io.IOException;
import java.util.List;

import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class FakeInput extends AbstractLogicalInput {

  private static final int NUM_RECORD_PER_SRC = 10;

  /**
   * Constructs an instance of {@code LogicalInput}.
   * <p>
   * Classes extending this one to create a custom {@code LogicalInput} must provide
   * the same constructor signature so that Tez can instantiate the class at runtime.
   * </p>
   *
   * @param inputContext       the {@link InputContext} that provides the input
   *                           with contextual information within the running task
   * @param numPhysicalInputs  the number of physical inputs associated with this
   *                           logical input
   */
  public FakeInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0, null);
    getContext().inputIsReady();
    return null;
  }

  @Override
  public void handleEvents(List<org.apache.tez.runtime.api.Event> inputEvents) throws Exception {
  }

  @Override
  public List<org.apache.tez.runtime.api.Event> close() throws Exception {
    return null;
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public Reader getReader() throws Exception {
    return new KeyValueReader() {
      private final String[] keys = new String[NUM_RECORD_PER_SRC];

      private int i = -1;

      @Override
      public boolean next() throws IOException {
        if (i == -1) {
          for (int j = 0; j < NUM_RECORD_PER_SRC; j++) {
            keys[j] = "" + j;
          }
        }
        i++;
        return i < keys.length;
      }

      @Override
      public Object getCurrentKey() throws IOException {
        return keys[i];
      }

      @Override
      public Object getCurrentValue() throws IOException {
        return keys[i];
      }
    };
  }
}

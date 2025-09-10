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

import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueWriter;

public class FakeOutput extends AbstractLogicalOutput {

  /**
   * Constructs an instance of {@code LogicalOutput}.
   * <p>
   * Classes extending this one to create a custom {@code LogicalOutput} must provide
   * the same constructor signature so that Tez can instantiate the class at runtime.
   * </p>
   *
   * @param outputContext       the {@link OutputContext} that provides the output
   *                            with contextual information within the running task
   * @param numPhysicalOutputs  the number of physical outputs associated with this
   *                            logical output
   */
  public FakeOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0, null);
    return null;
  }

  @Override
  public void handleEvents(List<org.apache.tez.runtime.api.Event> outputEvents) {

  }

  @Override
  public List<org.apache.tez.runtime.api.Event> close() throws Exception {
    return null;
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public Writer getWriter() throws Exception {
    return new KeyValueWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        System.out.println(key + " XXX " + value);
      }
    };
  }
}

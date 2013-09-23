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
package org.apache.tez.engine.lib.output;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.tez.common.TezUtils;
import org.apache.tez.engine.api.KVWriter;
import org.apache.tez.engine.common.sort.impl.dflt.InMemoryShuffleSorter;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.LogicalOutput;
import org.apache.tez.engine.newapi.Output;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.Writer;

/**
 * {@link InMemorySortedOutput} is an {@link Output} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class InMemorySortedOutput implements LogicalOutput {
  
  protected InMemoryShuffleSorter sorter;
  protected int numTasks;
  protected TezOutputContext outputContext;
  

  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws IOException {
    this.outputContext = outputContext;
    this.sorter = new InMemoryShuffleSorter();
    sorter.initialize(outputContext, TezUtils.createConfFromUserPayload(outputContext.getUserPayload()), numTasks);
    return Collections.emptyList();
  }

  @Override
  public Writer getWriter() throws IOException {
    return new KVWriter() {
      
      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    // No events expected.
  }

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    this.numTasks = numOutputs;
  }
  
  @Override
  public List<Event> close() throws IOException {
    sorter.flush();
    sorter.close();
    // TODO NEWTEZ Event generation
    return null;
  }
}

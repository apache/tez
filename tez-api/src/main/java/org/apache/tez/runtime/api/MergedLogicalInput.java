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

package org.apache.tez.runtime.api;

import java.util.List;
import com.google.common.collect.Lists;

/**
 * A LogicalInput that is used to merge the data from multiple inputs and provide a 
 * single <code>Reader</code> to read that data.
 * This Input is not initialized or closed. It is only expected to provide a 
 * merged view of the real inputs. It cannot send or receive events
 */
public abstract class MergedLogicalInput implements LogicalInput {

  private List<Input> inputs;
  
  public final void initialize(List<Input> inputs) {
    this.inputs = inputs;
  }
  
  protected List<Input> getInputs() {
    return inputs;
  }
  
  @Override
  public final List<Event> initialize(TezInputContext inputContext) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Event> start() throws Exception {
    List<Event> events = Lists.newLinkedList();
    for (Input input : inputs) {
      List<Event> inputEvents = input.start();
      if (inputEvents != null) {
        events.addAll(inputEvents);
      }
    }
    return events;
  }

  @Override
  public final void handleEvents(List<Event> inputEvents) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public final List<Event> close() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void setNumPhysicalInputs(int numInputs) {
    throw new UnsupportedOperationException();
  }

}

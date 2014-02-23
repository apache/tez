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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A LogicalInput that is used to merge the data from multiple inputs and provide a 
 * single <code>Reader</code> to read that data.
 * This Input is not initialized or closed. It is only expected to provide a 
 * merged view of the real inputs. It cannot send or receive events
 */
public abstract class MergedLogicalInput implements LogicalInput {

  // TODO Remove with TEZ-866
  private volatile InputReadyCallback inputReadyCallback;
  private AtomicBoolean notifiedInputReady = new AtomicBoolean(false);
  private List<Input> inputs;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public final void initialize(List<Input> inputs) {
    this.inputs = Collections.unmodifiableList(inputs);
  }
  
  public final List<Input> getInputs() {
    return inputs;
  }
  
  @Override
  public final List<Event> initialize(TezInputContext inputContext) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void start() throws Exception {
    if (!isStarted.getAndSet(true)) {
      for (Input input : inputs) {
        input.start();
      }
    }
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

  // TODO Remove with TEZ-866
  public void setInputReadyCallback(InputReadyCallback callback) {
    this.inputReadyCallback =  callback;
  }
  
  /**
   * Used by the actual MergedInput to notify that it's ready for consumption.
   * TBD eventually via the context.
   */
  protected final void informInputReady() {
    // TODO Fix with TEZ-866
    if (!notifiedInputReady.getAndSet(true)) {
      inputReadyCallback.setInputReady(this);
    }
  }

  /**
   * Used by the framework to inform the MergedInput that one of it's constituent Inputs is ready.
   */
  public abstract void setConstituentInputIsReady(Input input);
}

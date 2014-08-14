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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * A LogicalInput that is used to merge the data from multiple inputs and provide a
 * single <code>Reader</code> to read that data.
 * This Input is not initialized or closed. It is only expected to provide a
 * merged view of the real inputs. It cannot send or receive events
 * <p/>
 * <code>MergedLogicalInput</code> implementations must provide a 2 argument public constructor for
 * Tez to create the Input. The parameters to this constructor are 1) an instance of {@link
 * org.apache.tez.runtime.api.MergedInputContext} and 2) a list of constituent inputs. Tez will
 * take care of initializing and closing the Input after a {@link Processor} completes. </p>
 * <p/>
 */
@Public
@Evolving
public abstract class MergedLogicalInput implements LogicalInput {


  private AtomicBoolean notifiedInputReady = new AtomicBoolean(false);
  private List<Input> inputs;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final MergedInputContext context;

  /**
   * Constructor an instance of the MergedLogicalInputs. Classes extending this one to create a
   * MergedLogicalInput, must provide the same constructor so that Tez can create an instance of
   * the
   * class at runtime.
   *
   * @param context the {@link org.apache.tez.runtime.api.MergedInputContext} which provides
   *                the Input with context information within the running task.
   * @param inputs  the list of constituen Inputs.
   */
  public MergedLogicalInput(MergedInputContext context, List<Input> inputs) {
    this.inputs = Collections.unmodifiableList(inputs);
    this.context = context;
  }

  public final List<Input> getInputs() {
    return inputs;
  }
  
  public final MergedInputContext getContext() {
    return context;
  }
  
  @Override
  public final void start() throws Exception {
    if (!isStarted.getAndSet(true)) {
      for (Input input : inputs) {
        input.start();
      }
    }
  }

  /**
   * Used by the actual MergedInput to notify that it's ready for consumption.
   */
  protected final void informInputReady() {
    if (!notifiedInputReady.getAndSet(true)) {
      context.inputIsReady();
    }
  }

  /**
   * Used by the framework to inform the MergedInput that one of it's constituent Inputs is ready.
   */
  public abstract void setConstituentInputIsReady(Input input);
}

/*
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
package org.apache.tez.runtime.library.processor;

import java.util.List;
import java.util.Map;

import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

public abstract class SimpleProcessor extends AbstractLogicalIOProcessor {
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;

  public SimpleProcessor(ProcessorContext context) {
    super(context);
  }

  public void run(Map<String, LogicalInput> _inputs, Map<String, LogicalOutput> _outputs)
      throws Exception {
    this.inputs = _inputs;
    this.outputs = _outputs;
    preOp();
    run();
    postOp();
  }

  public abstract void run() throws Exception;

  protected void preOp() throws Exception {
    if (getInputs() != null) {
      for (LogicalInput input : getInputs().values()) {
        input.start();
      }
    }
    if (getOutputs() != null) {
      for (LogicalOutput output : getOutputs().values()) {
        output.start();
      }
    }
  }

  protected void postOp() throws Exception {
   //No-op
  }

  @Override
  public void initialize() throws Exception {

  }

  @Override
  public void handleEvents(List<Event> processorEvents) {

  }

  @Override
  public void close() throws Exception {

  }

  public Map<String, LogicalInput> getInputs() {
    return inputs;
  }

  public Map<String, LogicalOutput> getOutputs() {
    return outputs;
  }

}

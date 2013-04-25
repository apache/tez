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
package org.apache.tez.engine.task;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.api.Task;

public class RuntimeTask implements Task {

  private final Input[] inputs;
  private final Output[] outputs;
  private final Processor processor;
  
  private Configuration conf;
  private Master master;
  
  public RuntimeTask(
      Processor processor,
      Input[] inputs,
      Output[] outputs) {
    this.inputs = inputs;
    this.processor = processor;
    this.outputs = outputs;
  }

  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    this.conf = conf;
    this.master = master;

    // NOTE: Allow processor to initialize input/output
    processor.initialize(this.conf, this.master);
  }

  @Override
  public Input[] getInputs() {
    return inputs;
  }

  @Override
  public Processor getProcessor() {
    return processor;
  }

  @Override
  public Output[] getOutputs() {
    return outputs;
  }

  public void run() throws IOException, InterruptedException {
    processor.process(inputs, outputs);
  }

  public void close() throws IOException, InterruptedException {
    // NOTE: Allow processor to close input/output
    processor.close();
  }

}

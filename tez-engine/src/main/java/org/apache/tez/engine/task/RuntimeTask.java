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
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.api.Task;

public class RuntimeTask implements Task {

  protected final Input[] inputs;
  protected final Output[] outputs;
  protected final Processor processor;
  
  protected TezEngineTaskContext taskContext;
  protected ByteBuffer userPayload;
  protected Configuration conf;
  protected Master master;
  
  public RuntimeTask(TezEngineTaskContext taskContext,
      Processor processor,
      Input[] inputs,
      Output[] outputs) {
    this.taskContext = taskContext;
    this.inputs = inputs;
    this.processor = processor;
    this.outputs = outputs;
  }

  @Override
  public void initialize(Configuration conf, ByteBuffer userPayload,
      Master master) throws IOException, InterruptedException {
    this.conf = conf;
    this.userPayload = userPayload;
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
    // This can be changed to close input/output since MRRuntimeTask is used for
    // MR jobs, which changes the order.
    processor.close();
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }
}

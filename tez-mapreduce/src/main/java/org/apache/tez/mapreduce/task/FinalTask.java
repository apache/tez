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
package org.apache.tez.mapreduce.task;

import org.apache.tez.api.Input;
import org.apache.tez.api.Output;
import org.apache.tez.api.Processor;
import org.apache.tez.api.Task;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.runtime.InputFactory;
import org.apache.tez.engine.runtime.TezEngineFactory;
import org.apache.tez.engine.runtime.TezEngineFactoryImpl;
import org.apache.tez.engine.runtime.OutputFactory;
import org.apache.tez.engine.runtime.ProcessorFactory;
import org.apache.tez.engine.runtime.TaskFactory;
import org.apache.tez.engine.task.RuntimeTask;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class FinalTask extends AbstractModule {

  @Override
  protected void configure() {    
    install(
        new FactoryModuleBuilder().implement(
            Output.class, SimpleOutput.class).
        build(OutputFactory.class)
        );
    install(
        new FactoryModuleBuilder().implement(
            Input.class, ShuffledMergedInput.class).
        build(InputFactory.class)
        );
    install(
        new FactoryModuleBuilder().implement(
            Processor.class, ReduceProcessor.class).
        build(ProcessorFactory.class)
        );
    install(
        new FactoryModuleBuilder().implement(
            Task.class, RuntimeTask.class).
        build(TaskFactory.class)
        );
    
    bind(TezEngineFactory.class).to(TezEngineFactoryImpl.class);
  }

}

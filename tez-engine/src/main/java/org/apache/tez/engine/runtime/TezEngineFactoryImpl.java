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

package org.apache.tez.engine.runtime;

import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.api.Task;

import com.google.inject.Inject;

public class TezEngineFactoryImpl 
implements TezEngineFactory {

  private final InputFactory inputFactory;
  private final ProcessorFactory processorFactory;
  private final OutputFactory outputFactory;
  private final TaskFactory taskFactory;
  
  @Inject
  public TezEngineFactoryImpl(
      InputFactory inputFactory, 
      ProcessorFactory processorFactory,
      OutputFactory outputFactory,
      TaskFactory taskFactory
      ) {
    this.inputFactory = inputFactory;
    this.processorFactory = processorFactory;
    this.outputFactory = outputFactory;
    this.taskFactory = taskFactory;
  }
  
  public Task createTask(TezEngineTaskContext taskContext) {
    Input in = inputFactory.create(taskContext);
    Output out = outputFactory.create(taskContext);
    Processor processor = processorFactory.create(taskContext);
    return taskFactory.create(in, processor, out);
  }  
}
